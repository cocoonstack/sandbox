package pool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// Hibernate atomically snapshots a claimed sandbox and stops its VM, freeing
// memory; the next agent access wakes it. Idempotent on an already-hibernated
// sandbox. When to hibernate is the caller's policy — the node only provides
// the transition.
func (m *Manager) Hibernate(ctx context.Context, id, token string) error {
	sb, ok := m.claim(id, token)
	if !ok {
		return ErrUnknownSandbox
	}
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	return m.hibernateLocked(ctx, sb)
}

// WakeAgentSocket resolves the sandbox's vsock UDS for the relay, first
// restoring the VM if it is hibernated; concurrent wakes queue on the
// transition lock and find the fast path.
func (m *Manager) WakeAgentSocket(ctx context.Context, id, token string) (string, error) {
	sb, ok := m.claim(id, token)
	if !ok {
		return "", ErrUnknownSandbox
	}
	sb.Touch()
	return m.wakeResolved(ctx, sb)
}

// hibernateLocked is Hibernate's body; the caller holds sb.Transition.
func (m *Manager) hibernateLocked(ctx context.Context, sb *types.Sandbox) error {
	if sb.HibernateSnap != "" {
		return nil
	}
	// A started transition must finish even if the caller hangs up (the
	// engine bounds every step), or the record would disagree with the VM.
	ctx = context.WithoutCancel(ctx)
	// Derived from VMName, not sb.ID: cocoon snapshot names reject the
	// underscore in the "sb_" prefix.
	snap := hibernatePrefix + strings.TrimPrefix(sb.VMName, vmPrefix)
	if err := m.eng.Hibernate(ctx, sb.VMName, snap); err != nil {
		return err
	}
	if !m.commitTransition(ctx, sb, snap, sb.VsockSocket) {
		// Released mid-transition: the VM is gone, drop our snapshot.
		m.dropSnap(ctx, snap)
		return ErrUnknownSandbox
	}
	m.counters.hibernates.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "hibernate", ID: sb.ID, VMName: sb.VMName})
	return nil
}

// wakeResolved is the wake body shared by the token and id-only entry points.
func (m *Manager) wakeResolved(ctx context.Context, sb *types.Sandbox) (string, error) {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if sb.ArchiveCk != "" {
		return m.wakeArchived(ctx, sb)
	}
	if sb.HibernateSnap == "" {
		return sb.VsockSocket, nil
	}
	// See Hibernate: a half-restored VM is worse than a wasted wake.
	ctx = context.WithoutCancel(ctx)
	wakeStart := time.Now()
	snap := sb.HibernateSnap
	if err := m.eng.Restore(ctx, sb.VMName, snap); err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	sock, err := m.probeReady(ctx, sb.VMName, claimProbeTimeout)
	if err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	if !m.commitTransition(ctx, sb, "", sock) {
		// Released mid-transition: destroy the VM we just resurrected.
		m.destroy(ctx, sb.VMName)
		m.dropSnap(ctx, snap)
		return "", ErrUnknownSandbox
	}
	// The memory image is consumed by the resume; drop it to free disk.
	m.dropSnap(ctx, snap)
	m.counters.wakes.Add(1)
	m.counters.wakeNanos.Add(uint64(time.Since(wakeStart))) //nolint:gosec // durations are positive
	m.recordUsage(ctx, usageEvent{Event: "wake", ID: sb.ID, VMName: sb.VMName})
	return sock, nil
}

// idleOnce hibernates claims idle past their pool's (or the node's)
// threshold. Best-effort: a connection racing the sweep may see its sandbox
// hibernate right after — the next call wakes it transparently.
func (m *Manager) idleOnce(ctx context.Context) {
	if !m.idleEnabled {
		return
	}
	if !m.idleSweep.CompareAndSwap(false, true) {
		return // the previous sweep's hibernates are still draining
	}
	now := time.Now()
	type victim struct{ id, token string }
	var victims []victim
	m.mu.Lock()
	for _, sb := range m.claimed {
		idle := m.idleDefault
		if p, pooled := m.pools[sb.Key]; pooled {
			idle = p.idle // pooled keys never take the node default
		}
		if idle <= 0 || sb.HibernateSnap != "" || sb.ArchiveCk != "" || now.Sub(sb.LastSeen()) < idle {
			continue
		}
		victims = append(victims, victim{sb.ID, sb.Token})
	}
	m.mu.Unlock()
	if len(victims) == 0 {
		m.idleSweep.Store(false)
		return
	}
	// Hibernates are seconds-long engine snapshots: run them off the
	// housekeeping loop, fanned out on the refill budget, so a big sweep
	// finishes in parallel while refill ticks keep flowing.
	go func() {
		defer m.idleSweep.Store(false)
		logger := log.WithFunc("pool.idleOnce")
		m.runBounded(ctx, len(victims), func(ctx context.Context, i int) {
			v := victims[i]
			switch err := m.idleHibernate(ctx, v.id, v.token, now); {
			case err == nil:
				logger.Infof(ctx, "idle-hibernated %s", v.id)
			case !errors.Is(err, ErrUnknownSandbox) && !errors.Is(err, errWokeMeanwhile):
				logger.Errorf(ctx, err, "idle-hibernate %s", v.id)
			}
		}).Wait()
	}()
}

// idleHibernate re-validates a sweep victim under the Transition lock: a
// data-plane connection that arrived after the sweep's snapshot refreshes
// LastActivity, and hibernating underneath it would cut a live call.
func (m *Manager) idleHibernate(ctx context.Context, id, token string, sweepStart time.Time) error {
	sb, ok := m.claim(id, token)
	if !ok {
		return ErrUnknownSandbox
	}
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	m.mu.Lock()
	woke := sb.LastSeen().After(sweepStart) || sb.HibernateSnap != ""
	m.mu.Unlock()
	if woke {
		return errWokeMeanwhile
	}
	return m.hibernateLocked(ctx, sb)
}

// commitTransition publishes a hibernate/wake result and persists the
// journal, but only if the claim is still live — Release and reap do not
// take the transition lock, so a sandbox can be destroyed mid-transition
// and publishing then would resurrect state nobody owns. Returns liveness;
// a failed journal write only warns (the live state is authoritative).
func (m *Manager) commitTransition(ctx context.Context, sb *types.Sandbox, snap, sock string) bool {
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb
	var saveErr error
	if live {
		sb.HibernateSnap = snap
		sb.VsockSocket = sock
		saveErr = m.store.save(m.claimed)
	}
	m.mu.Unlock()
	if saveErr != nil {
		log.WithFunc("pool.commitTransition").Warnf(ctx, "persist claims: %v", saveErr)
	}
	return live
}
