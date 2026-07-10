package pool

import (
	"context"
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
	// From VMName (not sb.ID, whose "sb_" underscore cocoon rejects) plus a
	// random suffix, so the wake's async snapshot drop can't collide with a
	// re-hibernate reusing the name.
	snap := hibernatePrefix + strings.TrimPrefix(sb.VMName, vmPrefix) + "-" + randHex(3)
	if err := m.eng.Hibernate(ctx, sb.VMName, snap); err != nil {
		return err
	}
	live, err := m.commitTransition(ctx, sb, snap, sb.VsockSocket)
	if !live {
		// Released mid-transition: the VM is gone, drop our snapshot.
		m.dropSnap(ctx, snap)
		return ErrUnknownSandbox
	}
	// The VM is hibernated either way, so the billing window closes here.
	m.counters.hibernates.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "hibernate", ID: sb.ID, VMName: sb.VMName})
	if err != nil {
		return fmt.Errorf("hibernate %s: persist claims: %w", sb.ID, err)
	}
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
	restoredSock, err := m.eng.Restore(ctx, sb.VMName, snap)
	if err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	sock, err := m.probeReady(ctx, sb.VMName, restoredSock, claimProbeTimeout)
	if err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	live, err := m.commitTransition(ctx, sb, "", sock)
	if !live {
		// Released mid-transition: destroy the VM we just resurrected.
		m.destroy(ctx, sb.VMName)
		m.dropSnap(ctx, snap)
		return "", ErrUnknownSandbox
	}
	m.counters.wakes.Add(1)
	m.counters.wakeNanos.Add(uint64(time.Since(wakeStart))) //nolint:gosec // durations are positive
	m.recordUsage(ctx, usageEvent{Event: "wake", ID: sb.ID, VMName: sb.VMName})
	if err != nil {
		// Keep the snapshot the lagging journal still references.
		return "", fmt.Errorf("wake %s: persist claims: %w", sb.ID, err)
	}
	// The resume consumed the memory image; reclaim its disk off the
	// wake-return path (a stale-name re-hibernate is guarded by randHex above,
	// a failed drop by the orphan-snapshot sweep).
	go m.dropSnap(ctx, snap)
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
			case !benignSweepErr(err):
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
// and publishing then would resurrect state nobody owns. A failed journal
// write is returned (a restart before disk converges would drop the claim,
// so the caller must not report success) while recommit converges disk to
// the published state, which the VM already embodies.
func (m *Manager) commitTransition(ctx context.Context, sb *types.Sandbox, snap, sock string) (live bool, err error) {
	m.mu.Lock()
	live = m.claimed[sb.ID] == sb
	var js claimSnapshot
	if live {
		sb.HibernateSnap = snap
		sb.VsockSocket = sock
		js = m.store.snapshot(m.claimed)
	}
	m.mu.Unlock()
	if !live {
		return false, nil
	}
	if err := m.store.commit(js); err != nil {
		m.recommit(ctx, js)
		return true, err
	}
	return true, nil
}
