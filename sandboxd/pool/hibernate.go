package pool

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
	// Settle a dangling intent from a prior unconfirmed attempt first — never
	// overwrite it with a fresh snapshot name, or its real snapshot leaks. An
	// adopted transition was never recorded (its confirming list had failed),
	// so bill it even if this resolve's own persist is still converging.
	adopted, resolveErr := m.resolvePendingSnap(ctx, sb)
	if adopted {
		m.recordHibernate(ctx, sb)
	}
	if resolveErr != nil {
		return resolveErr
	}
	if sb.HibernateSnap != "" {
		if err := m.syncClaims(ctx, sb); err != nil {
			return fmt.Errorf("hibernate %s: persist claims: %w", sb.ID, err)
		}
		return nil
	}
	// A started transition must finish even if the caller hangs up (the
	// engine bounds every step), or the record would disagree with the VM.
	ctx = context.WithoutCancel(ctx)
	// From VMName (not sb.ID, whose "sb_" underscore cocoon rejects) plus a
	// random suffix, so the wake's async snapshot drop can't collide with a
	// re-hibernate reusing the name.
	snap := hibernatePrefix + strings.TrimPrefix(sb.VMName, vmPrefix) + "-" + randHex(3)
	// Journal the intent before the engine stops anything: an intent that
	// cannot be written aborts with the VM untouched, and a committed intent
	// lets Reconcile adopt a hibernate whose final commit never landed.
	if err := m.store.commit(m.setPendingSnap(sb, snap)); err != nil {
		m.mu.Lock()
		sb.PendingSnap = ""
		m.mu.Unlock()
		return fmt.Errorf("hibernate %s: persist intent: %w", sb.ID, err)
	}
	if hibErr := m.eng.Hibernate(ctx, sb.VMName, snap); hibErr != nil {
		// The engine can report failure after the snapshot landed (a CLI
		// timeout); resolve the intent against the real snapshot instead of
		// trusting the error. A confirmed snapshot is a completed hibernate
		// (bill it); an unusable list or verified-absent snapshot is not.
		adopted, resolveErr := m.resolvePendingSnap(ctx, sb)
		if adopted {
			m.recordHibernate(ctx, sb)
			if resolveErr != nil {
				return fmt.Errorf("hibernate %s: persist claims: %w", sb.ID, resolveErr)
			}
			m.dropStale(ctx, sb)
			return nil
		}
		if resolveErr != nil {
			return errors.Join(fmt.Errorf("hibernate %s: %w", sb.ID, hibErr), resolveErr)
		}
		return fmt.Errorf("hibernate %s: %w", sb.ID, hibErr)
	}
	live, err := m.commitTransition(ctx, sb, snap, sb.VsockSocket)
	if !live {
		// Released mid-transition: the VM is gone, drop our snapshot.
		m.dropSnap(ctx, snap)
		return ErrUnknownSandbox
	}
	// The VM is hibernated either way, so the billing window closes here.
	m.recordHibernate(ctx, sb)
	if err != nil {
		return fmt.Errorf("hibernate %s: persist claims: %w", sb.ID, err)
	}
	m.dropStale(ctx, sb)
	return nil
}

// wakeResolved is the wake body shared by the token and id-only entry points.
func (m *Manager) wakeResolved(ctx context.Context, sb *types.Sandbox) (string, error) {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if sb.ArchiveCk != "" {
		return m.wakeArchived(ctx, sb)
	}
	// Settle a dangling intent before choosing running vs hibernated, or a
	// stopped VM's stale socket would answer as if the sandbox were live. An
	// adopted hibernate happened on a prior unconfirmed attempt; record it
	// before the wake so the hibernate→wake window bills correctly.
	adopted, err := m.resolvePendingSnap(ctx, sb)
	if adopted {
		m.recordHibernate(ctx, sb)
	}
	if err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	if sb.HibernateSnap == "" {
		// No journal sync here — this is the data-plane fast path, and a
		// lagging journal only says hibernated while the VM runs, which a
		// restart heals (Reconcile adopts running VMs). Reclaim a parked
		// snapshot once the journal no longer references it.
		if sb.StaleSnap != "" && m.store.synced() {
			m.dropStale(ctx, sb)
		}
		return sb.VsockSocket, nil
	}
	// See Hibernate: a half-restored VM is worse than a wasted wake.
	ctx = context.WithoutCancel(ctx)
	wakeStart := time.Now()
	snap := sb.HibernateSnap
	restoredSock, err := m.eng.Restore(ctx, sb.VMName, snap)
	if err != nil {
		// Restore may have booted the VM before the engine errored (a CLI
		// timeout); tear it down, keeping the snapshot, so the next wake
		// restores cleanly instead of re-restoring an already-running VM.
		m.destroy(ctx, sb.VMName)
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	sock, err := m.probeReady(ctx, sb.VMName, restoredSock, claimProbeTimeout)
	if err != nil {
		// Restore already booted the VM; tear it down so the next wake
		// restores cleanly from the kept snapshot instead of re-restoring a
		// VM that is already running.
		m.destroy(ctx, sb.VMName)
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
		// The lagging journal still references the snapshot; park it for
		// reclaim after a later write lands, not just the restart sweep.
		sb.StaleSnap = snap
		return "", fmt.Errorf("wake %s: persist claims: %w", sb.ID, err)
	}
	m.dropStale(ctx, sb)
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
// journal, but only if the claim is still live — Release and reap skip the
// transition lock, and publishing after them would resurrect state nobody
// owns. A failed write is returned (the caller must not report success)
// while recommit converges disk to the state the VM already embodies.
func (m *Manager) commitTransition(ctx context.Context, sb *types.Sandbox, snap, sock string) (live bool, err error) {
	m.mu.Lock()
	live = m.claimed[sb.ID] == sb
	var js claimSnapshot
	if live {
		sb.HibernateSnap = snap
		sb.VsockSocket = sock
		sb.PendingSnap = ""
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

// syncClaims backs the idempotent hibernate fast path: a retry after a
// failed persist must not answer success while the journal still lags the
// transition it reports. The caller holds sb.Transition.
func (m *Manager) syncClaims(ctx context.Context, sb *types.Sandbox) error {
	if !m.store.synced() {
		if err := m.store.commit(m.claimsSnapshot()); err != nil {
			return err
		}
	}
	m.dropStale(ctx, sb)
	return nil
}

func (m *Manager) dropStale(ctx context.Context, sb *types.Sandbox) {
	if snap := sb.StaleSnap; snap != "" {
		sb.StaleSnap = ""
		go m.dropSnap(ctx, snap)
	}
}

// setPendingSnap stamps a hibernate intent under the manager mutex and hands
// back the snapshot carrying it.
func (m *Manager) setPendingSnap(sb *types.Sandbox, snap string) claimSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb.PendingSnap = snap
	return m.store.snapshot(m.claimed)
}

// resolvePendingSnap settles a hibernate intent left by an attempt whose
// engine result could not be confirmed: the snapshot's presence decides
// whether it hibernated (adopt it as HibernateSnap) or not (clear the intent).
// It reports whether it adopted a completed-but-unrecorded hibernate, so the
// caller can bill it. An unusable snapshot list leaves the intent unresolved
// and errors, so the caller never acts over a dangling intent — never
// overwriting it or answering a stopped VM's stale socket. A no-op when no
// intent is pending, so the wake fast path pays only a field read. The caller
// holds sb.Transition.
func (m *Manager) resolvePendingSnap(ctx context.Context, sb *types.Sandbox) (adopted bool, err error) {
	if sb.PendingSnap == "" {
		return false, nil
	}
	snaps, err := m.eng.SnapshotList(ctx)
	if err != nil {
		return false, fmt.Errorf("resolve hibernate intent %s: %w", sb.ID, err)
	}
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb
	pending := sb.PendingSnap
	exists := slices.Contains(snaps, pending)
	if live {
		if exists {
			sb.HibernateSnap = pending
		}
		sb.PendingSnap = ""
	}
	js := m.store.snapshot(m.claimed)
	m.mu.Unlock()
	if !live {
		// Released mid-resolve: Release captured an empty HibernateSnap, so a
		// completed hibernate's snapshot is an orphan it could not drop.
		if exists {
			m.dropSnap(ctx, pending)
		}
		return false, ErrUnknownSandbox
	}
	if err := m.store.commit(js); err != nil {
		m.recommit(ctx, js)
		// The transition is decided in memory; report the adoption so the
		// caller still bills it, and surface the persist error while recommit
		// converges — the commitTransition contract.
		return exists, fmt.Errorf("resolve hibernate intent %s: persist: %w", sb.ID, err)
	}
	return exists, nil
}

// recordHibernate bills one hibernate transition.
func (m *Manager) recordHibernate(ctx context.Context, sb *types.Sandbox) {
	m.counters.hibernates.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "hibernate", ID: sb.ID, VMName: sb.VMName})
}
