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
// sandbox.
func (m *Manager) Hibernate(ctx context.Context, id string, cred Cred) error {
	sb, ok := m.resolve(id, cred)
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
	if hasAppliedVolumes(sb) {
		return ErrVolumeCapture
	}
	if sb.Key.Net == types.NetEgress {
		// cocoon resumes the guest before its fresh tap can be re-locked, so a
		// woken egress guest would egress unlocked; keep the lane live instead.
		return ErrNoEgressHibernate
	}
	// An adopted hibernate was never billed (its confirming list had failed),
	// so record it even if this resolve's own persist is still converging.
	adopted, resolveErr := m.resolvePendingSnap(ctx, sb)
	if adopted {
		m.recordHibernate(ctx, sb)
	}
	if resolveErr != nil {
		return resolveErr
	}
	if sb.HibernateSnap != "" {
		m.disarmEgress(sb.ID, true)
		if err := m.syncClaims(ctx, sb); err != nil {
			return fmt.Errorf("hibernate %s: persist claims: %w", sb.ID, err)
		}
		return nil
	}
	// A started transition must finish even if the caller hangs up (the
	// engine bounds every step), or the record would disagree with the VM.
	ctx = context.WithoutCancel(ctx)
	// VMName-based (cocoon rejects sb.ID's "sb_" underscore) plus a random
	// suffix so the wake's async snapshot drop can't collide with a re-hibernate.
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
		// timeout); resolve the intent against the real snapshot.
		adopted, resolveErr := m.resolvePendingSnap(ctx, sb)
		if adopted {
			m.recordHibernate(ctx, sb)
			m.disarmEgress(sb.ID, true)
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
		m.dropSnap(ctx, snap)
		return ErrUnknownSandbox
	}
	// The VM is hibernated either way, so the billing window closes here.
	m.recordHibernate(ctx, sb)
	m.disarmEgress(sb.ID, true)
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
	// Settle a dangling intent before choosing running vs hibernated; bill an
	// adopted hibernate before the wake closes its window.
	adopted, err := m.resolvePendingSnap(ctx, sb)
	if adopted {
		m.recordHibernate(ctx, sb)
	}
	if err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	if sb.HibernateSnap == "" {
		// No journal sync here — data-plane fast path; a lagging journal only
		// says hibernated while the VM runs, which restart Reconcile heals.
		if sb.StaleSnap != "" && m.store.synced() {
			m.dropStale(ctx, sb)
		}
		return sb.VsockSocket, nil
	}
	// The egress lane never hibernates (its fresh tap can't be locked before the
	// guest resumes); a hibernated one is corrupt state — fail closed.
	if sb.Key.Net == types.NetEgress {
		return "", fmt.Errorf("wake %s: egress lane cannot resume from hibernation", sb.ID)
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
		m.destroy(ctx, sb.VMName)
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	live, err := m.commitTransition(ctx, sb, "", sock)
	if !live {
		m.destroy(ctx, sb.VMName)
		m.dropSnap(ctx, snap)
		return "", ErrUnknownSandbox
	}
	m.counters.wakes.Add(1)
	m.counters.wakeNanos.Add(uint64(time.Since(wakeStart))) //nolint:gosec // durations are positive
	m.recordUsage(ctx, usageEvent{Event: "wake", ID: sb.ID, VMName: sb.VMName})
	if proxyErr := m.armEgressProxy(ctx, sb); proxyErr != nil {
		log.WithFunc("pool.wakeResolved").Errorf(ctx, proxyErr, "arm egress proxy %s", sb.ID)
	}
	if m.disarmIfReleased(sb) {
		return "", ErrUnknownSandbox
	}
	if err != nil {
		// The lagging journal still references the snapshot; park it for
		// reclaim after a later write lands, not just the restart sweep.
		sb.StaleSnap = snap
		return "", fmt.Errorf("wake %s: persist claims: %w", sb.ID, err)
	}
	m.dropStale(ctx, sb)
	// The resume consumed the memory image; reclaim its disk off the wake-
	// return path (name reuse guarded by randHex, failed drops by the sweep).
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
		if p, pooled := m.activePool(sb.Key); pooled {
			idle = p.idle
		}
		if idle <= 0 || sb.Key.Net == types.NetEgress || hasAppliedVolumes(sb) || sb.HibernateSnap != "" || sb.ArchiveCk != "" || now.Sub(sb.LastSeen()) < idle {
			continue
		}
		victims = append(victims, victim{sb.ID, sb.Token})
	}
	m.mu.Unlock()
	if len(victims) == 0 {
		m.idleSweep.Store(false)
		return
	}
	// Hibernates are seconds-long engine snapshots: fan out off the
	// housekeeping loop on the refill budget.
	go func() {
		defer m.idleSweep.Store(false)
		logger := log.WithFunc("pool.idleOnce")
		m.runBounded(ctx, len(victims), func(ctx context.Context, i int) {
			v := victims[i]
			logSweepResult(ctx, logger, m.idleHibernate(ctx, v.id, v.token, now), "idle-hibernated "+v.id, "idle-hibernate "+v.id)
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

// commitTransition publishes a hibernate/wake result and persists the journal
// only if the claim is still live — Release/reap skip the transition lock, and
// publishing after them resurrects state nobody owns. Returns a failed write
// (caller must not report success); recommit converges disk in the background.
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

func (m *Manager) setPendingSnap(sb *types.Sandbox, snap string) claimSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb.PendingSnap = snap
	return m.store.snapshot(m.claimed)
}

// resolvePendingSnap settles a hibernate intent whose engine result was never
// confirmed: the snapshot's presence decides whether to adopt it as
// HibernateSnap or clear the intent. An unusable snapshot list keeps the
// intent and errors. The caller holds sb.Transition.
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
		// The transition is decided in memory: report the adoption so the
		// caller bills it, surfacing the persist error — commitTransition contract.
		return exists, fmt.Errorf("resolve hibernate intent %s: persist: %w", sb.ID, err)
	}
	return exists, nil
}

func (m *Manager) recordHibernate(ctx context.Context, sb *types.Sandbox) {
	m.counters.hibernates.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "hibernate", ID: sb.ID, VMName: sb.VMName})
}
