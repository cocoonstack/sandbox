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

func (m *Manager) Hibernate(ctx context.Context, id string, cred Cred) error {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return ErrUnknownSandbox
	}
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	return m.hibernateLocked(ctx, sb)
}

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
		// cocoon resumes the guest before its fresh tap can be re-locked
		return ErrNoEgressHibernate
	}
	// an adopted hibernate was never billed, so record it here
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
	// a started transition must finish, or the record disagrees with the VM
	ctx = context.WithoutCancel(ctx)
	// cocoon rejects sb.ID's underscore, so the snapshot name is VMName-based
	snap := hibernatePrefix + strings.TrimPrefix(sb.VMName, vmPrefix) + "-" + randHex(3)
	// journal the intent first so Reconcile can adopt a hibernate whose commit never landed
	if err := m.store.commit(m.setPendingSnap(sb, snap)); err != nil {
		m.mu.Lock()
		sb.PendingSnap = ""
		m.store.set(sb)
		m.mu.Unlock()
		return fmt.Errorf("hibernate %s: persist intent: %w", sb.ID, err)
	}
	if hibErr := m.eng.Hibernate(ctx, sb.VMName, snap); hibErr != nil {
		// the engine can report failure after the snapshot landed
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
	// settle a dangling intent before choosing running vs hibernated
	adopted, err := m.resolvePendingSnap(ctx, sb)
	if adopted {
		m.recordHibernate(ctx, sb)
	}
	if err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	if sb.HibernateSnap == "" {
		// no journal sync on this data-plane fast path; restart Reconcile heals the lag
		if sb.StaleSnap != "" && m.store.synced() {
			m.dropStale(ctx, sb)
		}
		return sb.VsockSocket, nil
	}
	// the egress lane never hibernates, so a hibernated one is corrupt state
	if sb.Key.Net == types.NetEgress {
		return "", fmt.Errorf("wake %s: egress lane cannot resume from hibernation", sb.ID)
	}
	// See Hibernate: a half-restored VM is worse than a wasted wake.
	ctx = context.WithoutCancel(ctx)
	wakeStart := time.Now()
	snap := sb.HibernateSnap
	restoredSock, err := m.eng.Restore(ctx, sb.VMName, snap)
	if err != nil {
		// restore may have booted the VM before the engine errored
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
		// the lagging journal still references the snapshot, so park it for later reclaim
		sb.StaleSnap = snap
		return "", fmt.Errorf("wake %s: persist claims: %w", sb.ID, err)
	}
	m.dropStale(ctx, sb)
	// the resume consumed the memory image; reclaim its disk off the wake-return path (the randHex suffix keeps a re-hibernate from reusing the name)
	go m.dropSnap(ctx, snap)
	return sock, nil
}

// idleOnce hibernates claims idle past their pool's (or the node's) threshold.
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
		if skipIdle(sb, idle, now) {
			continue
		}
		victims = append(victims, victim{sb.ID, sb.Token})
	}
	m.mu.Unlock()
	if len(victims) == 0 {
		m.idleSweep.Store(false)
		return
	}
	// hibernates are seconds-long engine snapshots, so fan out off the housekeeping loop
	go func() {
		defer m.idleSweep.Store(false)
		logger := log.WithFunc("pool.idleOnce")
		m.runBounded(ctx, len(victims), func(ctx context.Context, i int) {
			v := victims[i]
			logSweepResult(ctx, logger, m.idleHibernate(ctx, v.id, v.token, now), "idle-hibernated "+v.id, "idle-hibernate "+v.id)
		}).Wait()
	}()
}

// idleHibernate re-validates a sweep victim under the Transition lock.
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

// commitTransition publishes a hibernate/wake result only if the claim is still live.
func (m *Manager) commitTransition(ctx context.Context, sb *types.Sandbox, snap, sock string) (live bool, err error) {
	m.mu.Lock()
	live = m.claimed[sb.ID] == sb
	var js claimSnapshot
	if live {
		sb.HibernateSnap = snap
		sb.VsockSocket = sock
		sb.PendingSnap = ""
		js = m.store.set(sb)
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

// syncClaims flushes a lagging journal so a hibernate retry cannot report a false success.
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
	return m.store.set(sb)
}

// resolvePendingSnap settles an unconfirmed hibernate intent against the engine's snapshot list.
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
	var js claimSnapshot
	if live {
		if exists {
			sb.HibernateSnap = pending
		}
		sb.PendingSnap = ""
		js = m.store.set(sb)
	}
	m.mu.Unlock()
	if !live {
		// Release captured an empty HibernateSnap, so a completed hibernate's snapshot is an orphan
		if exists {
			m.dropSnap(ctx, pending)
		}
		return false, ErrUnknownSandbox
	}
	if err := m.store.commit(js); err != nil {
		m.recommit(ctx, js)
		// the transition is decided in memory, so report the adoption for the caller to bill
		return exists, fmt.Errorf("resolve hibernate intent %s: persist: %w", sb.ID, err)
	}
	return exists, nil
}

func (m *Manager) recordHibernate(ctx context.Context, sb *types.Sandbox) {
	m.counters.hibernates.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "hibernate", ID: sb.ID, VMName: sb.VMName})
}

// skipIdle reports the claims an idle sweep must leave alone.
func skipIdle(sb *types.Sandbox, idle time.Duration, now time.Time) bool {
	return idle <= 0 || sb.Key.Net == types.NetEgress || hasAppliedVolumes(sb) ||
		sb.HibernateSnap != "" || sb.ArchiveCk != "" || now.Sub(sb.LastSeen()) < idle
}
