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
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

func (m *Manager) Hibernate(ctx context.Context, id string, cred Cred) error {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return ErrUnknownSandbox
	}
	if err := m.lockTransition(sb); err != nil {
		return err
	}
	defer sb.Transition.Unlock()
	return m.hibernateLocked(ctx, sb)
}

// WakeAgentSocket resolves a data-plane connection's guest socket; the returned func ends the hold it takes, and the channel closes when the stream must end.
func (m *Manager) WakeAgentSocket(ctx context.Context, id, token string) (string, <-chan struct{}, func(), error) {
	return m.agentSocket(ctx, id, token, false)
}

// WakeAgentSocketOrRead is WakeAgentSocket, except that a sleep a reader can serve answers ErrSleeping instead of waking.
func (m *Manager) WakeAgentSocketOrRead(ctx context.Context, id, token string) (string, <-chan struct{}, func(), error) {
	return m.agentSocket(ctx, id, token, true)
}

func (m *Manager) agentSocket(ctx context.Context, id, token string, readerOK bool) (string, <-chan struct{}, func(), error) {
	sb, ok := m.claim(id, token)
	if !ok {
		return "", nil, nil, ErrUnknownSandbox
	}
	fenced, err := m.fenceWatch(sb)
	if err != nil {
		return "", nil, nil, err
	}
	sb.Hold()
	sock, err := m.wakeOrRead(ctx, sb, readerOK)
	switch {
	case errors.Is(err, ErrSleeping):
		sb.UnholdIdle()
		return "", nil, nil, err
	case err != nil:
		sb.Unhold()
		return "", nil, nil, err
	}
	sb.Touch()
	return sock, fenced, sb.Unhold, nil
}

// Caller holds sb.Transition.
func (m *Manager) hibernateLocked(ctx context.Context, sb *types.Sandbox) error {
	if sb.ArchiveCk != "" {
		return nil // already off the node
	}
	if _, err := m.fenceWatch(sb); err != nil {
		return err
	}
	if sb.Failed != "" {
		return ErrFailed
	}
	m.mu.Lock()
	moving, volumeBlocked := m.midMoveLocked(sb), m.hasVolumeHolds(sb)
	m.mu.Unlock()
	if moving {
		return ErrEvacuating
	}
	if volumeBlocked {
		return ErrVolumeCapture
	}
	if sb.Key.Net == types.NetEgress {
		// cocoon resumes the guest before its fresh tap can be re-locked
		return ErrNoEgressHibernate
	}
	if err := m.settleCapture(ctx, sb); err != nil {
		return err
	}
	if sb.HibernateSnap != "" {
		m.out.Disarm(sb.ID, true)
		if err := m.syncClaims(ctx, sb); err != nil {
			return fmt.Errorf("hibernate %s: persist claims: %w", sb.ID, err)
		}
		return nil
	}
	if err := m.diskErr(); err != nil {
		return fmt.Errorf("hibernate %s: %w", sb.ID, err)
	}
	// A started transition must finish, or the record disagrees with the VM
	ctx = context.WithoutCancel(ctx)
	// cocoon rejects sb.ID's underscore, so the snapshot name is VMName-based
	snap := hibernatePrefix + strings.TrimPrefix(sb.VMName, vmPrefix) + "-" + randHex(3)
	// Journal the intent first so Reconcile can adopt a hibernate whose commit never landed
	intent, live := m.setIfLive(sb, func() { sb.PendingSnap = snap })
	if !live {
		return ErrUnknownSandbox
	}
	if err := m.store.commit(intent); err != nil {
		m.setIfLive(sb, func() { sb.PendingSnap = "" })
		return fmt.Errorf("hibernate %s: persist intent: %w", sb.ID, err)
	}
	readerSafe := false
	if m.view.Load().poolReaders[sb.Key] {
		err := m.eng.SyncGuest(ctx, sb.VsockSocket)
		if err != nil {
			log.WithFunc("pool.hibernateLocked").Warnf(ctx, "sync %s before hibernate, reads will wake it: %v", sb.ID, err)
		}
		readerSafe = err == nil
	}
	m.mu.Lock()
	sb.ReaderSafe = readerSafe
	m.mu.Unlock()
	if hibErr := m.eng.Hibernate(ctx, sb.VMName, snap); hibErr != nil {
		// The engine can report failure after the snapshot landed
		adopted, settleErr := m.settleFailedCapture(ctx, sb)
		if adopted {
			if settleErr != nil {
				return fmt.Errorf("hibernate %s: persist claims: %w", sb.ID, settleErr)
			}
			m.dropStale(ctx, sb)
			return nil
		}
		if settleErr != nil {
			return errors.Join(fmt.Errorf("hibernate %s: %w", sb.ID, hibErr), settleErr)
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
	m.out.Disarm(sb.ID, true)
	if err != nil {
		return fmt.Errorf("hibernate %s: persist claims: %w", sb.ID, err)
	}
	m.dropStale(ctx, sb)
	return nil
}

func (m *Manager) wakeResolved(ctx context.Context, sb *types.Sandbox) (string, error) {
	return m.wakeOrRead(ctx, sb, false)
}

func (m *Manager) wakeOrRead(ctx context.Context, sb *types.Sandbox, readerOK bool) (string, error) {
	if m.pausedForMove(sb) {
		return "", ErrEvacuating
	}
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if sb.ArchiveCk != "" {
		if readerOK && sb.ReaderSafe {
			return "", ErrSleeping
		}
		return m.wakeArchived(ctx, sb)
	}
	if sb.Failed != "" {
		return "", fmt.Errorf("%w: %s", ErrFailed, sb.Failed)
	}
	if err := m.settleCapture(ctx, sb); err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	if sb.HibernateSnap == "" {
		// No journal sync on this data-plane fast path; restart Reconcile heals the lag
		if sb.StaleSnap != "" && m.store.synced() {
			m.dropStale(ctx, sb)
		}
		return sb.VsockSocket, nil
	}
	// The egress lane never hibernates, so a hibernated one is corrupt state
	if sb.Key.Net == types.NetEgress {
		return "", fmt.Errorf("wake %s: egress lane cannot resume from hibernation", sb.ID)
	}
	if readerOK && sb.ReaderSafe {
		return "", ErrSleeping
	}
	if err := m.beginWake(sb); err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	defer m.endWake(sb)
	// A half-restored VM is worse than a wasted wake.
	ctx = context.WithoutCancel(ctx)
	if err := m.resolveIntent(ctx, sb); err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	wakeStart := time.Now()
	snap := sb.HibernateSnap
	restoredSock, err := m.eng.Restore(ctx, sb.VMName, snap)
	if err != nil {
		// Restore may have booted the VM before the engine errored
		m.stopVM(ctx, sb.VMName)
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	sock, err := m.probeReady(ctx, sb.VMName, restoredSock, claimProbeTimeout)
	if err != nil {
		m.stopVM(ctx, sb.VMName)
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
	m.recordUsage(ctx, usage.Event{Event: "wake", ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant, Deadline: m.deadlineOf(sb)})
	if proxyErr := m.out.ArmProxy(ctx, sb); proxyErr != nil {
		log.WithFunc("pool.wakeOrRead").Errorf(ctx, proxyErr, "arm egress proxy %s", sb.ID)
	}
	if m.disarmIfReleased(sb) {
		if err == nil {
			m.dropStale(ctx, sb)
			m.dropSnap(ctx, snap)
		}
		return "", ErrUnknownSandbox
	}
	if err != nil {
		// The lagging journal still references the snapshot, so park it for later reclaim
		sb.StaleSnap = snap
		return "", fmt.Errorf("wake %s: persist claims: %w", sb.ID, err)
	}
	m.dropStale(ctx, sb)
	// The randHex suffix keeps a re-hibernate from reusing the name
	go m.dropSnap(ctx, snap)
	return sock, nil
}

func (m *Manager) stopVM(ctx context.Context, name string) {
	if err := m.eng.Stop(ctx, name); err != nil {
		log.WithFunc("pool.stopVM").Warnf(ctx, "stop vm %s: %v", name, err)
	}
}

func (m *Manager) idleOnce(ctx context.Context) {
	if !m.sweeps.idle.CompareAndSwap(false, true) {
		return // the previous sweep's hibernates are still draining
	}
	now := time.Now()
	var victims []*types.Sandbox
	m.mu.Lock()
	idleDefault := m.view.Load().idleDefault
	idlePolicy := idleDefault > 0
	for _, p := range m.pools {
		idlePolicy = idlePolicy || p.idle > 0
	}
	if !idlePolicy {
		m.mu.Unlock()
		m.sweeps.idle.Store(false)
		return
	}
	for _, sb := range m.claimed {
		if skipIdle(sb) || m.midMoveLocked(sb) {
			continue
		}
		idle := idleDefault
		if p, pooled := m.activePool(sb.Key); pooled {
			idle = p.idle
		}
		if idle <= 0 || now.Sub(sb.LastSeen()) < idle {
			continue
		}
		victims = append(victims, sb)
	}
	m.mu.Unlock()
	if len(victims) == 0 {
		m.sweeps.idle.Store(false)
		return
	}
	// Hibernates are seconds-long engine snapshots, so fan out off the housekeeping loop
	go func() {
		defer m.sweeps.idle.Store(false)
		m.runBounded(ctx, len(victims), func(ctx context.Context, i int) {
			sb := victims[i]
			logSweepResult(ctx, log.WithFunc("pool.idleOnce"), m.idleHibernate(ctx, sb, now), "idle-hibernated "+sb.ID, "idle-hibernate "+sb.ID)
		}).Wait()
	}()
}

func (m *Manager) idleHibernate(ctx context.Context, sb *types.Sandbox, sweepStart time.Time) error {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	m.mu.Lock()
	woke := sb.Busy() || sb.LastSeen().After(sweepStart) || sb.HibernateSnap != ""
	m.mu.Unlock()
	if woke {
		return errWokeMeanwhile
	}
	return m.hibernateLocked(ctx, sb)
}

func (m *Manager) commitTransition(ctx context.Context, sb *types.Sandbox, snap, sock string) (live bool, err error) {
	return m.commitIfLive(ctx, sb, func() {
		sb.HibernateSnap = snap
		sb.VsockSocket = sock
		sb.PendingSnap = ""
		sb.ReaderSafe = sb.ReaderSafe && snap != ""
		if now := time.Now(); snap == "" && (sb.OnExpire == types.ExpireArchive || m.archiveAfterFor(sb.Key) > 0) && !m.tenantGone(sb.Tenant) && now.After(sb.Deadline) {
			sb.Deadline = now.Add(clampTTL(types.Seconds(sb.LeaseSeconds)))
		}
	})
}

// syncClaims flushes a lagging journal so a hibernate retry cannot report a false success.
func (m *Manager) syncClaims(ctx context.Context, sb *types.Sandbox) error {
	if !m.store.synced() {
		if err := m.store.commit(m.store.mark()); err != nil {
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

// settlePendingSnap settles a dangling hibernate intent and bills an adopted one, which never was.
func (m *Manager) settlePendingSnap(ctx context.Context, sb *types.Sandbox) (adopted bool, err error) {
	adopted, err = m.resolvePendingSnap(ctx, sb)
	if adopted {
		m.recordHibernate(ctx, sb)
	}
	return adopted, err
}

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
		m.store.recommit(ctx, js)
		// The transition is decided in memory, so report the adoption for the caller to bill
		return exists, fmt.Errorf("resolve hibernate intent %s: persist: %w", sb.ID, err)
	}
	return exists, nil
}

func (m *Manager) recordHibernate(ctx context.Context, sb *types.Sandbox) {
	m.counters.hibernates.Add(1)
	m.recordUsage(ctx, usage.Event{Event: "hibernate", ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant})
}

func skipIdle(sb *types.Sandbox) bool {
	return sb.Key.Net == types.NetEgress || hasVolumes(sb) ||
		sb.HibernateSnap != "" || sb.ArchiveCk != "" || sb.Failed != "" || sb.Busy()
}
