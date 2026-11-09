package pool

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

// handOffQuiet spaces a node's capacity hand-offs after one failed, so retried wakes do not churn the registry.
const handOffQuiet = 30 * time.Second

// archiveAfterFor resolves a key's threshold (pool's, else node default); callers hold m.mu.
func (m *Manager) archiveAfterFor(key types.PoolKey) time.Duration {
	if p, ok := m.activePool(key); ok {
		return p.archiveAfter
	}
	return m.view.Load().archiveAfterDefault
}

// retentionDeadline is zero for an archive kept forever; callers hold m.mu.
func (m *Manager) retentionDeadline(key types.PoolKey) time.Time {
	d := m.view.Load().archiveDeleteDefault
	if p, ok := m.activePool(key); ok {
		d = p.archiveDelete
	}
	if d > 0 {
		return time.Now().Add(d)
	}
	return time.Time{}
}

// archiveDeadline is the retention deadline unless the claim's lease must survive the archive.
func (m *Manager) archiveDeadline(sb *types.Sandbox) time.Time {
	retention := m.retentionDeadline(sb.Key)
	optedIn := sb.OnExpire == types.ExpireArchive || m.archiveAfterFor(sb.Key) > 0
	if !optedIn || m.evacuatingLocked(sb.ID) && !retention.IsZero() && sb.Deadline.After(retention) {
		return sb.Deadline
	}
	return retention
}

func (m *Manager) archiveOnce(ctx context.Context) {
	if !m.sweeps.archive.CompareAndSwap(false, true) {
		return // the previous sweep's archives are still draining
	}
	now := time.Now()
	var victims []*types.Sandbox
	m.mu.Lock()
	for _, sb := range m.claimed {
		if sb.HibernateSnap == "" || sb.ArchiveCk != "" || m.tenantGone(sb.Tenant) {
			continue
		}
		if _, busy := m.archiving[sb.ID]; busy {
			continue // a reap-triggered archive is already exporting this one
		}
		after := m.archiveAfterFor(sb.Key)
		if after <= 0 || now.Sub(sb.LastSeen()) < after || now.Before(sb.ArchiveRetryAt) {
			continue
		}
		m.archiving[sb.ID] = struct{}{}
		victims = append(victims, sb)
	}
	m.mu.Unlock()
	if len(victims) == 0 {
		m.sweeps.archive.Store(false)
		return
	}
	go func() {
		defer m.sweeps.archive.Store(false)
		logger := log.WithFunc("pool.archiveOnce")
		m.runBounded(ctx, len(victims), func(ctx context.Context, i int) {
			sb := victims[i]
			logSweepResult(ctx, logger, m.archive(ctx, sb), "archived "+sb.ID, "archive "+sb.ID)
		}).Wait()
	}()
}

func (m *Manager) archive(ctx context.Context, sb *types.Sandbox) (err error) {
	// Must finish even if the sweep ctx is canceled, or the record diverges from the store.
	ctx = context.WithoutCancel(ctx)
	defer func() {
		m.mu.Lock()
		delete(m.archiving, sb.ID)
		if err != nil {
			sb.ArchiveRetryAt = time.Now().Add(archiveRetryDelay)
		}
		m.mu.Unlock()
	}()
	m.mu.Lock()
	ok := m.claimed[sb.ID] == sb && sb.HibernateSnap != "" && sb.ArchiveCk == ""
	m.mu.Unlock()
	if !ok {
		return errWokeMeanwhile
	}
	sb.Transition.Lock()
	err = m.resolveIntent(ctx, sb)
	snap := sb.HibernateSnap
	sb.Transition.Unlock()
	if err != nil {
		return fmt.Errorf("archive %s: settle intent: %w", sb.ID, err)
	}
	if snap == "" {
		return errWokeMeanwhile
	}
	if err = m.checkArchiveRow(ctx, sb.ID); err != nil {
		return fmt.Errorf("archive %s: registry: %w", sb.ID, err)
	}
	// Pin the ck before Publish makes it listable, or a delete would strand the claim
	ckID := store.CheckpointID(randHex(8))
	m.mu.Lock()
	m.pendingCks[ckID] = struct{}{}
	m.mu.Unlock()
	defer m.untrack(m.pendingCks, ckID)
	// A hibernated source is copied from its wake image, so no VM starts.
	ck, srcSnap, err := m.publishCheckpoint(ctx, sb, ckID, "archive", sb.Tenant, snap, true)
	if err != nil {
		m.deleteOrphanArchiveCk(ctx, ckID)
		return err
	}
	// The claim must still hold the exact snapshot the export captured, or the ck is stale
	sb.Transition.Lock()
	m.mu.Lock()
	if m.claimed[sb.ID] != sb || sb.HibernateSnap != srcSnap || sb.ArchiveCk != "" {
		m.mu.Unlock()
		sb.Transition.Unlock()
		m.deleteOrphanArchiveCk(ctx, ck.ID)
		return errWokeMeanwhile
	}
	var epoch int64
	if m.archives != nil {
		sb.ArchivePending = ck.ID
		intent := m.store.set(sb)
		m.mu.Unlock()
		if err = m.store.commit(intent); err != nil {
			rb, _ := m.setIfLive(sb, func() { sb.ArchivePending = "" })
			sb.Transition.Unlock()
			m.store.recommit(ctx, rb)
			m.deleteOrphanArchiveCk(ctx, ck.ID)
			return fmt.Errorf("archive %s: persist intent: %w", sb.ID, err)
		}
		if epoch, err = m.writeArchiveRow(ctx, sb, ck.ID); err != nil {
			sb.Transition.Unlock()
			m.abandonRefusedPut(ctx, sb, ck.ID, err)
			return fmt.Errorf("archive %s: registry: %w", sb.ID, err)
		}
		m.mu.Lock()
		if m.claimed[sb.ID] != sb {
			m.mu.Unlock()
			sb.Transition.Unlock()
			m.abandonArchive(ctx, sb, epoch, ck.ID)
			return errWokeMeanwhile
		}
	}
	snap, vmName, sock, prevDeadline, prevPending, replica := sb.HibernateSnap, sb.VMName, sb.VsockSocket, sb.Deadline, sb.ArchivePending, sb.ReplicaCk
	sb.ArchiveCk, sb.ArchiveEpoch, sb.ArchivePending, sb.ReplicaCk = ck.ID, epoch, "", ""
	sb.HibernateSnap, sb.VsockSocket = "", ""
	m.setVMNameLocked(sb, "")
	sb.Deadline = m.archiveDeadline(sb)
	js, archivedAt := m.store.set(sb), time.Now()
	m.mu.Unlock()
	if saveErr := m.store.commit(js); saveErr != nil {
		rb, live := m.setIfLive(sb, func() {
			sb.ArchiveCk, sb.ArchiveEpoch, sb.ArchivePending, sb.ReplicaCk = "", 0, prevPending, replica
			if replica != "" {
				sb.ArchiveEpoch = epoch
			}
			sb.HibernateSnap, sb.VsockSocket = snap, sock
			m.setVMNameLocked(sb, vmName)
			sb.Deadline = prevDeadline
		})
		sb.Transition.Unlock()
		m.store.recommit(ctx, rb)
		if !live || replica == "" {
			m.abandonArchive(ctx, sb, epoch, ck.ID)
		}
		return fmt.Errorf("archive %s: persist claims: %w", sb.ID, saveErr)
	}
	m.counters.archives.Add(1)
	m.recordUsage(ctx, usage.Event{Time: archivedAt, Event: "archive", ID: sb.ID, Reference: ck.ID, Tenant: sb.Tenant, Deadline: sb.Deadline})
	// Disarm under Transition so a wake right after the release is not clobbered by a late disarm
	m.out.Disarm(sb.ID, true)
	sb.Transition.Unlock()
	// The store ck is authoritative from here
	m.destroy(ctx, vmName)
	m.dropSnap(ctx, snap)
	if replica != "" {
		m.deleteOrphanArchiveCk(ctx, replica)
	}
	return nil
}

// Caller holds Transition.
func (m *Manager) wakeArchived(ctx context.Context, sb *types.Sandbox) (string, error) {
	// Egress never archives; a corrupt archived egress claim must fail closed.
	if sb.Key.Net == types.NetEgress {
		return "", fmt.Errorf("wake %s: egress lane cannot resume from archive", sb.ID)
	}
	if err := m.tenantRemoved(ctx, sb.Tenant); err != nil {
		return "", err
	}
	if err := m.beginWake(sb, true); err != nil {
		if errors.Is(err, ErrQuota) && m.handOffForRoom(ctx, sb) {
			return "", fmt.Errorf("wake %s: %w", sb.ID, ErrUnknownSandbox)
		}
		// memory only spreads load, so a wake no peer took runs here
		if !errors.Is(err, errMemoryLow) {
			return "", fmt.Errorf("wake %s: %w", sb.ID, err)
		}
		if err = m.beginWake(sb, false); err != nil {
			return "", fmt.Errorf("wake %s: %w", sb.ID, err)
		}
		m.counters.wakeMemoryFallbacks.Add(1)
	}
	defer m.endWake(sb)
	ctx = context.WithoutCancel(ctx)
	epoch, err := m.takeArchive(ctx, sb)
	if err != nil {
		return "", err
	}
	ck := sb.ArchiveCk
	defer m.recs.lockRec(ck)()
	dir, _, _, err := m.ckpts.Fetch(ctx, ck)
	if errors.Is(err, store.ErrNotFound) {
		return "", ErrUnknownSandbox // record disagrees with the store
	}
	if err != nil {
		return "", fmt.Errorf("wake %s: fetch archive: %w", sb.ID, err)
	}
	built, err := m.provision(ctx, sb.Key, dir)
	if err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	consumed, fenceErr := m.consumeRow(ctx, sb, epoch, "wake")
	if fenceErr != nil {
		m.destroy(ctx, built.VMName)
		return "", fenceErr
	}
	if markErr := m.ckMarks.mark(ck); markErr != nil {
		m.destroy(ctx, built.VMName)
		return "", fmt.Errorf("wake %s: track archive checkpoint: %w", sb.ID, markErr)
	}
	if err = m.commitWake(ctx, sb, built.VMName, built.VsockSocket); err != nil {
		m.destroy(ctx, built.VMName)
		return "", err
	}
	m.counters.unarchives.Add(1)
	logger := log.WithFunc("pool.wakeArchived")
	// Only the none lane reaches here, so there is no NIC to re-lock
	if proxyErr := m.out.ArmProxy(ctx, sb); proxyErr != nil {
		logger.Errorf(ctx, proxyErr, "arm egress proxy %s", sb.ID)
	}
	// The pre-commit marker keeps a failed delete retryable after the journal update.
	if delErr := m.ckpts.Delete(ctx, ck); delErr != nil {
		logger.Warnf(ctx, "delete consumed archive ck %s: %v", ck, delErr)
	} else {
		if clearErr := m.ckMarks.clear(ck); clearErr != nil {
			logger.Warnf(ctx, "clear archive ck %s: %v", ck, clearErr)
		}
		m.dropTombstone(ctx, sb.ID, consumed)
	}
	if m.disarmIfReleased(sb) {
		return "", ErrUnknownSandbox
	}
	return built.VsockSocket, nil
}

// handOffForRoom runs one capacity hand-off at a time and reports whether a peer adopted the claim; the caller holds Transition.
func (m *Manager) handOffForRoom(ctx context.Context, sb *types.Sandbox) bool {
	if m.archives == nil || !m.handOffBusy.CompareAndSwap(false, true) {
		return false
	}
	defer m.handOffBusy.Store(false)
	if time.Now().UnixNano() < m.handOffQuietUntil.Load() {
		return false
	}
	logger := log.WithFunc("pool.handOffForRoom")
	err := m.handOff(ctx, sb, true)
	switch {
	case err == nil || errors.Is(err, ErrUnknownSandbox):
		m.counters.wakeHandoffs.Add(1)
		logger.Infof(ctx, "handed archived %s to a peer with room to wake it", sb.ID)
		return true
	case errors.Is(err, errNoArchiveRow):
		return false
	case !errors.Is(err, errNoAdopter):
		logger.Warnf(ctx, "hand %s to a peer with room: %v", sb.ID, err)
	}
	m.handOffQuietUntil.Store(time.Now().Add(handOffQuiet).UnixNano())
	return false
}

func (m *Manager) commitWake(ctx context.Context, sb *types.Sandbox, vmName, sock string) error {
	m.mu.Lock()
	if m.claimed[sb.ID] != sb {
		m.mu.Unlock()
		return ErrUnknownSandbox
	}
	ck, deadline, epoch, safe := sb.ArchiveCk, sb.Deadline, sb.ArchiveEpoch, sb.ReaderSafe
	sb.VsockSocket, sb.ArchiveCk, sb.ArchiveEpoch, sb.ReaderSafe = sock, "", 0, false
	m.setVMNameLocked(sb, vmName)
	wokeAt := time.Now()
	sb.Deadline = wokeAt.Add(clampTTL(types.Seconds(sb.LeaseSeconds))) // a woken sandbox is a fresh lease
	js, lease := m.store.set(sb), sb.Deadline
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		rb, _ := m.setIfLive(sb, func() {
			sb.VsockSocket, sb.ArchiveCk, sb.ArchiveEpoch, sb.Deadline, sb.ReaderSafe = "", ck, epoch, deadline, safe
			m.setVMNameLocked(sb, "")
		})
		m.store.recommit(ctx, rb)
		return fmt.Errorf("wake %s: persist claims: %w", sb.ID, err)
	}
	sb.Touch() // restart the idle→hibernate→archive clock
	m.recordUsage(ctx, usage.Event{Time: wokeAt, Event: "unarchive", ID: sb.ID, Reference: ck, Tenant: sb.Tenant, Deadline: lease})
	return nil
}

func (m *Manager) deleteOrphanArchiveCk(ctx context.Context, ckID string) {
	if _, err := m.deleteArchiveCk(ctx, ckID); err != nil {
		log.WithFunc("pool.deleteOrphanArchiveCk").Warnf(ctx, "delete orphaned archive ck %s: %v", ckID, err)
	}
}
