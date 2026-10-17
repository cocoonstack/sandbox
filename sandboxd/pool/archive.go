package pool

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const archiveDeleteDir = "archive-deletes"

// archive*For resolve a key's thresholds (pool's, else node default); callers hold m.mu.
func (m *Manager) archiveAfterFor(key types.PoolKey) time.Duration {
	if p, ok := m.activePool(key); ok {
		return p.archiveAfter
	}
	return m.archiveAfterDefault
}

func (m *Manager) archiveDeleteFor(key types.PoolKey) time.Duration {
	if p, ok := m.activePool(key); ok {
		return p.archiveDelete
	}
	return m.archiveDeleteDefault
}

// retentionDeadline is zero for an archive kept forever; callers hold m.mu.
func (m *Manager) retentionDeadline(key types.PoolKey) time.Time {
	if d := m.archiveDeleteFor(key); d > 0 {
		return time.Now().Add(d)
	}
	return time.Time{}
}

func (m *Manager) archiveOnce(ctx context.Context) {
	if !m.archiveSweep.CompareAndSwap(false, true) {
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
		if after <= 0 || now.Sub(sb.LastSeen()) < after {
			continue
		}
		m.archiving[sb.ID] = struct{}{}
		victims = append(victims, sb)
	}
	m.mu.Unlock()
	if len(victims) == 0 {
		m.archiveSweep.Store(false)
		return
	}
	go func() {
		defer m.archiveSweep.Store(false)
		logger := log.WithFunc("pool.archiveOnce")
		m.runBounded(ctx, len(victims), func(ctx context.Context, i int) {
			sb := victims[i]
			logSweepResult(ctx, logger, m.archive(ctx, sb), "archived "+sb.ID, "archive "+sb.ID)
		}).Wait()
	}()
}

func (m *Manager) archive(ctx context.Context, sb *types.Sandbox) error {
	// must finish even if the sweep ctx is canceled, or the record diverges from the store.
	ctx = context.WithoutCancel(ctx)
	defer m.untrack(m.archiving, sb.ID)
	m.mu.Lock()
	ok := m.claimed[sb.ID] == sb && sb.HibernateSnap != "" && sb.ArchiveCk == ""
	m.mu.Unlock()
	if !ok {
		return errWokeMeanwhile
	}
	sb.Transition.Lock()
	err := m.resolveIntent(ctx, sb)
	sb.Transition.Unlock()
	if err != nil {
		return fmt.Errorf("archive %s: settle intent: %w", sb.ID, err)
	}
	if err = m.checkArchiveRow(ctx, sb.ID); err != nil {
		return fmt.Errorf("archive %s: registry: %w", sb.ID, err)
	}
	// pin the ck before Publish makes it listable, or a delete would strand the claim
	ckID := store.CheckpointID(randHex(8))
	m.mu.Lock()
	m.pendingCks[ckID] = struct{}{}
	m.mu.Unlock()
	defer m.untrack(m.pendingCks, ckID)
	// a hibernated source is copied from its wake image, so no VM starts.
	ck, srcSnap, err := m.publishCheckpoint(ctx, sb, ckID, "archive", sb.Tenant, true)
	if err != nil {
		return err
	}
	// the claim must still hold the exact snapshot the export captured, or the ck is stale
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
			m.mu.Lock()
			sb.ArchivePending = ""
			rb := m.store.set(sb)
			m.mu.Unlock()
			sb.Transition.Unlock()
			m.recommit(ctx, rb)
			m.deleteOrphanArchiveCk(ctx, ck.ID)
			return fmt.Errorf("archive %s: persist intent: %w", sb.ID, err)
		}
		if epoch, err = m.putArchive(ctx, sb, ck.ID); err != nil {
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
	snap, vmName, sock, prevDeadline, prevEpoch, prevPending := sb.HibernateSnap, sb.VMName, sb.VsockSocket, sb.Deadline, sb.ArchiveEpoch, sb.ArchivePending
	sb.ArchiveCk, sb.ArchiveEpoch, sb.ArchivePending = ck.ID, epoch, ""
	sb.HibernateSnap, sb.VsockSocket, sb.VMName = "", "", ""
	sb.Deadline = m.retentionDeadline(sb.Key)
	js := m.store.set(sb)
	m.mu.Unlock()
	if saveErr := m.store.commit(js); saveErr != nil {
		// roll back so memory matches the still-durable hibernated record
		m.mu.Lock()
		var rb claimSnapshot
		// a Release that landed meanwhile already deleted the claim; re-setting it would resurrect it
		if m.claimed[sb.ID] == sb {
			sb.ArchiveCk, sb.ArchiveEpoch, sb.ArchivePending = "", prevEpoch, prevPending
			sb.HibernateSnap, sb.VsockSocket, sb.VMName = snap, sock, vmName
			sb.Deadline = prevDeadline
			rb = m.store.set(sb)
		}
		m.mu.Unlock()
		sb.Transition.Unlock()
		m.recommit(ctx, rb)
		m.abandonArchive(ctx, sb, epoch, ck.ID)
		return fmt.Errorf("archive %s: persist claims: %w", sb.ID, saveErr)
	}
	// disarm under Transition so a wake right after the release is not clobbered by a late disarm
	m.out.Disarm(sb.ID, true)
	sb.Transition.Unlock()
	// the store ck is authoritative from here
	m.destroy(ctx, vmName)
	m.dropSnap(ctx, snap)
	m.counters.archives.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "archive", ID: sb.ID, Reference: ck.ID, Tenant: sb.Tenant})
	return nil
}

// wakeArchived restores an archived claim into a fresh local VM; caller holds Transition.
func (m *Manager) wakeArchived(ctx context.Context, sb *types.Sandbox) (string, error) {
	// egress never archives; a corrupt archived egress claim must fail closed.
	if sb.Key.Net == types.NetEgress {
		return "", fmt.Errorf("wake %s: egress lane cannot resume from archive", sb.ID)
	}
	if err := m.tenantRemoved(ctx, sb.Tenant); err != nil {
		return "", err
	}
	if err := m.diskErr(); err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	ctx = context.WithoutCancel(ctx)
	epoch, err := m.takeArchive(ctx, sb)
	if err != nil {
		return "", err
	}
	ck := sb.ArchiveCk
	defer m.lockRec(ck)()
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
	if markErr := m.markArchiveCk(ck); markErr != nil {
		m.destroy(ctx, built.VMName)
		return "", fmt.Errorf("wake %s: track archive checkpoint: %w", sb.ID, markErr)
	}
	live, saved := m.commitWake(ctx, sb, built.VMName, built.VsockSocket)
	if !live || !saved {
		m.destroy(ctx, built.VMName) // released, or persist rolled back to archived
		if !live {
			return "", ErrUnknownSandbox
		}
		return "", fmt.Errorf("wake %s: persist claims", sb.ID)
	}
	logger := log.WithFunc("pool.wakeArchived")
	// the pre-commit marker keeps a failed delete retryable after the journal update.
	if delErr := m.ckpts.Delete(ctx, ck); delErr != nil {
		logger.Warnf(ctx, "delete consumed archive ck %s: %v", ck, delErr)
	} else {
		if clearErr := m.clearArchiveCk(ck); clearErr != nil {
			logger.Warnf(ctx, "clear archive ck %s: %v", ck, clearErr)
		}
		m.dropTombstone(ctx, sb.ID, consumed)
	}
	// only the none lane reaches here, so there is no NIC to re-lock
	if proxyErr := m.out.ArmProxy(ctx, sb); proxyErr != nil {
		logger.Errorf(ctx, proxyErr, "arm egress proxy %s", sb.ID)
	}
	if m.disarmIfReleased(sb) {
		return "", ErrUnknownSandbox
	}
	m.counters.unarchives.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "unarchive", ID: sb.ID, Reference: ck, Tenant: sb.Tenant})
	return built.VsockSocket, nil
}

// commitWake swaps an archived record back to a live VM if the claim is still live.
func (m *Manager) commitWake(ctx context.Context, sb *types.Sandbox, vmName, sock string) (live, saved bool) {
	m.mu.Lock()
	live = m.claimed[sb.ID] == sb
	if !live {
		m.mu.Unlock()
		return false, false
	}
	ck, deadline := sb.ArchiveCk, sb.Deadline
	sb.VMName, sb.VsockSocket, sb.ArchiveCk = vmName, sock, ""
	sb.Deadline = time.Now().Add(clampTTL(types.Seconds(sb.LeaseSeconds))) // a woken sandbox is a fresh lease
	js := m.store.set(sb)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.mu.Lock()
		var rb claimSnapshot
		if m.claimed[sb.ID] == sb {
			sb.VMName, sb.VsockSocket, sb.ArchiveCk, sb.Deadline = "", "", ck, deadline
			rb = m.store.set(sb)
		}
		m.mu.Unlock()
		m.recommit(ctx, rb)
		log.WithFunc("pool.commitWake").Warnf(ctx, "persist claims: %v", err)
		return true, false
	}
	sb.Touch() // restart the idle→hibernate→archive clock
	return true, true
}

func (m *Manager) deleteOrphanArchiveCk(ctx context.Context, ckID string) {
	if err := m.deleteArchiveCk(ctx, ckID); err != nil {
		log.WithFunc("pool.deleteOrphanArchiveCk").Warnf(ctx, "delete orphaned archive ck %s: %v", ckID, err)
	}
}

// markArchiveCk records durable delete intent before the journal drops its reference.
func (m *Manager) markArchiveCk(ckID string) error {
	return m.markArchiveCks([]string{ckID})[ckID]
}

// markArchiveCks writes a delete marker per checkpoint and syncs the directory once; failed holds each id whose marker is not durable.
func (m *Manager) markArchiveCks(ckIDs []string) (failed map[string]error) {
	if len(ckIDs) == 0 {
		return nil
	}
	failed = map[string]error{}
	dir := filepath.Join(m.dataDir, archiveDeleteDir)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		for _, id := range ckIDs {
			failed[id] = err
		}
		return failed
	}
	written := make([]string, 0, len(ckIDs))
	for _, id := range ckIDs {
		if !store.CheckpointIDRe.MatchString(id) {
			failed[id] = fmt.Errorf("invalid checkpoint id %q", id)
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, id), nil, 0o600); err != nil {
			failed[id] = err
			continue
		}
		written = append(written, id)
	}
	if len(written) == 0 {
		return failed
	}
	if err := syncDir(dir); err != nil {
		for _, id := range written {
			failed[id] = err
		}
	}
	return failed
}

func (m *Manager) archiveDeleteMarkers() ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(m.dataDir, archiveDeleteDir))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Type().IsRegular() && store.CheckpointIDRe.MatchString(entry.Name()) {
			ids = append(ids, entry.Name())
		}
	}
	return ids, nil
}

func (m *Manager) clearArchiveCk(ckID string) error {
	err := os.Remove(filepath.Join(m.dataDir, archiveDeleteDir, ckID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (m *Manager) deleteArchiveCk(ctx context.Context, ckID string) error {
	if err := m.markArchiveCk(ckID); err != nil {
		return fmt.Errorf("track: %w", err)
	}
	if err := m.deleteCkLocked(ctx, ckID); err != nil {
		return err
	}
	if err := m.clearArchiveCk(ckID); err != nil {
		return fmt.Errorf("clear: %w", err)
	}
	return nil
}

func (m *Manager) retryArchiveDeletes(ctx context.Context) {
	if !m.archiveDeleteSweep.CompareAndSwap(false, true) {
		return
	}
	defer m.archiveDeleteSweep.Store(false)
	ids, err := m.archiveDeleteMarkers()
	if err != nil {
		log.WithFunc("pool.retryArchiveDeletes").Warnf(ctx, "list: %v", err)
		return
	}
	if len(ids) == 0 {
		return
	}
	pinned := m.pinnedArchiveCks()
	ids = slices.DeleteFunc(ids, func(id string) bool { _, ok := pinned[id]; return ok })
	m.runBounded(ctx, len(ids), func(ctx context.Context, i int) {
		m.retryArchiveDelete(ctx, ids[i])
	}).Wait()
}

func (m *Manager) retryArchiveDelete(ctx context.Context, ckID string) {
	unlock := m.lockRec(ckID)
	if m.archiveCkPinned(ckID) || !m.deleteMarked(ckID) {
		unlock()
		return
	}
	err := m.ckpts.Delete(ctx, ckID)
	unlock()
	if err != nil {
		log.WithFunc("pool.retryArchiveDelete").Warnf(ctx, "delete %s: %v", ckID, err)
		return
	}
	if err := m.clearArchiveCk(ckID); err != nil {
		log.WithFunc("pool.retryArchiveDelete").Warnf(ctx, "clear %s: %v", ckID, err)
	}
}

func (m *Manager) deleteMarked(ckID string) bool {
	_, err := os.Stat(filepath.Join(m.dataDir, archiveDeleteDir, ckID))
	return err == nil
}

func (m *Manager) archiveCkPinned(ckID string) bool {
	_, pinned := m.pinnedArchiveCks()[ckID]
	return pinned
}
