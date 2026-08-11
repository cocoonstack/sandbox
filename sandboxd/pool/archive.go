package pool

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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

func (m *Manager) archiveEnabledFor(key types.PoolKey) bool {
	return m.archiveAfterFor(key) > 0
}

// archiveOnce checkpoints hibernated claims idle past their archive threshold
// to the store and drops their local VM; the next access restores them.
func (m *Manager) archiveOnce(ctx context.Context) {
	if !m.archiveEnabled {
		return
	}
	if !m.archiveSweep.CompareAndSwap(false, true) {
		return // the previous sweep's archives are still draining
	}
	now := time.Now()
	var victims []*types.Sandbox
	m.mu.Lock()
	for _, sb := range m.claimed {
		if sb.HibernateSnap == "" || sb.ArchiveCk != "" {
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

// archive publishes a hibernated claim's wake image to the store, then swaps
// its compute backing from local VM to store checkpoint (ArchiveCk), preserving
// id/token/tenant so a later access restores it (wakeArchived).
func (m *Manager) archive(ctx context.Context, sb *types.Sandbox) error {
	// Must finish even if the sweep ctx is canceled, or the record diverges from the store.
	ctx = context.WithoutCancel(ctx)
	defer m.untrack(m.archiving, sb.ID)
	m.mu.Lock()
	ok := m.claimed[sb.ID] == sb && sb.HibernateSnap != "" && sb.ArchiveCk == ""
	m.mu.Unlock()
	if !ok {
		return errWokeMeanwhile
	}
	// Pin the wake image before it exists: from the moment Publish makes it
	// listable, a tenant/root delete would strand the claim it will back.
	ckID := store.CheckpointID(randHex(8))
	m.mu.Lock()
	m.pendingCks[ckID] = struct{}{}
	m.mu.Unlock()
	defer m.untrack(m.pendingCks, ckID)
	// A hibernated source is copied from its wake image, so no VM starts.
	ck, srcSnap, err := m.publishCheckpoint(ctx, sb, ckID, "archive", sb.Tenant, true)
	if err != nil {
		return err
	}
	// Re-validate under Transition+m.mu: the claim must still be hibernated on
	// the exact snapshot the export captured — a wake, or a wake+re-hibernate
	// landing inside the publish window, means the checkpoint holds stale state.
	sb.Transition.Lock()
	m.mu.Lock()
	if m.claimed[sb.ID] != sb || sb.HibernateSnap != srcSnap || sb.ArchiveCk != "" {
		m.mu.Unlock()
		sb.Transition.Unlock()
		m.deleteOrphanArchiveCk(ctx, ck.ID)
		return errWokeMeanwhile
	}
	snap, vmName, sock, prevDeadline := sb.HibernateSnap, sb.VMName, sb.VsockSocket, sb.Deadline
	sb.ArchiveCk = ck.ID
	sb.HibernateSnap, sb.VsockSocket, sb.VMName = "", "", ""
	if d := m.archiveDeleteFor(sb.Key); d > 0 {
		sb.Deadline = time.Now().Add(d) // retention window, not the claim lease
	} else {
		sb.Deadline = time.Time{} // keep forever: no retention (reap skips a zero deadline)
	}
	js := m.store.snapshot(m.claimed)
	m.mu.Unlock()
	if saveErr := m.store.commit(js); saveErr != nil {
		// Roll back so memory matches the still-durable hibernated record; drop the orphan ck.
		m.mu.Lock()
		sb.ArchiveCk = ""
		sb.HibernateSnap, sb.VsockSocket, sb.VMName = snap, sock, vmName
		sb.Deadline = prevDeadline
		rb := m.store.snapshot(m.claimed)
		m.mu.Unlock()
		sb.Transition.Unlock()
		m.recommit(ctx, rb)
		m.deleteOrphanArchiveCk(ctx, ck.ID)
		return fmt.Errorf("archive %s: persist claims: %w", sb.ID, saveErr)
	}
	// Disarm under Transition so a wake that runs the instant we release it
	// re-arms cleanly instead of being clobbered by a late disarm.
	m.disarmEgress(sb.ID, true)
	sb.Transition.Unlock()
	// Committed: the store ck is authoritative now; reclaim the local footprint.
	m.destroy(ctx, vmName)
	m.dropSnap(ctx, snap)
	m.counters.archives.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "archive", ID: sb.ID, Reference: ck.ID, Tenant: sb.Tenant})
	return nil
}

// wakeArchived restores an archived claim from its store checkpoint into a
// fresh local VM, keeping id/token/tenant; the caller holds the Transition lock.
func (m *Manager) wakeArchived(ctx context.Context, sb *types.Sandbox) (string, error) {
	// Egress never archives; a corrupt archived egress claim must fail closed.
	if sb.Key.Net == types.NetEgress {
		return "", fmt.Errorf("wake %s: egress lane cannot resume from archive", sb.ID)
	}
	ctx = context.WithoutCancel(ctx)
	ck := sb.ArchiveCk
	// Exclusive, not shared: this path deletes the consumed ck below. The
	// lock releases before eviction is considered, below — evicting while
	// still held would let a concurrent recLock for ck split onto a
	// different mutex.
	l := m.recLock(ck)
	l.Lock()
	consumed := false
	defer func() {
		l.Unlock()
		if consumed {
			m.recDoneEvict(ck)
		} else {
			m.recDone(ck)
		}
	}()
	dir, _, release, err := m.ckpts.Fetch(ctx, ck)
	if errors.Is(err, store.ErrNotFound) {
		return "", ErrUnknownSandbox // record disagrees with the store
	}
	if err != nil {
		return "", fmt.Errorf("wake %s: fetch archive: %w", sb.ID, err)
	}
	built, err := m.provision(ctx, sb.Key, dir)
	release()
	if err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
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
	// The pre-commit marker keeps a failed delete retryable after the journal update.
	if delErr := m.ckpts.Delete(ctx, ck); delErr != nil {
		log.WithFunc("pool.wakeArchived").Warnf(ctx, "delete consumed archive ck %s: %v", ck, delErr)
	} else {
		consumed = true
		if clearErr := m.clearArchiveCk(ck); clearErr != nil {
			log.WithFunc("pool.wakeArchived").Warnf(ctx, "clear archive ck %s: %v", ck, clearErr)
		}
	}
	// Only the none lane reaches here (the egress guard above fails closed), so
	// this rebinds the none-lane proxy; there is no NIC to re-lock.
	if proxyErr := m.armEgressProxy(ctx, sb); proxyErr != nil {
		log.WithFunc("pool.wakeArchived").Errorf(ctx, proxyErr, "arm egress proxy %s", sb.ID)
	}
	if m.disarmIfReleased(sb) {
		return "", ErrUnknownSandbox
	}
	m.counters.unarchives.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "unarchive", ID: sb.ID, Reference: ck, Tenant: sb.Tenant})
	return built.VsockSocket, nil
}

// commitWake swaps an archived record back to a live VM if still claimed
// (Release/reap skip the Transition lock). A failed persist rolls the record
// back to archived so memory matches disk and the ck stays pinned.
func (m *Manager) commitWake(ctx context.Context, sb *types.Sandbox, vmName, sock string) (live, saved bool) {
	m.mu.Lock()
	live = m.claimed[sb.ID] == sb
	if !live {
		m.mu.Unlock()
		return false, false
	}
	ck, deadline := sb.ArchiveCk, sb.Deadline
	sb.VMName, sb.VsockSocket, sb.ArchiveCk = vmName, sock, ""
	sb.Deadline = time.Now().Add(clampTTL(0)) // a woken sandbox is a fresh lease
	js := m.store.snapshot(m.claimed)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.mu.Lock()
		sb.VMName, sb.VsockSocket, sb.ArchiveCk, sb.Deadline = "", "", ck, deadline
		rb := m.store.snapshot(m.claimed)
		m.mu.Unlock()
		m.recommit(ctx, rb)
		log.WithFunc("pool.commitWake").Warnf(ctx, "persist claims: %v", err)
		return true, false
	}
	sb.Touch() // restart the idle→hibernate→archive clock
	return true, true
}

// deleteOrphanArchiveCk drops the published ck when archive() aborts pre-commit.
func (m *Manager) deleteOrphanArchiveCk(ctx context.Context, ckID string) {
	if err := m.deleteArchiveCk(ctx, ckID); err != nil {
		log.WithFunc("pool.deleteOrphanArchiveCk").Warnf(ctx, "delete orphaned archive ck %s: %v", ckID, err)
	}
}

// markArchiveCk records durable delete intent before the journal drops its reference.
func (m *Manager) markArchiveCk(ckID string) error {
	if !store.CheckpointIDRe.MatchString(ckID) {
		return fmt.Errorf("invalid checkpoint id %q", ckID)
	}
	dir := filepath.Join(m.dataDir, archiveDeleteDir)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, ckID), nil, 0o600)
}

func (m *Manager) clearCanceledArchiveDeletes(ctx context.Context, claims map[string]*types.Sandbox) {
	logger := log.WithFunc("pool.clearCanceledArchiveDeletes")
	entries, err := os.ReadDir(filepath.Join(m.dataDir, archiveDeleteDir))
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		logger.Warnf(ctx, "list: %v", err)
		return
	}
	if len(entries) == 0 {
		return
	}
	live := make(map[string]struct{})
	for _, sb := range claims {
		if sb.ArchiveCk != "" {
			live[sb.ArchiveCk] = struct{}{}
		}
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !store.CheckpointIDRe.MatchString(entry.Name()) {
			continue
		}
		if _, ok := live[entry.Name()]; !ok {
			continue
		}
		if err := m.clearArchiveCk(entry.Name()); err != nil {
			logger.Warnf(ctx, "clear %s: %v", entry.Name(), err)
		}
	}
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
	entries, err := os.ReadDir(filepath.Join(m.dataDir, archiveDeleteDir))
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		log.WithFunc("pool.retryArchiveDeletes").Warnf(ctx, "list: %v", err)
		return
	}
	if len(entries) == 0 {
		return
	}
	pinned := m.pinnedArchiveCks()
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !store.CheckpointIDRe.MatchString(entry.Name()) {
			continue
		}
		if _, ok := pinned[entry.Name()]; !ok {
			ids = append(ids, entry.Name())
		}
	}
	m.runBounded(ctx, len(ids), func(ctx context.Context, i int) {
		m.retryArchiveDelete(ctx, ids[i])
	}).Wait()
}

func (m *Manager) retryArchiveDelete(ctx context.Context, ckID string) {
	l := m.recLock(ckID)
	l.Lock()
	if m.archiveCkPinned(ckID) {
		l.Unlock()
		m.recDone(ckID)
		return
	}
	err := m.ckpts.Delete(ctx, ckID)
	l.Unlock()
	if err != nil {
		m.recDone(ckID)
		log.WithFunc("pool.retryArchiveDeletes").Warnf(ctx, "delete %s: %v", ckID, err)
		return
	}
	m.recDoneEvict(ckID)
	if err := m.clearArchiveCk(ckID); err != nil {
		log.WithFunc("pool.retryArchiveDeletes").Warnf(ctx, "clear %s: %v", ckID, err)
	}
}

func (m *Manager) archiveCkPinned(ckID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.pendingCks[ckID]; ok {
		return true
	}
	for _, sb := range m.claimed {
		if sb.ArchiveCk == ckID {
			return true
		}
	}
	return false
}
