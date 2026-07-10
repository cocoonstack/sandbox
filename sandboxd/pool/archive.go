package pool

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// archive*For resolve a key's thresholds (pool's, else node default); callers hold m.mu.
func (m *Manager) archiveAfterFor(key types.PoolKey) time.Duration {
	if p, ok := m.pools[key]; ok {
		return p.archiveAfter
	}
	return m.archiveAfterDefault
}

func (m *Manager) archiveDeleteFor(key types.PoolKey) time.Duration {
	if p, ok := m.pools[key]; ok {
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
			switch err := m.archive(ctx, sb); {
			case err == nil:
				logger.Infof(ctx, "archived %s", sb.ID)
			case !benignSweepErr(err):
				logger.Errorf(ctx, err, "archive %s", sb.ID)
			}
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
	sb.Transition.Unlock()
	// Committed: the store ck is authoritative now; reclaim the local footprint.
	if rmErr := m.eng.Remove(ctx, vmName); rmErr != nil {
		log.WithFunc("pool.archive").Warnf(ctx, "remove archived VM %s: %v", vmName, rmErr)
	}
	m.dropSnap(ctx, snap)
	m.counters.archives.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "archive", ID: sb.ID, Reference: ck.ID, Tenant: sb.Tenant})
	return nil
}

// wakeArchived restores an archived claim from its store checkpoint into a
// fresh local VM, keeping id/token/tenant; the caller holds the Transition lock.
func (m *Manager) wakeArchived(ctx context.Context, sb *types.Sandbox) (string, error) {
	ctx = context.WithoutCancel(ctx)
	ck := sb.ArchiveCk
	l := m.recLock(ck)
	l.RLock()
	defer l.RUnlock()
	dir, _, release, err := m.ckpts.Fetch(ctx, ck)
	if errors.Is(err, store.ErrNotFound) {
		return "", ErrUnknownSandbox // record disagrees with the store
	}
	if err != nil {
		return "", fmt.Errorf("wake %s: fetch archive: %w", sb.ID, err)
	}
	defer release()
	built, err := m.provision(ctx, sb.Key, dir)
	if err != nil {
		return "", fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	live, saved := m.commitWake(ctx, sb, built.VMName, built.VsockSocket)
	if !live || !saved {
		m.destroy(ctx, built.VMName) // released, or persist rolled back to archived
		if !live {
			return "", ErrUnknownSandbox
		}
		return "", fmt.Errorf("wake %s: persist claims", sb.ID)
	}
	// Consumed into the live VM; drop the store copy, and its now-dead lock.
	if delErr := m.ckpts.Delete(ctx, ck); delErr != nil {
		log.WithFunc("pool.wakeArchived").Warnf(ctx, "delete consumed archive ck %s: %v", ck, delErr)
	} else {
		m.dropRecLock(ck)
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
	if err := m.ckpts.Delete(ctx, ckID); err != nil {
		log.WithFunc("pool.deleteOrphanArchiveCk").Warnf(ctx, "delete orphaned archive ck %s: %v", ckID, err)
	}
}
