package pool

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/netfilter"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// Reconcile aligns claims and VMs after a restart; it must run before the server starts.
func (m *Manager) Reconcile(ctx context.Context) error {
	claims, err := m.store.load()
	if err != nil {
		return err
	}
	m.clearCanceledArchiveDeletes(ctx, claims)
	vms, err := m.eng.List(ctx)
	if err != nil {
		return fmt.Errorf("list vms: %w", err)
	}
	live := make(map[string]types.VMRecord, len(vms))
	for _, vm := range vms {
		live[vm.Config.Name] = vm
	}
	snaps, snapsErr := m.eng.SnapshotList(ctx)

	owned := map[string]bool{}
	referenced := map[string]bool{}
	m.mu.Lock()
	for id, sb := range claims {
		rec, ok := live[sb.VMName]
		switch {
		case sb.ArchiveCk != "":
			// archived: no local VM by design, so adopt the stub and let the first exec wake it
			m.claimed[id] = sb
			m.tenantDelta(sb.Tenant, 1)
			continue
		case ok && rec.State == vmStateRunning:
			sb.VsockSocket = rec.VsockSocket
			// running with either flag set means a crashed wake or an intent the engine never saw
			sb.HibernateSnap, sb.PendingSnap = "", ""
		case ok && sb.HibernateSnap != "":
			// Hibernated: the VM is stopped by design and wakes on demand.
			sb.PendingSnap = ""
		case ok && sb.PendingSnap != "" && (snapsErr != nil || slices.Contains(snaps, sb.PendingSnap)):
			// the journaled intent names the wake image, so adopt it when the snapshot is there
			sb.HibernateSnap, sb.PendingSnap = sb.PendingSnap, ""
		default:
			continue
		}
		m.claimed[id] = sb
		m.tenantDelta(sb.Tenant, 1)
		m.adoptVolumes(sb.Volumes)
		owned[sb.VMName] = true
		referenced[sb.HibernateSnap] = true
	}
	saveErr := m.store.save(m.claimed)
	for _, p := range m.pools {
		m.adoptGolden(p)
	}
	m.mu.Unlock()
	// a crash mid-export leaves a *.tmp staging dir nothing in this life reuses
	tmps, _ := filepath.Glob(filepath.Join(m.goldensDir(), "*.tmp"))
	if err := m.ckpts.SweepStaging(); err != nil {
		log.WithFunc("pool.Reconcile").Error(ctx, err, "sweep checkpoint staging")
	}
	if err := m.tpls.SweepStaging(); err != nil {
		log.WithFunc("pool.Reconcile").Error(ctx, err, "sweep template staging")
	}
	for _, tmp := range tmps {
		_ = os.RemoveAll(tmp)
	}

	logger := log.WithFunc("pool.Reconcile")
	m.reclaimOrphanArchiveCks(ctx, claims)
	m.retryArchiveDeletes(ctx)
	removed := m.sweepStaleVMs(ctx, live, owned)

	// an unreferenced hibernate snapshot is an orphan; fork and golden snapshots never span a restart
	if snapsErr != nil {
		logger.Warnf(ctx, "snapshot sweep skipped: %v", snapsErr)
	} else {
		var orphans []string
		for _, snap := range snaps {
			orphanHib := strings.HasPrefix(snap, hibernatePrefix) && !referenced[snap]
			if orphanHib || strings.HasPrefix(snap, forkPrefix) || strings.HasPrefix(snap, goldenPrefix) {
				orphans = append(orphans, snap)
			}
		}
		m.runBounded(ctx, len(orphans), func(ctx context.Context, i int) {
			m.dropSnap(ctx, orphans[i])
			logger.Infof(ctx, "removed orphan snapshot %s", orphans[i])
		}).Wait()
	}
	m.resyncEgress(ctx, live, removed)
	logger.Infof(ctx, "adopted %d claims, %d VMs live", len(m.claimed), len(live))
	return saveErr
}

// sweepStaleVMs removes sbx-prefixed VMs no claim owns and returns the ones confirmed gone.
func (m *Manager) sweepStaleVMs(ctx context.Context, live map[string]types.VMRecord, owned map[string]bool) map[string]bool {
	logger := log.WithFunc("pool.sweepStaleVMs")
	var stale []string
	for name := range live {
		if strings.HasPrefix(name, vmPrefix) && !owned[name] {
			stale = append(stale, name)
		}
	}
	gone := make([]bool, len(stale)) // distinct indices: no lock under the Wait barrier
	m.runBounded(ctx, len(stale), func(ctx context.Context, i int) {
		if m.removeStaleVM(ctx, stale[i], live[stale[i]]) {
			gone[i] = true
			logger.Infof(ctx, "removed stale VM %s", stale[i])
		}
	}).Wait()
	removed := make(map[string]bool, len(stale))
	for i, ok := range gone {
		if ok {
			removed[stale[i]] = true
		}
	}
	return removed
}

// removeStaleVM reclaims one unowned VM, reporting whether it is gone.
func (m *Manager) removeStaleVM(ctx context.Context, name string, rec types.VMRecord) bool {
	logger := log.WithFunc("pool.removeStaleVM")
	if rec.State == vmStateCreating {
		// a canceled ctx must not skip the busy check guarding the forced remove
		switch outcome, err := m.eng.ReconcileStaleCreate(context.WithoutCancel(ctx), name); {
		case err != nil:
			// Verb missing (cocoon < v0.5.8) or failed: keep the old sweep.
			logger.Warnf(ctx, "reconcile stale create %s: %v; removing", name, err)
		case outcome == engine.StaleCreateCollected, outcome == engine.StaleCreateNotFound:
			return true
		case outcome == engine.StaleCreateBusy:
			logger.Infof(ctx, "stale create %s has an in-flight owner; queued for retry", name)
			m.queueStaleCreate(name, rec.TapDevice())
			return false
		}
		// not-creating: the record moved on under the lock; remove normally.
	}
	return m.removeOrRetry(ctx, name, "", rec.TapDevice(), volumeTeardown{})
}

// resyncEgress re-locks adopted egress claims, quarantining any it cannot lock.
func (m *Manager) resyncEgress(ctx context.Context, live map[string]types.VMRecord, removed map[string]bool) {
	logger := log.WithFunc("pool.resyncEgress")
	now := time.Now()
	var locked map[string]bool
	var lockedErr error
	if m.lockEgress {
		locked, lockedErr = netfilter.LockedTaps()
	}
	var quarantine []*types.Sandbox
	for _, sb := range m.claimed {
		sb.TouchAt(now)
		if m.lockEgress && sb.Key.Net == types.NetEgress {
			tap := m.readoptEgressTap(sb, live)
			if tap == "" {
				logger.Errorf(ctx, errNoEgressTap, "egress claim %s has no lockable tap; quarantining", sb.ID)
				quarantine = append(quarantine, sb)
				continue
			}
			err := lockedErr
			if err == nil && !locked[tap] {
				err = netfilter.Lock(tap)
			}
			if err != nil {
				logger.Errorf(ctx, err, "ensure egress lock %s; quarantining", sb.ID)
				quarantine = append(quarantine, sb)
				continue
			}
		}
		if proxyErr := m.armEgressProxy(ctx, sb); proxyErr != nil {
			logger.Errorf(ctx, proxyErr, "arm egress proxy %s", sb.ID)
		}
	}
	// Quarantine before the sweep so a failed remove's still-running tap is kept.
	keep := make(map[string]bool, len(live))
	for _, sb := range quarantine {
		if m.quarantineClaim(ctx, sb) {
			removed[sb.VMName] = true
		} else if sb.TAP != "" {
			keep[sb.TAP] = true
		}
	}
	// Keep every still-running VM's tap; sweep only a confirmed-gone VM's table.
	for name, rec := range live {
		if tap := rec.TapDevice(); tap != "" && !removed[name] {
			keep[tap] = true
		}
	}
	if sweepErr := m.sweep(keep); sweepErr != nil {
		logger.Warnf(ctx, "sweep orphan egress tables: %v", sweepErr)
	}
}

// A failed remove stays out of service and queued until teardown succeeds.
func (m *Manager) quarantineClaim(ctx context.Context, sb *types.Sandbox) bool {
	td := m.quiesceVolumes(ctx, sb)
	gone := m.removeOrRetry(ctx, sb.VMName, sb.ID, "", td)
	m.mu.Lock()
	delete(m.claimed, sb.ID)
	m.tenantDelta(sb.Tenant, -1)
	js := m.store.del(sb.ID)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.recommit(ctx, js)
	}
	return gone
}

// readoptEgressTap records and returns a live egress claim's tap, "" when there is none.
func (m *Manager) readoptEgressTap(sb *types.Sandbox, live map[string]types.VMRecord) string {
	if sb.Key.Net != types.NetEgress {
		return ""
	}
	rec, ok := live[sb.VMName]
	tap := rec.TapDevice()
	if !ok || tap == "" {
		return ""
	}
	m.mu.Lock()
	m.egressTaps[sb.ID] = tap
	sb.TAP = tap
	m.store.set(sb)
	m.mu.Unlock()
	return tap
}

// reclaimOrphanArchiveCks reaps archives that crashed between publish and journal commit.
func (m *Manager) reclaimOrphanArchiveCks(ctx context.Context, claims map[string]*types.Sandbox) {
	logger := log.WithFunc("pool.reclaimOrphanArchiveCks")
	metas, err := m.ckpts.Metas(ctx)
	if err != nil {
		logger.Warnf(ctx, "list archive cks skipped: %v", err)
		return
	}
	for _, raw := range metas {
		var ckpt types.Checkpoint
		if json.Unmarshal(raw, &ckpt) != nil || !ckpt.Archive {
			continue
		}
		orig, mine := claims[ckpt.SandboxID]
		if !mine || orig.ArchiveCk == ckpt.ID {
			continue
		}
		if err := m.deleteArchiveCk(ctx, ckpt.ID); err != nil {
			logger.Warnf(ctx, "reclaim %s: %v", ckpt.ID, err)
		} else {
			logger.Infof(ctx, "reclaimed orphan archive ck %s", ckpt.ID)
		}
	}
}
