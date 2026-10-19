package pool

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

// Reconcile aligns claims and VMs after a restart; it runs before the server because it swaps in fresh records that would bypass in-flight Transition locks.
func (m *Manager) Reconcile(ctx context.Context) error {
	claims, err := m.store.load()
	if err != nil {
		return err
	}
	fenced := m.fenceArchived(ctx, claims)
	vms, err := m.eng.List(ctx)
	if err != nil {
		return fmt.Errorf("list vms: %w", err)
	}
	live := make(map[string]types.VMRecord, len(vms))
	for _, vm := range vms {
		live[vm.Config.Name] = vm
	}
	snaps, snapsErr := m.eng.SnapshotList(ctx)
	imageIDs := m.imageIDs(ctx)

	if m.managed != nil {
		m.store.reset(claims)
	}
	owned, err := m.recoverManaged(ctx, claims, live)
	if err != nil {
		return err
	}
	if err := m.recoverManagedOrphans(ctx, claims, live, owned); err != nil {
		return err
	}
	referenced := map[string]bool{}
	var adopted []*types.Sandbox
	m.mu.Lock()
	for _, sb := range claims {
		sb.SetEnv(sb.Env)
		rec, ok := live[sb.VMName]
		switch {
		case sb.ArchiveCk != "":
			// archived: no local VM by design, so adopt the stub and let the first exec wake it
			m.addClaimLocked(sb)
			continue
		case ok && rec.State == vmStateRunning:
			sb.VsockSocket = rec.VsockSocket
			// running with either flag set means a crashed wake or an intent the engine never saw
			sb.HibernateSnap, sb.PendingSnap = "", ""
		case ok && sb.HibernateSnap != "":
			// hibernated: the VM is stopped by design and wakes on demand.
			sb.PendingSnap = ""
		case ok && sb.PendingSnap != "" && (snapsErr != nil || slices.Contains(snaps, sb.PendingSnap)):
			// the journaled intent names the wake image, so adopt it when the snapshot is there
			sb.HibernateSnap, sb.PendingSnap, sb.ReaderSafe = sb.PendingSnap, "", false
			adopted = append(adopted, sb)
		case ok:
			// the VMM died with the host or on its own; the liveness pass restarts or fails it
			sb.PendingSnap = ""
		default:
			continue
		}
		m.addClaimLocked(sb)
		if _, managed := m.managedHold(sb.ID); !managed {
			m.adoptVolumes(heldVolumes(sb))
		}
		owned[sb.VMName] = true
		referenced[sb.HibernateSnap] = true
	}
	saveErr := m.store.save(m.claimed)
	pooled := make(map[string]bool, len(m.pools))
	for _, p := range m.pools {
		m.adoptGolden(p, imageIDs[p.key.Template])
		pooled[p.hash] = true
	}
	m.mu.Unlock()
	for _, sb := range adopted {
		m.recordHibernate(ctx, sb)
	}
	logger := log.WithFunc("pool.Reconcile")
	if err := m.ckpts.SweepStaging(); err != nil {
		logger.Error(ctx, err, "sweep checkpoint staging")
	}
	if err := m.tpls.SweepStaging(); err != nil {
		logger.Error(ctx, err, "sweep template staging")
	}
	dir := m.goldensDir()
	goldens, _ := os.ReadDir(dir)
	for _, e := range goldens {
		name := e.Name()
		if pooled[strings.TrimSuffix(name, goldenStampSuffix)] {
			continue
		}
		if os.RemoveAll(filepath.Join(dir, name)) == nil && filepath.Ext(name) == "" {
			logger.Infof(ctx, "removed orphan golden %s", name)
		}
	}
	// a crash mid-export leaves a fork-* staging dir nothing in this life reuses
	_ = utils.RemoveDirEntries(m.dataDir, func(name string) bool { return strings.HasPrefix(name, "fork-") })
	_ = utils.RemoveDirEntries(filepath.Join(m.dataDir, readerDir), nil)

	if fenced {
		m.reclaimOrphanArchiveCks(ctx, claims)
	}
	m.retryArchiveDeletes(ctx)
	removed := m.sweepStaleVMs(ctx, live, owned)

	// an unreferenced hibernate snapshot is an orphan; fork and golden snapshots never span a restart
	if snapsErr != nil {
		logger.Warnf(ctx, "snapshot sweep skipped: %v", snapsErr)
	} else {
		orphans := slices.DeleteFunc(snaps, func(snap string) bool {
			if strings.HasPrefix(snap, hibernatePrefix) {
				return referenced[snap]
			}
			return !strings.HasPrefix(snap, forkPrefix) && !strings.HasPrefix(snap, goldenPrefix)
		})
		m.runBounded(ctx, len(orphans), func(ctx context.Context, i int) {
			m.dropSnap(ctx, orphans[i])
			logger.Infof(ctx, "removed orphan snapshot %s", orphans[i])
		}).Wait()
	}
	m.resyncEgress(ctx, live, removed)
	m.warnDeniedClaims(ctx)
	logger.Infof(ctx, "adopted %d claims, %d VMs live", len(m.claimed), len(live))
	return saveErr
}

// sweepStaleVMs removes sbx-prefixed VMs no claim owns and returns the ones confirmed gone.
func (m *Manager) sweepStaleVMs(ctx context.Context, live map[string]types.VMRecord, owned map[string]bool) map[string]bool {
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
			log.WithFunc("pool.sweepStaleVMs").Infof(ctx, "removed stale VM %s", stale[i])
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
			// verb missing (cocoon < v0.5.8) or failed: keep the old sweep.
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
	if m.removeVM(ctx, name) {
		return true
	}
	m.queueRemoval(name, "", rec.TapDevice(), volumeTeardown{holds: m.holdStaleVolumes(ctx, name, rec)}, time.Time{})
	return false
}

func (m *Manager) holdStaleVolumes(ctx context.Context, name string, rec types.VMRecord) []types.Volume {
	if rec.State != vmStateRunning {
		return nil
	}
	inspected, _, err := m.eng.Inspect(ctx, name)
	if err != nil {
		log.WithFunc("pool.holdStaleVolumes").Warnf(ctx, "inspect stale vm %s, its volumes stay unheld: %v", name, err)
		return nil
	}
	holds := make([]types.Volume, len(inspected.AttachedDevices.Disks))
	for i, disk := range inspected.AttachedDevices.Disks {
		holds[i].Name = disk.Name
		if !disk.ReadOnly {
			holds[i].Mode = types.VolumeModeRW
		}
	}
	m.mu.Lock()
	m.adoptVolumes(holds)
	m.mu.Unlock()
	return holds
}

// resyncEgress re-locks adopted egress claims, quarantining any it cannot lock.
func (m *Manager) resyncEgress(ctx context.Context, live map[string]types.VMRecord, removed map[string]bool) {
	logger := log.WithFunc("pool.resyncEgress")
	now := time.Now()
	locked, lockedErr := m.out.LockedTaps()
	var quarantine []*types.Sandbox
	for _, sb := range m.claimed {
		sb.TouchAt(now)
		// a hibernated or archived claim has no guest to serve; its wake arms the door
		if sb.HibernateSnap != "" || sb.ArchiveCk != "" || sb.Failed != "" {
			continue
		}
		if m.out.LocksNIC(sb.Key) {
			tap := m.readoptEgressTap(sb, live)
			// a down egress-lane VM has no guest to lock or mark; the liveness pass fails it
			if live[sb.VMName].State != vmStateRunning {
				continue
			}
			if tap == "" {
				logger.Errorf(ctx, errNoEgressTap, "egress claim %s has no lockable tap; quarantining", sb.ID)
				quarantine = append(quarantine, sb)
				continue
			}
			err := lockedErr
			if err == nil && !locked[tap] {
				err = m.out.Relock(ctx, sb, tap)
			}
			if err != nil {
				logger.Errorf(ctx, err, "ensure egress lock %s; quarantining", sb.ID)
				quarantine = append(quarantine, sb)
				continue
			}
		}
		if proxyErr := m.out.ArmProxy(ctx, sb); proxyErr != nil {
			logger.Errorf(ctx, proxyErr, "arm egress proxy %s", sb.ID)
		}
	}
	// quarantine before the sweep so a failed remove's still-running tap is kept.
	keep := make(map[string]bool, len(live))
	for _, sb := range quarantine {
		if m.quarantineClaim(ctx, sb) {
			removed[sb.VMName] = true
		} else if sb.TAP != "" {
			keep[sb.TAP] = true
		}
	}
	// keep every still-running VM's tap; sweep only a confirmed-gone VM's table.
	for name, rec := range live {
		if tap := rec.TapDevice(); tap != "" && !removed[name] {
			keep[tap] = true
		}
	}
	if sweepErr := m.out.Sweep(keep); sweepErr != nil {
		logger.Warnf(ctx, "sweep orphan egress tables: %v", sweepErr)
	}
}

const egressQuarantineFailure = "sandbox quarantined after egress recovery failure"

// quarantineClaim parks a claim whose teardown failed: out of service and queued until a remove succeeds.
func (m *Manager) quarantineClaim(ctx context.Context, sb *types.Sandbox) bool {
	if m.retireManaged(sb.ID) != nil {
		m.mu.Lock()
		sb.Failed = egressQuarantineFailure
		m.cancelVolumeRun(sb)
		js := m.store.set(sb)
		m.mu.Unlock()
		m.store.recommit(ctx, js)
		td := m.quiesceVolumes(ctx, sb)
		m.removeOrRetry(ctx, sb.VMName, sb.ID, td)
		return false
	}
	td := m.quiesceVolumes(ctx, sb)
	gone := m.removeOrRetry(ctx, sb.VMName, sb.ID, td)
	m.forgetClaim(ctx, sb)
	return gone
}

// readoptEgressTap records and returns a live egress claim's tap, "" when there is none.
func (m *Manager) readoptEgressTap(sb *types.Sandbox, live map[string]types.VMRecord) string {
	rec, ok := live[sb.VMName]
	tap := rec.TapDevice()
	if !ok || tap == "" {
		return ""
	}
	m.out.KeepLock(sb.ID, tap)
	m.mu.Lock()
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
	var orphans []types.Checkpoint
	var ids []string
	for _, rec := range metas {
		var ckpt types.Checkpoint
		if json.Unmarshal(rec.Meta, &ckpt) != nil || !ckpt.Archive {
			continue
		}
		orig, mine := claims[ckpt.SandboxID]
		if !mine || ckpt.Node != m.nodeID || orig.ArchiveCk == ckpt.ID || orig.ReplicaCk == ckpt.ID || orig.ReplicaPending == ckpt.ID || orig.ArchivePending == ckpt.ID {
			continue
		}
		orphans = append(orphans, ckpt)
		ids = append(ids, ckpt.SandboxID)
	}
	var rows map[string]archives.Record
	if m.archives != nil && len(orphans) > 0 {
		if rows, err = m.archives.Rows(ctx, ids); err != nil {
			logger.Warnf(ctx, "reclaim orphan archive cks skipped: %v", err)
			return
		}
	}
	for _, ckpt := range orphans {
		if row, ok := rows[ckpt.SandboxID]; ok && (row.Owner != m.nodeID || row.Checkpoint == ckpt.ID) {
			continue
		}
		if err := m.deleteArchiveCk(ctx, ckpt.ID); err != nil {
			logger.Warnf(ctx, "reclaim %s: %v", ckpt.ID, err)
		} else {
			logger.Infof(ctx, "reclaimed orphan archive ck %s", ckpt.ID)
		}
	}
}
