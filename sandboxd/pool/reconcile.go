package pool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

// Reconcile aligns claims and VMs after a restart; it runs before the server because it swaps in fresh records that would bypass in-flight Transition locks.
func (m *Manager) Reconcile(ctx context.Context) error {
	claims, err := m.store.load()
	if err != nil {
		return err
	}
	answered := m.fenceArchived(ctx, claims)
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

	owned := map[string]bool{}
	referenced := map[string]bool{}
	var adopted, dropped, resumed, interrupted []*types.Sandbox
	m.mu.Lock()
	for _, sb := range claims {
		sb.SetEnv(sb.Env)
		m.forgetHostEnvLocked(sb)
		m.forgetBalloonLocked(sb)
		rec, ok := live[sb.VMName]
		if !ok || rec.State != vmStateRunning {
			sb.SaveSnap = ""
		}
		switch {
		case sb.ArchiveCk != "":
			// Archived: no local VM by design, so adopt the stub and let the first exec wake it
			m.addClaimLocked(sb)
			continue
		case ok && rec.State == vmStateRunning:
			sb.VsockSocket = rec.VsockSocket
			if sb.HibernateSnap != "" {
				resumed = append(resumed, sb)
			}
			sb.HibernateSnap = ""
			if sb.PendingSnap != "" || sb.SaveSnap != "" {
				referenced[sb.PendingSnap] = true
				interrupted = append(interrupted, sb)
			}
		case ok && sb.HibernateSnap != "":
			// Hibernated: the VM is stopped by design and wakes on demand.
			sb.PendingSnap = ""
		case ok && sb.PendingSnap != "" && snapsErr != nil:
			referenced[sb.PendingSnap] = true
			interrupted = append(interrupted, sb)
		case ok && sb.PendingSnap != "" && slices.Contains(snaps, sb.PendingSnap):
			// The journaled intent names the wake image, so adopt it when the snapshot is there
			sb.HibernateSnap, sb.PendingSnap, sb.ReaderSafe = sb.PendingSnap, "", false
			adopted = append(adopted, sb)
		case ok:
			// The VMM died with the host or on its own; the liveness pass restarts or fails it
			sb.PendingSnap = ""
		default:
			dropped = append(dropped, sb)
			continue
		}
		m.addClaimLocked(sb)
		m.catalog.adoptVolumes(heldVolumes(sb))
		owned[sb.VMName] = true
		referenced[sb.HibernateSnap] = true
	}
	outs, ins := m.pendingMovesLocked()
	saveErr := m.store.save(m.claimed)
	pooled := make(map[string]bool, len(m.pools))
	for _, p := range m.pools {
		m.adoptGolden(p, imageIDs[p.key.Template])
		pooled[p.hash] = true
	}
	m.mu.Unlock()
	m.seedTun()
	for _, sb := range adopted {
		m.recordHibernate(ctx, sb)
	}
	// A failed save keeps the records, so the next boot records them
	if saveErr == nil {
		m.recordReconciled(ctx, resumed, dropped, answered)
	}
	logger := log.WithFunc("pool.Reconcile")
	if err := m.ckpts.SweepStaging(); err != nil {
		logger.Error(ctx, err, "sweep checkpoint staging")
	}
	if err := m.tpls.SweepStaging(); err != nil {
		logger.Error(ctx, err, "sweep template staging")
	}
	m.sweepOrphanGoldens(ctx, pooled)
	// A crash mid-export leaves a fork-* staging dir nothing in this life reuses
	_ = utils.RemoveDirEntries(m.dataDir, func(name string) bool { return strings.HasPrefix(name, "fork-") })
	_ = utils.RemoveDirEntries(filepath.Join(m.dataDir, readerDir), nil)
	_ = utils.RemoveDirEntries(filepath.Join(m.dataDir, movesDir), func(id string) bool { return m.claimed[id] == nil || !m.claimed[id].MoveIn })

	if answered {
		m.reclaimOrphanArchiveCks(ctx, claims)
	}
	m.retryArchiveDeletes(ctx)
	removed := m.sweepStaleVMs(ctx, live, owned)

	m.sweepOrphanSnaps(ctx, snaps, snapsErr, referenced)
	m.resyncEgress(ctx, live, removed)
	m.warnDeniedClaims(ctx)
	logger.Infof(ctx, "adopted %d claims, %d VMs live", len(m.claimed), len(live))
	m.resumeMoves(context.WithoutCancel(ctx), outs, ins)
	for _, sb := range interrupted {
		sb.Transition.Lock()
		go m.settleInterrupted(ctx, sb)
	}
	return saveErr
}

func (m *Manager) sweepOrphanGoldens(ctx context.Context, pooled map[string]bool) {
	logger := log.WithFunc("pool.sweepOrphanGoldens")
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
}

func (m *Manager) sweepOrphanSnaps(ctx context.Context, snaps []string, snapsErr error, referenced map[string]bool) {
	logger := log.WithFunc("pool.sweepOrphanSnaps")
	// An unreferenced hibernate snapshot is an orphan; fork and golden snapshots never span a restart
	if snapsErr != nil {
		logger.Warnf(ctx, "snapshot sweep skipped: %v", snapsErr)
		return
	}
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

// pendingMovesLocked marks the journaled moves of loaded claims: a source keeps its claim gated until the record resolves; callers hold m.mu.
func (m *Manager) pendingMovesLocked() (map[*types.Sandbox]*outMove, []*types.Sandbox) {
	if m.moves == nil {
		return nil, nil
	}
	outs := map[*types.Sandbox]*outMove{}
	var ins []*types.Sandbox
	for id, sb := range m.claimed {
		switch {
		case sb.MoveOut != nil:
			mv := &outMove{MoveOut: *sb.MoveOut, final: true}
			m.moving[id], outs[sb] = mv, mv
		case sb.MoveIn:
			ins = append(ins, sb)
		}
	}
	return outs, ins
}

// resumeMoves resolves each journaled move through its record in the background, retrying while the registry is down.
func (m *Manager) resumeMoves(ctx context.Context, outs map[*types.Sandbox]*outMove, ins []*types.Sandbox) {
	for sb, mv := range outs {
		go func() {
			sb.Transition.Lock()
			mv.locked = true
			m.retryMoveOut(ctx, sb, mv)
		}()
	}
	for _, sb := range ins {
		go m.settleMoveIn(ctx, sb)
	}
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

func (m *Manager) removeStaleVM(ctx context.Context, name string, rec types.VMRecord) bool {
	logger := log.WithFunc("pool.removeStaleVM")
	if rec.State == vmStateCreating {
		// A canceled ctx must not skip the busy check guarding the forced remove
		switch outcome, err := m.eng.ReconcileStaleCreate(context.WithoutCancel(ctx), name); {
		case err != nil:
			logger.Warnf(ctx, "reconcile stale create %s: %v; removing", name, err)
		case outcome == engine.StaleCreateCollected, outcome == engine.StaleCreateNotFound:
			return true
		case outcome == engine.StaleCreateBusy:
			logger.Infof(ctx, "stale create %s has an in-flight owner; queued for retry", name)
			m.queueStaleCreate(name, rec.TapDevice())
			return false
		}
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
	m.catalog.adoptVolumes(holds)
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
		sb.TouchAt(seenAt(sb, now))
		// A hibernated or archived claim has no guest to serve; its wake arms the door
		if sb.HibernateSnap != "" || sb.ArchiveCk != "" || sb.Failed != "" || sb.MoveIn {
			continue
		}
		if m.out.LocksNIC(sb.Key) {
			tap := m.readoptEgressTap(sb, live)
			// A down egress-lane VM has no guest to lock or mark; the liveness pass fails it
			if live[sb.VMName].State != vmStateRunning {
				continue
			}
			if tap == "" {
				logger.Errorf(ctx, errNoEgressTap, "egress claim %s has no lockable tap; quarantining", sb.ID)
				quarantine = append(quarantine, sb)
				continue
			}
			var err error
			if lockedErr != nil || !locked[tap] {
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
	// Quarantine before the sweep so a failed remove's still-running tap is kept.
	keep := make(map[string]bool, len(live))
	for _, sb := range quarantine {
		if m.quarantineClaim(ctx, sb) {
			removed[sb.VMName] = true
		} else if sb.TAP != "" {
			keep[sb.TAP] = true
		}
	}
	for name, rec := range live {
		if tap := rec.TapDevice(); tap != "" && !removed[name] {
			keep[tap] = true
		}
	}
	if sweepErr := m.out.Sweep(keep); sweepErr != nil {
		logger.Warnf(ctx, "sweep orphan egress tables: %v", sweepErr)
	}
}

// quarantineClaim parks a claim whose teardown failed: out of service and queued until a remove succeeds.
func (m *Manager) quarantineClaim(ctx context.Context, sb *types.Sandbox) bool {
	td := m.quiesceVolumes(ctx, sb)
	gone := m.removeOrRetry(ctx, sb.VMName, sb.ID, td)
	m.forgetClaim(ctx, sb)
	m.releaseTun(ctx, sb.ID, sb.TunAddr)
	m.recordEnd(ctx, sb, "quarantined")
	return gone
}

func (m *Manager) recordReconciled(ctx context.Context, resumed, dropped []*types.Sandbox, answered bool) {
	for _, sb := range resumed {
		m.recordUsage(ctx, usage.Event{Event: "wake", ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant, Deadline: sb.Deadline})
	}
	for _, sb := range dropped {
		if sb.MoveIn || sb.MoveOut != nil {
			continue
		}
		if answered || !sb.HasReplica() {
			m.recordEnd(ctx, sb, "vm_gone")
		}
		if !sb.HasReplica() {
			m.releaseTun(ctx, sb.ID, sb.TunAddr)
		}
	}
}

func (m *Manager) recordEnd(ctx context.Context, sb *types.Sandbox, reason string) {
	m.recordUsage(ctx, usage.Event{Event: reapEvent, ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant, Deadline: sb.Deadline, Reference: reason})
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
	metas, err := m.ckpts.Metas(ctx, store.ArchivePrefix)
	if err != nil {
		logger.Warnf(ctx, "list archive cks skipped: %v", err)
		return
	}
	var orphans []types.Checkpoint
	var ids []string
	for _, rec := range metas {
		ckpt, parseErr := parseCheckpoint(rec.Meta)
		if parseErr != nil {
			continue
		}
		orig, mine := claims[ckpt.SandboxID]
		if !mine || ckpt.Node != m.nodeID {
			continue
		}
		if refs := checkpointRefs(orig); slices.Contains(refs[:], ckpt.ID) {
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
		if deleted, err := m.deleteArchiveCk(ctx, ckpt.ID); err != nil {
			logger.Warnf(ctx, "reclaim %s: %v", ckpt.ID, err)
		} else if deleted {
			logger.Infof(ctx, "reclaimed orphan archive ck %s", ckpt.ID)
		}
	}
}

func seenAt(sb *types.Sandbox, now time.Time) time.Time {
	if t := sb.SeenAtShutdown; !t.IsZero() && t.Before(now) {
		return t
	}
	return now
}
