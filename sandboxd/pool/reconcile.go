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

	"github.com/cocoonstack/sandbox/sandboxd/netfilter"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// Reconcile aligns state after a daemon restart: re-adopt persisted claims
// whose VMs are still running (or hibernated), drop the rest, and remove any
// sbx-prefixed VM nobody owns (stale pool VMs and golden builders from a
// previous life). It must run once at startup, before the server: it swaps
// in fresh records, which would bypass in-flight Transition locks.
func (m *Manager) Reconcile(ctx context.Context) error {
	claims, err := m.store.load()
	if err != nil {
		return err
	}
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
			// Archived: no local VM by design; the store ck is the durable
			// state. Adopt the stub so the id/token survive restart and the
			// first exec wakes it (wakeArchived).
			m.claimed[id] = sb
			m.tenantDelta(sb.Tenant, 1)
			continue
		case ok && rec.State == vmStateRunning:
			sb.VsockSocket = rec.VsockSocket
			// Running with either flag set = a wake crashed between restore
			// and commit, or a hibernate intent never reached the engine;
			// clearing them un-bricks the claim and unreferences the
			// snapshot for the sweep below.
			sb.HibernateSnap, sb.PendingSnap = "", ""
		case ok && sb.HibernateSnap != "":
			// Hibernated: the VM is stopped by design and wakes on demand.
			sb.PendingSnap = ""
		case ok && sb.PendingSnap != "" && (snapsErr != nil || slices.Contains(snaps, sb.PendingSnap)):
			// Stopped under a journaled hibernate intent whose commit never
			// landed: the intent names the wake image, adopt it. A verified-
			// missing image means the hibernate never completed — fall
			// through and drop; an unverifiable list adopts (a failed wake
			// beats a destroyed claim).
			sb.HibernateSnap, sb.PendingSnap = sb.PendingSnap, ""
		default:
			continue
		}
		m.claimed[id] = sb
		m.tenantDelta(sb.Tenant, 1)
		owned[sb.VMName] = true
		referenced[sb.HibernateSnap] = true
	}
	saveErr := m.store.save(m.claimed)
	for _, p := range m.pools {
		dir := filepath.Join(m.goldensDir(), p.key.Hash())
		if dirExists(dir) {
			p.goldenDir = dir
		}
	}
	m.mu.Unlock()
	// A crash mid-export leaves a *.tmp staging dir no build or promote of
	// this life will reuse.
	tmps, _ := filepath.Glob(filepath.Join(m.goldensDir(), "*.tmp"))
	if err := m.ckpts.SweepStaging(); err != nil {
		log.WithFunc("pool.Reconcile").Error(ctx, err, "sweep checkpoint staging")
	}
	if err := m.tpls.SweepStaging(); err != nil {
		log.WithFunc("pool.Reconcile").Error(ctx, err, "sweep template staging")
	}
	m.migrateLegacyTemplates(ctx)
	for _, tmp := range tmps {
		_ = os.RemoveAll(tmp)
	}

	logger := log.WithFunc("pool.Reconcile")
	m.reclaimOrphanArchiveCks(ctx, claims)
	var stale []string
	for name := range live {
		if strings.HasPrefix(name, vmPrefix) && !owned[name] {
			stale = append(stale, name)
		}
	}
	m.runBounded(ctx, len(stale), func(ctx context.Context, i int) {
		m.destroy(ctx, stale[i])
		logger.Infof(ctx, "removed stale VM %s", stale[i])
	}).Wait()

	// Snapshot sweep, symmetric to the VM sweep: a hibernate snapshot no
	// adopted claim references is an orphan, and fork/golden-build snapshots
	// are transient by construction — none can span a restart. A list failure
	// only skips the sweep: GC must not brick startup.
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
	m.resyncEgress(ctx, live)
	logger.Infof(ctx, "adopted %d claims, %d VMs live", len(m.claimed), len(live))
	return saveErr
}

// resyncEgress re-establishes egress state after a restart, then sweeps tables
// orphaned by VMs that vanished while the daemon was down. A live VM kept its
// nft lock across the restart (the table lives in the root netns, not in this
// process), so the tap is only re-recorded — never re-locked — to avoid a window
// where a running guest is briefly unlocked; only the proxy listener is rebound.
func (m *Manager) resyncEgress(ctx context.Context, live map[string]types.VMRecord) {
	logger := log.WithFunc("pool.resyncEgress")
	now := time.Now()
	lockedTaps := map[string]bool{}
	for _, sb := range m.claimed {
		sb.TouchAt(now)
		// Gate on guardedEgress like every other lock site: with guarding off the
		// claim path leaves the egress NIC free, so a restart must not lock it.
		if !m.guardedEgress {
			continue
		}
		if tap := m.readoptEgressTap(sb, live); tap != "" {
			lockedTaps[tap] = true
			if err := netfilter.EnsureLock(tap); err != nil { // re-apply only if the table did not survive
				logger.Errorf(ctx, err, "ensure egress lock %s", sb.ID)
			}
		}
		if proxyErr := m.armEgressProxy(ctx, sb); proxyErr != nil {
			logger.Errorf(ctx, proxyErr, "arm egress proxy %s", sb.ID)
		}
	}
	if sweepErr := netfilter.SweepExcept(lockedTaps); sweepErr != nil {
		logger.Warnf(ctx, "sweep orphan egress tables: %v", sweepErr)
	}
}

// readoptEgressTap records a live egress-lane claim's tap for lifecycle after a
// restart and returns it; "" for the none lane or a VM whose tap is not listed.
func (m *Manager) readoptEgressTap(sb *types.Sandbox, live map[string]types.VMRecord) string {
	if sb.Key.Net != types.NetEgress {
		return ""
	}
	rec, ok := live[sb.VMName]
	if !ok || len(rec.NetworkConfigs) == 0 || rec.NetworkConfigs[0].TAP == "" {
		return ""
	}
	tap := rec.NetworkConfigs[0].TAP
	m.mu.Lock()
	m.egressTaps[sb.ID] = tap
	m.mu.Unlock()
	return tap
}

// reclaimOrphanArchiveCks reaps archives that crashed between their
// checkpoint publish and the journal commit: the flag hides them from
// listings and deletes, so only the journal identifies ours — the sandbox is
// in it under a different (or no) ArchiveCk.
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
		if err := m.deleteCkLocked(ctx, ckpt.ID); err != nil {
			logger.Warnf(ctx, "reclaim %s: %v", ckpt.ID, err)
		} else {
			logger.Infof(ctx, "reclaimed orphan archive ck %s", ckpt.ID)
		}
	}
}
