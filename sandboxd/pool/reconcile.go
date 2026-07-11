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

// sweepExcept is a seam so a test can observe the keep set (netfilter is a
// no-op off Linux and an unmockable netlink call on it).
var sweepExcept = netfilter.SweepExcept

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
	removed := m.sweepStaleVMs(ctx, live, owned)

	// A hibernate snapshot no adopted claim references is an orphan;
	// fork/golden-build snapshots never span a restart. A list failure only
	// skips the sweep — GC must not brick startup.
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

// sweepStaleVMs removes sbx-prefixed VMs no claim owns and returns the ones
// confirmed gone; a failed remove is omitted so the egress sweep keeps its lock.
func (m *Manager) sweepStaleVMs(ctx context.Context, live map[string]types.VMRecord, owned map[string]bool) map[string]bool {
	logger := log.WithFunc("pool.Reconcile")
	var stale []string
	for name := range live {
		if strings.HasPrefix(name, vmPrefix) && !owned[name] {
			stale = append(stale, name)
		}
	}
	gone := make([]bool, len(stale)) // distinct indices: no lock under the Wait barrier
	m.runBounded(ctx, len(stale), func(ctx context.Context, i int) {
		if m.removeVM(ctx, stale[i]) {
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

// resyncEgress re-locks adopted egress claims after a restart, quarantines any
// it cannot lock, and sweeps tables orphaned by VMs confirmed gone (in removed).
func (m *Manager) resyncEgress(ctx context.Context, live map[string]types.VMRecord, removed map[string]bool) {
	logger := log.WithFunc("pool.resyncEgress")
	now := time.Now()
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
			if err := netfilter.EnsureLock(tap); err != nil {
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
	if sweepErr := sweepExcept(keep); sweepErr != nil {
		logger.Warnf(ctx, "sweep orphan egress tables: %v", sweepErr)
	}
}

// quarantineClaim removes a claim whose egress NIC could not be locked and drops
// it from service; it returns whether the VM is confirmed gone (a failed remove
// leaves it for the next restart's stale sweep). A never-applied lock plus a
// failed remove leaves the VM unguarded until a later remove succeeds.
func (m *Manager) quarantineClaim(ctx context.Context, sb *types.Sandbox) bool {
	gone := m.removeVM(ctx, sb.VMName)
	m.mu.Lock()
	delete(m.claimed, sb.ID)
	m.tenantDelta(sb.Tenant, -1)
	js := m.store.snapshot(m.claimed)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.recommit(ctx, js)
	}
	return gone
}

// readoptEgressTap records a live egress-lane claim's tap for lifecycle after a
// restart and returns it; "" for the none lane or a VM whose tap is not listed.
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
