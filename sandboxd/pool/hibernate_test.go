package pool

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// TestHibernatePersistFailureSurfacesAndConverges guards the durability fix
// where a hibernate whose journal write failed still reported success — a
// restart then dropped the claim and destroyed the VM and snapshot. With the
// intent write first, an unwritable journal aborts before the engine stops
// anything; a retry after the store heals runs the full transition.
func TestHibernatePersistFailureSurfacesAndConverges(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	broken := filepath.Join(t.TempDir(), "gone", "claims.json")
	m.store.path = broken

	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err == nil {
		t.Fatal("Hibernate reported success despite a persist failure")
	}
	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err == nil {
		t.Fatal("Hibernate retry reported success while the journal is unwritable")
	}
	if len(eng.hibernates) != 0 {
		t.Fatalf("engine hibernated %d times before the intent could persist, want 0", len(eng.hibernates))
	}
	if err := os.MkdirAll(filepath.Dir(broken), 0o750); err != nil {
		t.Fatalf("heal store dir: %v", err)
	}
	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Hibernate retry after heal: %v", err)
	}
	got, err := (&claimStore{path: broken}).load()
	if err != nil || got[sb.ID] == nil || got[sb.ID].HibernateSnap == "" || got[sb.ID].PendingSnap != "" {
		t.Fatalf("journal after healed retry: %+v, %v", got[sb.ID], err)
	}
}

// TestWakePersistFailureSurfaces is the wake side of the same fix: retries
// must surface it, the snapshot the lagging journal references must survive
// (a fast-path retry keeps serving the awake VM — a restart adopts running
// VMs, so the lag is harmless there), and it is reclaimed once a write lands.
func TestWakePersistFailureSurfaces(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	snap := eng.hibernates[0]
	broken := filepath.Join(t.TempDir(), "gone", "claims.json")
	m.store.path = broken

	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err == nil {
		t.Fatal("wake reported success despite a persist failure")
	}
	if eng.snapRemoved(snap) {
		t.Error("wake dropped the snapshot the lagging journal references")
	}
	if sock, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil || sock == "" {
		t.Fatalf("fast-path wake of the awake VM: %q, %v", sock, err)
	}
	if eng.snapRemoved(snap) {
		t.Error("fast path dropped the snapshot while the journal still lags")
	}
	if err := os.MkdirAll(filepath.Dir(broken), 0o750); err != nil {
		t.Fatalf("heal store dir: %v", err)
	}
	// The background recommit converges the journal; the next fast-path wake
	// then reclaims the parked snapshot.
	waitFor(t, func() bool {
		if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
			t.Fatalf("fast-path wake: %v", err)
		}
		return eng.snapRemoved(snap)
	})
	got, loadErr := (&claimStore{path: broken}).load()
	if loadErr != nil || got[sb.ID] == nil || got[sb.ID].HibernateSnap != "" {
		t.Fatalf("journal after converge: %+v, %v", got[sb.ID], loadErr)
	}
}

// TestFlushClaimsClosesShutdownWindow: the shutdown hook persists what the
// background recommit has not converged yet (here, a wake whose commit failed).
func TestFlushClaimsClosesShutdownWindow(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	broken := filepath.Join(t.TempDir(), "gone", "claims.json")
	m.store.path = broken

	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err == nil {
		t.Fatal("wake reported success despite a persist failure")
	}
	if err := os.MkdirAll(filepath.Dir(broken), 0o750); err != nil {
		t.Fatalf("heal store dir: %v", err)
	}
	if err := m.FlushClaims(); err != nil {
		t.Fatalf("FlushClaims: %v", err)
	}
	got, err := (&claimStore{path: broken}).load()
	if err != nil || got[sb.ID] == nil || got[sb.ID].HibernateSnap != "" {
		t.Fatalf("journal after flush: %+v, %v", got[sb.ID], err)
	}
}

func TestHibernateWakeCycle(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)

	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("repeat Hibernate: %v", err)
	}
	if len(eng.hibernates) != 1 {
		t.Errorf("engine hibernated %d times, want 1 (idempotent)", len(eng.hibernates))
	}
	if _, g := m.Info(); g.Hibernated != 1 {
		t.Errorf("hibernated count %d, want 1", g.Hibernated)
	}
	if got, _ := newClaimStore(m.dataDir).load(); got[sb.ID] == nil || got[sb.ID].HibernateSnap == "" {
		t.Error("hibernation flag not persisted")
	}
	hibSnap := eng.hibernates[0]
	if !strings.HasPrefix(hibSnap, hibernatePrefix) {
		t.Errorf("snapshot name %q, want %s prefix", hibSnap, hibernatePrefix)
	}

	sock, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatalf("WakeAgentSocket: %v", err)
	}
	if sock != sb.VsockSocket {
		t.Errorf("sock %q, want %q", sock, sb.VsockSocket)
	}
	if len(eng.restores) != 1 {
		t.Errorf("engine restored %d times, want 1", len(eng.restores))
	}
	waitFor(t, func() bool { return eng.snapRemoved(hibSnap) }) // wake drops it off the return path
	if _, g := m.Info(); g.Hibernated != 0 {
		t.Errorf("hibernated count %d after wake, want 0", g.Hibernated)
	}

	// Awake sandbox: the fast path must not touch the engine again.
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("fast-path WakeAgentSocket: %v", err)
	}
	if len(eng.restores) != 1 {
		t.Errorf("fast path restored again: %d", len(eng.restores))
	}
}

// TestWakeDropsSnapshotOffReturnPath: wake returns the socket without waiting
// for the consumed snapshot's (subprocess) removal.
func TestWakeDropsSnapshotOffReturnPath(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	eng.snapRemoveStall = make(chan struct{})

	sock, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatalf("wake: %v", err)
	}
	if sock == "" {
		t.Error("wake returned an empty socket")
	}
	if n := eng.snapRemoveCount(); n != 0 {
		t.Errorf("snapshot removed %d times before wake returned, want it off the return path", n)
	}
	close(eng.snapRemoveStall)
	waitFor(t, func() bool { return eng.snapRemoveCount() == 1 })
}

func TestWakeFailureKeepsHibernated(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}

	eng.restoreErr = errors.New("restore boom")
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err == nil {
		t.Fatal("WakeAgentSocket succeeded despite restore failure")
	}
	if _, g := m.Info(); g.Hibernated != 1 {
		t.Errorf("hibernated count %d, want 1 (wake failed, snapshot kept)", g.Hibernated)
	}
	if len(eng.snapRemoves) != 0 {
		t.Errorf("snapRemoves=%v, want none after failed wake", eng.snapRemoves)
	}
}

func TestReleaseHibernatedDropsSnapshot(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}

	if err := m.Release(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !slices.Contains(eng.removes, sb.VMName) {
		t.Errorf("removes=%v, want %s", eng.removes, sb.VMName)
	}
	if !slices.Contains(eng.snapRemoves, eng.hibernates[0]) {
		t.Errorf("snapRemoves=%v, want the hibernate snapshot", eng.snapRemoves)
	}
}

func TestReleaseMidHibernateDropsOrphanSnapshot(t *testing.T) {
	eng := newFakeEngine()
	eng.hibernateStall = make(chan struct{})
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)

	hibErr := make(chan error, 1)
	go func() { hibErr <- m.Hibernate(t.Context(), sb.ID, sb.Token) }()
	waitFor(t, func() bool {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		return len(eng.hibernates) == 1
	})

	// Release lands while the engine transition is in flight: the claim is
	// gone before Hibernate can commit, so its snapshot must not leak.
	if err := m.Release(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Release: %v", err)
	}
	close(eng.hibernateStall)
	if err := <-hibErr; !errors.Is(err, ErrUnknownSandbox) {
		t.Fatalf("Hibernate: %v, want ErrUnknownSandbox", err)
	}
	if !slices.Contains(eng.snapRemoves, eng.hibernates[0]) {
		t.Errorf("snapRemoves=%v, want the orphaned snapshot dropped", eng.snapRemoves)
	}
	if _, g := m.Info(); g.Hibernated != 0 {
		t.Errorf("hibernated count %d, want 0", g.Hibernated)
	}
}

// TestReconcileAdoptsJournaledIntent: a hibernate that stopped the VM but
// whose final commit never landed leaves a journaled intent (PendingSnap)
// over a stopped VM; reconcile adopts it as hibernated — no heuristics, the
// intent names the wake image.
func TestReconcileAdoptsJournaledIntent(t *testing.T) {
	eng := newFakeEngine()
	eng.vms["sbx-lagged-1"] = "/vsock/lagged"
	eng.stopped["sbx-lagged-1"] = true
	eng.snapshots = append(eng.snapshots, hibernatePrefix+"lagged-1-abc123")
	dataDir := t.TempDir()
	claims := map[string]*types.Sandbox{
		"sb_lag": {
			ID: "sb_lag", VMName: "sbx-lagged-1", Key: testKey, Token: "tok",
			VsockSocket: "/vsock/lagged", PendingSnap: hibernatePrefix + "lagged-1-abc123",
		},
	}
	if err := newClaimStore(dataDir).save(claims); err != nil {
		t.Fatalf("setup: %v", err)
	}
	m := newTestManagerAt(t, eng, dataDir)

	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, g := m.Info(); g.Hibernated != 1 {
		t.Fatalf("hibernated count %d after reconcile, want the intent adopted", g.Hibernated)
	}
	if eng.snapRemoved(hibernatePrefix + "lagged-1-abc123") {
		t.Error("reconcile swept the wake image it adopted")
	}
	if _, err := m.WakeAgentSocket(t.Context(), "sb_lag", "tok"); err != nil {
		t.Fatalf("wake adopted claim: %v", err)
	}
}

// TestReconcileIgnoresStaleOrphanSnapshot: a leftover wake image without a
// journaled intent must never be mistaken for a hibernate — adopting it
// would silently roll the sandbox back to pre-wake state. The claim drops,
// the orphan sweeps.
func TestReconcileIgnoresStaleOrphanSnapshot(t *testing.T) {
	eng := newFakeEngine()
	eng.vms["sbx-orphan-1"] = "/vsock/orphan"
	eng.stopped["sbx-orphan-1"] = true
	eng.snapshots = append(eng.snapshots, hibernatePrefix+"orphan-1-old111")
	dataDir := t.TempDir()
	claims := map[string]*types.Sandbox{
		"sb_orp": {ID: "sb_orp", VMName: "sbx-orphan-1", Key: testKey, Token: "tok", VsockSocket: "/vsock/orphan"},
	}
	if err := newClaimStore(dataDir).save(claims); err != nil {
		t.Fatalf("setup: %v", err)
	}
	m := newTestManagerAt(t, eng, dataDir)

	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, g := m.Info(); g.Claimed != 0 {
		t.Errorf("claimed %d, want the intent-less claim dropped", g.Claimed)
	}
	waitFor(t, func() bool { return eng.snapRemoved(hibernatePrefix + "orphan-1-old111") })
}

// TestReconcileDropsIntentWithoutImage: an intent whose wake image is
// verified missing means the hibernate never completed; the claim drops.
func TestReconcileDropsIntentWithoutImage(t *testing.T) {
	eng := newFakeEngine()
	eng.vms["sbx-noimg-1"] = "/vsock/noimg"
	eng.stopped["sbx-noimg-1"] = true
	dataDir := t.TempDir()
	claims := map[string]*types.Sandbox{
		"sb_no": {
			ID: "sb_no", VMName: "sbx-noimg-1", Key: testKey, Token: "tok",
			VsockSocket: "/vsock/noimg", PendingSnap: hibernatePrefix + "noimg-1-abc123",
		},
	}
	if err := newClaimStore(dataDir).save(claims); err != nil {
		t.Fatalf("setup: %v", err)
	}
	m := newTestManagerAt(t, eng, dataDir)

	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, g := m.Info(); g.Claimed != 0 {
		t.Errorf("claimed %d, want the imageless intent dropped", g.Claimed)
	}
}

func TestReconcileAdoptsHibernated(t *testing.T) {
	eng := newFakeEngine()
	eng.vms["sbx-hibernated-1"] = "/vsock/hib"
	eng.stopped["sbx-hibernated-1"] = true
	dataDir := t.TempDir()
	claims := map[string]*types.Sandbox{
		"sb_hib": {
			ID: "sb_hib", VMName: "sbx-hibernated-1", Key: testKey, Token: "tok",
			HibernateSnap: hibernatePrefix + "hibernated-1",
		},
	}
	if err := newClaimStore(dataDir).save(claims); err != nil {
		t.Fatalf("setup: %v", err)
	}
	m := newTestManagerAt(t, eng, dataDir, config.PoolSpec{PoolKey: testKey, Warm: 1})

	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if slices.Contains(eng.removes, "sbx-hibernated-1") {
		t.Error("reconcile destroyed a hibernated claim's VM")
	}
	if _, g := m.Info(); g.Hibernated != 1 {
		t.Errorf("hibernated count %d after reconcile, want 1", g.Hibernated)
	}
	if _, err := m.WakeAgentSocket(t.Context(), "sb_hib", "tok"); err != nil {
		t.Fatalf("wake after reconcile: %v", err)
	}
	if len(eng.restores) != 1 {
		t.Errorf("restores=%v, want the adopted claim woken", eng.restores)
	}
}
