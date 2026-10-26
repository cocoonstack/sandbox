package pool

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestRemovedVMKeepsVolumeUntilCanceledWorkerReturns(t *testing.T) {
	base := newFakeEngine()
	m := newVolumeManager(t, base, nil)
	if err := m.RegisterVolume(t.Context(), "data", types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes"), Writable: true}); err != nil {
		t.Fatal(err)
	}
	sb := mustClaim(t, m, testKey)
	eng := newGatedVolumeEngine(base)
	m.eng = eng
	changed := make(chan error, 1)
	go func() { _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); changed <- err }()
	<-eng.entered
	resumed := false
	defer func() {
		if !resumed {
			close(eng.resume)
			<-changed
		}
	}()
	released := make(chan error, 1)
	go func() { released <- m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
	select {
	case err := <-released:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("release waited for paused worker")
	}
	if got := volumeHoldersOf(m, "data"); got != (volumeHolders{readers: 1}) {
		t.Errorf("removed VM freed active worker count: %+v", got)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Errorf("writer overtook paused canceled worker: %v", err)
	}
	if err := m.UnregisterVolume(t.Context(), "data"); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Errorf("unregister overtook paused worker: %v", err)
	}
	m.retryRemovals(t.Context()).Wait()
	if got := volumeHoldersOf(m, "data"); got != (volumeHolders{readers: 1}) {
		t.Errorf("maintenance freed active worker count: %+v", got)
	}
	close(eng.resume)
	resumed = true
	if err := <-changed; err == nil {
		t.Fatal("removed claim accepted late attachment")
	}
	m.retryRemovals(t.Context()).Wait()
	if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
		t.Fatalf("completed worker leaked count: %+v", got)
	}
	if err := m.UnregisterVolume(t.Context(), "data"); err != nil {
		t.Fatal(err)
	}
}

func TestDispatchedCleanupRetainsCatalogReference(t *testing.T) {
	base := newFakeEngine()
	m := newVolumeManager(t, base, nil)
	if err := m.RegisterVolume(t.Context(), "data", types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes")}); err != nil {
		t.Fatal(err)
	}
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}})
	if err != nil {
		t.Fatal(err)
	}
	base.removeErrFor = sb.VMName
	if err = m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("expected survivor")
	}
	for range cap(m.maintSem) {
		m.maintSem <- struct{}{}
	}
	wg := m.retryRemovals(t.Context())
	m.mu.Lock()
	queued := len(m.pendingRemovals)
	m.mu.Unlock()
	if queued != 0 {
		t.Fatal("cleanup was not dispatched")
	}
	err = m.UnregisterVolume(t.Context(), "data")
	for range cap(m.maintSem) {
		<-m.maintSem
	}
	wg.Wait()
	if !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("dispatched cleanup lost guard: %v", err)
	}
	base.removeErrFor = ""
	m.retryRemovals(t.Context()).Wait()
	if err := m.UnregisterVolume(t.Context(), "data"); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownStaleVolumesFenceUntilInspectionOrRemoval(t *testing.T) {
	for _, tc := range []struct {
		name, state  string
		inspectError bool
	}{
		{"running omitted", "running", false},
		{"running error", "running", true},
		{"inferred stopped omitted", "stopped", false},
		{"inferred stopped error", "stopped", true},
		{"creating omitted", "creating", false},
		{"creating error", "creating", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := newFakeEngine()
			m := newVolumeManager(t, base, nil)
			path := writeVolumeImage(t, "runtime", "bytes")
			if err := m.RegisterVolume(t.Context(), "data", types.VolumeRegistration{Path: path, Writable: true}); err != nil {
				t.Fatal(err)
			}
			name := "sbx-stale-unknown"
			base.vms[name] = "/vsock/stale"
			base.stopped[name] = tc.state == "stopped"
			base.creating[name] = tc.state == "creating"
			base.inspectStale = tc.state == "stopped"
			base.inspectDisksUnknown = !tc.inspectError
			base.staleOutcome = engine.StaleCreateBusy
			base.removeErrFor = name
			base.disks = map[string][]types.VMDisk{name: {{Name: "data", ReadOnly: false}}}
			var inspectFailed atomic.Bool
			inspectFailed.Store(true)
			base.inspectHook = func() error {
				if tc.inspectError && inspectFailed.Load() {
					return errors.New("inspection unavailable")
				}
				return nil
			}
			rec := base.record(name)
			rec.State = tc.state
			if m.removeStaleVM(t.Context(), name, rec) {
				t.Fatal("unknown stale VM unexpectedly gone")
			}
			for range 2 {
				if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
					t.Fatalf("unknown stale attachment admitted reader: %v", err)
				}
				if err := m.UnregisterVolume(t.Context(), "data"); !errors.Is(err, ErrVolumeCatalogUnavailable) {
					t.Fatalf("unknown stale attachment allowed unregister: %v", err)
				}
				if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); err != nil {
					t.Fatalf("volume-free claim blocked: %v", err)
				}
				for range cap(m.maintSem) {
					m.maintSem <- struct{}{}
				}
				wg := m.retryRemovals(t.Context())
				if err := m.RegisterVolume(t.Context(), "new", types.VolumeRegistration{Path: writeVolumeImage(t, "new", "bytes")}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
					t.Errorf("dispatched unknown fence lost: %v", err)
				}
				for range cap(m.maintSem) {
					<-m.maintSem
				}
				wg.Wait()
			}
			inspectFailed.Store(false)
			base.stopped[name], base.creating[name] = false, false
			base.inspectStale, base.inspectDisksUnknown = false, false
			if tc.state == "creating" {
				m.retryRemovals(t.Context()).Wait()
				if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
					t.Fatalf("busy partial inventory cleared unknown: %v", err)
				}
			}
			base.staleOutcome = engine.StaleCreateNotCreating
			m.retryRemovals(t.Context()).Wait()
			if got := volumeHoldersOf(m, "data"); got != (volumeHolders{writers: 1}) {
				t.Fatalf("inspection did not replace fence with known holds: %+v", got)
			}
			if err := m.RegisterVolume(t.Context(), "unrelated", types.VolumeRegistration{Path: writeVolumeImage(t, "unrelated", "bytes")}); err != nil {
				t.Fatalf("resolved inspection left global fence: %v", err)
			}
			if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}}); !errors.Is(err, ErrVolumeBusy) {
				t.Fatalf("known attachment lost exclusion: %v", err)
			}
			base.removeErrFor = ""
			m.retryRemovals(t.Context()).Wait()
			if err := m.UnregisterVolume(t.Context(), "data"); err != nil {
				t.Fatalf("confirmed removal retained fence: %v", err)
			}
		})
	}
}

func TestRuntimeCatalogLoadedBeforeClaimReconciliation(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, nil)
	path := writeVolumeImage(t, "runtime", "bytes")
	if err := m.RegisterVolume(t.Context(), "data", types.VolumeRegistration{Path: path, Writable: true, Tenants: []string{"acme"}}); err != nil {
		t.Fatal(err)
	}
	sb, claimErr := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}})
	if claimErr != nil {
		t.Fatal(claimErr)
	}
	m2 := newVolumeManagerAt(t, eng, m.dataDir, nil)
	if got := m2.Volumes("acme", nil); len(got) != 1 || got[0].Name != "data" || !got[0].Available || !got[0].Writable {
		t.Fatalf("catalog absent before reconcile: %+v", got)
	}
	if local, err := m2.VolumePlacement(testKey, "acme", []string{"data"}); err != nil || !local {
		t.Fatalf("placement before reconcile: %v %v", local, err)
	}
	if err := m2.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	view, err := m2.GetVolumes(sb.ID, "")
	if err != nil || !slices.Equal(view.Volumes, sb.Volumes) {
		t.Fatalf("claim recovery: %+v %v", view, err)
	}
	if _, err := m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("recovered holder missing: %v", err)
	}
}

func TestUnknownFencePrecedesInspectionAndClearsAfterConfirmedRemoval(t *testing.T) {
	base := newFakeEngine()
	m := newVolumeManager(t, base, liveCatalog(t))
	name := "sbx-stale-before-inspect"
	base.vms[name] = "/vsock/stale"
	base.removeErrFor = name
	entered, resume := make(chan struct{}), make(chan struct{})
	var gated atomic.Bool
	base.inspectHook = func() error {
		if gated.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
		return errors.New("inspection unavailable")
	}
	done := make(chan bool, 1)
	go func() { done <- m.removeStaleVM(t.Context(), name, base.record(name)) }()
	<-entered
	if !m.mu.TryLock() {
		t.Fatal("stale inspection holds manager lock")
	}
	m.mu.Unlock()
	_, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
	close(resume)
	if gone := <-done; gone {
		t.Fatal("failed inspect/removal unexpectedly confirmed gone")
	}
	if !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("admitted volume while inspection was blocked: %v", err)
	}
	base.removeErrFor = ""
	m.retryRemovals(t.Context()).Wait()
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); err != nil {
		t.Fatalf("successful removal retained unknown fence: %v", err)
	}
}

func TestCanceledRemovalDispatchKeepsUnknownFenceOwned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := newFakeEngine()
		m := newVolumeManager(t, base, liveCatalog(t))
		name := "sbx-canceled-dispatch"
		base.vms[name] = "/vsock/stale"
		base.removeErrFor = name
		base.inspectHook = func() error { return errors.New("inspection unavailable") }
		if m.removeStaleVM(t.Context(), name, base.record(name)) {
			t.Fatal("expected survivor")
		}
		for range cap(m.maintSem) {
			m.maintSem <- struct{}{}
		}
		ctx, cancel := context.WithCancel(t.Context())
		wg := m.retryRemovals(ctx)
		cancel()
		synctest.Wait()
		base.removeErrFor = ""
		for range cap(m.maintSem) {
			<-m.maintSem
		}
		wg.Wait()
		m.retryRemovals(t.Context()).Wait()
		if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); err != nil {
			t.Fatalf("canceled dispatch stranded unknown fence: %v", err)
		}
	})
}

func TestWarmAttachBeforeFinalizeNeedsUnknownFenceAfterRestart(t *testing.T) {
	base := newFakeEngine()
	name := "sbx-warm-unfinalized"
	base.vms[name] = "/vsock/warm"
	base.diskAttachErrFor = "broken"
	base.removeErrFor = name
	rendezvousAttaches(base, 2)
	catalog := []config.VolumeSpec{
		{Name: "data", Path: writeVolumeImage(t, "warm-data", "data"), Writable: true},
		{Name: "broken", Path: writeVolumeImage(t, "warm-broken", "broken")},
	}
	dir := t.TempDir()
	m := newVolumePoolManager(t, base, dir, catalog)
	base.inspectDisksUnknown, base.trackDisks = true, true
	warm := &types.Sandbox{VMName: name, Key: testKey, VsockSocket: "/vsock/warm"}
	m.pools[testKey].warm = append(m.pools[testKey].warm, warm)
	_, err := m.ClaimWarm(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{
		{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true},
		{Name: "broken", AttachOnly: true},
	}})
	if err == nil {
		t.Fatal("expected attach failure before finalization")
	}
	if warm.ID != "" {
		t.Fatal("claim unexpectedly finalized")
	}
	base.mu.Lock()
	attached := slices.Clone(base.disks[name])
	base.attachRendezvous = nil
	base.mu.Unlock()
	if !slices.ContainsFunc(attached, func(d types.VMDisk) bool { return d.Name == "data" && !d.ReadOnly }) {
		t.Fatalf("warm VM has no attached writer: %+v", attached)
	}
	claims, err := newClaimStore(dir, false).load()
	if err != nil || len(claims) != 0 {
		t.Fatalf("pre-finalize journal: %+v, %v", claims, err)
	}
	restarted := newVolumeManagerAt(t, base, dir, catalog)
	restarted.eng = base
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("ambiguous warm survivor admitted competing writer: %v", err)
	}
	base.removeErrFor = ""
	restarted.retryRemovals(t.Context()).Wait()
	if _, err := restarted.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); err != nil {
		t.Fatalf("confirmed removal failed to clear fence: %v", err)
	}
}

func TestArchivedReleaseWithUnvalidatedVolumeRunHasNoCleanupHolds(t *testing.T) {
	base := newFakeEngine()
	m := newTestManager(t, base, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if err := m.archive(t.Context(), sb); err != nil {
		t.Fatal(err)
	}
	if sb.VMName != "" {
		t.Fatalf("archived VM name = %q", sb.VMName)
	}
	ctx, run, err := m.beginVolumeRun(t.Context(), sb)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.checkVolumeRun(ctx, sb, run); !errors.Is(err, ErrArchived) {
		t.Fatalf("archived mutation validation: %v", err)
	}
	td := m.quiesceVolumes(t.Context(), sb)
	if td.done == nil || len(td.holds) != 0 || len(td.clears) != 0 || td.unknown {
		t.Fatalf("archived teardown has cleanup obligations: %+v", td)
	}
	if m.finishVolumeTeardown(t.Context(), td) {
		t.Fatal("open volume run unexpectedly completed")
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	m.finishVolumeRun(sb, run)
	if len(m.pendingRemovals) != 0 {
		t.Fatalf("empty teardown enqueued removal: %+v", m.pendingRemovals)
	}
	if slices.Contains(base.removedNames(), "") {
		t.Fatal("release removed an empty VM name")
	}
	if m.store.referencesVolume("data") {
		t.Fatal("release retained archived journal references")
	}
}

func TestStaleInventoryClassifiesStoppedAndRunning(t *testing.T) {
	for _, tc := range []struct {
		name           string
		stopped, stale bool
		disks          []types.VMDisk
		wantErr        error
	}{
		{"running unknown", false, false, nil, ErrVolumeCatalogUnavailable},
		{"running empty", false, false, []types.VMDisk{}, nil},
		{"ordinary stopped", true, false, nil, nil},
		{"inferred stopped", true, true, nil, ErrVolumeCatalogUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := newFakeEngine()
			m := newVolumeManager(t, base, liveCatalog(t))
			name := "sbx-inventory-shape"
			base.vms[name], base.removeErrFor = "/vsock/stale", name
			base.disks = map[string][]types.VMDisk{name: tc.disks}
			base.stopped[name], base.inspectStale = tc.stopped, tc.stale
			if m.removeStaleVM(t.Context(), name, base.record(name)) {
				t.Fatal("stale VM unexpectedly disappeared")
			}
			_, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("admission=%v, want=%v", err, tc.wantErr)
			}
			if _, queued := m.pendingRemovals[name]; !queued {
				t.Fatal("surviving VM lost pending removal")
			}
			if err := m.RegisterVolume(t.Context(), "before-removal", types.VolumeRegistration{Path: "/images/before-removal"}); !errors.Is(err, tc.wantErr) {
				t.Fatalf("registration before removal: %v", err)
			}
			base.removeErrFor = ""
			m.retryRemovals(t.Context()).Wait()
			if err := m.RegisterVolume(t.Context(), "unrelated", types.VolumeRegistration{Path: "/images/unrelated"}); err != nil {
				t.Fatalf("confirmed removal retained registration fence: %v", err)
			}
			if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}}); err != nil {
				t.Fatalf("confirmed removal retained admission fence: %v", err)
			}
		})
	}
}

func TestStaleInventoryRetainsFenceUntilRemovalFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newVolumeManager(t, eng, liveCatalog(t))
		name := "sbx-attach-during-remove"
		eng.vms[name], eng.removeErrFor = "/vsock/stale", name
		eng.disks = map[string][]types.VMDisk{name: {{Name: "data", ReadOnly: true}}}
		eng.removeStall = make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			if m.removeStaleVM(t.Context(), name, eng.record(name)) {
				t.Error("failed removal reported gone")
			}
		}()
		checkStaleRemovalInventory(t, m, eng, name, done)
	})
}

func TestStaleInventoryRetainsFenceUntilRetryFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newVolumeManager(t, eng, liveCatalog(t))
		name := "sbx-attach-during-retry"
		eng.vms[name], eng.removeErrFor = "/vsock/stale", name
		eng.disks = map[string][]types.VMDisk{name: {{Name: "data", ReadOnly: true}}}
		eng.inspectDisksUnknown = true
		if m.removeStaleVM(t.Context(), name, eng.record(name)) {
			t.Fatal("failed removal reported gone")
		}
		eng.inspectDisksUnknown = false
		eng.removeStall = make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			m.retryRemovals(t.Context()).Wait()
		}()
		checkStaleRemovalInventory(t, m, eng, name, done)
	})
}

func TestBusyStaleCreateKeepsVolumeFenceUntilCleanup(t *testing.T) {
	for _, outcome := range []engine.StaleCreateOutcome{engine.StaleCreateCollected, engine.StaleCreateNotFound, engine.StaleCreateBusy} {
		t.Run(string(outcome), func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, liveCatalog(t))
			name := "sbx-busy-create"
			eng.vms[name], eng.creating[name] = "/vsock/creating", true
			eng.staleOutcome = engine.StaleCreateBusy
			eng.disks = map[string][]types.VMDisk{name: {{Name: "other"}}}
			rec := eng.record(name)
			rec.State = vmStateCreating
			if m.removeStaleVM(t.Context(), name, rec) {
				t.Fatal("busy create reported gone")
			}
			eng.creating[name] = false
			for _, stopped := range []bool{false, true} {
				eng.stopped[name] = stopped
				before := eng.inspects
				m.retryRemovals(t.Context()).Wait()
				if eng.inspects != before+1 {
					t.Fatal("busy retry did not inspect")
				}
			}
			if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
				t.Fatalf("busy create admitted a volume: %v", err)
			}
			eng.staleOutcome = outcome
			if outcome == engine.StaleCreateBusy {
				delete(eng.vms, name)
			}
			m.retryRemovals(t.Context()).Wait()
			m.retryRemovals(t.Context()).Wait()
			if m.catalog.unknown != 0 || len(m.pendingRemovals) != 0 {
				t.Fatalf("cleanup retained or double-released its fence: unknown=%d pending=%d", m.catalog.unknown, len(m.pendingRemovals))
			}
			if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}}); err != nil {
				t.Fatalf("confirmed cleanup retained fence: %v", err)
			}
		})
	}
}

func checkStaleRemovalInventory(t *testing.T, m *Manager, eng *fakeEngine, name string, done <-chan struct{}) {
	t.Helper()
	synctest.Wait()
	registration := types.VolumeRegistration{Path: writeVolumeImage(t, "unrelated.img", "unrelated")}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Errorf("in-flight removal admitted a reader: %v", err)
	}
	if err := m.RegisterVolume(t.Context(), "unrelated", registration); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Errorf("in-flight removal admitted registration: %v", err)
	}
	eng.mu.Lock()
	eng.disks[name] = append(eng.disks[name], types.VMDisk{Name: "other"})
	eng.mu.Unlock()
	close(eng.removeStall)
	<-done
	if _, queued := m.pendingRemovals[name]; !queued {
		t.Fatal("failed removal lost its retry")
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "other", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Errorf("writer attached during removal lost its hold: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Errorf("failed removal lost its reader hold: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}}); err != nil {
		t.Errorf("failed removal blocked a compatible reader: %v", err)
	}
	if err := m.RegisterVolume(t.Context(), "unrelated", registration); err != nil {
		t.Fatalf("failed removal blocked unrelated registration: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "unrelated"}}}); err != nil {
		t.Errorf("failed removal blocked an unrelated reader: %v", err)
	}
}
