package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestIncompleteStaleInventoryRetainsVolumeProtection(t *testing.T) {
	requireCatalogRoot(t)
	for _, state := range []string{vmStateRunning, vmStateCreating, "stopped"} {
		t.Run(state, func(t *testing.T) {
			base := newFakeEngine()
			m := newVolumeManager(t, base, nil)
			path := writeVolumeImage(t, "incomplete-inventory", "original")
			if err := m.RegisterVolume(t.Context(), "data", types.VolumeRegistration{Path: path, Writable: true}); err != nil {
				t.Fatal(err)
			}
			name := "sbx-incomplete-inventory"
			base.vms[name] = "/vsock/incomplete"
			base.creating[name] = state == vmStateCreating
			base.staleOutcome = engine.StaleCreateBusy
			base.removeErrFor = name
			base.disks = map[string][]types.VMDisk{name: {{Name: "data", ReadOnly: false}}}
			cli := filepath.Join(t.TempDir(), "inspect")
			writeInspect := func(state, inventory string) {
				t.Helper()
				raw := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' '{\"config\":{\"name\":\"%s\"},\"state\":\"%s\"%s}'\n", name, state, inventory)
				if err := os.WriteFile(cli, []byte(raw), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			writeInspect(state, "")
			m.eng = &incompleteInventoryEngine{fakeEngine: base, cli: engine.New(cli, nil, nil, false, false, "")}
			rec := base.record(name)
			rec.State = state
			if m.removeStaleVM(t.Context(), name, rec) {
				t.Fatal("stale VM unexpectedly disappeared")
			}
			for range 2 {
				if err := m.UnregisterVolume(t.Context(), "data"); !errors.Is(err, ErrVolumeCatalogUnavailable) {
					t.Fatalf("unregister must refuse incomplete inventory: %v", err)
				}
				if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
					t.Fatalf("conflicting writer must refuse incomplete inventory: %v", err)
				}
				m.retryRemovals(t.Context()).Wait()
			}
			base.creating[name] = false
			writeInspect(vmStateRunning, `,"attached_devices":{"disks":[{"name":"data","readonly":false}]}`)
			if state == vmStateCreating {
				m.retryRemovals(t.Context()).Wait()
				if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
					t.Fatalf("busy owner must retain the unknown fence: %v", err)
				}
			}
			base.staleOutcome = engine.StaleCreateNotCreating
			m.retryRemovals(t.Context()).Wait()
			if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
				t.Errorf("complete inventory lost the surviving writer: %v", err)
			}
			other := writeVolumeImage(t, "complete-inventory-other", "other")
			if err := m.RegisterVolume(t.Context(), "other", types.VolumeRegistration{Path: other}); err != nil {
				t.Fatalf("complete inventory retained the global fence: %v", err)
			}
			base.removeErrFor = ""
			m.retryRemovals(t.Context()).Wait()
			if err := m.UnregisterVolume(t.Context(), "data"); err != nil {
				t.Fatalf("confirmed removal retained volume protection: %v", err)
			}
		})
	}
}

type incompleteInventoryEngine struct {
	*fakeEngine
	cli *engine.Engine
}

func (e *incompleteInventoryEngine) Inspect(ctx context.Context, name string) (types.VMRecord, bool, error) {
	return e.cli.Inspect(ctx, name)
}

func TestRemovedVMKeepsVolumeUntilCanceledWorkerReturns(t *testing.T) {
	requireCatalogRoot(t)
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
	requireCatalogRoot(t)
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

type unknownVolumeEngine struct {
	*fakeEngine
	inspectFailed  atomic.Bool
	inspectEntered chan struct{}
	inspectResume  chan struct{}
	inspectGated   atomic.Bool
}

func (e *unknownVolumeEngine) Inspect(ctx context.Context, name string) (types.VMRecord, bool, error) {
	if e.inspectEntered != nil && e.inspectGated.CompareAndSwap(false, true) {
		close(e.inspectEntered)
		<-e.inspectResume
	}
	if e.inspectFailed.Load() {
		return types.VMRecord{}, false, errors.New("inspection unavailable")
	}
	return e.fakeEngine.Inspect(ctx, name)
}

func TestUnknownStaleVolumesFenceUntilInspectionOrRemoval(t *testing.T) {
	requireCatalogRoot(t)
	for _, state := range []string{"running", "stopped", "creating"} {
		t.Run(state, func(t *testing.T) {
			base := newFakeEngine()
			m := newVolumeManager(t, base, nil)
			path := writeVolumeImage(t, "runtime", "bytes")
			if err := m.RegisterVolume(t.Context(), "data", types.VolumeRegistration{Path: path, Writable: true}); err != nil {
				t.Fatal(err)
			}
			name := "sbx-stale-unknown"
			base.vms[name] = "/vsock/stale"
			base.stopped[name] = state == "stopped"
			base.creating[name] = state == "creating"
			base.staleOutcome = engine.StaleCreateBusy
			base.removeErrFor = name
			base.disks = map[string][]types.VMDisk{name: {{Name: "data", ReadOnly: false}}}
			eng := &unknownVolumeEngine{fakeEngine: base}
			eng.inspectFailed.Store(true)
			m.eng = eng
			if m.removeStaleVM(t.Context(), name, base.record(name)) {
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
			eng.inspectFailed.Store(false)
			base.stopped[name], base.creating[name] = false, false
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
	requireCatalogRoot(t)
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
	eng := &unknownVolumeEngine{fakeEngine: base, inspectEntered: make(chan struct{}), inspectResume: make(chan struct{})}
	eng.inspectFailed.Store(true)
	m.eng = eng
	done := make(chan bool, 1)
	go func() { done <- m.removeStaleVM(t.Context(), name, base.record(name)) }()
	<-eng.inspectEntered
	if !m.mu.TryLock() {
		t.Fatal("stale inspection holds manager lock")
	}
	m.mu.Unlock()
	_, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
	close(eng.inspectResume)
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
		eng := &unknownVolumeEngine{fakeEngine: base}
		eng.inspectFailed.Store(true)
		m.eng = eng
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
	m.eng = &emptyWarmInventoryEngine{fakeEngine: base}
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
	restarted.eng = &emptyWarmInventoryEngine{fakeEngine: base}
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

type emptyWarmInventoryEngine struct{ *fakeEngine }

func (e *emptyWarmInventoryEngine) Inspect(ctx context.Context, name string) (types.VMRecord, bool, error) {
	rec, present, err := e.fakeEngine.Inspect(ctx, name)
	rec.AttachedDevices.Disks = nil
	return rec, present, err
}

func (e *emptyWarmInventoryEngine) DiskAttach(ctx context.Context, name string, spec engine.VolumeSpec) error {
	if err := e.fakeEngine.DiskAttach(ctx, name, spec); err != nil {
		return err
	}
	e.mu.Lock()
	if e.disks == nil {
		e.disks = map[string][]types.VMDisk{}
	}
	e.disks[name] = append(e.disks[name], types.VMDisk{Name: spec.Name, ReadOnly: !spec.RW})
	e.mu.Unlock()
	return nil
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
	ctx, run, err := m.beginVolumeRun(t.Context(), sb, sb.Tenant)
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
	if len(m.store.recoveryVolumes()) != 0 {
		t.Fatal("release retained archived journal references")
	}
}
