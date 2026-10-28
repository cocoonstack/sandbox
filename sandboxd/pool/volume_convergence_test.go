package pool

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestSettledVolumesDoNotCallDevicesOrResolveCatalog(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); err != nil {
		t.Fatal(err)
	}
	delete(m.catalog.entries, "data")
	before := len(eng.volumeSpecs)
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}, {Name: "other"}}); err != nil {
		t.Fatal(err)
	}
	if len(eng.volumeSpecs) != before+1 || eng.volumeSpecs[before].Name != "other" {
		t.Fatalf("retained volume reapplied: %v", eng.volumeSpecs)
	}
	before = len(eng.volumeSpecs)
	journaled := m.store.mark()
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}, {Name: "other"}}); err != nil {
		t.Fatal(err)
	}
	if len(eng.volumeSpecs) != before || m.store.mark() != journaled {
		t.Fatal("identical target reached devices or the claims journal")
	}
}

func TestRemovedTenantRetainsAndRepairsHeldVolumes(t *testing.T) {
	eng := newFakeEngine()
	m, sb := newTenantVolumeManager(t, eng, liveCatalog(t))
	if _, err := m.SetVolumes(t.Context(), sb.ID, "acme", []types.Volume{{Name: "data"}, {Name: "other"}}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteTenant(t.Context(), "acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Remove: []string{"other"}}); err != nil {
		t.Fatalf("remove other while retaining data: %v", err)
	}
	eng.detachVolumeErr = errors.New("detach result unknown")
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err == nil {
		t.Fatal("expected detaching state")
	}
	eng.detachVolumeErr = nil
	entry := m.catalog.entries["data"]
	entry.tenants = []string{"other-tenant"}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); err != nil {
		t.Fatalf("held repair checked new authorization: %v", err)
	}
	if _, err := m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Add: []types.Volume{{Name: "other"}}}); !errors.Is(err, ErrTenantRemoved) {
		t.Fatalf("new admission after tenant removal: %v", err)
	}
}

func TestReaddedDetachIsPendingAtTheDurableIntentBoundary(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	target := []types.Volume{{Name: "data"}}
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, target); err != nil {
		t.Fatal(err)
	}
	eng.detachVolumeErr = errors.New("guest unmounted; eject outcome unknown")
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{}); err == nil {
		t.Fatal("expected detach failure")
	}
	ctx, run, err := m.beginVolumeRun(t.Context(), sb)
	if err != nil {
		t.Fatal(err)
	}
	target = slices.Clone(sb.Volumes)
	if checkErr := m.prepareVolumeTarget(ctx, sb, run, target, nil, nil); checkErr != nil {
		t.Fatal(checkErr)
	}
	m.finishVolumeRun(sb, run)
	m2 := newVolumeManagerAt(t, eng, m.dataDir, catalog)
	if checkErr := m2.Reconcile(t.Context()); checkErr != nil {
		t.Fatal(checkErr)
	}
	view, err := m2.GetVolumes(sb.ID, sb.Tenant)
	if err != nil || len(view.Volumes) != 0 || len(view.VolumesWant) != 1 || view.States[0].State != types.VolumePending || view.Converging {
		t.Fatalf("reloaded reversal: %+v %v", view, err)
	}
	if !strings.Contains(view.States[0].Error, "interrupted") {
		t.Fatalf("reloaded diagnostics: %+v", view)
	}
	eng.detachVolumeErr = nil
	before := len(eng.volumeSpecs)
	if _, err := m2.SetVolumes(t.Context(), sb.ID, sb.Tenant, target); err != nil {
		t.Fatal(err)
	}
	if len(eng.volumeSpecs) != before+1 {
		t.Fatal("readded volume was not repaired")
	}
}

func TestReleaseBeforeReservationDoesNotLeakAdmission(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	ctx, run, err := m.beginVolumeRun(t.Context(), sb)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.checkVolumeRun(ctx, sb, run); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	target := []types.Volume{{Name: "data", Mount: "/volumes/data"}}
	if err := m.prepareVolumeTarget(ctx, sb, run, target, target, nil); err == nil {
		t.Fatal("released claim admitted volume")
	}
	m.finishVolumeRun(sb, run)
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: "rw"}}}); err != nil {
		t.Fatalf("canceled request leaked hold: %v", err)
	}
}

func TestVolumeDiagnosticsTrackActualWorkerAndRetry(t *testing.T) {
	base := newFakeEngine()
	m := newVolumeManager(t, base, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	eng := newGatedVolumeEngine(base)
	m.eng = eng
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := m.SetVolumes(ctx, sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); done <- err }()
	<-eng.entered
	cancel()
	view, err := m.GetVolumes(sb.ID, sb.Tenant)
	if err != nil || !view.Converging {
		t.Fatalf("canceled worker still owns hardware: %+v %v", view, err)
	}
	close(eng.resume)
	if checkErr := <-done; !errors.Is(checkErr, context.Canceled) {
		t.Fatal(checkErr)
	}
	view, err = m.GetVolumes(sb.ID, sb.Tenant)
	if err != nil || view.Converging || view.States[0].Error == "" {
		t.Fatalf("completed worker diagnostics: %+v %v", view, err)
	}
	view, err = m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}})
	if err != nil || view.States[0].Error != "" || view.Converging {
		t.Fatalf("successful retry retained errors: %+v %v", view, err)
	}
	conflict := unmountConflict()
	base.detachVolumeErr = conflict
	view, err = m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{})
	if !errors.Is(err, conflict) || view.States[0].Error != conflict.Error() {
		t.Fatalf("conflict diagnostics: %+v %v", view, err)
	}
	got, err := m.GetVolumes(sb.ID, sb.Tenant)
	if err != nil || got.States[0].Error != view.States[0].Error {
		t.Fatalf("GET lost last conflict: %+v %v", got, err)
	}
}

func TestReleaseDropsVolumeDiagnostics(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); err != nil {
		t.Fatal(err)
	}
	eng.detachVolumeErr = unmountConflict()
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{}); err == nil {
		t.Fatal("expected mount conflict")
	}
	eng.detachVolumeErr = nil
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.volumeErrors) != 0 {
		t.Fatalf("released claim kept diagnostics: %v", m.volumeErrors)
	}
}

func TestLiveVolumeRunStampsActivityBeforeDeviceCalls(t *testing.T) {
	base := newFakeEngine()
	m := newVolumeManager(t, base, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	sb.TouchAt(time.Now().Add(-time.Hour))
	eng := newGatedVolumeEngine(base)
	m.eng = eng
	done := make(chan error, 1)
	go func() {
		_, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}})
		done <- err
	}()
	<-eng.entered
	idle := time.Since(sb.LastSeen())
	close(eng.resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if idle > time.Minute {
		t.Fatalf("a running volume change left the sandbox idle for %v", idle)
	}
}

func TestRejectedVolumeRequestLeavesActivityUntouched(t *testing.T) {
	for _, tt := range []struct {
		name    string
		request []types.Volume
		paused  bool
		want    error
	}{
		{"invalid target", []types.Volume{{Name: "data"}, {Name: "data"}}, false, ErrBadVolume},
		{"hibernated claim", []types.Volume{{Name: "data"}}, true, ErrPaused},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, liveCatalog(t))
			sb := mustClaim(t, m, testKey)
			if tt.paused {
				m.mu.Lock()
				sb.HibernateSnap = "snap"
				m.mu.Unlock()
			}
			idleSince := time.Now().Add(-time.Hour)
			sb.TouchAt(idleSince)
			if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, tt.request); !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
			if !sb.LastSeen().Equal(idleSince) {
				t.Fatalf("a refused request stamped activity: %v", sb.LastSeen())
			}
		})
	}
}

func TestRejectedVolumeTargetPreservesConvergenceDiagnostics(t *testing.T) {
	for _, priorConflict := range []bool{false, true} {
		name := "settled"
		if priorConflict {
			name = "conflict"
		}
		t.Run(name, func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, liveCatalog(t))
			sb := mustClaim(t, m, testKey)
			if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); err != nil {
				t.Fatal(err)
			}
			if priorConflict {
				eng.detachVolumeErr = unmountConflict()
				if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{}); err == nil {
					t.Fatal("expected mount conflict")
				}
			}
			before, err := m.GetVolumes(sb.ID, sb.Tenant)
			if err != nil {
				t.Fatal(err)
			}
			if _, checkErr := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}, {Name: "data"}}); !errors.Is(checkErr, ErrBadVolume) {
				t.Fatalf("invalid target: %v", checkErr)
			}
			after, err := m.GetVolumes(sb.ID, sb.Tenant)
			if err != nil || !slices.Equal(after.States, before.States) || !slices.Equal(after.Volumes, before.Volumes) || !slices.Equal(after.VolumesWant, before.VolumesWant) {
				t.Fatalf("rejected request changed convergence view: before=%+v after=%+v err=%v", before, after, err)
			}
		})
	}
}

func TestUnsupportedRuntimeDoesNotChangeVolumeIntent(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	eng.liveVolumeSupportErr = &engine.LiveVolumeUnsupported{}
	before, err := os.ReadFile(m.store.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, patch := range []bool{false, true} {
		if patch {
			_, err = m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Add: []types.Volume{{Name: "data"}}})
		} else {
			_, err = m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}})
		}
		if _, ok := errors.AsType[*engine.LiveVolumeUnsupported](err); !ok {
			t.Fatalf("runtime gate: %v", err)
		}
	}
	after, err := os.ReadFile(m.store.path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) || len(eng.volumeSpecs) != 0 || volumeHoldersOf(m, "data") != (volumeHolders{}) {
		t.Fatal("unsupported runtime changed target, hold, or device")
	}
}

func TestVolumeHoldsRejectCaptureBeforeTrim(t *testing.T) {
	for _, state := range []string{"applied", "desired", "admitted"} {
		t.Run(state, func(t *testing.T) {
			eng := newFakeEngine()
			m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, CaptureTrim: true})
			sb := mustClaim(t, m, testKey)
			held := []types.Volume{{Name: "data", Mount: "/volumes/data"}}
			switch state {
			case "applied":
				sb.Volumes = held
			case "desired":
				sb.VolumesWant = held
			case "admitted":
				_, run, err := m.beginVolumeRun(t.Context(), sb)
				if err != nil {
					t.Fatal(err)
				}
				m.mu.Lock()
				m.adoptVolumes(held)
				run.holds = held
				m.mu.Unlock()
				defer m.finishVolumeRun(sb, run)
			}
			if _, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "blocked", ""); !errors.Is(err, ErrVolumeCapture) {
				t.Fatalf("checkpoint: %v", err)
			}
			if _, _, err := m.Promote(t.Context(), sb.ID, Cred{Token: sb.Token}, "blocked", ""); !errors.Is(err, ErrVolumeCapture) {
				t.Fatalf("promote: %v", err)
			}
			if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrVolumeCapture) {
				t.Fatalf("hibernate: %v", err)
			}
			if len(eng.trims) != 0 {
				t.Fatalf("capture trimmed held volume: %v", eng.trims)
			}
		})
	}
}

func TestUncertainTargetCommitReleasesOnlyConfirmedPrecleanedHolds(t *testing.T) {
	for _, durable := range []string{"old", "new"} {
		t.Run(durable, func(t *testing.T) {
			eng := newFakeEngine()
			catalog := liveCatalog(t)
			m := newVolumeManager(t, eng, catalog)
			sb := mustClaim(t, m, testKey)
			eng.mountVolumeErr = errors.New("mount result unknown")
			if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); err == nil {
				t.Fatal("expected pending volume")
			}
			eng.mountVolumeErr = nil
			m.store.sync = true
			eng.liveVolumeHook = func(verb string) error {
				if verb == "detach" {
					if err := os.Mkdir(m.store.path+".tmp", 0o700); err != nil {
						t.Fatal(err)
					}
				}
				return nil
			}
			target := []types.Volume{{Name: "other", Mount: "/volumes/data"}}
			view, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, target)
			if err == nil || len(view.VolumesWant) != 1 || view.VolumesWant[0].Name != "other" || len(view.Volumes) != 0 {
				t.Fatalf("uncertain target evidence: %+v %v", view, err)
			}
			if got := volumeHoldersOf(m, "other"); got.readers != 1 {
				t.Fatalf("uncertain target lost new hold: %+v", got)
			}
			if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
				t.Fatalf("confirmed preclean retained hold: %+v", got)
			}
			if !slices.Contains(eng.volumeOpsLog(), "detach:data") || len(eng.volumeSpecs) != 1 {
				t.Fatalf("device calls before uncertain target: attaches=%v detaches=%v", eng.volumeSpecs, eng.volumeOpsLog())
			}
			if durable == "old" {
				m.store.writeMu.Lock()
				t.Cleanup(func() {
					m.store.writeMu.Unlock()
					waitFor(t, func() bool { return !m.store.recommitting.Load() })
				})
			}
			if checkErr := os.Remove(m.store.path + ".tmp"); checkErr != nil {
				t.Fatal(checkErr)
			}
			eng.liveVolumeHook = nil
			if durable == "new" {
				if checkErr := m.store.commit(m.store.mark()); checkErr != nil {
					t.Fatal(checkErr)
				}
				waitFor(t, func() bool { return !m.store.recommitting.Load() })
			}
			m2 := newVolumeManagerAt(t, eng, m.dataDir, catalog)
			if checkErr := m2.Reconcile(t.Context()); checkErr != nil {
				t.Fatal(checkErr)
			}
			view, err = m2.SetVolumes(t.Context(), sb.ID, sb.Tenant, target)
			if err != nil || len(view.VolumesWant) != 1 || view.VolumesWant[0].Name != "other" {
				t.Fatalf("retry did not accept target: %+v %v", view, err)
			}
			if got := volumeHoldersOf(m2, "data"); got != (volumeHolders{}) {
				t.Fatalf("successful retry retained discarded hold: %+v", got)
			}
			if _, err := m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: "rw"}}}); err != nil {
				t.Fatalf("writer after preclean and recovery: %v", err)
			}
		})
	}
}

func unmountConflict() *engine.VolumeMountConflict {
	return &engine.VolumeMountConflict{Message: `cannot unmount "/volumes/data"; close guest references and retry`}
}
