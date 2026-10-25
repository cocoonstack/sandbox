package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestLiveVolumesCommitAndRetry(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	request := []types.Volume{{Name: "data"}, {Name: "other", Mount: "/models"}}
	for range 2 {
		got, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, request)
		if err != nil || len(got.Volumes) != 2 || !slices.Equal(sb.VolumesWant, sb.Volumes) {
			t.Fatalf("attach = %v, %v; pending=%v", got, err, sb.VolumesWant)
		}
		got.Volumes[0].Name = "caller copy"
	}
	if got := volumeHoldersOf(m, "data"); got.readers != 1 {
		t.Fatalf("duplicate reservation: %+v", got)
	}
	assertLiveJournal(t, m, sb)
	for range 2 {
		got, err := m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Remove: []string{"data", "other"}})
		if err != nil || len(got.Volumes) != 0 {
			t.Fatalf("detach = %v, %v", got, err)
		}
	}
	assertLiveJournal(t, m, sb)
	if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
		t.Fatalf("leaked holds: %+v", got)
	}
	if _, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "after-detach", ""); err != nil {
		t.Fatalf("capture after detach: %v", err)
	}
}

func TestLiveAttachPreservesWritableAndAttachOnlyVolumes(t *testing.T) {
	for _, attachOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("attach_only=%t", attachOnly), func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, liveCatalog(t))
			sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "other", Mode: "rw", AttachOnly: attachOnly}}})
			if err != nil {
				t.Fatal(err)
			}
			original := sb.Volumes[0]
			if _, err := m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Add: []types.Volume{{Name: "data"}}}); err != nil {
				t.Fatal(err)
			}
			if sb.Volumes[0] != original {
				t.Fatalf("changed existing volume: %v -> %v", original, sb.Volumes[0])
			}
			if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "other", Mount: original.Mount}}); !errors.Is(err, ErrBadVolume) {
				t.Fatalf("accepted mode change: %v", err)
			}
			if _, err := m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Remove: []string{"data", "other"}}); err != nil || len(sb.Volumes) != 0 {
				t.Fatalf("writable detach failed: %v", err)
			}
		})
	}
}

func TestLiveVolumeValidationPrecedesDeviceCalls(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	for _, request := range [][]types.Volume{
		{{Name: "data", Mode: "unknown"}},
		{{Name: "data", AttachOnly: true, Mount: "/data"}},
		{{Name: "data", Mount: "/etc"}},
		{{Name: "data"}, {Name: "data"}},
		{{Name: "data", Mount: "/a"}, {Name: "other", Mount: "/a/b"}},
		{{Name: "missing"}},
	} {
		if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, request); !errors.Is(err, ErrBadVolume) {
			t.Fatalf("request %v: %v", request, err)
		}
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "wrong", []types.Volume{{Name: "data"}}); !errors.Is(err, ErrUnknownSandbox) {
		t.Fatalf("wrong tenant: %v", err)
	}
	if _, err := m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Remove: []string{"data", "data"}}); !errors.Is(err, ErrBadVolume) {
		t.Fatalf("duplicate detach: %v", err)
	}
	if len(eng.volumeSpecs) != 0 || !slices.Equal(sb.VolumesWant, sb.Volumes) {
		t.Fatal("invalid request reached engine")
	}
}

func TestLiveAttachFailureSurvivesRestartAndCanBeDetached(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	eng.mountVolumeErr = errors.New("guest unavailable after host attach")
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); err == nil {
		t.Fatal("want mount failure")
	}
	assertLiveJournal(t, m, sb)
	if slices.Equal(sb.VolumesWant, sb.Volumes) || volumeHoldersOf(m, "data").readers != 1 {
		t.Fatal("lost pending device or hold")
	}
	m2 := newVolumeManagerAt(t, eng, m.dataDir, catalog)
	if reconcileErr := m2.Reconcile(t.Context()); reconcileErr != nil {
		t.Fatal(reconcileErr)
	}
	if volumeHoldersOf(m2, "data").readers != 1 {
		t.Fatal("restart lost pending hold")
	}
	if _, err := m2.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "pending", ""); !errors.Is(err, ErrVolumeCapture) {
		t.Fatalf("captured pending device: %v", err)
	}
	eng.detachVolumeErr = errors.New("host detach failed")
	if _, err := m2.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Remove: []string{"data"}}); err == nil || volumeHoldersOf(m2, "data").readers != 1 {
		t.Fatal("failed detach released its hold")
	}
	eng.detachVolumeErr = nil
	if _, err := m2.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Remove: []string{"data"}}); err != nil {
		t.Fatal(err)
	}
	if volumeHoldersOf(m2, "data") != (volumeHolders{}) {
		t.Fatal("successful cleanup leaked hold")
	}
}

func TestLiveVolumePersistenceFailureRetainsRecoverableState(t *testing.T) {
	for _, stage := range []string{"intent", "attach result", "detach result"} {
		t.Run(stage, func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, liveCatalog(t))
			sb := mustClaim(t, m, testKey)
			request := []types.Volume{{Name: "data"}}
			if stage == "detach result" {
				if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, request); err != nil {
					t.Fatal(err)
				}
			}
			var restore func()
			if stage == "intent" {
				restore = breakClaimStore(t, m)
			} else {
				eng.liveVolumeHook = func(string) error { restore = breakClaimStore(t, m); return nil }
			}
			var err error
			if stage == "detach result" {
				_, err = m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Remove: []string{"data"}})
			} else {
				_, err = m.SetVolumes(t.Context(), sb.ID, sb.Tenant, request)
			}
			if err == nil {
				t.Fatal("expected persistence failure")
			}
			eng.liveVolumeHook = nil
			restore()
			assertLiveJournal(t, m, sb)
			wantApplied := map[string]int{"intent": 0, "attach result": 1, "detach result": 0}[stage]
			wantHolders := map[string]int{"intent": 1, "attach result": 1, "detach result": 0}[stage]
			if len(sb.Volumes) != wantApplied || volumeHoldersOf(m, "data").readers != wantHolders {
				t.Fatalf("failed write lost the physical result or its hold: volumes=%v holders=%+v", sb.Volumes, volumeHoldersOf(m, "data"))
			}
			if stage == "intent" && len(eng.volumeSpecs) != 0 {
				t.Fatal("failed intent reached the engine")
			}
			var view types.SandboxVolumesResponse
			if stage == "detach result" {
				view, err = m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Remove: []string{"data"}})
			} else {
				view, err = m.SetVolumes(t.Context(), sb.ID, sb.Tenant, request)
			}
			if err != nil {
				t.Fatal(err)
			}
			if slices.ContainsFunc(view.States, func(s types.VolumeState) bool { return s.Error != "" }) {
				t.Fatalf("converged view kept a stale error: %+v", view.States)
			}
			assertLiveJournal(t, m, sb)
		})
	}
}

func TestLiveBatchPartialFailureRetriesWithoutDoubleHolds(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	request := []types.Volume{{Name: "data"}, {Name: "other"}}
	eng.diskAttachErrFor = "other"
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, request); err == nil {
		t.Fatal("want partial failure")
	}
	if len(sb.Volumes) != 1 || slices.Equal(sb.VolumesWant, sb.Volumes) || len(sb.VolumesWant) != 2 {
		t.Fatalf("partial state: %v %v", sb.Volumes, sb.VolumesWant)
	}
	eng.diskAttachErrFor = ""
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, request); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"data", "other"} {
		if volumeHoldersOf(m, name).readers != 1 {
			t.Fatal("retry double-reserved " + name)
		}
	}
}

func TestLiveVolumeDoesNotBlockLifecycle(t *testing.T) {
	for _, action := range []string{"release", "checkpoint", "fork", "promote", "hibernate", "reap", "renew", "get", "relay"} {
		t.Run(action, func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, liveCatalog(t))
			sb := mustClaim(t, m, testKey)
			entered, resume := make(chan struct{}), make(chan struct{})
			eng.liveVolumeHook = func(string) error { close(entered); <-resume; return nil }
			attached := make(chan error, 1)
			go func() {
				_, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}})
				attached <- err
			}()
			<-entered
			assertLiveJournal(t, m, sb)
			done := make(chan error, 1)
			go func() {
				var err error
				switch action {
				case "release":
					err = m.Release(t.Context(), sb.ID, Cred{Token: sb.Token})
				case "checkpoint":
					_, err = m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "race", "")
				case "fork":
					_, err = m.Fork(t.Context(), sb.ID, Cred{Token: sb.Token}, 1, 0, "", "")
				case "promote":
					_, _, err = m.Promote(t.Context(), sb.ID, Cred{Token: sb.Token}, "race", "")
				case "hibernate":
					err = m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token})
				case "reap":
					m.mu.Lock()
					sb.Deadline = time.Now().Add(-time.Second)
					m.mu.Unlock()
					m.reapOnce(t.Context())
				case "renew":
					_, err = m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Minute, "")
				case "get":
					_, err = m.GetVolumes(sb.ID, sb.Tenant)
				case "relay":
					var unhold func()
					_, _, unhold, err = m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
					if unhold != nil {
						unhold()
					}
				}
				done <- err
			}()
			var err error
			select {
			case err = <-done:
			case <-time.After(2 * time.Second):
				close(resume)
				<-attached
				t.Fatal("lifecycle blocked behind device call")
			}
			if slices.Contains([]string{"checkpoint", "fork", "promote", "hibernate"}, action) {
				if !errors.Is(err, ErrVolumeCapture) {
					t.Errorf("capture=%v", err)
				}
			} else if err != nil {
				t.Errorf("lifecycle=%v", err)
			}
			close(resume)
			attachErr := <-attached
			if action == "release" || action == "reap" {
				waitFor(t, func() bool {
					m.retryRemovals(t.Context()).Wait()
					return volumeHoldersOf(m, "data") == (volumeHolders{})
				})
			}
			if action == "release" || action == "reap" {
				if attachErr == nil {
					t.Fatal("released worker reported success")
				}
				claims, err := m.store.load()
				if err != nil || claims[sb.ID] != nil {
					t.Fatalf("late worker resurrected claim: %v %v", claims, err)
				}
				if volumeHoldersOf(m, "data") != (volumeHolders{}) {
					t.Fatal("teardown leaked holds")
				}
			} else if attachErr != nil {
				t.Fatal(attachErr)
			}
		})
	}
}

func TestLiveVolumeCanceledRequestRetainsIntent(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	ctx, cancel := context.WithCancel(t.Context())
	eng.liveVolumeHook = func(string) error { cancel(); return nil }
	_, err := m.SetVolumes(ctx, sb.ID, sb.Tenant, []types.Volume{{Name: "data"}})
	if !errors.Is(err, context.Canceled) || slices.Equal(sb.VolumesWant, sb.Volumes) {
		t.Fatalf("canceled attach: %v, pending=%v", err, sb.VolumesWant)
	}
	eng.liveVolumeHook = nil
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil || volumeHoldersOf(m, "data") != (volumeHolders{}) {
		t.Fatalf("release pending claim: %v", err)
	}
}

func TestLiveVolumeAdmissionAndState(t *testing.T) {
	for _, state := range []string{"paused", "failed", "archived", "archive policy", "dirty", "writer"} {
		t.Run(state, func(t *testing.T) {
			eng := newFakeEngine()
			catalog := liveCatalog(t)
			m := newVolumeManager(t, eng, catalog)
			sb := mustClaim(t, m, testKey)
			var want error
			switch state {
			case "paused":
				sb.HibernateSnap, want = "sleep", ErrPaused
			case "failed":
				sb.Failed, want = "down", ErrFailed
			case "archived":
				sb.ArchiveCk, want = "ck_old", ErrArchived
			case "archive policy":
				sb.OnExpire, want = types.ExpireArchive, ErrVolumeCapture
			case "dirty":
				if err := markVolumeDirty(catalog[0].Path); err != nil {
					t.Fatal(err)
				}
				want = ErrVolumeNeedsRecovery
			case "writer":
				if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: "rw"}}}); err != nil {
					t.Fatal(err)
				}
				want = ErrVolumeBusy
			}
			before := len(eng.volumeSpecs)
			_, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}})
			if !errors.Is(err, want) || len(eng.volumeSpecs) != before || !slices.Equal(sb.VolumesWant, sb.Volumes) {
				t.Fatalf("got %v want %v", err, want)
			}
		})
	}
}

func TestLiveVolumesRespectOwningTenantAndRemoval(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	catalog[1].Tenants = []string{"beta"}
	m, sb := newTenantVolumeManager(t, eng, catalog)
	for _, auth := range []string{sb.Tenant, ""} {
		if _, err := m.SetVolumes(t.Context(), sb.ID, auth, []types.Volume{{Name: "other"}}); !errors.Is(err, ErrVolumeUnavailable) {
			t.Fatalf("catalog ACL: %v", err)
		}
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteTenant(t.Context(), "acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); err != nil {
		t.Fatalf("held volume after tenant removal: %v", err)
	}
	if _, err := m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Remove: []string{"data"}}); err != nil {
		t.Fatalf("cleanup after tenant removal: %v", err)
	}
}

func TestDesiredVolumesJournalMigration(t *testing.T) {
	for _, tt := range []struct {
		name, field string
		want        []types.Volume
	}{
		{"legacy", "", []types.Volume{{Name: "data", Mount: "/data"}}},
		{"explicit empty", `,"volumes_want":[]`, nil},
		{"desired", `,"volumes_want":[{"name":"other","mount":"/other"}]`, []types.Volume{{Name: "other", Mount: "/other"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := newClaimStore(t.TempDir(), false)
			raw := `{"sb_1":{"id":"sb_1","volumes":[{"name":"data","mount":"/data"}]` + tt.field + `}}`
			if err := os.WriteFile(store.path, []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
			claims, err := store.load()
			if err != nil {
				t.Fatal(err)
			}
			if got := claims["sb_1"].VolumesWant; !slices.Equal(got, tt.want) {
				t.Fatalf("loaded target %v, want %v", got, tt.want)
			}
			if err = store.save(claims); err != nil {
				t.Fatal(err)
			}
			rawBytes, err := os.ReadFile(store.path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(rawBytes), `"volumes_want":`) != (tt.field != "") {
				t.Fatalf("persisted target %s, want field only when it differs", rawBytes)
			}
		})
	}
}

func TestFailedDetachReaddRepairsAppliedMountAfterRestart(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	target := []types.Volume{{Name: "data"}}
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, target); err != nil {
		t.Fatal(err)
	}
	eng.detachVolumeErr = errors.New("eject did not complete")
	view, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, nil)
	if err == nil || len(view.Volumes) != 1 || len(view.VolumesWant) != 0 || view.States[0].State != "detaching" || view.States[0].Error == "" {
		t.Fatalf("detach state=%+v error=%v", view, err)
	}
	m2 := newVolumeManagerAt(t, eng, m.dataDir, catalog)
	if reconcileErr := m2.Reconcile(t.Context()); reconcileErr != nil {
		t.Fatal(reconcileErr)
	}
	before := len(eng.volumeMounts)
	eng.detachVolumeErr = nil
	view, err = m2.SetVolumes(t.Context(), sb.ID, sb.Tenant, target)
	if err != nil || len(eng.volumeMounts) != before+1 || view.States[0].State != "attached" {
		t.Fatalf("readd did not repair: %+v %v", view, err)
	}
	if volumeHoldersOf(m2, "data").readers != 1 {
		t.Fatal("repair changed reservation count")
	}
}

func TestSupersedingTargetPrecleansUncertainAddBeforeJournaling(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	eng.mountVolumeErr = errors.New("mount interrupted")
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); err == nil {
		t.Fatal("expected attach failure")
	}
	eng.mountVolumeErr = nil
	entered, resume := make(chan struct{}), make(chan struct{})
	eng.liveVolumeHook = func(verb string) error {
		if verb == "detach" {
			close(entered)
			<-resume
		}
		return nil
	}
	done := make(chan error, 1)
	go func() {
		_, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "other"}})
		done <- err
	}()
	<-entered
	view, err := m.GetVolumes(sb.ID, sb.Tenant)
	if err != nil || len(view.VolumesWant) != 1 || view.VolumesWant[0].Name != "data" || volumeHoldersOf(m, "data").readers != 1 {
		t.Errorf("forgot uncertain disk before detach: %+v %v", view, err)
	}
	assertLiveJournal(t, m, sb)
	close(resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if volumeHoldersOf(m, "data") != (volumeHolders{}) || volumeHoldersOf(m, "other").readers != 1 {
		t.Fatal("superseding target leaked or lost holds")
	}
}

func TestCanceledVolumeWriterOwnsHardwareUntilReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := newFakeEngine()
		m := newVolumeManager(t, base, liveCatalog(t))
		sb := mustClaim(t, m, testKey)
		eng := newGatedVolumeEngine(base)
		m.eng = eng
		ctx, cancel := context.WithCancel(t.Context())
		first := make(chan error, 1)
		go func() { _, err := m.SetVolumes(ctx, sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); first <- err }()
		<-eng.entered
		cancel()
		waitingCtx, stopWaiting := context.WithTimeout(t.Context(), 25*time.Millisecond)
		defer stopWaiting()
		_, err := m.PatchVolumes(waitingCtx, sb.ID, sb.Tenant, types.VolumePatch{Add: []types.Volume{{Name: "other"}}})
		if !errors.Is(err, context.DeadlineExceeded) || eng.calls.Load() != 1 {
			close(eng.resume)
			t.Fatalf("new writer overtook canceled device call: %v calls=%d", err, eng.calls.Load())
		}
		close(eng.resume)
		if firstErr := <-first; !errors.Is(firstErr, context.Canceled) {
			t.Fatalf("canceled result=%v", firstErr)
		}
		if len(base.volumeMounts) != 0 {
			t.Fatal("canceled writer issued another device call")
		}
		view, err := m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Add: []types.Volume{{Name: "other"}}})
		if err != nil || len(view.Volumes) != 2 {
			t.Fatalf("retry lost prior desired state: %+v %v", view, err)
		}
	})
}

func TestReleaseOwnsUncertainHoldsUntilVMRemoval(t *testing.T) {
	base := newFakeEngine()
	m := newVolumeManager(t, base, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	eng := newGatedVolumeEngine(base)
	m.eng = eng
	base.removeErrFor = sb.VMName
	changed := make(chan error, 1)
	go func() {
		_, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}})
		changed <- err
	}()
	<-eng.entered
	released := make(chan error, 1)
	go func() { released <- m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
	select {
	case err := <-released:
		if err == nil {
			t.Fatal("expected failed VM removal")
		}
	case <-time.After(time.Second):
		close(eng.resume)
		t.Fatal("release waited for volume writer")
	}
	if volumeHoldersOf(m, "data").readers != 1 {
		t.Fatal("unconfirmed removal dropped hold")
	}
	close(eng.resume)
	if err := <-changed; err == nil {
		t.Fatal("removed claim accepted late device result")
	}
	if volumeHoldersOf(m, "data").readers != 1 {
		t.Fatal("late writer freed teardown-owned hold")
	}
	claims, err := m.store.load()
	if err != nil || claims[sb.ID] != nil {
		t.Fatalf("late writer resurrected claim: %v %v", claims, err)
	}
	base.removeErrFor = ""
	m.retryRemovals(t.Context()).Wait()
	if volumeHoldersOf(m, "data") != (volumeHolders{}) {
		t.Fatal("confirmed VM removal leaked hold")
	}
}

func TestConcurrentVolumePatchesUseLatestDesiredSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := newFakeEngine()
		m := newVolumeManager(t, base, liveCatalog(t))
		sb := mustClaim(t, m, testKey)
		eng := newGatedVolumeEngine(base)
		m.eng = eng
		first := make(chan error, 1)
		go func() {
			_, err := m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Add: []types.Volume{{Name: "data"}}})
			first <- err
		}()
		<-eng.entered
		second := make(chan error, 1)
		go func() {
			_, err := m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Add: []types.Volume{{Name: "other"}}})
			second <- err
		}()
		synctest.Wait()
		if len(second) != 0 || eng.calls.Load() != 1 {
			close(eng.resume)
			t.Fatalf("second patch was not queued behind the first: calls=%d", eng.calls.Load())
		}
		close(eng.resume)
		if err := <-first; err != nil {
			t.Fatal(err)
		}
		if err := <-second; err != nil {
			t.Fatal(err)
		}
		view, err := m.GetVolumes(sb.ID, sb.Tenant)
		if err != nil || len(view.Volumes) != 2 || len(view.VolumesWant) != 2 {
			t.Fatalf("lost concurrent patch: %+v %v", view, err)
		}
		for _, name := range []string{"data", "other"} {
			if volumeHoldersOf(m, name).readers != 1 {
				t.Fatal("duplicate hold for " + name)
			}
		}
	})
}

func TestSyncIntentFailureRetainsConservativeDesiredHold(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	m.store.sync = true
	restore := breakClaimStore(t, m)
	view, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}})
	if err == nil || len(view.VolumesWant) != 1 || len(view.Volumes) != 0 || len(eng.volumeSpecs) != 0 || volumeHoldersOf(m, "data").readers != 1 {
		t.Fatalf("ambiguous write lost recovery state: %+v %v", view, err)
	}
	restore()
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, nil); err != nil {
		t.Fatal(err)
	}
	if volumeHoldersOf(m, "data") != (volumeHolders{}) {
		t.Fatal("cleanup leaked conservative hold")
	}
}

func TestFailedReleaseRestoresVolumeHoldOwnership(t *testing.T) {
	for _, action := range []string{"release", "reap"} {
		for _, runFinished := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/finished=%t", action, runFinished), func(t *testing.T) {
				eng := newFakeEngine()
				m := newVolumeManager(t, eng, liveCatalog(t))
				sb := mustClaim(t, m, testKey)
				run := beginAdmittedVolumeRun(t, m, sb)
				m.mu.Lock()
				if action == "reap" {
					sb.Deadline = time.Now().Add(-time.Second)
				}
				m.mu.Unlock()
				restore := breakClaimStore(t, m)
				m.store.writeMu.Lock()
				done := make(chan error, 1)
				go func() {
					if action == "reap" {
						m.reapOnce(t.Context())
						done <- nil
					} else {
						done <- m.Release(t.Context(), sb.ID, Cred{Token: sb.Token})
					}
				}()
				waitFor(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.claimed[sb.ID] == nil })
				if runFinished {
					m.finishVolumeRun(sb, run)
				}
				m.store.writeMu.Unlock()
				err := <-done
				if action == "release" && err == nil {
					t.Fatal("release ignored commit failure")
				}
				m.mu.Lock()
				restored := m.claimed[sb.ID] == sb
				m.mu.Unlock()
				if !restored {
					t.Fatal("failed release did not restore claim")
				}
				if !runFinished {
					m.finishVolumeRun(sb, run)
				}
				if volumeHoldersOf(m, "data") != (volumeHolders{}) {
					t.Fatal("rollback orphaned transient admission hold")
				}
				restore()
				if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); err != nil {
					t.Fatal(err)
				}
				if volumeHoldersOf(m, "data").readers != 1 {
					t.Fatal("retry duplicated admission hold")
				}
			})
		}
	}
}

func TestArchiveRenewRejectsAdmittedUnpublishedVolumes(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	run := beginAdmittedVolumeRun(t, m, sb)
	if _, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Minute, types.ExpireArchive); !errors.Is(err, ErrVolumeCapture) {
		t.Fatalf("archive admitted during volume intent: %v", err)
	}
	m.finishVolumeRun(sb, run)
	if _, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Minute, types.ExpireArchive); err != nil {
		t.Fatalf("archive still blocked after abandoned intent: %v", err)
	}
}

func TestRetriedTargetPersistsIntentBeforeDeviceCalls(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	restore := breakClaimStore(t, m)
	request := []types.Volume{{Name: "data"}}
	for range 2 {
		if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, request); err == nil {
			t.Fatal("expected the intent write to fail")
		}
	}
	if len(eng.volumeSpecs) != 0 {
		t.Fatalf("device calls before the intent was durable: %v", eng.volumeSpecs)
	}
	restore()
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, request); err != nil {
		t.Fatal(err)
	}
	assertLiveJournal(t, m, sb)
}

func TestRollbackClaimTakesOverAdmittedRunHolds(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	run := beginAdmittedVolumeRun(t, m, sb)
	m.rollbackClaim(t.Context(), []*types.Sandbox{sb})
	m.finishVolumeRun(sb, run)
	m.retryRemovals(t.Context()).Wait()
	waitFor(t, m.store.synced)
	if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
		t.Fatalf("rolled-back claim leaked its admitted hold: %+v", got)
	}
}

func TestLiveAttachBoundsWholeDeviceOperation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := newFakeEngine()
		m := newVolumeManager(t, base, liveCatalog(t))
		sb := mustClaim(t, m, testKey)
		eng := &deadlineVolumeEngine{fakeEngine: base, deadlines: make(chan time.Time, 1)}
		m.eng = eng
		start := time.Now()
		if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); err != nil {
			t.Fatal(err)
		}
		if deadline := <-eng.deadlines; !deadline.Equal(start.Add(engine.VolumeMutationTimeout)) {
			t.Fatalf("live attach deadline=%v, want %v", deadline, start.Add(engine.VolumeMutationTimeout))
		}
	})
}

func TestLivenessRefusesAdmittedVolumeRun(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	run := beginAdmittedVolumeRun(t, m, sb)
	killVMM(eng, sb.VMName)
	m.recoverVMM(t.Context(), sb)
	if len(eng.starts) != 0 || !strings.Contains(sb.Failed, "volume mounts") {
		t.Fatalf("cold boot raced volume intent: starts=%v failed=%q", eng.starts, sb.Failed)
	}
	m.finishVolumeRun(sb, run)
}

func TestRemovedTenantCanRemoveReadOnlyVolumeBesideImmutableVolume(t *testing.T) {
	eng := newFakeEngine()
	m, sb := newTenantVolumeManager(t, eng, liveCatalog(t), types.Volume{Name: "other", Mode: "rw"})
	if _, err := m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Add: []types.Volume{{Name: "data"}}}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteTenant(t.Context(), "acme"); err != nil {
		t.Fatal(err)
	}
	view, err := m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Remove: []string{"data"}})
	if err != nil || len(view.Volumes) != 1 || view.Volumes[0].Name != "other" || !view.Volumes[0].RW() {
		t.Fatalf("removed tenant cleanup=%+v %v", view, err)
	}
	if volumeHoldersOf(m, "data") != (volumeHolders{}) || volumeHoldersOf(m, "other").writers != 1 {
		t.Fatal("cleanup altered immutable hold")
	}
}

type gatedVolumeEngine struct {
	*fakeEngine
	entered chan struct{}
	resume  chan struct{}
	calls   atomic.Int32
}

func newGatedVolumeEngine(base *fakeEngine) *gatedVolumeEngine {
	return &gatedVolumeEngine{fakeEngine: base, entered: make(chan struct{}), resume: make(chan struct{})}
}

func (e *gatedVolumeEngine) DiskAttach(ctx context.Context, vmName string, spec engine.VolumeSpec) error {
	if e.calls.Add(1) == 1 {
		close(e.entered)
		<-e.resume
		ctx = context.WithoutCancel(ctx)
	}
	return e.fakeEngine.DiskAttach(ctx, vmName, spec)
}

type deadlineVolumeEngine struct {
	*fakeEngine
	deadlines chan time.Time
}

func (e *deadlineVolumeEngine) DiskAttach(ctx context.Context, vmName string, spec engine.VolumeSpec) error {
	deadline, _ := ctx.Deadline()
	e.deadlines <- deadline
	return e.fakeEngine.DiskAttach(ctx, vmName, spec)
}

func liveCatalog(t *testing.T) []config.VolumeSpec {
	t.Helper()
	return []config.VolumeSpec{
		{Name: "data", Path: writeVolumeImage(t, "data.img", "data"), Writable: true},
		{Name: "other", Path: writeVolumeImage(t, "other.img", "other"), Writable: true},
	}
}

func newTenantVolumeManager(t *testing.T, eng *fakeEngine, catalog []config.VolumeSpec, volumes ...types.Volume) (*Manager, *types.Sandbox) {
	t.Helper()
	cfg := &config.Config{DataDir: t.TempDir(), APIToken: "root", Volumes: catalog}
	m, err := NewManager(t.Context(), cfg, eng, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	putTenants(t, m, tenants.TenantSpec{Name: "acme", Token: "acme-token"})
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "acme", Volumes: volumes})
	if err != nil {
		t.Fatal(err)
	}
	return m, sb
}

func assertLiveJournal(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	claims, err := newClaimStore(m.dataDir, false).load()
	if err != nil {
		t.Fatal(err)
	}
	got := claims[sb.ID]
	if got == nil || !slices.Equal(got.Volumes, sb.Volumes) {
		t.Fatalf("journal volumes: %+v", got)
	}
	if !slices.Equal(got.VolumesWant, sb.VolumesWant) {
		t.Fatalf("journal target: %+v, want %+v", got.VolumesWant, sb.VolumesWant)
	}
	if _, err := os.Stat(m.store.path); err != nil {
		t.Fatal(err)
	}
}

func beginAdmittedVolumeRun(t *testing.T, m *Manager, sb *types.Sandbox) *volumeRun {
	t.Helper()
	_, run, err := m.beginVolumeRun(t.Context(), sb)
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	transient := []types.Volume{{Name: "data", Mount: "/volumes/data"}}
	if err = m.reserveVolumes(transient); err != nil {
		t.Fatal(err)
	}
	run.holds = append(run.holds, transient...)
	return run
}

func breakClaimStore(t *testing.T, m *Manager) (restore func()) {
	t.Helper()
	tmp := m.store.path + ".tmp"
	m.store.writeMu.Lock()
	err := os.Mkdir(tmp, 0o700)
	m.store.writeMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		m.store.writeMu.Lock()
		err := os.Remove(tmp)
		m.store.writeMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return !m.store.recommitting.Load() })
	}
}
