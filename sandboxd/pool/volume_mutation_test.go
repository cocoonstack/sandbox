package pool

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestLiveVolumesCommitAndRetry(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	cred := Cred{Token: sb.Token}
	request := []types.Volume{{Name: "data"}, {Name: "other", Mount: "/models"}}
	for range 2 {
		got, err := m.AttachVolumes(t.Context(), sb.ID, cred, request)
		if err != nil || len(got) != 2 || sb.PendingVolume != nil {
			t.Fatalf("attach = %v, %v; pending=%v", got, err, sb.PendingVolume)
		}
		got[0].Name = "caller copy"
	}
	if got := volumeHoldersOf(m, "data"); got.readers != 1 {
		t.Fatalf("duplicate reservation: %+v", got)
	}
	assertLiveJournal(t, m, sb)
	for range 2 {
		got, err := m.DetachVolumes(t.Context(), sb.ID, cred, []string{"data", "other"})
		if err != nil || len(got) != 0 {
			t.Fatalf("detach = %v, %v", got, err)
		}
	}
	assertLiveJournal(t, m, sb)
	if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
		t.Fatalf("leaked holds: %+v", got)
	}
	if _, err := m.Checkpoint(t.Context(), sb.ID, cred, "after-detach", ""); err != nil {
		t.Fatalf("capture after detach: %v", err)
	}
}

func TestLiveAttachPreservesWritableAndAttachOnlyVolumes(t *testing.T) {
	for _, attachOnly := range []bool{false, true} {
		eng := newFakeEngine()
		m := newVolumeManager(t, eng, liveCatalog(t))
		sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "other", Mode: "rw", AttachOnly: attachOnly}}})
		if err != nil {
			t.Fatal(err)
		}
		original := sb.Volumes[0]
		cred := Cred{Token: sb.Token}
		if _, err := m.AttachVolumes(t.Context(), sb.ID, cred, []types.Volume{{Name: "data"}}); err != nil {
			t.Fatal(err)
		}
		if sb.Volumes[0] != original {
			t.Fatalf("changed existing volume: %v -> %v", original, sb.Volumes[0])
		}
		if _, err := m.AttachVolumes(t.Context(), sb.ID, cred, []types.Volume{{Name: "other", Mount: original.Mount}}); !errors.Is(err, ErrBadVolume) {
			t.Fatalf("accepted mode change: %v", err)
		}
		if _, err := m.DetachVolumes(t.Context(), sb.ID, cred, []string{"data", "other"}); !errors.Is(err, ErrBadVolume) || len(sb.Volumes) != 2 {
			t.Fatalf("writable detach changed the batch: %v", err)
		}
	}
}

func TestLiveVolumeValidationPrecedesDeviceCalls(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	cred := Cred{Token: sb.Token}
	for _, request := range [][]types.Volume{
		{{Name: "data", Mode: "rw"}},
		{{Name: "data", AttachOnly: true}},
		{{Name: "data", Mount: "/etc"}},
		{{Name: "data"}, {Name: "data"}},
		{{Name: "data", Mount: "/a"}, {Name: "other", Mount: "/a/b"}},
		{{Name: "missing"}},
	} {
		if _, err := m.AttachVolumes(t.Context(), sb.ID, cred, request); !errors.Is(err, ErrBadVolume) {
			t.Fatalf("request %v: %v", request, err)
		}
	}
	if _, err := m.AttachVolumes(t.Context(), sb.ID, Cred{Token: "wrong"}, []types.Volume{{Name: "data"}}); !errors.Is(err, ErrUnknownSandbox) {
		t.Fatalf("wrong token: %v", err)
	}
	if _, err := m.DetachVolumes(t.Context(), sb.ID, cred, []string{"data", "data"}); !errors.Is(err, ErrBadVolume) {
		t.Fatalf("duplicate detach: %v", err)
	}
	if len(eng.volumeSpecs) != 0 || sb.PendingVolume != nil {
		t.Fatal("invalid request reached engine")
	}
}

func TestLiveAttachFailureSurvivesRestartAndCanBeDetached(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	eng.mountVolumeErr = errors.New("guest unavailable after host attach")
	if _, err := m.AttachVolumes(t.Context(), sb.ID, Cred{Token: sb.Token}, []types.Volume{{Name: "data"}}); err == nil {
		t.Fatal("want mount failure")
	}
	assertLiveJournal(t, m, sb)
	if sb.PendingVolume == nil || volumeHoldersOf(m, "data").readers != 1 {
		t.Fatal("lost pending device or hold")
	}
	m2 := newVolumeManagerAt(t, eng, m.dataDir, catalog)
	if err := m2.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if volumeHoldersOf(m2, "data").readers != 1 {
		t.Fatal("restart lost pending hold")
	}
	cred := Cred{Token: sb.Token}
	if _, err := m2.Checkpoint(t.Context(), sb.ID, cred, "pending", ""); !errors.Is(err, ErrVolumeCapture) {
		t.Fatalf("captured pending device: %v", err)
	}
	if _, err := m2.AttachVolumes(t.Context(), sb.ID, cred, []types.Volume{{Name: "other"}}); !errors.Is(err, ErrVolumePending) {
		t.Fatalf("overwrote pending operation: %v", err)
	}
	eng.detachVolumeErr = errors.New("host detach failed")
	if _, err := m2.DetachVolumes(t.Context(), sb.ID, cred, []string{"data"}); err == nil || volumeHoldersOf(m2, "data").readers != 1 {
		t.Fatal("failed detach released its hold")
	}
	eng.detachVolumeErr = nil
	if _, err := m2.DetachVolumes(t.Context(), sb.ID, cred, []string{"data"}); err != nil {
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
			cred := Cred{Token: sb.Token}
			request := []types.Volume{{Name: "data"}}
			if stage == "detach result" {
				if _, err := m.AttachVolumes(t.Context(), sb.ID, cred, request); err != nil {
					t.Fatal(err)
				}
			}
			path := m.store.path + ".tmp"
			breakStore := func() {
				m.store.writeMu.Lock()
				defer m.store.writeMu.Unlock()
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if stage == "intent" {
				breakStore()
			} else {
				eng.liveVolumeHook = func(string) error { breakStore(); return nil }
			}
			var err error
			if stage == "detach result" {
				_, err = m.DetachVolumes(t.Context(), sb.ID, cred, []string{"data"})
			} else {
				_, err = m.AttachVolumes(t.Context(), sb.ID, cred, request)
			}
			if err == nil {
				t.Fatal("expected persistence failure")
			}
			m.store.writeMu.Lock()
			removeErr := os.Remove(path)
			m.store.writeMu.Unlock()
			if removeErr != nil {
				t.Fatal(removeErr)
			}
			eng.liveVolumeHook = nil
			if stage == "intent" {
				if sb.PendingVolume != nil || len(eng.volumeSpecs) != 0 || volumeHoldersOf(m, "data") != (volumeHolders{}) {
					t.Fatal("failed intent must not reach the engine or retain a hold")
				}
				return
			}
			if sb.PendingVolume == nil || volumeHoldersOf(m, "data").readers != 1 {
				t.Fatal("uncommitted physical result lost its intent or hold")
			}
			if stage == "detach result" {
				_, err = m.DetachVolumes(t.Context(), sb.ID, cred, []string{"data"})
			} else {
				_, err = m.AttachVolumes(t.Context(), sb.ID, cred, request)
			}
			if err != nil {
				t.Fatal(err)
			}
			assertLiveJournal(t, m, sb)
		})
	}
}

func TestLiveBatchPartialFailureRetriesWithoutDoubleHolds(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	cred := Cred{Token: sb.Token}
	request := []types.Volume{{Name: "data"}, {Name: "other"}}
	eng.diskAttachErrFor = "other"
	if _, err := m.AttachVolumes(t.Context(), sb.ID, cred, request); err == nil {
		t.Fatal("want partial failure")
	}
	if len(sb.Volumes) != 1 || sb.PendingVolume == nil || sb.PendingVolume.Volume.Name != "other" {
		t.Fatalf("partial state: %v %v", sb.Volumes, sb.PendingVolume)
	}
	eng.diskAttachErrFor = ""
	if _, err := m.AttachVolumes(t.Context(), sb.ID, cred, request); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"data", "other"} {
		if volumeHoldersOf(m, name).readers != 1 {
			t.Fatal("retry double-reserved " + name)
		}
	}
}

func TestLiveVolumeSerializesReleaseAndCapture(t *testing.T) {
	for _, action := range []string{"release", "checkpoint", "fork", "promote", "hibernate", "reap"} {
		t.Run(action, func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, liveCatalog(t))
			sb := mustClaim(t, m, testKey)
			cred := Cred{Token: sb.Token}
			entered, resume := make(chan struct{}), make(chan struct{})
			eng.liveVolumeHook = func(string) error { close(entered); <-resume; return nil }
			attached := make(chan error, 1)
			go func() {
				_, err := m.AttachVolumes(t.Context(), sb.ID, cred, []types.Volume{{Name: "data"}})
				attached <- err
			}()
			<-entered
			// The durable intent must precede any physical operation.
			assertLiveJournal(t, m, sb)
			done := make(chan error, 1)
			go func() {
				var err error
				switch action {
				case "release":
					err = m.Release(t.Context(), sb.ID, cred)
				case "checkpoint":
					_, err = m.Checkpoint(t.Context(), sb.ID, cred, "race", "")
				case "fork":
					_, err = m.Fork(t.Context(), sb.ID, cred, 1, 0, "", "")
				case "promote":
					_, _, err = m.Promote(t.Context(), sb.ID, cred, "race", "")
				case "hibernate":
					err = m.Hibernate(t.Context(), sb.ID, cred)
				case "reap":
					m.mu.Lock()
					sb.Deadline = time.Now().Add(-time.Second)
					m.mu.Unlock()
					m.reapOnce(t.Context())
				}
				done <- err
			}()
			if sb.VolumeMu.TryLock() {
				sb.VolumeMu.Unlock()
				t.Fatal("volume mutation did not exclude release")
			}
			if sb.Transition.TryLock() {
				sb.Transition.Unlock()
				t.Fatal("volume mutation did not exclude capture")
			}
			if len(eng.removedNames()) != 0 || len(eng.snapSaves) != 0 {
				t.Fatal("teardown or capture overlapped device mutation")
			}
			close(resume)
			if err := <-attached; err != nil {
				t.Fatal(err)
			}
			err := <-done
			if action != "release" && action != "reap" && !errors.Is(err, ErrVolumeCapture) {
				t.Fatalf("capture: %v", err)
			}
			if action == "release" && (err != nil || volumeHoldersOf(m, "data") != (volumeHolders{})) {
				t.Fatalf("release: %v", err)
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
	_, err := m.AttachVolumes(ctx, sb.ID, Cred{Token: sb.Token}, []types.Volume{{Name: "data"}})
	if !errors.Is(err, context.Canceled) || sb.PendingVolume == nil {
		t.Fatalf("canceled attach: %v, pending=%v", err, sb.PendingVolume)
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
			_, err := m.AttachVolumes(t.Context(), sb.ID, Cred{Token: sb.Token}, []types.Volume{{Name: "data"}})
			if !errors.Is(err, want) || len(eng.volumeSpecs) != before || sb.PendingVolume != nil {
				t.Fatalf("got %v want %v", err, want)
			}
		})
	}
}

func TestLiveVolumesRespectOwningTenantAndRemoval(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	catalog[1].Tenants = []string{"beta"}
	m, err := NewManager(t.Context(), &config.Config{DataDir: t.TempDir(), APIToken: "root", Volumes: catalog}, eng, testSecrets(t))
	if err != nil {
		t.Fatal(err)
	}
	putTenants(t, m, config.TenantSpec{Name: "acme", Token: "acme-token"})
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	cred := Cred{Token: sb.Token}
	for _, auth := range []Cred{cred, {Operator: true}} {
		if _, err := m.AttachVolumes(t.Context(), sb.ID, auth, []types.Volume{{Name: "other"}}); !errors.Is(err, ErrVolumeUnavailable) {
			t.Fatalf("catalog ACL: %v", err)
		}
	}
	if _, err := m.AttachVolumes(t.Context(), sb.ID, cred, []types.Volume{{Name: "data"}}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteTenant(t.Context(), "acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AttachVolumes(t.Context(), sb.ID, cred, []types.Volume{{Name: "data"}}); !errors.Is(err, ErrTenantRemoved) {
		t.Fatalf("removed tenant: %v", err)
	}
	if _, err := m.DetachVolumes(t.Context(), sb.ID, cred, []string{"data"}); err != nil {
		t.Fatalf("cleanup after tenant removal: %v", err)
	}
}

func liveCatalog(t *testing.T) []config.VolumeSpec {
	t.Helper()
	return []config.VolumeSpec{
		{Name: "data", Path: writeVolumeImage(t, "data.img", "data"), Writable: true},
		{Name: "other", Path: writeVolumeImage(t, "other.img", "other"), Writable: true},
	}
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
	if (got.PendingVolume == nil) != (sb.PendingVolume == nil) || got.PendingVolume != nil && *got.PendingVolume != *sb.PendingVolume {
		t.Fatalf("journal intent: %+v, want %+v", got.PendingVolume, sb.PendingVolume)
	}
	if _, err := os.Stat(m.store.path); err != nil {
		t.Fatal(err)
	}
}
