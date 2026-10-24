package pool

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestDetachedVolumeRetainsRecoveryProtectionUntilCommit(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); err != nil {
		t.Fatal(err)
	}
	m.store.recommitting.Store(true)
	tmp := m.store.path + ".tmp"
	eng.liveVolumeHook = func(verb string) error {
		if verb == "detach" {
			return os.Mkdir(tmp, 0o700)
		}
		return nil
	}
	view, err := m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Remove: []string{"data"}})
	eng.liveVolumeHook = nil
	if err == nil || len(view.Volumes) != 0 || len(view.VolumesWant) != 0 {
		t.Fatalf("detached result = %+v, %v", view, err)
	}
	claims, err := newClaimStore(m.dataDir, false).load()
	if err != nil || claims[sb.ID] == nil || len(claims[sb.ID].Volumes) != 1 {
		t.Fatalf("failed write did not leave old recovery reference: %+v, %v", claims, err)
	}
	if _, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Errorf("writer admitted while old claim still references detached volume: %v", err)
	}
	if _, err = m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "unretired", ""); !errors.Is(err, ErrVolumeCapture) {
		t.Errorf("capture passed before recovery reference retired: %v", err)
	}
	if err = os.Remove(tmp); err != nil {
		t.Fatal(err)
	}
	m2 := newVolumeManagerAt(t, eng, m.dataDir, catalog)
	if err = m2.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err = m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("restart lost old claim's reader exclusion: %v", err)
	}
	if _, err = m2.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Remove: []string{"data"}}); err != nil {
		t.Fatalf("retry after restart: %v", err)
	}
	if _, err = m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); err != nil {
		t.Fatalf("writer still blocked after successful retirement: %v", err)
	}
}

func TestDetachedVolumeOwnRetryAndLaterAcknowledgment(t *testing.T) {
	for _, retry := range []string{"empty", "same-shape", "unrelated-snapshot"} {
		t.Run(retry, func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, liveCatalog(t))
			sb := mustClaim(t, m, testKey)
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); err != nil {
				t.Fatal(err)
			}
			m.store.recommitting.Store(true)
			tmp := m.store.path + ".tmp"
			eng.liveVolumeHook = func(verb string) error {
				if verb == "detach" {
					return os.Mkdir(tmp, 0o700)
				}
				return nil
			}
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err == nil {
				t.Fatal("detach result unexpectedly persisted")
			}
			eng.liveVolumeHook = nil
			if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
				t.Fatalf("physical count retained after detach: %+v", got)
			}
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err == nil {
				t.Fatal("empty retry acknowledged an unretired file")
			}
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mount: "/different"}}); !errors.Is(err, ErrBadVolume) {
				t.Fatalf("recovery-only shape changed: %v", err)
			}
			view, err := m.GetVolumes(sb.ID, "")
			if err != nil || len(view.States) != 1 || view.States[0].State != types.VolumeDetaching {
				t.Fatalf("unretired view: %+v, %v", view, err)
			}
			if err := os.Remove(tmp); err != nil {
				t.Fatal(err)
			}
			switch retry {
			case "empty":
				_, err = m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{})
			case "same-shape":
				for range 2 {
					if _, err = m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); err != nil {
						t.Fatal(err)
					}
					if got := volumeHoldersOf(m, "data"); got.readers != 1 || got.writers != 0 {
						t.Fatalf("own retry counts: %+v", got)
					}
				}
				_, err = m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{})
			case "unrelated-snapshot":
				_, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{})
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
				t.Fatalf("retired count: %+v", got)
			}
			if _, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); err != nil {
				t.Fatalf("writer after acknowledgment: %v", err)
			}
		})
	}
}

func TestFailedClaimIntroductionAfterRenameRemainsProtected(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	m.store.recommitting.Store(true)
	calls := 0
	m.store.replaceFile = func(raw []byte) error {
		calls++
		if calls == 1 {
			if err := m.store.replace(raw); err != nil {
				return err
			}
			return errors.New("directory sync failed after rename")
		}
		return errors.New("replacement failed before rename")
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); err == nil {
		t.Fatal("expected post-rename intent error")
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err == nil {
		t.Fatal("expected empty snapshot error")
	}
	if len(sb.Volumes) != 0 || len(sb.VolumesWant) != 0 {
		t.Fatalf("memory not empty: %+v", sb)
	}
	if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
		t.Fatalf("unexpected physical count: %+v", got)
	}
	claims, err := newClaimStore(m.dataDir, false).load()
	if err != nil || len(claims[sb.ID].VolumesWant) != 1 {
		t.Fatalf("actual recovery file: %+v, %v", claims, err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("writer admitted after uncertain introduction: %v", err)
	}
	if _, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "uncertain-introduction", ""); !errors.Is(err, ErrVolumeCapture) {
		t.Fatalf("capture admitted: %v", err)
	}
	m2 := newVolumeManagerAt(t, eng, m.dataDir, catalog)
	if err := m2.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("restart admitted writer: %v", err)
	}
	m.store.replaceFile = nil
	if err := m.store.commit(m.store.mark()); err != nil {
		t.Fatal(err)
	}
	if len(m.store.recoveryVolumes()[sb.ID]) != 0 {
		t.Fatal("successful whole snapshot did not retire attempted row")
	}
}

func TestClaimRecoveryReferencesSurviveResetAndAreCopied(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	sb := &types.Sandbox{ID: "owner", Volumes: []types.Volume{{Name: "data", Mount: "/data"}}}
	if err := s.commit(s.set(sb)); err != nil {
		t.Fatal(err)
	}
	s = newClaimStore(filepath.Dir(s.path), false)
	if _, err := s.load(); err != nil {
		t.Fatal(err)
	}
	s.reset(map[string]*types.Sandbox{})
	refs := s.recoveryVolumes()
	refs[sb.ID][0].Name = "caller-copy"
	if got := s.recoveryVolumes()[sb.ID]; len(got) != 1 || got[0].Name != "data" {
		t.Fatalf("reset or caller erased actual file: %+v", got)
	}
	if err := s.commit(s.mark()); err != nil {
		t.Fatal(err)
	}
	if len(s.recoveryVolumes()) != 0 {
		t.Fatal("empty snapshot did not retire file references")
	}
}
