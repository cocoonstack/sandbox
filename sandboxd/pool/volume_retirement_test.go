package pool

import (
	"errors"
	"os"
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
