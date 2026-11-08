package pool

import (
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestRuntimeCatalogEmptyACLIsPublic(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, nil)
	spec := types.VolumeRegistration{Path: writeVolumeImage(t, "home.img", "private"), Writable: true, Tenants: []string{}}
	if err := m.RegisterVolume(t.Context(), "home", spec); err != nil {
		t.Fatalf("RegisterVolume: %v", err)
	}
	for _, current := range []*Manager{m, newVolumeManagerAt(t, eng, m.dataDir, nil)} {
		if got := current.Volumes("acme", nil); len(got) != 1 || got[0].Name != "home" {
			t.Fatalf("public runtime volume hidden from a tenant: %+v", got)
		}
		if local, err := current.VolumePlacement(testKey, "acme", []string{"home"}); err != nil || !local {
			t.Fatalf("tenant placement: %v %v", local, err)
		}
		sb, err := current.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "acme", Volumes: []types.Volume{{Name: "home", Mount: "/home"}}})
		if err != nil {
			t.Fatalf("tenant attachment: %v", err)
		}
		if err = current.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
			t.Fatalf("Release: %v", err)
		}
	}
}
