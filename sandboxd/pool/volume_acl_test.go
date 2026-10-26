package pool

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestRuntimeCatalogEmptyACLIsPublic(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, nil)
	spec := types.VolumeRegistration{Path: writeVolumeImage(t, "home.img", "private"), Writable: true, Tenants: []string{}}
	if err := m.RegisterVolume(t.Context(), "home", spec); err != nil {
		t.Fatal(err)
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
			t.Fatal(err)
		}
	}
}

func TestInvalidStaticTenantListNeverBecomesPublic(t *testing.T) {
	path := writeVolumeImage(t, "data", "private contents")
	file := filepath.Join(t.TempDir(), "config.json")
	raw := fmt.Sprintf(`{"pools":[],"volumes":[{"name":"data","path":%q,"tenants":["has space"]}]}`, path)
	if err := os.WriteFile(file, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(file)
	if err != nil {
		t.Fatalf("legacy config stopped booting: %v", err)
	}
	m := newVolumeManager(t, newFakeEngine(), cfg.Volumes)
	if got := m.Volumes("acme", nil); len(got) != 0 {
		t.Fatalf("invalid restricted ACL became public: %+v", got)
	}
	if err = m.RegisterVolume(t.Context(), "other", types.VolumeRegistration{Path: path + ".other"}); err != nil {
		t.Fatalf("invalid legacy ACL blocked unrelated registration: %v", err)
	}
}
