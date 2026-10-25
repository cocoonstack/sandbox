package pool

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

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

func TestRuntimeRegistrationRejectsInvalidTenantNames(t *testing.T) {
	for _, tenant := range []string{"", "has space", "_leading"} {
		t.Run(tenant, func(t *testing.T) {
			m := newVolumeManager(t, newFakeEngine(), nil)
			if err := m.RegisterVolume(t.Context(), "data", types.VolumeRegistration{Path: "/images/data", Tenants: []string{tenant}}); err == nil {
				t.Fatalf("runtime registration accepted invalid tenant %q", tenant)
			}
		})
	}
}
