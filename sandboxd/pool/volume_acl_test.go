package pool

import (
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestRuntimeCatalogDefaultACL(t *testing.T) {
	for _, tenants := range [][]string{nil, {}} {
		t.Run("empty", func(t *testing.T) {
			eng := newFakeEngine()
			catalog := liveCatalog(t)
			m, _ := newTenantVolumeManager(t, eng, catalog)
			spec := types.VolumeRegistration{Path: writeVolumeImage(t, "home.img", "private"), Writable: true, Tenants: tenants}
			if err := m.RegisterVolume(t.Context(), "home", spec); err != nil {
				t.Fatal(err)
			}
			for _, current := range []*Manager{m, restartACLManager(t, eng, m.dataDir, catalog)} {
				if !slices.ContainsFunc(current.Volumes("acme", nil), func(v types.VolumeInfo) bool { return v.Name == "home" }) {
					t.Error("public runtime volume is hidden from a tenant")
				}
				if !slices.ContainsFunc(current.Volumes("", nil), func(v types.VolumeInfo) bool { return v.Name == "home" }) {
					t.Error("operator cannot see runtime volume")
				}
				if _, err := current.VolumePlacement(testKey, "acme", []string{"home"}); err != nil {
					t.Errorf("tenant placement: %v", err)
				}
				if local, err := current.VolumePlacement(testKey, "", []string{"home"}); err != nil || !local {
					t.Errorf("operator placement: %v %v", local, err)
				}
				tenantClaim, err := current.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "acme", Volumes: []types.Volume{{Name: "home", Mount: "/home"}}})
				if err != nil {
					t.Fatalf("tenant initial attachment: %v", err)
				}
				if err = current.Release(t.Context(), tenantClaim.ID, Cred{Token: tenantClaim.Token}); err != nil {
					t.Fatal(err)
				}
				if _, err = current.resolveVolumes(t.Context(), "acme", []types.Volume{{Name: "data"}}); err != nil {
					t.Errorf("public static volume changed: %v", err)
				}
				rootClaim, err := current.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "home", Mount: "/home"}}})
				if err != nil {
					t.Fatalf("operator initial attachment: %v", err)
				}
				if err := current.Release(t.Context(), rootClaim.ID, Cred{Token: rootClaim.Token}); err != nil {
					t.Fatalf("operator initial claim cleanup: %v", err)
				}
			}
		})
	}
}

func TestRuntimeCatalogOperatorKeepsExplicitOwnerACL(t *testing.T) {
	m, sb := newTenantVolumeManager(t, newFakeEngine(), nil)
	if err := m.RegisterVolume(t.Context(), "home", types.VolumeRegistration{Path: writeVolumeImage(t, "home.img", "private"), Tenants: []string{"other"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "home"}}); !errors.Is(err, ErrVolumeUnavailable) {
		t.Fatalf("operator bypassed explicit claim-owner ACL: %v", err)
	}
}

func TestRuntimeCatalogPendingWritePreservesPrivateNameMasking(t *testing.T) {
	for _, operation := range []string{"register", "unregister"} {
		t.Run(operation, func(t *testing.T) {
			eng := newFakeEngine()
			m, sb := newTenantVolumeManager(t, eng, liveCatalog(t))
			spec := types.VolumeRegistration{Path: writeVolumeImage(t, "private", "bytes"), Tenants: []string{"other"}}
			if operation == "unregister" {
				if err := m.RegisterVolume(t.Context(), "private", spec); err != nil {
					t.Fatal(err)
				}
			}
			blockCatalogRename(t, m)
			var err error
			if operation == "unregister" {
				err = m.UnregisterVolume(t.Context(), "private")
			} else {
				err = m.RegisterVolume(t.Context(), "private", spec)
			}
			if !errors.Is(err, ErrVolumeCatalogUnavailable) {
				t.Fatalf("obstructed catalog write: %v", err)
			}
			for _, requested := range [][]types.Volume{
				{{Name: "private"}},
				{{Name: "data"}, {Name: "private"}},
				{{Name: "private"}, {Name: "data"}},
			} {
				missing := slices.Clone(requested)
				for i := range missing {
					if missing[i].Name == "private" {
						missing[i].Name = "missing"
					}
				}
				_, deniedErr := m.SetVolumes(t.Context(), sb.ID, "acme", requested)
				_, missingErr := m.SetVolumes(t.Context(), sb.ID, "acme", missing)
				if !errors.Is(deniedErr, ErrVolumeUnavailable) || !errors.Is(missingErr, ErrVolumeUnavailable) || deniedErr.Error() != missingErr.Error() {
					t.Errorf("private name distinguishable for %+v: denied=%v missing=%v", requested, deniedErr, missingErr)
				}
			}
			if view, err := m.GetVolumes(sb.ID, "acme"); err != nil || len(view.Volumes) != 0 || len(view.VolumesWant) != 0 {
				t.Fatalf("refused request changed claim volumes: %+v, %v", view, err)
			}
			if len(eng.volumeSpecs) != 0 {
				t.Fatal("refused request reached device attachment")
			}
			for _, name := range []string{"data", "private", "missing"} {
				if got := volumeHoldersOf(m, name); got != (volumeHolders{}) {
					t.Fatalf("refused request reserved %s: %+v", name, got)
				}
			}
		})
	}
}

func TestRuntimeCatalogRecoveryReferenceDoesNotAuthorizeAttach(t *testing.T) {
	for _, operation := range []string{"put", "patch"} {
		t.Run(operation, func(t *testing.T) {
			eng := newFakeEngine()
			m, sb := newTenantVolumeManager(t, eng, nil)
			if err := m.RegisterVolume(t.Context(), "home", types.VolumeRegistration{Path: writeVolumeImage(t, "home.img", "private"), Writable: true, Tenants: []string{"acme"}}); err != nil {
				t.Fatal(err)
			}
			want := []types.Volume{{Name: "home", Mount: "/home"}}
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", want); err != nil {
				t.Fatal(err)
			}
			if _, err := m.SetVolumes(t.Context(), sb.ID, "acme", want); err != nil {
				t.Fatalf("still-held retry: %v", err)
			}
			m.store.recommitting.Store(true)
			tmp := m.store.path + ".tmp"
			eng.liveVolumeHook = func(verb string) error {
				if verb == "detach" {
					return os.Mkdir(tmp, 0o700)
				}
				return nil
			}
			view, err := m.SetVolumes(t.Context(), sb.ID, "acme", nil)
			eng.liveVolumeHook = nil
			if err == nil || len(view.Volumes) != 0 || len(view.VolumesWant) != 0 {
				t.Fatalf("detached result: %+v, %v", view, err)
			}
			claims, err := newClaimStore(m.dataDir, false).load()
			if err != nil || claims[sb.ID] == nil || !slices.Equal(claims[sb.ID].Volumes, want) {
				t.Fatalf("old durable attachment missing: %+v, %v", claims, err)
			}
			if _, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "home", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
				t.Fatalf("recovery reference lost writer exclusion: %v", err)
			}
			if _, err = m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "unretired", ""); !errors.Is(err, ErrVolumeCapture) {
				t.Fatalf("capture ignored recovery reference: %v", err)
			}
			if err = m.UnregisterVolume(t.Context(), "home"); !errors.Is(err, ErrVolumeRegistrationConflict) {
				t.Fatalf("unregister ignored recovery reference: %v", err)
			}
			if err = os.Remove(tmp); err != nil {
				t.Fatal(err)
			}
			m.mu.Lock()
			entry := *m.catalog.entries["home"]
			entry.tenants = []string{"other"}
			m.catalog.entries["home"] = &entry
			m.mu.Unlock()
			before := len(eng.volumeSpecs)
			switch operation {
			case "put":
				_, err = m.SetVolumes(t.Context(), sb.ID, "acme", want)
			case "patch":
				_, err = m.PatchVolumes(t.Context(), sb.ID, "acme", types.VolumePatch{Add: want})
			}
			if !errors.Is(err, ErrVolumeUnavailable) {
				t.Fatalf("recovery-only reference authorized tenant attachment: %v", err)
			}
			if len(eng.volumeSpecs) != before {
				t.Fatal("rejected tenant reached device attachment")
			}
			m.mu.Lock()
			entry.tenants = []string{"acme"}
			m.catalog.entries["home"] = &entry
			m.mu.Unlock()
			if _, err = m.SetVolumes(t.Context(), sb.ID, "", want); err != nil {
				t.Fatalf("operator reattach: %v", err)
			}
			if _, err = m.SetVolumes(t.Context(), sb.ID, "acme", nil); err != nil {
				t.Fatalf("tenant cleanup after operator reattach: %v", err)
			}
		})
	}
}

func restartACLManager(t *testing.T, eng *fakeEngine, dataDir string, catalog []config.VolumeSpec) *Manager {
	t.Helper()
	cfg := &config.Config{DataDir: dataDir, APIToken: "root", Volumes: catalog}
	m, err := NewManager(t.Context(), cfg, eng, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	return m
}
