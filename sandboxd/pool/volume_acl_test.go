package pool

import (
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestUnsetVolumeACLRejectsAccess(t *testing.T) {
	var acl volumeACL
	for _, caller := range []string{"", "acme"} {
		for _, owner := range []string{"", "acme"} {
			if acl.allowedFor(volumeAccessFor(caller, owner)) {
				t.Errorf("unset volume ACL allowed caller %q for owner %q", caller, owner)
			}
		}
	}
}

func TestRuntimeCatalogDefaultACL(t *testing.T) {
	requireCatalogRoot(t)
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
				if slices.ContainsFunc(current.Volumes("acme", nil), func(v types.VolumeInfo) bool { return v.Name == "home" }) {
					t.Error("runtime volume with no tenant grants is visible to a tenant")
				}
				if !slices.ContainsFunc(current.Volumes("", nil), func(v types.VolumeInfo) bool { return v.Name == "home" }) {
					t.Error("operator cannot see runtime volume")
				}
				if _, err := current.VolumePlacement(testKey, "acme", []string{"home"}); !errors.Is(err, ErrVolumeUnavailable) {
					t.Errorf("tenant placement: %v", err)
				}
				if local, err := current.VolumePlacement(testKey, "", []string{"home"}); err != nil || !local {
					t.Errorf("operator placement: %v %v", local, err)
				}
				if _, err := current.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "acme", Volumes: []types.Volume{{Name: "home", Mode: types.VolumeModeRW, AttachOnly: true}}}); !errors.Is(err, ErrVolumeUnavailable) {
					t.Errorf("tenant initial attachment: %v", err)
				}
				if _, err := current.resolveVolumes(t.Context(), "acme", []types.Volume{{Name: "data"}}); err != nil {
					t.Errorf("public static volume changed: %v", err)
				}
				rootClaim, err := current.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "home", Mode: types.VolumeModeRW, AttachOnly: true}}})
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

func TestRuntimeCatalogOperatorLiveAttachment(t *testing.T) {
	requireCatalogRoot(t)
	for _, patch := range []bool{false, true} {
		t.Run(map[bool]string{false: "put", true: "patch"}[patch], func(t *testing.T) {
			eng := newFakeEngine()
			m, sb := newTenantVolumeManager(t, eng, nil)
			if err := m.RegisterVolume(t.Context(), "home", types.VolumeRegistration{Path: writeVolumeImage(t, "home.img", "private"), Writable: true}); err != nil {
				t.Fatal(err)
			}
			want := []types.Volume{{Name: "home", Mode: types.VolumeModeRW, AttachOnly: true}}
			change := func(caller string, volumes []types.Volume) error {
				if patch {
					_, err := m.PatchVolumes(t.Context(), sb.ID, caller, types.VolumePatch{Add: volumes})
					return err
				}
				_, err := m.SetVolumes(t.Context(), sb.ID, caller, volumes)
				return err
			}
			if err := change("acme", want); !errors.Is(err, ErrVolumeUnavailable) {
				t.Fatalf("tenant fresh attachment: %v", err)
			}
			if err := change("", want); err != nil {
				t.Fatalf("operator attachment to tenant claim: %v", err)
			}
			if err := change("acme", want); err != nil {
				t.Fatalf("held retry: %v", err)
			}
			if _, err := m.PatchVolumes(t.Context(), sb.ID, "acme", types.VolumePatch{Remove: []string{"home"}}); err != nil {
				t.Fatalf("tenant cleanup: %v", err)
			}
			if err := change("acme", want); !errors.Is(err, ErrVolumeUnavailable) {
				t.Fatalf("tenant fresh attachment after detach: %v", err)
			}
			if err := change("", want); err != nil {
				t.Fatalf("operator reattach: %v", err)
			}
			m2 := restartACLManager(t, eng, m.dataDir, nil)
			if err := m2.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got, err := m2.GetVolumes(sb.ID, "acme"); err != nil || len(got.Volumes) != 1 {
				t.Fatalf("operator attachment lost at restart: %+v %v", got, err)
			}
			if _, err := m2.SetVolumes(t.Context(), sb.ID, "acme", []types.Volume{}); err != nil {
				t.Fatalf("recovered cleanup: %v", err)
			}
		})
	}
}

func TestRuntimeCatalogOperatorKeepsExplicitOwnerACL(t *testing.T) {
	requireCatalogRoot(t)
	m, sb := newTenantVolumeManager(t, newFakeEngine(), nil)
	if err := m.RegisterVolume(t.Context(), "home", types.VolumeRegistration{Path: writeVolumeImage(t, "home.img", "private"), Tenants: []string{"other"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "home"}}); !errors.Is(err, ErrVolumeUnavailable) {
		t.Fatalf("operator bypassed explicit claim-owner ACL: %v", err)
	}
}

func TestRuntimeCatalogRecoveryReferenceDoesNotAuthorizeAttach(t *testing.T) {
	requireCatalogRoot(t)
	for _, operation := range []string{"put", "patch", "reservation"} {
		t.Run(operation, func(t *testing.T) {
			eng := newFakeEngine()
			m, sb := newTenantVolumeManager(t, eng, nil)
			if err := m.RegisterVolume(t.Context(), "home", types.VolumeRegistration{Path: writeVolumeImage(t, "home.img", "private"), Writable: true}); err != nil {
				t.Fatal(err)
			}
			want := []types.Volume{{Name: "home", Mode: types.VolumeModeRW, AttachOnly: true}}
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
			if _, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: want}); !errors.Is(err, ErrVolumeBusy) {
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
			before := len(eng.volumeSpecs)
			switch operation {
			case "put":
				_, err = m.SetVolumes(t.Context(), sb.ID, "acme", want)
			case "patch":
				_, err = m.PatchVolumes(t.Context(), sb.ID, "acme", types.VolumePatch{Add: want})
			case "reservation":
				resolved, resolveErr := m.resolveVolumes(t.Context(), "", want)
				if resolveErr != nil {
					t.Fatal(resolveErr)
				}
				_, run, beginErr := m.beginVolumeRun(t.Context(), sb, "acme")
				if beginErr != nil {
					t.Fatal(beginErr)
				}
				m.mu.Lock()
				err = m.reserveResolved(run.access, resolved, sb)
				m.mu.Unlock()
				m.finishVolumeRun(sb, run)
			}
			if !errors.Is(err, ErrVolumeUnavailable) {
				t.Fatalf("recovery-only reference authorized tenant attachment: %v", err)
			}
			if len(eng.volumeSpecs) != before {
				t.Fatal("rejected tenant reached device attachment")
			}
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
