package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestManagedStoreRequiresAuthenticatedRootConfiguration(t *testing.T) {
	cfg := managedConfig(t)
	cfg.APIToken = ""
	_, err := NewManager(t.Context(), cfg, newManagedEngine(), testSecrets(t), Backends{})
	if err == nil || !strings.Contains(err.Error(), "api_token") {
		t.Fatalf("anonymous managed store opened: %v", err)
	}
}

func TestManagedNamesAndCatalogSymlinksSurviveRestart(t *testing.T) {
	cfg := managedConfig(t)
	image := writeVolumeImage(t, "catalog.img", "catalog")
	link := filepath.Join(t.TempDir(), "catalog-link")
	if err := os.Symlink(image, link); err != nil {
		t.Fatal(err)
	}
	cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: link, Writable: true}}
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	v := createManagedTest(t, m, "kb-data")
	sb, err := m.ClaimProvision(t.Context(), testKey, managedOptions("kb-data", "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	m.Close()
	m = openManagedTest(t, cfg, e)
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, err := m.GetVolume("kb-data", "t1"); err != nil || got.ID != v.ID {
		t.Fatalf("reopened name: %+v %v", got, err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err != nil {
		t.Fatal(err)
	}
}

func TestManagedRejectsCatalogSymlinkToManagedBacking(t *testing.T) {
	cfg := managedConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	v := createManagedTest(t, m, "data")
	link := filepath.Join(t.TempDir(), "catalog-link")
	if err := os.Symlink(m.managed.path(volumeID(v.ID)), link); err != nil {
		t.Fatal(err)
	}
	m.Close()
	cfg.Volumes = []config.VolumeSpec{{Name: "alias", Path: link, Writable: true}}
	if next, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg)); err == nil {
		next.Close()
		t.Fatal("managed backing alias accepted as an independent catalog writer")
	}
}

type managedCancelEngine struct {
	*managedTestEngine
	entered chan struct{}
	vm      string
}

func (e *managedCancelEngine) DiskAttach(ctx context.Context, name string, spec engine.VolumeSpec) error {
	if spec.Name == "catalog" && (e.vm == "" || e.vm == name) {
		close(e.entered)
		<-ctx.Done()
		return ctx.Err()
	}
	return e.managedTestEngine.DiskAttach(ctx, name, spec)
}

func TestManagedReleaseCancelsExistingVolumeRunAndRetainsFullUnion(t *testing.T) {
	cfg := managedConfig(t)
	cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: writeVolumeImage(t, "catalog.img", "catalog"), Writable: true}}
	e := &managedCancelEngine{managedTestEngine: newManagedEngine(), entered: make(chan struct{})}
	m, openErr := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { m.Close() })
	if err := m.PutTenant(t.Context(), tenants.TenantSpec{Name: "t1", Token: "t1-token"}); err != nil {
		t.Fatal(err)
	}
	createManagedTest(t, m, "data")
	sb, openErr := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	if openErr != nil {
		t.Fatal(openErr)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := m.PatchVolumes(ctx, sb.ID, "", types.VolumePatch{Add: []types.Volume{{Name: "catalog"}}})
		done <- err
	}()
	<-e.entered
	e.stuckRemove.Store(true)
	if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err == nil {
		t.Fatal("surviving VMM reported removed")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("mutation cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("release did not cancel the live volume run")
	}
	requireManagedBusy(t, m, "data", holderReleasing)
	_, openErr = m.ClaimProvision(t.Context(), testKey, managedOptions("catalog"))
	if !errors.Is(openErr, ErrVolumeBusy) {
		t.Fatalf("uncertain catalog writer admitted: %v", openErr)
	}
	e.stuckRemove.Store(false)
	m.retryRemovals(t.Context()).Wait()
	requireManagedClaimable(t, m, "data")
}

func TestManagedExpiryCancelsExistingVolumeRun(t *testing.T) {
	cfg := managedConfig(t)
	cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: writeVolumeImage(t, "catalog.img", "catalog"), Writable: true}}
	e := &managedCancelEngine{managedTestEngine: newManagedEngine(), entered: make(chan struct{})}
	m, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	if err = m.PutTenant(t.Context(), tenants.TenantSpec{Name: "t1", Token: "t1-token"}); err != nil {
		t.Fatal(err)
	}
	createManagedTest(t, m, "data")
	sb, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	if err != nil {
		t.Fatal(err)
	}
	e.vm = sb.VMName
	catalogReader(t, m)
	done := make(chan error, 1)
	go func() {
		_, err := m.PatchVolumes(t.Context(), sb.ID, "t1", types.VolumePatch{Add: []types.Volume{{Name: "catalog"}}})
		done <- err
	}()
	<-e.entered
	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Second)
	m.mu.Unlock()
	m.reapOnce(t.Context())
	waitFor(t, func() bool { return volumeHoldersOf(m, "data") == (volumeHolders{}) })
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("mutation cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expiry did not cancel the live volume run")
	}
	if got := volumeHoldersOf(m, "catalog"); got != (volumeHolders{readers: 1}) {
		t.Fatalf("expired claim left catalog admission %+v, want only the bystander reader", got)
	}
}

func catalogReader(t *testing.T, m *Manager) *types.Sandbox {
	t.Helper()
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "t1", Volumes: []types.Volume{{Name: "catalog", Mode: types.VolumeModeRO, AttachOnly: true}}})
	if err != nil {
		t.Fatalf("bystander catalog reader: %v", err)
	}
	return sb
}

func TestManagedExpiryDropsVolumeDiagnostics(t *testing.T) {
	cfg := managedConfig(t)
	cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: writeVolumeImage(t, "catalog.img", "catalog")}}
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	createManagedTest(t, m, "data")
	sb := foundationClaim(t, m, "data")
	if _, err := m.PatchVolumes(t.Context(), sb.ID, "t1", types.VolumePatch{Add: []types.Volume{{Name: "catalog"}}}); err != nil {
		t.Fatal(err)
	}
	e.detachVolumeErr = &engine.VolumeMountConflict{Message: `cannot unmount "/volumes/catalog"; close guest references and retry`}
	if _, err := m.PatchVolumes(t.Context(), sb.ID, "t1", types.VolumePatch{Remove: []string{"catalog"}}); err == nil {
		t.Fatal("busy detach succeeded")
	}
	e.detachVolumeErr = nil
	m.mu.Lock()
	recorded := len(m.volumeErrors[sb])
	sb.Deadline = time.Now().Add(-time.Second)
	m.mu.Unlock()
	if recorded == 0 {
		t.Fatal("busy detach recorded no diagnostic")
	}
	m.reapOnce(t.Context())
	waitFor(t, func() bool { _, live := m.Sandbox(sb.ID); return !live })
	m.mu.Lock()
	defer m.mu.Unlock()
	if leaked := m.volumeErrors[sb]; leaked != nil {
		t.Fatalf("expired claim kept its diagnostics: %v", leaked)
	}
}
