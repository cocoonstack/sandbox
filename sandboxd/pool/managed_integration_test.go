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

func TestManagedRejectsCatalogReplacementBeforeAttach(t *testing.T) {
	for _, alias := range []bool{false, true} {
		t.Run(map[bool]string{false: "different inode", true: "managed alias"}[alias], func(t *testing.T) {
			cfg := managedConfig(t)
			path := writeVolumeImage(t, "catalog.img", "original")
			cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: path, Writable: true}}
			e := newManagedEngine()
			m := openManagedTest(t, cfg, e)
			v := createManagedTest(t, m, "data")
			specs, _, err := m.admitOptions(t.Context(), testKey, managedOptions("data", "catalog"))
			if err != nil {
				t.Fatal(err)
			}
			rec, err := e.RunCold(t.Context(), "sbx-replaced", testKey, "")
			if err != nil {
				t.Fatal(err)
			}
			sb := &types.Sandbox{VMName: rec.Config.Name, VsockSocket: rec.VsockSocket, Key: testKey}
			if err := m.bindManaged(t.Context(), sb, specs); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(path, path+".old"); err != nil {
				t.Fatal(err)
			}
			if alias {
				if err := os.Symlink(m.managed.path(volumeID(v.ID)), path); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(path, []byte("replacement"), 0600); err != nil {
				t.Fatal(err)
			}
			for _, spec := range specs {
				if spec.applied.Name != "catalog" {
					continue
				}
				if err := m.applyVolume(t.Context(), sb, spec, false); err == nil {
					t.Fatal("replacement attached after admission pinned another backing")
				}
			}
			e.mu.Lock()
			defer e.mu.Unlock()
			if len(e.volumeSpecs) != 0 {
				t.Fatalf("device effects before replacement rejection: %v", e.volumeOps)
			}
		})
	}
}

func TestManagedReleaseDoesNotClearReplacementCatalogDirtyMarker(t *testing.T) {
	cfg := managedConfig(t)
	path := writeVolumeImage(t, "catalog.img", "old")
	cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: path, Writable: true}}
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	createManagedTest(t, m, "data")
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "t1", Volumes: []types.Volume{
		{Name: "data", Mode: "rw"}, {Name: "catalog", Mode: "rw"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err != nil {
		t.Fatal(err)
	}
	if !volumeDirty(path) {
		t.Fatal("release cleared a replacement backing's dirty marker")
	}
	_, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "t1", Volumes: []types.Volume{{Name: "catalog"}}})
	if !errors.Is(err, ErrVolumeNeedsRecovery) {
		t.Fatalf("replacement reader admitted: %v", err)
	}
}

func TestManagedSettledRetryDrainsPreviouslyUncertainCatalogRef(t *testing.T) {
	cfg := managedConfig(t)
	cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: writeVolumeImage(t, "catalog.img", "catalog"), Writable: true}}
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	createManagedTest(t, m, "data")
	sb, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	if err != nil {
		t.Fatal(err)
	}
	e.diskAttachErrFor, e.diskAttachErr = "catalog", errors.New("uncertain attach")
	_, err = m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Add: []types.Volume{{Name: "catalog"}}})
	if err == nil {
		t.Fatal("injected attach succeeded")
	}
	e.diskAttachErr, e.diskAttachErrFor = nil, ""
	if err := os.Mkdir(m.store.path+".tmp", 0700); err != nil {
		t.Fatal(err)
	}
	_, err = m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Remove: []string{"catalog"}})
	if err == nil {
		t.Fatal("injected claim commit succeeded")
	}
	if err := os.Remove(m.store.path + ".tmp"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, m.store.synced)
	if _, err := m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{}); err != nil {
		t.Fatal(err)
	}
	h, ok := m.managedHold(sb.ID)
	if !ok || len(h.Refs) != 1 {
		t.Fatalf("settled retry leaked uncertain reference: %+v", h)
	}
	writer, err := m.ClaimProvision(t.Context(), testKey, managedOptions("catalog"))
	if err != nil {
		t.Fatalf("confirmed detached catalog still busy: %v", err)
	}
	if err := m.Release(t.Context(), writer.ID, Cred{Operator: true}); err != nil {
		t.Fatal(err)
	}
}

type managedCancelEngine struct {
	*managedTestEngine
	entered chan struct{}
}

func (e *managedCancelEngine) DiskAttach(ctx context.Context, name string, spec engine.VolumeSpec) error {
	if spec.Name == "catalog" {
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
	m, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	m.managed.process = e.identity
	if err := m.PutTenant(t.Context(), tenants.TenantSpec{Name: "t1", Token: "t1-token"}); err != nil {
		t.Fatal(err)
	}
	createManagedTest(t, m, "data")
	sb, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := m.PatchVolumes(ctx, sb.ID, "", types.VolumePatch{Add: []types.Volume{{Name: "catalog"}}})
		done <- err
	}()
	<-e.entered
	e.falseRemove.Store(true)
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
	h, ok := m.managedHold(sb.ID)
	if !ok || h.State != holdRetiring || len(h.Refs) != 2 {
		t.Fatalf("retirement lost possible device references: %+v", h)
	}
	_, err = m.ClaimProvision(t.Context(), testKey, managedOptions("catalog"))
	if !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("uncertain catalog writer admitted: %v", err)
	}
	e.falseRemove.Store(false)
	m.retryRemovals(t.Context()).Wait()
	if _, held := m.managedHold(sb.ID); held {
		t.Fatal("hold retained after confirmed VMM removal")
	}
}

func TestManagedQuarantineStillRemovesVMWhenRetirementCannotCommit(t *testing.T) {
	cfg := managedConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	createManagedTest(t, m, "data")
	sb, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	if err != nil {
		t.Fatal(err)
	}
	m.managed.commitFile = func(string, []byte) error { return errors.New("retirement unavailable") }
	if m.quarantineClaim(t.Context(), sb) {
		t.Fatal("uncommitted retirement reported fully cleaned")
	}
	if _, present, err := e.Inspect(t.Context(), sb.VMName); err != nil || present {
		t.Fatalf("quarantined VM remains alive: %v %v", present, err)
	}
	current, ok := m.Sandbox(sb.ID)
	if !ok || current.Failed == "" {
		t.Fatalf("quarantined row remained serviceable: %+v", current)
	}
	if _, held := m.managedHold(sb.ID); !held {
		t.Fatal("failed retirement discarded disk ownership")
	}
}
