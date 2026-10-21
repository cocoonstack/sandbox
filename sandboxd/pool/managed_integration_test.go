package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
			} else if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
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
	sb, claimErr := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "t1", Volumes: []types.Volume{
		{Name: "data", Mode: "rw"}, {Name: "catalog", Mode: "rw"},
	}})
	if claimErr != nil {
		t.Fatal(claimErr)
	}
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err != nil {
		t.Fatal(err)
	}
	if !volumeDirty(path) {
		t.Fatal("release cleared a replacement backing's dirty marker")
	}
	_, claimErr = m.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "t1", Volumes: []types.Volume{{Name: "catalog"}}})
	if !errors.Is(claimErr, ErrVolumeNeedsRecovery) {
		t.Fatalf("replacement reader admitted: %v", claimErr)
	}
}

func TestManagedSettledRetryDrainsPreviouslyUncertainCatalogRef(t *testing.T) {
	cfg := managedConfig(t)
	cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: writeVolumeImage(t, "catalog.img", "catalog"), Writable: true}}
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	createManagedTest(t, m, "data")
	sb, claimErr := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	if claimErr != nil {
		t.Fatal(claimErr)
	}
	e.diskAttachErrFor, e.diskAttachErr = "catalog", errors.New("uncertain attach")
	_, claimErr = m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Add: []types.Volume{{Name: "catalog"}}})
	if claimErr == nil {
		t.Fatal("injected attach succeeded")
	}
	e.diskAttachErr, e.diskAttachErrFor = nil, ""
	if err := os.Mkdir(m.store.path+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	_, claimErr = m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Remove: []string{"catalog"}})
	if claimErr == nil {
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
	writer, claimErr := m.ClaimProvision(t.Context(), testKey, managedOptions("catalog"))
	if claimErr != nil {
		t.Fatalf("confirmed detached catalog still busy: %v", claimErr)
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
	m, openErr := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { m.Close() })
	m.managed.process = e.identity
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
	_, openErr = m.ClaimProvision(t.Context(), testKey, managedOptions("catalog"))
	if !errors.Is(openErr, ErrVolumeBusy) {
		t.Fatalf("uncertain catalog writer admitted: %v", openErr)
	}
	e.falseRemove.Store(false)
	m.retryRemovals(t.Context()).Wait()
	if _, held := m.managedHold(sb.ID); held {
		t.Fatal("hold retained after confirmed VMM removal")
	}
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
	m.managed.process = e.identity
	if err = m.PutTenant(t.Context(), tenants.TenantSpec{Name: "t1", Token: "t1-token"}); err != nil {
		t.Fatal(err)
	}
	createManagedTest(t, m, "data")
	sb, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	if err != nil {
		t.Fatal(err)
	}
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
	waitFor(t, func() bool { _, held := m.managedHold(sb.ID); return !held })
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("mutation cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expiry did not cancel the live volume run")
	}
	if got := volumeHoldersOf(m, "catalog"); got != (volumeHolders{}) {
		t.Fatalf("expired claim kept the canceled run's charge: %+v", got)
	}
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

func TestManagedTeardownDuringLiveRemovalReleasesCatalogCharge(t *testing.T) {
	for _, path := range []string{"release", "reap", "sleep-capable release"} {
		t.Run(path, func(t *testing.T) {
			cfg := managedConfig(t)
			cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: writeVolumeImage(t, "catalog.img", "catalog"), Writable: true}}
			opts := managedOptions("data")
			base := newManagedEngine()
			var eng Engine = base
			process := base.identity
			if path == "sleep-capable release" {
				cfg.ManagedVolumeHibernate = &config.ManagedVolumeHibernate{HookArgv: []string{"/usr/local/bin/volume-hook"}, HookTimeoutSeconds: 1}
				se := &sleepTestEngine{managedTestEngine: base, generation: 2, reason: "boot", vmID: "immutable-vm", mounted: true}
				eng, process = se, se.identity
				opts.Volumes[0].AttachOnly = false
			}
			m, err := NewManager(t.Context(), cfg, eng, testSecrets(t), testBackends(t, cfg))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = m.Close() })
			m.managed.process = process
			if err = m.PutTenant(t.Context(), tenants.TenantSpec{Name: "t1", Token: "t1-token"}); err != nil {
				t.Fatal(err)
			}
			createManagedTest(t, m, "data")
			sb, err := m.ClaimProvision(t.Context(), testKey, opts)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := m.PatchVolumes(t.Context(), sb.ID, "t1", types.VolumePatch{Add: []types.Volume{{Name: "catalog"}}}); err != nil {
				t.Fatal(err)
			}
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			m.managed.mu.Lock()
			m.managed.commitFile = func(p string, raw []byte) error {
				once.Do(func() { close(entered); <-resume })
				return writeVolumeSnapshot(p, raw)
			}
			m.managed.mu.Unlock()
			patched := make(chan error, 1)
			go func() {
				_, err := m.PatchVolumes(t.Context(), sb.ID, "t1", types.VolumePatch{Remove: []string{"catalog"}})
				patched <- err
			}()
			<-entered
			torn := make(chan struct{})
			go func() {
				defer close(torn)
				if path == "reap" {
					m.mu.Lock()
					sb.Deadline = time.Now().Add(-time.Second)
					m.mu.Unlock()
					m.reapOnce(t.Context())
					return
				}
				_ = m.Release(t.Context(), sb.ID, Cred{Operator: true})
			}()
			waitFor(t, func() bool { _, live := m.Sandbox(sb.ID); return !live })
			close(resume)
			<-torn
			<-patched
			waitFor(t, func() bool { _, held := m.managedHold(sb.ID); return !held })
			if got := volumeHoldersOf(m, "catalog"); got != (volumeHolders{}) {
				t.Fatalf("%s during a live catalog removal kept its charge: %+v", path, got)
			}
			if _, err := m.ClaimProvision(t.Context(), testKey, managedOptions("catalog")); err != nil {
				t.Fatalf("catalog writer refused after teardown: %v", err)
			}
		})
	}
}

func TestManagedLiveRemovalReleasesCatalogChargeWhenRegistryWriteFails(t *testing.T) {
	for _, tc := range []struct {
		name       string
		dirty      bool
		goodWrites int32
	}{
		{"detached", false, 0},
		{"dirty catalog refused", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := managedConfig(t)
			catalog := writeVolumeImage(t, "catalog.img", "catalog")
			cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: catalog, Writable: true}}
			m := openManagedTest(t, cfg, newManagedEngine())
			createManagedTest(t, m, "data")
			sb := foundationClaim(t, m, "data")
			patch := types.VolumePatch{Add: []types.Volume{{Name: "catalog"}}}
			if tc.dirty {
				if err := markVolumeDirty(catalog); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := m.PatchVolumes(t.Context(), sb.ID, "t1", patch); err != nil {
					t.Fatal(err)
				}
				patch = types.VolumePatch{Remove: []string{"catalog"}}
			}
			var writes atomic.Int32
			writes.Store(tc.goodWrites)
			m.managed.mu.Lock()
			m.managed.commitFile = func(path string, raw []byte) error {
				if writes.Add(-1) < 0 {
					return errors.New("injected registry write failure")
				}
				return writeVolumeSnapshot(path, raw)
			}
			m.managed.mu.Unlock()
			if _, err := m.PatchVolumes(t.Context(), sb.ID, "t1", patch); err == nil {
				t.Fatal("registry write failure hidden")
			}
			writes.Store(1 << 20)
			m.retryManagedSnapshot()
			waitFor(t, func() bool { return m.ManagedVolumeStatus() == nil && !m.managed.retrying.Load() })
			if err := clearVolumeDirty(catalog); err != nil {
				t.Fatal(err)
			}
			if view, _ := m.GetVolumes(sb.ID, "t1"); len(view.States) != 1 {
				t.Fatalf("claim still lists the catalog: %+v", view)
			}
			writer, err := m.ClaimProvision(t.Context(), testKey, managedOptions("catalog"))
			if err != nil {
				t.Fatalf("catalog the claim no longer holds is still busy: %v", err)
			}
			for _, id := range []string{writer.ID, sb.ID} {
				if err := m.Release(t.Context(), id, Cred{Operator: true}); err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, func() bool { return volumeHoldersOf(m, "catalog") == (volumeHolders{}) })
		})
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
	waitFor(t, func() bool { return !m.store.recommitting.Load() && m.store.synced() })
	persisted, loadErr := m.store.load()
	if loadErr != nil || persisted[sb.ID] == nil || persisted[sb.ID].Failed != egressQuarantineFailure {
		t.Fatalf("quarantine marker was not persisted: %v %v", persisted[sb.ID], loadErr)
	}
}
