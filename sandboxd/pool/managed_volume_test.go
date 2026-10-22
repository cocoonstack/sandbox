package pool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type managedTestEngine struct {
	*fakeEngine
	next             atomic.Int64
	stuckRemove      atomic.Bool
	missingInventory atomic.Bool
	attachEntered    chan struct{}
	attachResume     chan struct{}
}

func newManagedEngine() *managedTestEngine { return &managedTestEngine{fakeEngine: newFakeEngine()} }

func (e *managedTestEngine) RunCold(ctx context.Context, name string, key types.PoolKey, storage string) (types.VMRecord, error) {
	e.mu.Lock()
	e.pids[name] = 1000 + int(e.next.Add(1))
	e.mu.Unlock()
	return e.fakeEngine.RunCold(ctx, name, key, storage)
}

func (e *managedTestEngine) Remove(ctx context.Context, name string) error {
	if e.stuckRemove.Load() {
		return errors.New("vm rm: device busy")
	}
	return e.fakeEngine.Remove(ctx, name)
}

func (e *managedTestEngine) Inspect(ctx context.Context, name string) (types.VMRecord, bool, error) {
	if e.missingInventory.Load() {
		return types.VMRecord{}, false, nil
	}
	return e.fakeEngine.Inspect(ctx, name)
}

func (e *managedTestEngine) DiskAttach(ctx context.Context, name string, spec engine.VolumeSpec) error {
	if e.attachEntered != nil {
		e.attachEntered <- struct{}{}
		<-e.attachResume
	}
	if err := e.fakeEngine.DiskAttach(ctx, name, spec); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.disks == nil {
		e.disks = map[string][]types.VMDisk{}
	}
	e.disks[name] = append(e.disks[name], types.VMDisk{Name: spec.Name, Path: spec.Path, ReadOnly: !spec.RW})
	return nil
}

func managedConfig(t *testing.T) *config.Config {
	t.Helper()
	dir := t.TempDir()
	return &config.Config{DataDir: filepath.Join(dir, "data"), ManagedVolumeDir: filepath.Join(dir, "managed"), APIToken: "root", Pools: []config.PoolSpec{{PoolKey: testKey, Warm: 1}}}
}

func openManagedTest(t *testing.T, cfg *config.Config, e *managedTestEngine) *Manager {
	t.Helper()
	m, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	if err := m.PutTenant(t.Context(), tenants.TenantSpec{Name: "t1", Token: "t1-token"}); err != nil {
		t.Fatal(err)
	}
	if err := m.PutTenant(t.Context(), tenants.TenantSpec{Name: "t2", Token: "t2-token"}); err != nil {
		t.Fatal(err)
	}
	return m
}

func createManagedTest(t *testing.T, m *Manager, name string) types.ManagedVolumeInfo {
	t.Helper()
	v, created, err := m.CreateVolume(t.Context(), name, types.VolumeCreate{SizeGiB: 1}, "t1")
	if err != nil || !created {
		t.Fatalf("create: created=%v err=%v", created, err)
	}
	return v
}

func managedOptions(names ...string) ClaimOptions {
	o := ClaimOptions{Tenant: "t1"}
	for _, name := range names {
		o.Volumes = append(o.Volumes, types.Volume{Name: name, Mode: types.VolumeModeRW, AttachOnly: true})
	}
	return o
}

func requireVolumeStatus(t *testing.T, err error, status int) *VolumeError {
	t.Helper()
	var v *VolumeError
	if !errors.As(err, &v) || v.Status != status {
		t.Fatalf("error=%v, want volume status %d", err, status)
	}
	return v
}

func TestManagedCreateReplaySparseOwnershipAndDeleteIdentity(t *testing.T) {
	cfg := managedConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	v := createManagedTest(t, m, "data")
	path := m.managed.path(volumeID(v.ID))
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 1<<30 || st.Sys().(*syscall.Stat_t).Blocks*512 >= st.Size() {
		t.Fatal("backing is not a sparse GiB raw image")
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte("preserved"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if v.Kind != "managed" || v.State != "ready" || v.Tenant != "t1" || v.Node == "" || v.CreatedAt.IsZero() {
		t.Fatalf("projection=%+v", v)
	}
	_, _, err = m.CreateVolume(t.Context(), "data", types.VolumeCreate{SizeGiB: 2}, "t1")
	requireVolumeStatus(t, err, 409)
	_, _, err = m.CreateVolume(t.Context(), "data", types.VolumeCreate{SizeGiB: 1}, "t2")
	requireVolumeStatus(t, err, 404)
	_, err = m.GetVolume("data", "t2")
	requireVolumeStatus(t, err, 404)
	m.Close()
	m = openManagedTest(t, cfg, e)
	repeat, created, err := m.CreateVolume(t.Context(), "data", types.VolumeCreate{SizeGiB: 1, Tenant: "t1"}, "t1")
	if err != nil || created || repeat.ID != v.ID {
		t.Fatalf("replay=%+v created=%v err=%v", repeat, created, err)
	}
	f, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 9)
	_, err = f.ReadAt(buf, 4096)
	f.Close()
	if err != nil || string(buf) != "preserved" {
		t.Fatalf("replay changed bytes: %q %v", buf, err)
	}
	requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", "", "t1"), 428)
	requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", "wrong", "t1"), 412)
	if err := m.DeleteVolume(t.Context(), "data", v.ID, "t1"); err != nil {
		t.Fatal(err)
	}
	newer := createManagedTest(t, m, "data")
	if newer.ID == v.ID {
		t.Fatal("identity reused")
	}
	requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", v.ID, "t1"), 412)
	requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", v.ID, "t2"), 404)
	if _, err := os.Stat(m.managed.path(volumeID(newer.ID))); err != nil {
		t.Fatal("stale delete removed replacement", err)
	}
	m.Close()
	m = openManagedTest(t, cfg, e)
	if got, err := m.GetVolume("data", "t1"); err != nil || got.ID != newer.ID {
		t.Fatalf("restart=%+v %v", got, err)
	}
}

func TestManagedConcurrentCreateAndWriterAdmission(t *testing.T) {
	m := openManagedTest(t, managedConfig(t), newManagedEngine())
	var created atomic.Int32
	var wg sync.WaitGroup
	ids := make(chan string, 12)
	for range 12 {
		wg.Go(func() {
			v, wasCreated, err := m.CreateVolume(t.Context(), "race", types.VolumeCreate{SizeGiB: 1}, "t1")
			if err != nil {
				t.Error(err)
				return
			}
			if wasCreated {
				created.Add(1)
			}
			ids <- v.ID
		})
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Error("concurrent identity changed")
		}
	}
	if created.Load() != 1 {
		t.Fatalf("created=%d", created.Load())
	}
	successes := make(chan *types.Sandbox, 8)
	for range 8 {
		wg.Go(func() {
			sb, err := m.ClaimProvision(t.Context(), testKey, managedOptions("race"))
			if err == nil {
				successes <- sb
				return
			}
			var ve *VolumeError
			if !errors.As(err, &ve) || ve.Status != 409 {
				t.Errorf("conflict=%+v", err)
			}
		})
	}
	wg.Wait()
	close(successes)
	var winner *types.Sandbox
	count := 0
	for sb := range successes {
		winner = sb
		count++
	}
	if count != 1 {
		t.Fatalf("writers=%d", count)
	}
	if err := m.Release(t.Context(), winner.ID, Cred{Operator: true}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteVolume(t.Context(), "race", first, "t1"); err != nil {
		t.Fatal(err)
	}
}

func TestManagedActiveClaimAdoption(t *testing.T) {
	for _, syncClaims := range []bool{false, true} {
		t.Run(fmt.Sprint(syncClaims), func(t *testing.T) {
			cfg := managedConfig(t)
			cfg.SyncClaims = syncClaims
			e := newManagedEngine()
			m := openManagedTest(t, cfg, e)
			v := createManagedTest(t, m, "data")
			sb, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
			if err != nil {
				t.Fatal(err)
			}
			m.Close()
			m = openManagedTest(t, cfg, e)
			if err := m.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			adopted, ok := m.Sandbox(sb.ID)
			if !ok || adopted.Tenant != "t1" {
				t.Fatal("writer not adopted")
			}
			requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", v.ID, "t1"), 409)
			e.stuckRemove.Store(true)
			if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err == nil {
				t.Fatal("a surviving VM reported removed")
			}
			requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", v.ID, "t1"), 409)
			e.stuckRemove.Store(false)
			m.retryRemovals(t.Context()).Wait()
			if err := m.DeleteVolume(t.Context(), "data", v.ID, "t1"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedCreateInterruptedRetriesOrDeletes(t *testing.T) {
	for _, finish := range []string{"put", "delete"} {
		t.Run(finish, func(t *testing.T) {
			cfg := managedConfig(t)
			e := newManagedEngine()
			m := openManagedTest(t, cfg, e)
			writes := 0
			m.managed.commitFile = func(path string, raw []byte) error {
				writes++
				if writes == 2 {
					return errors.New("ready metadata failed")
				}
				return writeVolumeSnapshot(path, raw)
			}
			_, _, err := m.CreateVolume(t.Context(), "data", types.VolumeCreate{SizeGiB: 1}, "t1")
			requireVolumeStatus(t, err, 503)
			m.Close()
			m = openManagedTest(t, cfg, e)
			v, err := m.GetVolume("data", "t1")
			if err != nil || v.State != "creating" {
				t.Fatalf("interrupted create = %+v %v, want creating", v, err)
			}
			path := m.managed.path(volumeID(v.ID))
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if finish == "put" {
				got, _, putErr := m.CreateVolume(t.Context(), "data", types.VolumeCreate{SizeGiB: 1}, "t1")
				after, statErr := os.Stat(path)
				if putErr != nil || got.State != "ready" || got.ID != v.ID || statErr != nil || os.SameFile(before, after) {
					t.Fatalf("identical PUT after an interrupted create = %+v %v, want the same volume ready on a fresh file", got, putErr)
				}
				return
			}
			if delErr := m.DeleteVolume(t.Context(), "data", v.ID, "t1"); delErr != nil {
				t.Fatalf("DELETE of an interrupted create = %v, want it removed", delErr)
			}
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("interrupted allocation file still present after DELETE: %v", statErr)
			}
		})
	}
}

func TestManagedDeleteOfLostBackingDropsRecord(t *testing.T) {
	m := openManagedTest(t, managedConfig(t), newManagedEngine())
	lost := createManagedTest(t, m, "lost")
	replaced := createManagedTest(t, m, "replaced")
	if err := os.Remove(m.managed.path(volumeID(lost.ID))); err != nil {
		t.Fatal(err)
	}
	replacedPath := m.managed.path(volumeID(replaced.ID))
	if err := os.Remove(replacedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(replacedPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := m.GetVolume("lost", "t1"); err != nil || got.State != string(volumeRecovery) {
		t.Fatalf("lost backing = %+v %v, want recovery_required", got, err)
	}
	if err := m.DeleteVolume(t.Context(), "lost", lost.ID, "t1"); err != nil {
		t.Fatalf("DELETE of an unheld volume whose backing is gone = %v, want the record dropped", err)
	}
	_, err := m.GetVolume("lost", "t1")
	requireVolumeStatus(t, err, 404)
	requireVolumeStatus(t, m.DeleteVolume(t.Context(), "lost", lost.ID, "t1"), 404)
	requireVolumeStatus(t, m.DeleteVolume(t.Context(), "replaced", replaced.ID, "t1"), 503)
	if _, err := os.Stat(replacedPath); err != nil {
		t.Fatalf("refused DELETE removed a replaced backing: %v", err)
	}
	if got, err := m.GetVolume("replaced", "t1"); err != nil || got.ID != replaced.ID {
		t.Fatalf("refused DELETE dropped the replaced volume's record: %+v %v", got, err)
	}
}

func TestManagedSlowDeleteDoesNotStallOtherCreates(t *testing.T) {
	m := openManagedTest(t, managedConfig(t), newManagedEngine())
	slow := createManagedTest(t, m, "slow")
	createManagedTest(t, m, "other")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	m.managed.commitFile = func(path string, raw []byte) error {
		once.Do(func() { close(entered); <-release })
		return writeVolumeSnapshot(path, raw)
	}
	deleted := make(chan error, 1)
	go func() { deleted <- m.DeleteVolume(t.Context(), "slow", slow.ID, "t1") }()
	<-entered
	replayed := make(chan error, 1)
	go func() {
		_, _, err := m.CreateVolume(t.Context(), "other", types.VolumeCreate{SizeGiB: 1}, "t1")
		replayed <- err
	}()
	select {
	case err := <-replayed:
		close(release)
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		<-replayed
		t.Fatal("a PUT replay of one volume waited behind another volume's delete")
	}
	if err := <-deleted; err != nil {
		t.Fatal(err)
	}
}

func TestManagedRegistryFormatHasNoVersion(t *testing.T) {
	cfg := managedConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	createManagedTest(t, m, "kept")
	m.Close()
	path := filepath.Join(cfg.ManagedVolumeDir, "registry.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err = json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	delete(doc, "version")
	doc["future"] = map[string]any{"field": true}
	doc["volumes"].(map[string]any)["kept"].(map[string]any)["future"] = "ignored"
	if raw, err = json.Marshal(doc); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	m = openManagedTest(t, cfg, e)
	if got, getErr := m.GetVolume("kept", "t1"); getErr != nil || got.State != "ready" {
		t.Fatalf("registry without version and with unknown keys: %+v %v, want kept ready", got, getErr)
	}
	gone := createManagedTest(t, m, "gone")
	if err = m.DeleteVolume(t.Context(), "gone", gone.ID, "t1"); err != nil {
		t.Fatal(err)
	}
	if raw, err = os.ReadFile(path); err != nil {
		t.Fatal(err)
	}
	var keys map[string]any
	if err := json.Unmarshal(raw, &keys); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"version", "deleted"} {
		if _, ok := keys[key]; ok {
			t.Fatalf("registry.json after create and delete has %q: %s", key, raw)
		}
	}
	requireVolumeStatus(t, m.DeleteVolume(t.Context(), "gone", gone.ID, "t1"), 404)
}

func TestManagedCorruptAndUnsafeStorage(t *testing.T) {
	for _, fault := range []string{"truncated", "state", "missing-registry", "replaced", "symlink", "missing", "permissions"} {
		t.Run(fault, func(t *testing.T) {
			cfg := managedConfig(t)
			e := newManagedEngine()
			m := openManagedTest(t, cfg, e)
			v := createManagedTest(t, m, "data")
			path := m.managed.path(volumeID(v.ID))
			m.Close()
			switch fault {
			case "truncated":
				if err := os.WriteFile(filepath.Join(cfg.ManagedVolumeDir, "registry.json"), []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "state":
				raw, err := os.ReadFile(filepath.Join(cfg.ManagedVolumeDir, "registry.json"))
				if err != nil {
					t.Fatal(err)
				}
				var data volumeRegistryData
				if err := json.Unmarshal(raw, &data); err != nil {
					t.Fatal(err)
				}
				corrupt := data.Volumes["data"]
				corrupt.State = "future_state"
				data.Volumes["data"] = corrupt
				raw, _ = json.Marshal(data)
				if err := os.WriteFile(filepath.Join(cfg.ManagedVolumeDir, "registry.json"), raw, 0o600); err != nil {
					t.Fatal(err)
				}
			case "missing-registry":
				if err := os.Remove(filepath.Join(cfg.ManagedVolumeDir, "registry.json")); err != nil {
					t.Fatal(err)
				}
			case "replaced":
				if err := os.Rename(path, path+".original"); err != nil {
					t.Fatal(err)
				}
				f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.Truncate(1 << 30); err != nil {
					t.Fatal(err)
				}
				f.Close()
			case "symlink":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("/etc/passwd", path); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "permissions":
				if err := os.Chmod(path, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "truncated" || fault == "state" || fault == "missing-registry" || fault == "replaced" {
				if _, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg)); err == nil {
					t.Fatal("an unsafe registry opened")
				}
				return
			}
			m = openManagedTest(t, cfg, e)
			_, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
			requireVolumeStatus(t, err, 503)
			if len(e.volumeSpecs) != 0 {
				t.Fatal("unsafe backing attached")
			}
			if fault == "symlink" || fault == "permissions" {
				requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", v.ID, "t1"), 503)
			}
		})
	}
}

func TestManagedStoreExclusiveAndDirectoryPermissions(t *testing.T) {
	cfg := managedConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	if _, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg)); err == nil {
		t.Fatal("second daemon accepted locked store")
	}
	m.Close()
	if err := os.Chmod(cfg.ManagedVolumeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg)); err == nil {
		t.Fatal("public managed directory accepted")
	}
}

func TestManagedStalledAttachDoesNotBlockOtherClaims(t *testing.T) {
	cfg := managedConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	v := createManagedTest(t, m, "data")
	unrelated, claimErr := m.ClaimProvision(t.Context(), testKey, ClaimOptions{})
	if claimErr != nil {
		t.Fatal(claimErr)
	}
	e.attachEntered = make(chan struct{}, 1)
	e.attachResume = make(chan struct{})
	done := make(chan error, 1)
	go func() { _, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data")); done <- err }()
	<-e.attachEntered
	if _, err := m.Renew(t.Context(), unrelated.ID, Cred{Operator: true}, time.Minute, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(t.Context(), unrelated.ID, Cred{Operator: true}); err != nil {
		t.Fatal(err)
	}
	requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", v.ID, "t1"), 409)
	close(e.attachResume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestManagedListReportsLostDirectoryAsUnavailableNotRecovery(t *testing.T) {
	cfg := managedConfig(t)
	m := openManagedTest(t, cfg, newManagedEngine())
	createManagedTest(t, m, "data")
	disks := filepath.Join(cfg.ManagedVolumeDir, "disks")
	if err := os.Rename(disks, disks+".aside"); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Rename(disks+".aside", disks) }()
	for _, info := range m.Volumes("t1", nil) {
		if info.Name == "data" && (info.Available || info.State != "ready") {
			t.Fatalf("lost directory listed as state=%q available=%v, want ready and unavailable like GET's storage error", info.State, info.Available)
		}
	}
}

func TestVolumeRefusalsIndependentOfManagedDir(t *testing.T) {
	cfg := managedConfig(t)
	m := openManagedTest(t, cfg, newManagedEngine())
	createManagedTest(t, m, "data")
	for name, opts := range map[string]ClaimOptions{
		"unknown name":   {Tenant: "t1", Volumes: []types.Volume{{Name: "nope", AttachOnly: true}}},
		"foreign volume": {Tenant: "t2", Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}},
	} {
		if _, err := m.ClaimProvision(t.Context(), testKey, opts); !errors.Is(err, ErrVolumeUnavailable) {
			t.Errorf("%s: claim = %v, want ErrVolumeUnavailable like a node without managed storage", name, err)
		}
		local, advertised, err := m.VolumePlacement(testKey, opts.Tenant, []string{opts.Volumes[0].Name})
		if err != nil || local || !slices.Equal(advertised, []string{types.ManagedVolumeGossipName(opts.Tenant, opts.Volumes[0].Name)}) {
			t.Errorf("%s: placement = local %v keys %v err %v, want a mesh lookup by the caller's own tenant key", name, local, advertised, err)
		}
	}
}

func TestVolumeNamesAdvertiseManagedKeys(t *testing.T) {
	m := openManagedTest(t, managedConfig(t), newManagedEngine())
	createManagedTest(t, m, "data")
	sum := sha256.Sum256([]byte("volume|t1|data"))
	key := "managed/" + hex.EncodeToString(sum[:16])
	names := m.VolumeNames()
	if !slices.Contains(names, key) || slices.Contains(names, "data") {
		t.Fatalf("gossiped volume names = %v, want the tenant-scoped key %s and never the bare name", names, key)
	}
	for _, info := range m.Volumes("", map[string]int{key: 1}) {
		if info.Name == key {
			t.Fatalf("root listing shows the gossip key %s as a volume", key)
		}
	}
}
