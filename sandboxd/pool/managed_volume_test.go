package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	liveEntered, liveResume chan struct{}
	*fakeEngine
	next             atomic.Int64
	falseRemove      atomic.Bool
	missingInventory atomic.Bool
	processLives     atomic.Bool
	processUnknown   atomic.Bool
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
	if e.falseRemove.Load() {
		return nil
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
	return e.fakeEngine.DiskAttach(ctx, name, spec)
}
func (e *managedTestEngine) identity(pid int) (processIdentity, error) {
	if e.processUnknown.Load() {
		return processIdentity{}, os.ErrPermission
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for name, p := range e.pids {
		if p == pid {
			if _, ok := e.vms[name]; ok || e.processLives.Load() {
				return processIdentity{PID: pid, Start: uint64(pid), BootID: "test-boot"}, nil
			}
		}
	}
	return processIdentity{}, os.ErrNotExist
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
	m.managed.process = e.identity
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
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
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
	if err := m.DeleteVolume(t.Context(), "data", v.ID, "t1"); err != nil {
		t.Fatal(err)
	}
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
			v, new, err := m.CreateVolume(t.Context(), "race", types.VolumeCreate{SizeGiB: 1}, "t1")
			if err != nil {
				t.Error(err)
				return
			}
			if new {
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
			if !errors.As(err, &ve) || ve.Status != 409 || len(ve.Holders) != 1 || ve.Holders[0].SandboxID == "" {
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

func TestManagedRetirementSurvivesRestartAndStaleClaim(t *testing.T) {
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
			e.falseRemove.Store(true)
			if err := m.retireManaged(sb.ID); err != nil {
				t.Fatal(err)
			}
			m.Close()
			m = openManagedTest(t, cfg, e)
			if err := m.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, ok := m.Sandbox(sb.ID); ok {
				t.Fatal("retiring stale claim was adopted")
			}
			_, err = m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
			busy := requireVolumeStatus(t, err, 409)
			if busy.Holders[0].State != "retiring" || busy.Holders[0].SandboxID != sb.ID {
				t.Fatalf("holder=%+v", busy)
			}
			requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", v.ID, "t1"), 409)
			e.falseRemove.Store(false)
			m.retryRemovals(t.Context()).Wait()
			if err := m.DeleteVolume(t.Context(), "data", v.ID, "t1"); err != nil {
				t.Fatal(err)
			}
			if len(m.managed.data.Holds) != 0 {
				t.Fatal("retirement leaked a hold")
			}
		})
	}
}

func TestManagedActiveClaimAdoptionAndProcessProof(t *testing.T) {
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
			e.processLives.Store(true)
			if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err == nil {
				t.Fatal("absence alone released a live process")
			}
			requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", v.ID, "t1"), 409)
			e.processUnknown.Store(true)
			m.retryRemovals(t.Context()).Wait()
			requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", v.ID, "t1"), 409)
			e.processUnknown.Store(false)
			e.processLives.Store(false)
			m.retryRemovals(t.Context()).Wait()
			if err := m.DeleteVolume(t.Context(), "data", v.ID, "t1"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedPreclaimCrashRetainsOwnership(t *testing.T) {
	for _, syncClaims := range []bool{false, true} {
		for _, boundary := range []string{"reserved", "bound", "attached", "mounted"} {
			t.Run(fmt.Sprintf("%s/sync=%v", boundary, syncClaims), func(t *testing.T) {
				cfg := managedConfig(t)
				cfg.SyncClaims = syncClaims
				e := newManagedEngine()
				m := openManagedTest(t, cfg, e)
				v := createManagedTest(t, m, "data")
				options := managedOptions("data")
				if boundary == "mounted" {
					options.Volumes[0].AttachOnly = false
				}
				specs, _, err := m.admitOptions(t.Context(), testKey, options)
				if err != nil {
					t.Fatal(err)
				}
				if boundary != "reserved" {
					rec, err := e.RunCold(t.Context(), "sbx-interrupted", testKey, "")
					if err != nil {
						t.Fatal(err)
					}
					sb := &types.Sandbox{VMName: rec.Config.Name, VsockSocket: rec.VsockSocket, Key: testKey}
					if err := m.bindManaged(t.Context(), sb, specs); err != nil {
						t.Fatal(err)
					}
					if boundary == "attached" || boundary == "mounted" {
						if err := m.applyVolume(t.Context(), sb, specs[0], false); err != nil {
							t.Fatal(err)
						}
					}
					e.falseRemove.Store(true)
				}
				m.Close()
				m = openManagedTest(t, cfg, e)
				if err := m.Reconcile(t.Context()); err != nil {
					t.Fatal(err)
				}
				if boundary != "reserved" {
					requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", v.ID, "t1"), 409)
				}
				e.falseRemove.Store(false)
				m.retryRemovals(t.Context()).Wait()
				if err := m.DeleteVolume(t.Context(), "data", v.ID, "t1"); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestManagedAmbiguousCommitFailsClosedAndRecovers(t *testing.T) {
	cfg := managedConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	v := createManagedTest(t, m, "data")
	writes := 0
	m.managed.commitFile = func(path string, raw []byte) error {
		writes++
		if err := writeVolumeSnapshot(path, raw); err != nil {
			return err
		}
		return errors.New("directory sync uncertain /private/path")
	}
	_, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	ve := requireVolumeStatus(t, err, 503)
	if strings.Contains(ve.Error(), "private") {
		t.Fatal("storage error leaked a path")
	}
	requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", v.ID, "t1"), 503)
	if writes != 1 || len(e.volumeSpecs) != 0 {
		t.Fatal("mutation or attach continued after uncertain commit")
	}
	m.Close()
	m = openManagedTest(t, cfg, e)
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteVolume(t.Context(), "data", v.ID, "t1"); err != nil {
		t.Fatal(err)
	}
}

func TestManagedCreateUnrecordedInodeRequiresQuarantine(t *testing.T) {
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
		t.Fatalf("interrupted create=%+v %v", v, err)
	}
	path := m.managed.path(volumeID(v.ID))
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = m.CreateVolume(t.Context(), "data", types.VolumeCreate{SizeGiB: 1}, "t1")
	ve := requireVolumeStatus(t, err, 503)
	if ve.Code != "volume_create_incomplete" {
		t.Fatal(ve)
	}
	requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", v.ID, "t1"), 503)
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("unknown inode modified")
	}
	m.Close()
	if err := os.Rename(path, filepath.Join(filepath.Dir(cfg.ManagedVolumeDir), "quarantine.raw")); err != nil {
		t.Fatal(err)
	}
	m = openManagedTest(t, cfg, e)
	repeat, created, err := m.CreateVolume(t.Context(), "data", types.VolumeCreate{SizeGiB: 1}, "t1")
	if err != nil || created || repeat.ID != v.ID {
		t.Fatalf("quarantine repair=%+v %v %v", repeat, created, err)
	}
}

func TestManagedCorruptAndUnsafeStorage(t *testing.T) {
	for _, fault := range []string{"truncated", "version", "missing-registry", "replaced", "symlink", "missing", "permissions"} {
		t.Run(fault, func(t *testing.T) {
			cfg := managedConfig(t)
			e := newManagedEngine()
			m := openManagedTest(t, cfg, e)
			v := createManagedTest(t, m, "data")
			path := m.managed.path(volumeID(v.ID))
			m.Close()
			switch fault {
			case "truncated":
				if err := os.WriteFile(filepath.Join(cfg.ManagedVolumeDir, "registry.json"), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "version":
				raw, err := os.ReadFile(filepath.Join(cfg.ManagedVolumeDir, "registry.json"))
				if err != nil {
					t.Fatal(err)
				}
				var data volumeRegistryData
				if err := json.Unmarshal(raw, &data); err != nil {
					t.Fatal(err)
				}
				data.Version = 99
				raw, _ = json.Marshal(data)
				if err := os.WriteFile(filepath.Join(cfg.ManagedVolumeDir, "registry.json"), raw, 0600); err != nil {
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
				f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
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
				if err := os.Chmod(path, 0000); err != nil {
					t.Fatal(err)
				}
			}
			m = openManagedTest(t, cfg, e)
			_, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
			requireVolumeStatus(t, err, 503)
			if len(e.volumeSpecs) != 0 {
				t.Fatal("unsafe backing attached")
			}
			if fault == "replaced" || fault == "symlink" || fault == "permissions" {
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
	if err := os.Chmod(cfg.ManagedVolumeDir, 0755); err != nil {
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
	unrelated, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{})
	if err != nil {
		t.Fatal(err)
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
	got, err := m.GetVolume("data", "t1")
	if err != nil || len(got.Holders) != 1 || got.Holders[0].State != "attaching" {
		t.Fatalf("attaching projection=%+v %v", got, err)
	}
	requireVolumeStatus(t, m.DeleteVolume(t.Context(), "data", v.ID, "t1"), 409)
	close(e.attachResume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
