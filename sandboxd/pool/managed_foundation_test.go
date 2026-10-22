package pool

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func foundationConfig(t *testing.T) *config.Config {
	t.Helper()
	dir := t.TempDir()
	return &config.Config{DataDir: filepath.Join(dir, "data"), ManagedVolumeDir: filepath.Join(dir, "managed"), APIToken: "root", Pools: []config.PoolSpec{{PoolKey: testKey, Warm: 1}}}
}

func foundationClaim(t *testing.T, m *Manager, names ...string) *types.Sandbox {
	t.Helper()
	sb, err := m.ClaimProvision(t.Context(), testKey, managedOptions(names...))
	if err != nil {
		t.Fatal(err)
	}
	return sb
}

func TestFoundationCreateCommitAllowsConcurrentVolumeRun(t *testing.T) {
	m := openManagedTest(t, foundationConfig(t), newManagedEngine())
	createManagedTest(t, m, "data")
	sb := foundationClaim(t, m, "data")
	unrelated := foundationClaim(t, m)
	entered, resume := make(chan struct{}), make(chan struct{})
	m.managed.commitFile = func(path string, raw []byte) error { close(entered); <-resume; return writeVolumeSnapshot(path, raw) }
	created := make(chan error, 1)
	go func() {
		_, _, err := m.CreateVolume(t.Context(), "other", types.VolumeCreate{SizeGiB: 1}, "t1")
		created <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("create never reached registry commit")
	}
	if !m.mu.TryLock() {
		close(resume)
		t.Fatal("create unexpectedly holds manager mutex before indirect waiter")
	}
	m.mu.Unlock()
	runDone := make(chan error, 1)
	go func() {
		_, run, err := m.beginVolumeRun(t.Context(), sb)
		if err == nil {
			m.finishVolumeRun(sb, run)
		}
		runDone <- err
	}()
	managerLocked := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !m.mu.TryLock() {
			managerLocked = true
			break
		}
		m.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	renewed := make(chan error, 1)
	go func() {
		_, err := m.Renew(t.Context(), unrelated.ID, Cred{Operator: true}, time.Minute, "")
		renewed <- err
	}()
	blocked := false
	select {
	case err := <-renewed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(150 * time.Millisecond):
		blocked = true
	}
	t.Logf("create commit initially manager unlocked; volume-run waiter manager_locked=%t unrelated_renew_blocked=%t", managerLocked, blocked)
	m.managed.commitFile = writeVolumeSnapshot
	close(resume)
	if err := <-created; err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if blocked {
		if err := <-renewed; err != nil {
			t.Fatal(err)
		}
	}
	if managerLocked || blocked {
		t.Error("registry I/O blocks global manager through volume-run lock ordering")
	}
}

func TestFoundationReleaseRollbackKeepsLiveClaim(t *testing.T) {
	cfg := foundationConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	createManagedTest(t, m, "data")
	sb := foundationClaim(t, m, "data")
	if err := os.Mkdir(m.store.path+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	releaseErr := m.Release(t.Context(), sb.ID, Cred{Operator: true})
	if releaseErr == nil {
		t.Fatal("injected claim persistence failure did not fail release")
	}
	_, live := m.Sandbox(sb.ID)
	_, patchErr := m.PatchVolumes(t.Context(), sb.ID, "t1", types.VolumePatch{})
	if err := os.Remove(m.store.path + ".tmp"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !m.store.recommitting.Load() && m.store.synced() })
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	next := openManagedTest(t, cfg, e)
	if err := next.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	next.retryRemovals(t.Context()).Wait()
	_, adopted := next.Sandbox(sb.ID)
	_, present, inspectErr := e.Inspect(t.Context(), sb.VMName)
	if !live || errors.Is(patchErr, ErrFailed) || !adopted || !present {
		t.Errorf("failed release: claim_restored=%t patch_error=%v after restart claim_present=%t VM_present=%t err=%v", live, patchErr, adopted, present, inspectErr)
	}
}

func TestManagedRegistryCloseWaitsForWriter(t *testing.T) {
	cfg := managedConfig(t)
	m := openManagedTest(t, cfg, newManagedEngine())
	entered, resume := make(chan struct{}), make(chan struct{})
	m.managed.commitFile = func(path string, raw []byte) error {
		close(entered)
		<-resume
		return writeVolumeSnapshot(path, raw)
	}
	created := make(chan error, 1)
	go func() {
		_, _, err := m.CreateVolume(t.Context(), "data", types.VolumeCreate{SizeGiB: 1}, "t1")
		created <- err
	}()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- m.Close() }()
	select {
	case <-closed:
		t.Fatal("closed the daemon lock during an active registry write")
	case <-time.After(20 * time.Millisecond):
	}
	if other, err := openVolumeRegistry(cfg.ManagedVolumeDir, "node"); err == nil {
		_ = other.close()
		t.Fatal("another daemon acquired the lock while a writer was active")
	}
	close(resume)
	<-created
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestManagedIdleRetryRecoversWriteFailure(t *testing.T) {
	cfg := managedConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	m.managed.commitFile = func(string, []byte) error { return syscall.ENOSPC }
	_, _, err := m.CreateVolume(t.Context(), "data", types.VolumeCreate{SizeGiB: 1}, "t1")
	requireVolumeStatus(t, err, 503)
	m.managed.commitFile = writeVolumeSnapshot
	m.reapOnce(t.Context())
	waitFor(t, func() bool {
		_, _, err := m.CreateVolume(t.Context(), "data", types.VolumeCreate{SizeGiB: 1}, "t1")
		return err == nil
	})
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m = openManagedTest(t, cfg, e)
	if _, err := m.GetVolume("data", "t1"); err != nil {
		t.Fatalf("maintenance did not persist the retained registry state: %v", err)
	}
}

func TestManagedRetiredCatalogNameRemainsReserved(t *testing.T) {
	cfg := managedConfig(t)
	path := filepath.Join(filepath.Dir(cfg.DataDir), "catalog.img")
	if err := os.WriteFile(path, []byte("catalog"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: path, Writable: true}}
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	sb := foundationClaim(t, m, "catalog")
	m.Close()
	cfg.Volumes = nil
	m = openManagedTest(t, cfg, e)
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, _, err := m.CreateVolume(t.Context(), "catalog", types.VolumeCreate{SizeGiB: 1}, "t1")
	requireVolumeStatus(t, err, 409)
	createManagedTest(t, m, "other")
	if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err != nil {
		t.Fatal(err)
	}
	createManagedTest(t, m, "catalog")
}

func TestManagedReadFailureDoesNotChangeDurableLifecycle(t *testing.T) {
	if os.Getenv("MANAGED_EMFILE_CHILD") == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestManagedReadFailureDoesNotChangeDurableLifecycle$")
		cmd.Env = append(os.Environ(), "MANAGED_EMFILE_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated EMFILE test: %v\n%s", err, out)
		}
		return
	}
	m := openManagedTest(t, managedConfig(t), newManagedEngine())
	createManagedTest(t, m, "data")
	path := filepath.Join(m.managed.dir, "registry.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var previous syscall.Rlimit
	if err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &previous); err != nil {
		t.Fatal(err)
	}
	limit := previous
	limit.Cur = 64
	if err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	var files []*os.File
	for {
		f, openErr := os.Open("/dev/null")
		if errors.Is(openErr, syscall.EMFILE) {
			break
		}
		if openErr != nil {
			t.Fatal(openErr)
		}
		files = append(files, f)
	}
	_, getErr := m.GetVolume("data", "t1")
	_, _, placementErr := m.VolumePlacement(testKey, "t1", []string{"data"})
	for _, f := range files {
		_ = f.Close()
	}
	if err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &previous); err != nil {
		t.Fatal(err)
	}
	requireVolumeStatus(t, getErr, 503)
	requireVolumeStatus(t, placementErr, 503)
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("read-only lookup changed the durable registry")
	}
	info, err := m.GetVolume("data", "t1")
	if err != nil || info.State != "ready" {
		t.Fatalf("transient open error persisted: %+v %v", info, err)
	}
}

func TestManagedRegistryParseFailureFailsStartup(t *testing.T) {
	cfg := managedConfig(t)
	m := openManagedTest(t, cfg, newManagedEngine())
	createManagedTest(t, m, "data")
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.ManagedVolumeDir, "registry.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewManager(t.Context(), cfg, newManagedEngine(), testSecrets(t), testBackends(t, cfg)); err == nil {
		t.Fatal("a corrupt registry opened")
	}
}
