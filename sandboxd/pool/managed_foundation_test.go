package pool

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
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

func TestFoundationReleaseCommitAllowsUnrelatedRenew(t *testing.T) {
	m := openManagedTest(t, foundationConfig(t), newManagedEngine())
	createManagedTest(t, m, "data")
	sb := foundationClaim(t, m, "data")
	unrelated := foundationClaim(t, m)
	entered, resume := make(chan struct{}), make(chan struct{})
	m.managed.commitFile = func(path string, raw []byte) error {
		close(entered)
		<-resume
		return writeVolumeSnapshot(path, raw)
	}
	released := make(chan error, 1)
	go func() { released <- m.Release(t.Context(), sb.ID, Cred{Operator: true}) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("release never reached registry commit")
	}
	managerLocked := !m.mu.TryLock()
	if !managerLocked {
		m.mu.Unlock()
	}
	registryLocked := !m.managed.mu.TryLock()
	if !registryLocked {
		m.managed.mu.Unlock()
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
	t.Logf("at stalled registry commit manager_locked=%t registry_locked=%t unrelated_renew_blocked=%t", managerLocked, registryLocked, blocked)
	m.managed.commitFile = writeVolumeSnapshot
	close(resume)
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	if blocked {
		if err := <-renewed; err != nil {
			t.Fatal(err)
		}
	}
	if managerLocked || registryLocked || blocked {
		t.Error("registry persistence blocks unrelated lease renewal while holding global locks")
	}
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

func TestFoundationRegistryFaultRecoversForReleaseAndReap(t *testing.T) {
	m := openManagedTest(t, foundationConfig(t), newManagedEngine())
	for _, name := range []string{"first", "second", "third"} {
		createManagedTest(t, m, name)
	}
	first := foundationClaim(t, m, "first")
	second := foundationClaim(t, m, "second")
	third := foundationClaim(t, m, "third")
	var calls atomic.Int64
	m.managed.commitFile = func(string, []byte) error { calls.Add(1); return syscall.ENOSPC }
	err := m.Release(t.Context(), first.ID, Cred{Operator: true})
	if _, live := m.Sandbox(first.ID); live {
		t.Fatal("registry failure prevented durable claim release")
	}
	if _, held := m.managedHold(first.ID); !held {
		t.Fatal("registry failure discharged the reservation")
	}
	m.managed.commitFile = func(path string, raw []byte) error { calls.Add(1); return writeVolumeSnapshot(path, raw) }
	secondErr := m.Release(t.Context(), second.ID, Cred{Operator: true})
	m.mu.Lock()
	third.Deadline = time.Now().Add(-time.Minute)
	m.mu.Unlock()
	m.reapOnce(t.Context())
	waitFor(t, func() bool { _, live := m.Sandbox(third.ID); return !live })
	_, retained := m.Sandbox(third.ID)
	t.Logf("first_release=%v second_release=%v expired_claim_retained=%t registry_commit_calls=%d", err, secondErr, retained, calls.Load())
	if secondErr != nil || retained {
		t.Error("one registry write fault blocks unrelated release and expiry after storage hook is healthy")
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
	h, held := m.managedHold(sb.ID)
	_, live := m.Sandbox(sb.ID)
	_, patchErr := m.PatchVolumes(t.Context(), sb.ID, "t1", types.VolumePatch{})
	t.Logf("release_error=%v claim_restored=%t hold_present=%t hold_phase=%s patch_error=%v", releaseErr, live, held, h.State, patchErr)
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
	t.Logf("after_restart claim_present=%t VM_present=%t inspect_error=%v", adopted, present, inspectErr)
	if h.State == holdRetiring || errors.Is(patchErr, ErrFailed) || !adopted || !present {
		t.Error("failed release restored claim but retained retirement and restart destroyed its VM")
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

func TestManagedCorruptionRetainsManagedClaimAndServesOrdinaryClaims(t *testing.T) {
	cfg := managedConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	createManagedTest(t, m, "data")
	managed := foundationClaim(t, m, "data")
	ordinary := foundationClaim(t, m)
	m.Close()
	if err := os.WriteFile(filepath.Join(cfg.ManagedVolumeDir, "registry.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	m = openManagedTest(t, cfg, e)
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, live := m.Sandbox(managed.ID); !live {
		t.Fatal("metadata uncertainty discarded the durable managed claim")
	}
	if _, err := m.Renew(t.Context(), ordinary.ID, Cred{Operator: true}, time.Minute, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(t.Context(), ordinary.ID, Cred{Operator: true}); err != nil {
		t.Fatal(err)
	}
	if _, present, err := e.Inspect(t.Context(), ordinary.VMName); err != nil || present {
		t.Fatalf("ordinary VM survived release: %t %v", present, err)
	}
	if _, present, err := e.Inspect(t.Context(), managed.VMName); err != nil || !present {
		t.Fatalf("uncertain managed VM was removed: %t %v", present, err)
	}
	requireVolumeStatus(t, m.Release(t.Context(), managed.ID, Cred{Operator: true}), 503)
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestManagedIdleRetryRecoversWriteFailure(t *testing.T) {
	m := openManagedTest(t, managedConfig(t), newManagedEngine())
	createManagedTest(t, m, "data")
	sb := foundationClaim(t, m, "data")
	m.managed.commitFile = func(string, []byte) error { return syscall.ENOSPC }
	_ = m.Release(t.Context(), sb.ID, Cred{Operator: true})
	if m.Counters().ManagedWriteFailures == 0 {
		t.Fatal("write failure was not counted")
	}
	m.managed.commitFile = writeVolumeSnapshot
	m.reapOnce(t.Context())
	waitFor(t, func() bool { return m.ManagedVolumeStatus() == nil })
	if m.Counters().ManagedWriteRecoveries != 1 {
		t.Fatal("maintenance did not retry the retained registry state")
	}
	m.retryRemovals(t.Context()).Wait()
	if _, held := m.managedHold(sb.ID); held {
		t.Fatal("recovered teardown retained a discharged hold")
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
	_, placementErr := m.VolumePlacement(testKey, "t1", []string{"data"})
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

func TestManagedCatalogAdmissionCanceledDuringCommitReleasesAllCharges(t *testing.T) {
	cfg := managedConfig(t)
	cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: writeVolumeImage(t, "catalog.img", "catalog"), Writable: true}}
	m := openManagedTest(t, cfg, newManagedEngine())
	createManagedTest(t, m, "data")
	sb := foundationClaim(t, m, "data")
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	m.managed.commitFile = func(path string, raw []byte) error {
		once.Do(func() { close(entered); <-resume })
		return writeVolumeSnapshot(path, raw)
	}
	patched := make(chan error, 1)
	go func() {
		_, err := m.PatchVolumes(t.Context(), sb.ID, "t1", types.VolumePatch{Add: []types.Volume{{Name: "catalog"}}})
		patched <- err
	}()
	<-entered
	released := make(chan error, 1)
	go func() { released <- m.Release(t.Context(), sb.ID, Cred{Operator: true}) }()
	waitFor(t, func() bool { _, live := m.Sandbox(sb.ID); return !live })
	close(resume)
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	if err := <-patched; err == nil {
		t.Fatal("canceled admission succeeded")
	}
	if got := volumeHoldersOf(m, "catalog"); got != (volumeHolders{}) {
		t.Fatalf("canceled catalog admission leaked charge: %+v", got)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, managedOptions("catalog")); err != nil {
		t.Fatal(err)
	}
}

func TestFoundationCatalogAddCommitsOutsideTransitionAndManager(t *testing.T) {
	cfg := foundationConfig(t)
	path := filepath.Join(filepath.Dir(cfg.DataDir), "catalog.raw")
	if err := os.WriteFile(path, []byte("catalog"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: path}}
	m := openManagedTest(t, cfg, newManagedEngine())
	createManagedTest(t, m, "data")
	sb := foundationClaim(t, m, "data")
	entered, resume := make(chan struct{}), make(chan struct{})
	m.managed.commitFile = func(path string, raw []byte) error { close(entered); <-resume; return writeVolumeSnapshot(path, raw) }
	patched := make(chan error, 1)
	go func() {
		_, err := m.PatchVolumes(t.Context(), sb.ID, "t1", types.VolumePatch{Add: []types.Volume{{Name: "catalog"}}})
		patched <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(resume)
		t.Fatal("catalog add never reached registry commit")
	}
	transitionLocked := !sb.Transition.TryLock()
	if !transitionLocked {
		sb.Transition.Unlock()
	}
	managerLocked := !m.mu.TryLock()
	if !managerLocked {
		m.mu.Unlock()
	}
	registryLocked := !m.managed.mu.TryLock()
	if !registryLocked {
		m.managed.mu.Unlock()
	}
	t.Logf("catalog add registry commit transition_locked=%t manager_locked=%t registry_locked=%t", transitionLocked, managerLocked, registryLocked)
	m.managed.commitFile = writeVolumeSnapshot
	close(resume)
	if err := <-patched; err != nil {
		t.Fatal(err)
	}
	if transitionLocked || managerLocked || registryLocked {
		t.Error("catalog live-add persistence runs under Transition and both global mutexes")
	}
}

func TestFoundationFinishSkipsAbsentClaimWrite(t *testing.T) {
	e := newManagedEngine()
	m := openManagedTest(t, foundationConfig(t), e)
	createManagedTest(t, m, "data")
	sb := foundationClaim(t, m, "data")
	m.releaseDelay = time.Hour
	if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err != nil {
		t.Fatal(err)
	}
	if err := e.Remove(t.Context(), sb.VMName); err != nil {
		t.Fatal(err)
	}
	before := m.store.mark()
	m.store.writeMu.Lock()
	done := make(chan bool, 1)
	go func() { done <- m.finishManaged(sb.ID, nil) }()
	select {
	case ok := <-done:
		if !ok {
			t.Error("finish failed")
		}
	case <-time.After(time.Second):
		m.store.writeMu.Unlock()
		<-done
		t.Fatal("finish waited for the claim writer with no claim row to remove")
	}
	m.store.writeMu.Unlock()
	if after := m.store.mark(); after != before {
		t.Fatalf("finish sequenced a redundant claim deletion: before=%v after=%v", before, after)
	}
}

func TestFoundationMountedReleaseRollbackKeepsLiveClaim(t *testing.T) {
	cfg := foundationConfig(t)
	cfg.ManagedVolumeHibernate = &config.ManagedVolumeHibernate{HookArgv: []string{"/usr/local/bin/volume-hook"}, HookTimeoutSeconds: 1}
	m := openManagedTest(t, cfg, newManagedEngine())
	createManagedTest(t, m, "data")
	opts := managedOptions("data")
	opts.Volumes[0].AttachOnly = false
	sb, err := m.ClaimProvision(t.Context(), testKey, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !m.managedLifecycle(sb) {
		t.Fatal("mounted claim did not select managed lifecycle release")
	}
	if err := os.Mkdir(m.store.path+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	releaseErr := m.Release(t.Context(), sb.ID, Cred{Operator: true})
	if releaseErr == nil {
		t.Fatal("injected mounted release persistence failure did not fail")
	}
	h, held := m.managedHold(sb.ID)
	_, live := m.Sandbox(sb.ID)
	if err := os.Remove(m.store.path + ".tmp"); err != nil {
		t.Fatal(err)
	}
	unsynced := !m.store.synced()
	t.Logf("managed lifecycle release_error=%v claim_restored=%t hold_present=%t hold_phase=%s claims_store_unsynced=%t recommitting=%t", releaseErr, live, held, h.State, unsynced, m.store.recommitting.Load())
	if err := m.store.commitDurable(m.store.mark()); err != nil {
		t.Fatal(err)
	}
	if h.State == holdRetiring {
		t.Error("managed lifecycle failed release also restores claim without restoring its hold phase")
	}
}

func TestManagedClaimPostRenameFailureFencesWithoutTeardown(t *testing.T) {
	m := openManagedTest(t, managedConfig(t), newManagedEngine())
	createManagedTest(t, m, "data")
	sb := foundationClaim(t, m, "data")
	m.store.syncDirectory = func(string) error { return syscall.ENOSPC }
	if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err == nil {
		t.Fatal("ambiguous claim deletion reported success")
	}
	waitFor(t, func() bool { return !m.store.recommitting.Load() && m.store.synced() })
	requireVolumeStatus(t, m.ManagedVolumeStatus(), 503)
	m.reapOnce(t.Context())
	requireVolumeStatus(t, m.ManagedVolumeStatus(), 503)
	if _, live := m.Sandbox(sb.ID); !live {
		t.Fatal("ambiguous deletion discarded the claim")
	}
	if _, present, err := m.eng.Inspect(t.Context(), sb.VMName); err != nil || !present {
		t.Fatalf("ambiguous deletion tore down the VM: %t %v", present, err)
	}
	if _, held := m.managedHold(sb.ID); !held {
		t.Fatal("ambiguous deletion discharged the hold")
	}
}

func TestManagedStoppedZeroPIDRequiresOriginalProcessGone(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		name := "live"
		if unknown {
			name = "unreadable"
		}
		t.Run(name, func(t *testing.T) {
			cfg := managedConfig(t)
			e := newManagedEngine()
			m := openManagedTest(t, cfg, e)
			createManagedTest(t, m, "data")
			sb := foundationClaim(t, m, "data")
			h, _ := m.managedHold(sb.ID)
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			e.mu.Lock()
			e.stopped[sb.VMName] = true
			e.pids[sb.VMName] = 0
			e.mu.Unlock()
			m = openManagedTest(t, cfg, e)
			m.managed.process = func(int) (processIdentity, error) {
				if unknown {
					return processIdentity{}, os.ErrPermission
				}
				return h.Process, nil
			}
			if err := m.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			retained, held := m.managedHold(sb.ID)
			if !held || retained.Process != h.Process || retained.State != holdClaimed {
				t.Fatalf("uncertain original process lost ownership: %+v", retained)
			}
			requireVolumeStatus(t, m.ManagedVolumeStatus(), 503)
			if _, live := m.Sandbox(sb.ID); !live {
				t.Fatal("uncertain original process lost its durable claim")
			}
			e.mu.Lock()
			defer e.mu.Unlock()
			if len(e.removes) != 0 || len(e.starts) != 0 {
				t.Fatalf("uncertain original process changed VM: removes=%v starts=%v", e.removes, e.starts)
			}
		})
	}
}

func TestManagedHealthyDurableClaimDefeatsRetirement(t *testing.T) {
	cfg := managedConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	createManagedTest(t, m, "data")
	sb := foundationClaim(t, m, "data")
	if err := m.retireManaged(sb.ID); err != nil {
		t.Fatal(err)
	}
	m.Close()
	m = openManagedTest(t, cfg, e)
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	h, held := m.managedHold(sb.ID)
	if _, live := m.Sandbox(sb.ID); !live || !held || h.State != holdClaimed {
		t.Fatalf("durable healthy owner was not restored: live=%t hold=%+v", live, h)
	}
	if _, present, err := e.Inspect(t.Context(), sb.VMName); err != nil || !present {
		t.Fatalf("durable owner's VM was removed: %t %v", present, err)
	}
}

func TestManagedFailedUnboundCancellationHealsWithoutRestart(t *testing.T) {
	m := openManagedTest(t, managedConfig(t), newManagedEngine())
	createManagedTest(t, m, "data")
	specs, reserved, err := m.admitOptions(t.Context(), testKey, managedOptions("data"))
	if err != nil {
		t.Fatal(err)
	}
	m.managed.commitFile = func(string, []byte) error { return syscall.ENOSPC }
	m.releaseUnbound(specs, reserved)
	if volumeHoldersOf(m, "data").writers != 1 {
		t.Fatal("failed discharge released the charge before durable deletion")
	}
	m.managed.commitFile = writeVolumeSnapshot
	m.reapOnce(t.Context())
	waitFor(t, func() bool { return volumeHoldersOf(m, "data") == (volumeHolders{}) })
	if _, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data")); err != nil {
		t.Fatal(err)
	}
}

func TestManagedBlockedRetryAllowsOrdinaryExpiry(t *testing.T) {
	m := openManagedTest(t, managedConfig(t), newManagedEngine())
	createManagedTest(t, m, "data")
	specs, reserved, err := m.admitOptions(t.Context(), testKey, managedOptions("data"))
	if err != nil {
		t.Fatal(err)
	}
	ordinary := foundationClaim(t, m)
	m.managed.commitFile = func(string, []byte) error { return syscall.ENOSPC }
	m.releaseUnbound(specs, reserved)
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	m.managed.commitFile = func(path string, raw []byte) error {
		once.Do(func() { close(entered); <-resume })
		return writeVolumeSnapshot(path, raw)
	}
	m.mu.Lock()
	ordinary.Deadline = time.Now().Add(-time.Minute)
	m.mu.Unlock()
	done := make(chan struct{})
	go func() { m.reapOnce(t.Context()); close(done) }()
	<-entered
	select {
	case <-done:
	case <-time.After(time.Second):
		close(resume)
		<-done
		t.Fatal("managed retry blocked ordinary expiry")
	}
	if _, live := m.Sandbox(ordinary.ID); live {
		t.Error("ordinary expired claim remained live during managed retry")
	}
	close(resume)
	waitFor(t, func() bool { return !m.managed.retrying.Load() })
}

func TestManagedMissingBootMetadataCannotProveProcessGone(t *testing.T) {
	proc := t.TempDir()
	r := &volumeRegistry{process: func(pid int) (processIdentity, error) { return readProcessIdentityAt(proc, pid) }}
	_, err := r.process(123)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("boot metadata error lost its underlying cause: %v", err)
	}
	if r.processGone(processIdentity{BootID: "boot", PID: 123, Start: 1}) {
		t.Fatal("missing boot metadata was treated as an absent VMM")
	}
}

func TestManagedRecoveryFencesAgentSocket(t *testing.T) {
	m, e, sb, _, _ := newGrowthTest(t)
	ordinary := &types.Sandbox{ID: "sb_ordinary", VMName: "sbx-ordinary", Token: "ordinary-token", VsockSocket: "/ordinary/socket"}
	e.vms[ordinary.VMName] = ordinary.VsockSocket
	m.mu.Lock()
	m.addClaimLocked(ordinary)
	m.mu.Unlock()
	m.fenceManagedRecovery(t.Context(), errors.New("inventory unavailable"), map[string]*types.Sandbox{sb.ID: sb, ordinary.ID: ordinary}, nil, true)
	if socket, err := m.AgentSocket(sb.ID, sb.Token); err == nil || socket != "" {
		t.Fatalf("uncertain managed claim exposes socket %q, error=%v", socket, err)
	}
	if socket, err := m.AgentSocket(ordinary.ID, ordinary.Token); err != nil || socket != ordinary.VsockSocket {
		t.Fatalf("ordinary claim affected: socket=%q error=%v", socket, err)
	}
	sb.Volumes = nil
	if socket, err := m.AgentSocket(sb.ID, sb.Token); err == nil || socket != "" {
		t.Fatalf("recorded uncertain VM exposes socket after empty volume projection: %q, %v", socket, err)
	}
}

func TestManagedFaultRetainsStaleCatalogWriter(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		name := "observed"
		if unknown {
			name = "unknown"
		}
		t.Run(name, func(t *testing.T) {
			cfg := managedConfig(t)
			path := filepath.Join(filepath.Dir(cfg.DataDir), "catalog.img")
			if err := os.WriteFile(path, []byte("catalog"), 0o600); err != nil {
				t.Fatal(err)
			}
			freePath := filepath.Join(filepath.Dir(cfg.DataDir), "other.img")
			if err := os.WriteFile(freePath, []byte("other"), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: path, Writable: true}, {Name: "other", Path: freePath, Writable: true}}
			e := newManagedEngine()
			m := openManagedTest(t, cfg, e)
			old := foundationClaim(t, m, "catalog")
			e.disks = map[string][]types.VMDisk{old.VMName: {{Name: "catalog", Path: path, ReadOnly: false}}}
			m.releaseDelay = time.Hour
			if err := m.Release(t.Context(), old.ID, Cred{Operator: true}); err != nil {
				t.Fatal(err)
			}
			if _, present := e.vms[old.VMName]; !present {
				t.Fatal("delayed release removed VM")
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(cfg.ManagedVolumeDir, "registry.json"), []byte("{"), 0o600); err != nil {
				t.Fatal(err)
			}
			e.missingInventory.Store(unknown)
			m = openManagedTest(t, cfg, e)
			if err := m.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, present := e.vms[old.VMName]; !present {
				t.Fatal("unknown stale writer was swept")
			}

			if newer, err := m.ClaimProvision(t.Context(), testKey, managedOptions("catalog")); err == nil {
				t.Fatalf("second writer %s admitted while old VM %s remains", newer.VMName, old.VMName)
			}
			if _, err := m.ClaimProvision(t.Context(), testKey, managedOptions("other")); (err != nil) != unknown {
				t.Fatalf("unrelated catalog availability: unknown=%v error=%v", unknown, err)
			}
			foundationClaim(t, m)
		})
	}
}

func TestManagedListFailureRetainsStateAndRejectsStartup(t *testing.T) {
	cfg := managedConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	createManagedTest(t, m, "data")
	sb := foundationClaim(t, m, "data")
	ordinary := foundationClaim(t, m)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m = openManagedTest(t, cfg, e)
	inventoryErr := errors.New("VM inventory unavailable")
	m.eng = &failedListEngine{managedTestEngine: e, err: inventoryErr}
	if err := m.Reconcile(t.Context()); !errors.Is(err, inventoryErr) {
		t.Errorf("incomplete VM inventory permitted startup: %v", err)
	}
	for _, id := range []string{sb.ID, ordinary.ID} {
		if _, present := m.Sandbox(id); !present {
			t.Errorf("inventory failure lost durable claim %s", id)
		}
	}
	if _, held := m.managedHold(sb.ID); !held {
		t.Error("inventory failure discharged managed reservation")
	}
	if rec, found, err := e.Inspect(t.Context(), sb.VMName); err != nil || !found || rec.State != vmStateRunning {
		t.Errorf("inventory failure destroyed retained VM: found=%v state=%s error=%v", found, rec.State, err)
	}
}

type failedListEngine struct {
	*managedTestEngine
	err error
}

func (e *failedListEngine) List(context.Context) ([]types.VMRecord, error) {
	return nil, e.err
}
