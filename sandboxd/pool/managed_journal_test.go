package pool

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// staleVMEngine fails `vm list`, `vm rm` or `vm inspect` on demand.
type staleVMEngine struct {
	*managedTestEngine
	mu                   sync.Mutex
	listErr              error
	stuck, uninspectable string
}

func (e *staleVMEngine) List(ctx context.Context) ([]types.VMRecord, error) {
	e.mu.Lock()
	err := e.listErr
	e.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return e.managedTestEngine.List(ctx)
}

func (e *staleVMEngine) Remove(ctx context.Context, name string) error {
	e.mu.Lock()
	stuck := e.stuck == name
	e.mu.Unlock()
	if stuck {
		return errors.New("vm rm: device busy")
	}
	return e.managedTestEngine.Remove(ctx, name)
}

func (e *staleVMEngine) Inspect(ctx context.Context, name string) (types.VMRecord, bool, error) {
	e.mu.Lock()
	failed := e.uninspectable == name
	e.mu.Unlock()
	if failed {
		return types.VMRecord{}, false, errors.New("vm inspect: transient failure")
	}
	return e.managedTestEngine.Inspect(ctx, name)
}

func (e *staleVMEngine) set(edit func(*staleVMEngine)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	edit(e)
}

func requireManagedBusy(t *testing.T, m *Manager, name, state string) {
	t.Helper()
	_, err := m.ClaimProvision(t.Context(), testKey, managedOptions(name))
	v := requireVolumeStatus(t, err, 409)
	if len(v.Holders) == 0 || v.Holders[0].State != state {
		t.Fatalf("claim of %s refused with holders %+v, want state %q", name, v.Holders, state)
	}
}

func TestManagedListFailureLeavesClaimsAndVMs(t *testing.T) {
	cfg := managedConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	sb := &types.Sandbox{ID: "sb_ordinary", VMName: "sbx-ordinary", Key: egKey, VsockSocket: "/fake/vsock", Token: "token"}
	e.vms[sb.VMName] = sb.VsockSocket
	if err := m.store.commit(m.store.set(sb)); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m = openManagedTest(t, cfg, e)
	withOutbound(t, m, func(o *outbound.Options) { o.LockNIC = true; o.Sweep = func(map[string]bool) error { return nil } })
	inventoryErr := errors.New("cocoon vm list: transient failure")
	m.eng = &staleVMEngine{managedTestEngine: e, listErr: inventoryErr}
	if err := m.Reconcile(t.Context()); !errors.Is(err, inventoryErr) {
		t.Fatalf("startup accepted an unknown inventory: %v", err)
	}
	durable, err := newClaimStore(cfg.DataDir, false).load()
	if err != nil {
		t.Fatal(err)
	}
	if _, kept := durable[sb.ID]; !kept {
		t.Error("a transient list failure deleted a claim row")
	}
	if rec, found, err := e.Inspect(t.Context(), sb.VMName); err != nil || !found || rec.State != vmStateRunning {
		t.Fatalf("a transient list failure touched a claim's VM: found=%v state=%s err=%v", found, rec.State, err)
	}
}

func TestManagedRestartAfterVMLossConverges(t *testing.T) {
	cfg := managedConfig(t)
	base := newManagedEngine()
	m := openManagedTest(t, cfg, base)
	createManagedTest(t, m, "data")
	sb, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
	if err = base.fakeEngine.Remove(t.Context(), sb.VMName); err != nil {
		t.Fatal(err)
	}
	if _, err = base.RunCold(t.Context(), "sbx-orphan", testKey, ""); err != nil {
		t.Fatal(err)
	}
	e := &staleVMEngine{managedTestEngine: base, stuck: "sbx-orphan"}
	for restart := range 2 {
		m = openManagedTest(t, cfg, base)
		m.eng = e
		if err = m.Reconcile(t.Context()); err != nil {
			t.Fatalf("restart %d: %v", restart, err)
		}
		if _, live := m.Sandbox(sb.ID); live {
			t.Fatalf("restart %d re-adopted a claim whose VM is gone", restart)
		}
		requireManagedClaimable(t, m, "data")
		if err = m.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRegistryWriteFaultIsLocal(t *testing.T) {
	cfg := managedConfig(t)
	cfg.Volumes = []config.VolumeSpec{{Name: "catalog", Path: writeVolumeImage(t, "catalog.img", "catalog"), Writable: true}}
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	createManagedTest(t, m, "data")
	managed := foundationClaim(t, m, "data")
	catalog := foundationClaim(t, m, "catalog")
	m.managed.commitFile = func(string, []byte) error { return errors.New("disk full") }
	_, _, err := m.CreateVolume(t.Context(), "other", types.VolumeCreate{SizeGiB: 1}, "t1")
	requireVolumeStatus(t, err, 503)
	for _, sb := range []*types.Sandbox{managed, catalog} {
		if _, err := m.AgentSocket(sb.ID, sb.Token); err != nil {
			t.Errorf("a registry write fault took the socket of %v: %v", sb.Volumes, err)
		}
		if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
			t.Errorf("a registry write fault blocked release of %v: %v", sb.Volumes, err)
		}
		if e.vmAlive(sb.VMName) {
			t.Errorf("release of %v left its VM", sb.Volumes)
		}
	}
}

func TestManagedStartupQueuesUnremovableStaleVM(t *testing.T) {
	for _, inspectFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "inventory known", true: "inspect fails"}[inspectFails], func(t *testing.T) {
			cfg := managedConfig(t)
			base := newManagedEngine()
			m := openManagedTest(t, cfg, base)
			work := createManagedTest(t, m, "work")
			createManagedTest(t, m, "other")
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := base.RunCold(t.Context(), "sbx-stale", testKey, ""); err != nil {
				t.Fatal(err)
			}
			base.mu.Lock()
			base.disks = map[string][]types.VMDisk{"sbx-stale": {{Name: "work", Path: m.managed.path(volumeID(work.ID))}}}
			base.mu.Unlock()
			e := &staleVMEngine{managedTestEngine: base, stuck: "sbx-stale"}
			if inspectFails {
				e.uninspectable = "sbx-stale"
			}
			m = openManagedTest(t, cfg, base)
			m.eng = e
			if err := m.Reconcile(t.Context()); err != nil {
				t.Fatalf("an unremovable stale VM failed startup: %v", err)
			}
			requireManagedBusy(t, m, "work", "releasing")
			if inspectFails {
				requireManagedBusy(t, m, "other", "releasing")
				e.set(func(e *staleVMEngine) { e.uninspectable = "" })
				m.retryRemovals(t.Context()).Wait()
				requireManagedBusy(t, m, "work", "releasing")
			}
			requireManagedClaimable(t, m, "other")
			e.set(func(e *staleVMEngine) { e.stuck = "" })
			m.retryRemovals(t.Context()).Wait()
			requireManagedClaimable(t, m, "work")
		})
	}
}

func TestManagedReleaseClaimsWriteFailureRecommits(t *testing.T) {
	m := openManagedTest(t, managedConfig(t), newManagedEngine())
	createManagedTest(t, m, "data")
	sb := foundationClaim(t, m, "data")
	tmp := m.store.path + ".tmp"
	if err := os.Mkdir(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("a release whose claims write failed reported success")
	}
	if _, live := m.Sandbox(sb.ID); !live {
		t.Fatal("a failed release dropped the claim")
	}
	if err := os.Remove(tmp); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !m.store.recommitting.Load() && m.store.synced() })
	createManagedTest(t, m, "other")
	requireManagedClaimable(t, m, "other")
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("retried release: %v", err)
	}
	requireManagedClaimable(t, m, "data")
}

func TestWarmManagedClaimWritesNoRegistry(t *testing.T) {
	e := newManagedEngine()
	m := openManagedTest(t, managedConfig(t), e)
	createManagedTest(t, m, "data")
	createManagedTest(t, m, "spare")
	warm := refillWarmVM(t, m)
	e.mu.Lock()
	e.pids[warm.VMName] = 4242
	e.mu.Unlock()
	var writes atomic.Int32
	m.managed.commitFile = func(path string, raw []byte) error {
		writes.Add(1)
		return writeVolumeSnapshot(path, raw)
	}
	if _, err := m.ClaimWarm(t.Context(), testKey, managedOptions("data")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ClaimWarm(t.Context(), testKey, managedOptions("spare")); !errors.Is(err, ErrNoWarm) {
		t.Fatalf("second warm claim: %v, want a warm miss", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, managedOptions("spare")); err != nil {
		t.Fatal(err)
	}
	if n := writes.Load(); n != 0 {
		t.Fatalf("a warm managed claim and a warm-miss fallback wrote registry.json %d times", n)
	}
}
