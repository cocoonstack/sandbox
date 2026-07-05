package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var testKey = types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall}

func TestClaimWarmHitTransfersOwnership(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	warm := &types.Sandbox{VMName: "sbx-warm-1", Key: testKey, VsockSocket: "/vsock/warm"}
	m.pools[testKey].warm = append(m.pools[testKey].warm, warm)

	sb, err := m.Claim(t.Context(), testKey, 0)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if sb.VMName != "sbx-warm-1" || sb.VsockSocket != "/vsock/warm" {
		t.Errorf("got %+v, want the warm sandbox", sb)
	}
	if !strings.HasPrefix(sb.ID, "sb_") || len(sb.Token) != 32 {
		t.Errorf("identity not stamped: id=%q token=%q", sb.ID, sb.Token)
	}
	wantDeadline := time.Now().Add(defaultTTL)
	if d := sb.Deadline.Sub(wantDeadline).Abs(); d > time.Second {
		t.Errorf("deadline off default TTL by %v", d)
	}
	if n := len(eng.clones) + len(eng.colds); n != 0 {
		t.Errorf("warm hit ran %d VM operations, want 0", n)
	}
	if got, _ := newStore(m.dataDir).load(); len(got) != 1 {
		t.Errorf("claim not persisted: %d entries", len(got))
	}
}

func TestClaimClampsTTL(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})

	sb, err := m.Claim(t.Context(), testKey, 48*time.Hour)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if d := sb.Deadline.Sub(time.Now().Add(maxTTL)).Abs(); d > time.Second {
		t.Errorf("deadline off max TTL by %v", d)
	}
}

func TestClaimMissClonesFromGolden(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	m.pools[testKey].goldenDir = "/goldens/x"

	sb, err := m.Claim(t.Context(), testKey, 0)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(eng.clones) != 1 || len(eng.colds) != 0 {
		t.Fatalf("got clones=%v colds=%v, want one clone", eng.clones, eng.colds)
	}
	if !strings.HasPrefix(sb.VMName, vmPrefix+testKey.Hash()) {
		t.Errorf("vm name %q missing pool prefix", sb.VMName)
	}
	if want := "/vsock/" + sb.VMName; sb.VsockSocket != want {
		t.Errorf("vsock %q, want %q", sb.VsockSocket, want)
	}
	if got := eng.probeTimeouts[0]; got != claimProbeTimeout {
		t.Errorf("probe timeout %v, want %v", got, claimProbeTimeout)
	}
}

func TestClaimUnpooledKeyColdBoots(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)

	if _, err := m.Claim(t.Context(), testKey, 0); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(eng.colds) != 1 || len(eng.clones) != 0 {
		t.Fatalf("got clones=%v colds=%v, want one cold boot", eng.clones, eng.colds)
	}
	if got := eng.probeTimeouts[0]; got != coldProbeTimeout {
		t.Errorf("probe timeout %v, want %v", got, coldProbeTimeout)
	}
}

func TestClaimProbeFailureDestroysVM(t *testing.T) {
	eng := newFakeEngine()
	eng.probeErr = errors.New("never ready")
	m := newTestManager(t, eng)

	if _, err := m.Claim(t.Context(), testKey, 0); err == nil {
		t.Fatal("Claim succeeded with failing probe")
	}
	if len(eng.removes) != 1 {
		t.Errorf("removes=%v, want the failed VM destroyed", eng.removes)
	}
	if _, n := m.Info(); n != 0 {
		t.Errorf("claimed=%d, want 0", n)
	}
}

func TestClaimEgressNeedsAttachment(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	key := types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeSmall}

	if _, err := m.Claim(t.Context(), key, 0); !errors.Is(err, ErrNoEgress) {
		t.Errorf("got %v, want ErrNoEgress", err)
	}
}

func TestReleaseValidatesToken(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb, err := m.Claim(t.Context(), testKey, 0)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	if err := m.Release(t.Context(), sb.ID, "wrong"); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("got %v, want ErrUnknownSandbox", err)
	}
	if err := m.Release(t.Context(), "sb_nope", sb.Token); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("got %v, want ErrUnknownSandbox", err)
	}
	if err := m.Release(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !slices.Contains(eng.removes, sb.VMName) {
		t.Errorf("removes=%v, want %s", eng.removes, sb.VMName)
	}
	if got, _ := newStore(m.dataDir).load(); len(got) != 0 {
		t.Errorf("release not persisted: %d entries", len(got))
	}
}

func TestAgentSocketValidatesToken(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb, err := m.Claim(t.Context(), testKey, 0)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	if _, err := m.AgentSocket(sb.ID, "wrong"); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("got %v, want ErrUnknownSandbox", err)
	}
	sock, err := m.AgentSocket(sb.ID, sb.Token)
	if err != nil {
		t.Fatalf("AgentSocket: %v", err)
	}
	if sock != sb.VsockSocket {
		t.Errorf("got %q, want %q", sock, sb.VsockSocket)
	}
}

func TestReapDestroysExpiredClaims(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb, err := m.Claim(t.Context(), testKey, 0)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	m.mu.Lock()
	m.claimed[sb.ID].Deadline = time.Now().Add(-time.Second)
	m.mu.Unlock()

	m.reapOnce(t.Context())
	if !slices.Contains(eng.removes, sb.VMName) {
		t.Errorf("removes=%v, want expired VM reaped", eng.removes)
	}
	if _, n := m.Info(); n != 0 {
		t.Errorf("claimed=%d, want 0", n)
	}
	if got, _ := newStore(m.dataDir).load(); len(got) != 0 {
		t.Errorf("reap not persisted: %d entries", len(got))
	}
}

func TestReconcile(t *testing.T) {
	eng := newFakeEngine()
	eng.vms["sbx-live-1"] = "/vsock/live-new"
	eng.vms["sbx-stale-1"] = "/vsock/stale"
	eng.vms["other-vm"] = "/vsock/other"
	dataDir := t.TempDir()
	claims := map[string]*types.Sandbox{
		"sb_live": {ID: "sb_live", VMName: "sbx-live-1", Key: testKey, Token: "tok", VsockSocket: "/vsock/live-old"},
		"sb_dead": {ID: "sb_dead", VMName: "sbx-dead-1", Key: testKey, Token: "tok"},
	}
	if err := newStore(dataDir).save(claims); err != nil {
		t.Fatalf("setup: %v", err)
	}
	m := newTestManagerAt(t, eng, dataDir, config.PoolSpec{PoolKey: testKey, Warm: 1})
	goldenDir := filepath.Join(dataDir, "goldens", testKey.Hash())
	if err := os.MkdirAll(goldenDir, 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := os.WriteFile(filepath.Join(goldenDir, "snapshot.json"), []byte("{}"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	sock, err := m.AgentSocket("sb_live", "tok")
	if err != nil {
		t.Fatalf("live claim not adopted: %v", err)
	}
	if sock != "/vsock/live-new" {
		t.Errorf("vsock %q, want refreshed /vsock/live-new", sock)
	}
	if _, err := m.AgentSocket("sb_dead", "tok"); !errors.Is(err, ErrUnknownSandbox) {
		t.Error("dead claim survived reconcile")
	}
	if !slices.Contains(eng.removes, "sbx-stale-1") {
		t.Errorf("removes=%v, want stale sbx VM destroyed", eng.removes)
	}
	if slices.Contains(eng.removes, "other-vm") || slices.Contains(eng.removes, "sbx-live-1") {
		t.Errorf("removes=%v touched an owned or foreign VM", eng.removes)
	}
	if m.pools[testKey].goldenDir != goldenDir {
		t.Errorf("golden not re-detected: %q", m.pools[testKey].goldenDir)
	}
}

func TestRefillTopsUpToTarget(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
	m.pools[testKey].goldenDir = "/goldens/x"

	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Warm == 2 && infos[0].Refilling == 0
	})
	if n := eng.cloneCount(); n != 2 {
		t.Errorf("clones=%d, want 2", n)
	}
}

func TestRefillRespectsSemaphore(t *testing.T) {
	eng := newFakeEngine()
	eng.probeStall = make(chan struct{})
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 6})
	m.pools[testKey].goldenDir = "/goldens/x"

	m.refillOnce(t.Context())
	waitFor(t, func() bool { return eng.cloneCount() == maxConcurrentRefills })
	time.Sleep(50 * time.Millisecond)
	if n := eng.cloneCount(); n != maxConcurrentRefills {
		t.Errorf("clones=%d while stalled, want %d", n, maxConcurrentRefills)
	}

	close(eng.probeStall)
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Warm+infos[0].Refilling >= maxConcurrentRefills && infos[0].Refilling == 0
	})
	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Warm == 6
	})
}

func TestGoldenBuildPipeline(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})

	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Golden
	})
	m.mu.Lock()
	golden := m.pools[testKey].goldenDir
	m.mu.Unlock()
	if _, err := os.Stat(filepath.Join(golden, "snapshot.json")); err != nil {
		t.Errorf("golden dir not exported: %v", err)
	}
	builder := vmPrefix + "gb-" + testKey.Hash()
	if removed := eng.removedNames(); !slices.Contains(removed, builder) {
		t.Errorf("removes=%v, want builder VM %s destroyed", removed, builder)
	}

	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Warm == 1
	})
}

func TestGoldenBuildFailureBacksOff(t *testing.T) {
	eng := newFakeEngine()
	eng.runColdErr = errors.New("image pull failed")
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})

	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		p := m.pools[testKey]
		return !p.building && p.nextBuild.After(time.Now())
	})

	m.refillOnce(t.Context())
	time.Sleep(20 * time.Millisecond)
	if n := eng.coldCount(); n != 1 {
		t.Errorf("cold boots=%d after failed build, want 1 (backoff)", n)
	}
}

func newTestManager(t *testing.T, eng *fakeEngine, pools ...config.PoolSpec) *Manager {
	t.Helper()
	return newTestManagerAt(t, eng, t.TempDir(), pools...)
}

func newTestManagerAt(t *testing.T, eng *fakeEngine, dataDir string, pools ...config.PoolSpec) *Manager {
	t.Helper()
	m, err := NewManager(&config.Config{DataDir: dataDir, Pools: pools}, eng)
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	return m
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 3s")
}

// fakeEngine implements Engine in memory: created VMs land in vms (backing
// List/vsockOf), destroy removes them, and error fields fail one verb at a
// time.
type fakeEngine struct {
	mu            sync.Mutex
	vms           map[string]string // VM name → vsock socket path
	clones        []string
	colds         []string
	removes       []string
	probeTimeouts []time.Duration

	cloneErr, runColdErr, probeErr error
	probeStall                     chan struct{} // non-nil: Probe blocks until closed
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{vms: map[string]string{}}
}

func (f *fakeEngine) Clone(_ context.Context, _, name string, _ types.PoolKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clones = append(f.clones, name)
	if f.cloneErr != nil {
		return f.cloneErr
	}
	f.vms[name] = "/vsock/" + name
	return nil
}

func (f *fakeEngine) RunCold(_ context.Context, name string, _ types.PoolKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.colds = append(f.colds, name)
	if f.runColdErr != nil {
		return f.runColdErr
	}
	f.vms[name] = "/vsock/" + name
	return nil
}

func (f *fakeEngine) Remove(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removes = append(f.removes, name)
	delete(f.vms, name)
	return nil
}

func (f *fakeEngine) SnapshotSave(_ context.Context, _, _ string) error { return nil }

func (f *fakeEngine) SnapshotExport(_ context.Context, _, toDir string) error {
	if err := os.MkdirAll(toDir, 0o750); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(toDir, "snapshot.json"), []byte("{}"), 0o600)
}

func (f *fakeEngine) SnapshotRemove(_ context.Context, _ string) error { return nil }

func (f *fakeEngine) List(_ context.Context, filters ...string) ([]types.VMRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var vms []types.VMRecord
	for name, sock := range f.vms {
		if len(filters) > 0 && !slices.Contains(filters, name) {
			continue
		}
		vms = append(vms, types.VMRecord{Name: name, State: vmStateRunning, VsockSocket: sock})
	}
	return vms, nil
}

func (f *fakeEngine) Probe(_ context.Context, _ string, timeout time.Duration) error {
	f.mu.Lock()
	f.probeTimeouts = append(f.probeTimeouts, timeout)
	stall := f.probeStall
	err := f.probeErr
	f.mu.Unlock()
	if stall != nil {
		<-stall
	}
	return err
}

func (f *fakeEngine) cloneCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.clones)
}

func (f *fakeEngine) coldCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.colds)
}

func (f *fakeEngine) removedNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.removes)
}
