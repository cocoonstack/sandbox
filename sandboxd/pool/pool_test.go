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

	sb := mustClaim(t, m, testKey)
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

	sb := mustClaim(t, m, testKey)
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
	if _, n, _ := m.Info(); n != 0 {
		t.Errorf("claimed=%d, want 0", n)
	}
}

func TestClaimCreateFailureDestroysVM(t *testing.T) {
	eng := newFakeEngine()
	eng.runColdErr = errors.New("cli killed by timeout")
	m := newTestManager(t, eng)

	if _, err := m.Claim(t.Context(), testKey, 0); err == nil {
		t.Fatal("Claim succeeded with failing create")
	}
	if len(eng.removes) != 1 {
		t.Errorf("removes=%v, want the half-created VM destroyed", eng.removes)
	}
}

func TestClaimCanceledRequestStillDestroys(t *testing.T) {
	eng := newFakeEngine()
	eng.probeErr = errors.New("not ready")
	m := newTestManager(t, eng)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := m.Claim(ctx, testKey, 0); err == nil {
		t.Fatal("Claim succeeded with failing probe")
	}
	if len(eng.removes) != 1 {
		t.Errorf("removes=%v, want destroy to survive the canceled request", eng.removes)
	}
}

func TestClaimEgressNeedsAttachment(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	key := types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeSmall}

	if _, err := m.Claim(t.Context(), key, 0); !errors.Is(err, ErrNoEgress) {
		t.Errorf("got %v, want ErrNoEgress", err)
	}
}

func TestClaimRejectsBadKey(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	key := types.PoolKey{Template: "rt:24.04", Net: "lan", Size: types.SizeSmall}

	if _, err := m.Claim(t.Context(), key, 0); !errors.Is(err, ErrBadKey) {
		t.Errorf("got %v, want ErrBadKey", err)
	}
}

func TestReleaseValidatesToken(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)

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
	sb := mustClaim(t, m, testKey)

	if _, wrongErr := m.AgentSocket(sb.ID, "wrong"); !errors.Is(wrongErr, ErrUnknownSandbox) {
		t.Errorf("got %v, want ErrUnknownSandbox", wrongErr)
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
	sb := mustClaim(t, m, testKey)
	m.mu.Lock()
	m.claimed[sb.ID].Deadline = time.Now().Add(-time.Second)
	m.mu.Unlock()

	m.reapOnce(t.Context())
	if !slices.Contains(eng.removes, sb.VMName) {
		t.Errorf("removes=%v, want expired VM reaped", eng.removes)
	}
	if _, n, _ := m.Info(); n != 0 {
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
		infos, _, _ := m.Info()
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
		infos, _, _ := m.Info()
		return infos[0].Warm+infos[0].Refilling >= maxConcurrentRefills && infos[0].Refilling == 0
	})
	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		infos, _, _ := m.Info()
		return infos[0].Warm == 6
	})
}

func TestGoldenBuildPipeline(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})

	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		infos, _, _ := m.Info()
		return infos[0].Golden
	})
	m.mu.Lock()
	golden := m.pools[testKey].goldenDir
	m.mu.Unlock()
	if fi, err := os.Stat(golden); err != nil || !fi.IsDir() {
		t.Errorf("golden dir not exported: %v", err)
	}
	builder := vmPrefix + "gb-" + testKey.Hash()
	if removed := eng.removedNames(); !slices.Contains(removed, builder) {
		t.Errorf("removes=%v, want builder VM %s destroyed", removed, builder)
	}

	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		infos, _, _ := m.Info()
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

// mustClaim claims with the default TTL or fails the test.
func mustClaim(t *testing.T, m *Manager, key types.PoolKey) *types.Sandbox {
	t.Helper()
	sb, err := m.Claim(t.Context(), key, 0)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return sb
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
	cloneFroms    []string
	colds         []string
	removes       []string
	probeTimeouts []time.Duration

	hibernates, restores, snapRemoves []string
	snapSaves, exports, snapshots     []string
	stopped                           map[string]bool

	cloneErr, runColdErr, probeErr, hibernateErr, restoreErr, snapSaveErr error
	cloneFailNth                                                          int // 1-based Clone call to fail; 0 = never

	probeStall     chan struct{} // non-nil: Probe blocks until closed
	hibernateStall chan struct{} // non-nil: Hibernate blocks until closed
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{vms: map[string]string{}, stopped: map[string]bool{}}
}

func (f *fakeEngine) Clone(_ context.Context, fromDir, name string, _ types.PoolKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clones = append(f.clones, name)
	f.cloneFroms = append(f.cloneFroms, fromDir)
	if f.cloneErr != nil {
		return f.cloneErr
	}
	if f.cloneFailNth > 0 && len(f.clones) == f.cloneFailNth {
		return errors.New("clone failed")
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

func (f *fakeEngine) Remove(ctx context.Context, name string) error {
	// Models exec.CommandContext: a canceled ctx never runs cocoon at all.
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removes = append(f.removes, name)
	delete(f.vms, name)
	return nil
}

func (f *fakeEngine) SnapshotSave(_ context.Context, _, snapName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapSaves = append(f.snapSaves, snapName)
	return f.snapSaveErr
}

func (f *fakeEngine) SnapshotExport(_ context.Context, snapName, toDir string) error {
	f.mu.Lock()
	f.exports = append(f.exports, snapName)
	f.mu.Unlock()
	return os.MkdirAll(toDir, 0o750)
}

func (f *fakeEngine) SnapshotList(_ context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.snapshots), nil
}

func (f *fakeEngine) SnapshotRemove(_ context.Context, snapName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapRemoves = append(f.snapRemoves, snapName)
	return nil
}

func (f *fakeEngine) Hibernate(_ context.Context, name, snapName string) error {
	f.mu.Lock()
	f.hibernates = append(f.hibernates, snapName)
	stall := f.hibernateStall
	err := f.hibernateErr
	f.mu.Unlock()
	if stall != nil {
		<-stall
	}
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.stopped[name] = true
	f.mu.Unlock()
	return nil
}

func (f *fakeEngine) Restore(_ context.Context, name, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restores = append(f.restores, name)
	if f.restoreErr != nil {
		return f.restoreErr
	}
	f.stopped[name] = false
	return nil
}

func (f *fakeEngine) List(_ context.Context, filters ...string) ([]types.VMRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var vms []types.VMRecord
	for name, sock := range f.vms {
		if len(filters) > 0 && !slices.Contains(filters, name) {
			continue
		}
		state := vmStateRunning
		if f.stopped[name] {
			state = "stopped"
		}
		vms = append(vms, types.VMRecord{State: state, VsockSocket: sock, Config: types.VMConfig{Name: name}})
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
