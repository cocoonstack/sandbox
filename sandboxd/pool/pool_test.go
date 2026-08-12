package pool

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var testKey = types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall, Engine: types.EngineCH}

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
	if got, _ := newClaimStore(m.dataDir).load(); len(got) != 1 {
		t.Errorf("claim not persisted: %d entries", len(got))
	}
}

func TestClaimClampsTTL(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})

	sb, err := claimAny(t.Context(), m, testKey, 48*time.Hour)
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
	prefix := vmPrefix + testKey.Hash() + "-"
	if !strings.HasPrefix(sb.VMName, prefix) {
		t.Fatalf("vm name %q missing pool prefix", sb.VMName)
	}
	suffix := strings.TrimPrefix(sb.VMName, prefix)
	if len(suffix) != 12 {
		t.Errorf("vm name suffix %q has %d chars, want 12", suffix, len(suffix))
	}
	if _, err := hex.DecodeString(suffix); err != nil {
		t.Errorf("vm name suffix %q is not hex: %v", suffix, err)
	}
	if want := "/vsock/" + sb.VMName; sb.VsockSocket != want {
		t.Errorf("vsock %q, want %q", sb.VsockSocket, want)
	}
	if got := eng.probeTimeouts[0]; got > claimProbeTimeout || got < claimProbeTimeout-time.Second {
		t.Errorf("probe timeout %v, want ~%v", got, claimProbeTimeout)
	}
}

func TestClaimUnpooledKeyColdBoots(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)

	if _, err := claimAny(t.Context(), m, testKey, 0); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(eng.colds) != 1 || len(eng.clones) != 0 {
		t.Fatalf("got clones=%v colds=%v, want one cold boot", eng.clones, eng.colds)
	}
	if got := eng.probeTimeouts[0]; got > coldProbeTimeout || got < coldProbeTimeout-time.Second {
		t.Errorf("probe timeout %v, want ~%v", got, coldProbeTimeout)
	}
}

func TestClaimProbeFailureDestroysVM(t *testing.T) {
	eng := newFakeEngine()
	eng.probeErr = errors.New("never ready")
	m := newTestManager(t, eng)

	if _, err := claimAny(t.Context(), m, testKey, 0); err == nil {
		t.Fatal("Claim succeeded with failing probe")
	}
	if len(eng.removes) != 1 {
		t.Errorf("removes=%v, want the failed VM destroyed", eng.removes)
	}
	if _, g := m.Info(); g.Claimed != 0 {
		t.Errorf("claimed=%d, want 0", g.Claimed)
	}
}

func TestClaimCreateFailureDestroysVM(t *testing.T) {
	eng := newFakeEngine()
	eng.runColdErr = errors.New("cli killed by timeout")
	m := newTestManager(t, eng)

	if _, err := claimAny(t.Context(), m, testKey, 0); err == nil {
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

	if _, err := claimAny(ctx, m, testKey, 0); err == nil {
		t.Fatal("Claim succeeded with failing probe")
	}
	if len(eng.removes) != 1 {
		t.Errorf("removes=%v, want destroy to survive the canceled request", eng.removes)
	}
}

func TestClaimEgressNeedsAttachment(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	key := types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeSmall}

	if _, err := claimAny(t.Context(), m, key, 0); !errors.Is(err, ErrNoEgress) {
		t.Errorf("got %v, want ErrNoEgress", err)
	}
}

func TestClaimRejectsBadKey(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	key := types.PoolKey{Template: "rt:24.04", Net: "lan", Size: types.SizeSmall}

	if _, err := claimAny(t.Context(), m, key, 0); !errors.Is(err, ErrBadKey) {
		t.Errorf("got %v, want ErrBadKey", err)
	}
}

func TestReleaseValidatesToken(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)

	if err := m.Release(t.Context(), sb.ID, Cred{Token: "wrong"}); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("got %v, want ErrUnknownSandbox", err)
	}
	if err := m.Release(t.Context(), "sb_nope", Cred{Token: sb.Token}); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("got %v, want ErrUnknownSandbox", err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !slices.Contains(eng.removes, sb.VMName) {
		t.Errorf("removes=%v, want %s", eng.removes, sb.VMName)
	}
	if got, _ := newClaimStore(m.dataDir).load(); len(got) != 0 {
		t.Errorf("release not persisted: %d entries", len(got))
	}
}

// TestReleaseAfterResolveTearsDownOnce: resolve authorizes off the manager
// mutex, so two callers can both hold the same resolved claim before either
// drops it — the state a live double-release reaches. Exactly one may tear the
// VM down; the loser must read as unknown rather than remove it twice.
func TestReleaseAfterResolveTearsDownOnce(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	cred := Cred{Token: sb.Token}

	first, ok := m.resolve(sb.ID, cred)
	if !ok {
		t.Fatal("resolve: claim not found")
	}
	second, ok := m.resolve(sb.ID, cred)
	if !ok {
		t.Fatal("resolve: claim not found on the second caller")
	}

	if err := m.releaseResolved(t.Context(), sb.ID, first); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if err := m.releaseResolved(t.Context(), sb.ID, second); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("second release: %v, want ErrUnknownSandbox", err)
	}
	if got := slices.Compact(slices.Clone(eng.removes)); len(got) != 1 || got[0] != sb.VMName {
		t.Errorf("removes=%v, want %s torn down exactly once", eng.removes, sb.VMName)
	}
}

func TestReleaseByOperatorCred(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)

	// Unknown id is 404-equivalent, exactly like a token release.
	if err := m.Release(t.Context(), "sb_nope", Cred{Operator: true}); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("got %v, want ErrUnknownSandbox", err)
	}
	// The operator releases by id alone — no per-sandbox token needed.
	if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err != nil {
		t.Fatalf("Release operator: %v", err)
	}
	if !slices.Contains(eng.removes, sb.VMName) {
		t.Errorf("removes=%v, want %s", eng.removes, sb.VMName)
	}
	if got, _ := newClaimStore(m.dataDir).load(); len(got) != 0 {
		t.Errorf("release not persisted: %d entries", len(got))
	}
}

// TestResolveRejectsEmptyToken documents resolve's explicit guard: an
// unclaimed slot must never match an empty token.
func TestResolveRejectsEmptyToken(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)

	if _, ok := m.resolve(sb.ID, Cred{Token: ""}); ok {
		t.Error("empty token resolved a claimed sandbox")
	}
	if _, ok := m.resolve(sb.ID, Cred{Operator: true}); !ok {
		t.Error("operator credential did not resolve the claim")
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
	// The destroy batch is fanned off the ticker loop; poll for it.
	waitFor(t, func() bool { return eng.removed(sb.VMName) })
	if _, g := m.Info(); g.Claimed != 0 {
		t.Errorf("claimed=%d, want 0", g.Claimed)
	}
	if got, _ := newClaimStore(m.dataDir).load(); len(got) != 0 {
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
	if err := newClaimStore(dataDir).save(claims); err != nil {
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
		infos, _ := m.Info()
		return infos[0].Warm == 2 && infos[0].Refilling == 0
	})
	if n := eng.cloneCount(); n != 2 {
		t.Errorf("clones=%d, want 2", n)
	}
}

func TestSetPoolsGrowShrinkAndDrain(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	goldenDir := filepath.Join(m.goldensDir(), testKey.Hash())
	if err := os.MkdirAll(goldenDir, 0o750); err != nil {
		t.Fatalf("setup golden: %v", err)
	}

	if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: testKey, Warm: 2}}); err != nil {
		t.Fatalf("SetPools grow: %v", err)
	}
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return len(infos) == 1 && infos[0].Target == 2 && infos[0].Warm == 2 && infos[0].Golden
	})
	if n := eng.cloneCount(); n != 2 {
		t.Fatalf("clones=%d, want 2", n)
	}

	if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: testKey, Warm: 1}}); err != nil {
		t.Fatalf("SetPools shrink: %v", err)
	}
	infos, _ := m.Info()
	if len(infos) != 1 || infos[0].Target != 1 || infos[0].Warm != 1 {
		t.Fatalf("after shrink info=%+v, want target=1 warm=1", infos)
	}
	if removed := eng.removedNames(); len(removed) != 1 {
		t.Fatalf("removed=%v, want one trimmed VM", removed)
	}

	if err := m.SetPools(t.Context(), nil); err != nil {
		t.Fatalf("SetPools drain: %v", err)
	}
	infos, _ = m.Info()
	if len(infos) != 0 {
		t.Fatalf("after drain info=%+v, want no configured pools", infos)
	}
	if removed := eng.removedNames(); len(removed) != 2 {
		t.Fatalf("removed=%v, want both warm VMs destroyed", removed)
	}
}

func TestRefillServicesEveryPoolUnderTightBudget(t *testing.T) {
	key2 := types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeMedium}
	m := newTestManager(t, newFakeEngine(),
		config.PoolSpec{PoolKey: testKey, Warm: 3},
		config.PoolSpec{PoolKey: key2, Warm: 3})
	m.refillSem = make(chan struct{}, 1)
	m.probeSem = make(chan struct{}, 1)
	m.mu.Lock()
	m.pools[testKey].goldenDir = "/goldens/a"
	m.pools[key2].goldenDir = "/goldens/b"
	m.mu.Unlock()

	m.refillOnce(t.Context())

	both := func() bool {
		infos, _ := m.Info()
		return len(infos) == 2 && infos[0].Warm == 3 && infos[1].Warm == 3
	}
	deadline := time.Now().Add(3 * time.Second)
	for !both() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !both() {
		infos, _ := m.Info()
		t.Fatalf("one refill tick did not chain both pools to target: %+v", infos)
	}
}

func TestSetPoolsRemovalShedsArchivePolicy(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 0, ArchiveAfterSeconds: 60, ArchiveDeleteAfterSeconds: 120})
	m.mu.Lock()
	m.pools[testKey].refilling = 1 // keep the removed pool lingering
	m.mu.Unlock()

	if err := m.SetPools(t.Context(), nil); err != nil {
		t.Fatalf("SetPools: %v", err)
	}
	m.mu.Lock()
	p := m.pools[testKey]
	if p == nil {
		m.mu.Unlock()
		t.Fatal("lingering pool was deleted despite refilling=1")
	}
	if p.archiveAfter != 0 || p.archiveDelete != 0 {
		t.Errorf("archive policy survived removal: after=%v delete=%v, want 0", p.archiveAfter, p.archiveDelete)
	}
	if m.archiveEnabled {
		t.Error("archiveEnabled still latched by a removed pool")
	}
	p.refilling = 0
	m.mu.Unlock()
	m.refillOnce(t.Context())
	m.mu.Lock()
	_, lingering := m.pools[testKey]
	m.mu.Unlock()
	if lingering {
		t.Error("quiescent removed pool not swept by refillOnce")
	}
}

func TestSetPoolsRejectsInvalidSpec(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	err := m.SetPools(t.Context(), []config.PoolSpec{{
		PoolKey: types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall},
		Warm:    -1,
	}})
	if !errors.Is(err, ErrBadCount) {
		t.Fatalf("got %v, want ErrBadCount", err)
	}
}

// SetPools neither validates nor keeps egress policy: accepting it would
// report success and silently drop it.
func TestSetPoolsRejectsEgress(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	err := m.SetPools(t.Context(), []config.PoolSpec{{
		PoolKey: types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall},
		Egress:  &egress.Policy{},
	}})
	if !errors.Is(err, ErrBadKey) {
		t.Fatalf("got %v, want ErrBadKey", err)
	}
}

func TestRefillRespectsSemaphore(t *testing.T) {
	eng := newFakeEngine()
	eng.probeStall = make(chan struct{})
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 7})
	m.refillSem = make(chan struct{}, 2)
	m.probeSem = make(chan struct{}, 2)
	m.pools[testKey].goldenDir = "/goldens/x"

	m.refillOnce(t.Context())
	waitFor(t, func() bool { return eng.cloneCount() == 4 && eng.probeCount() == 2 })
	time.Sleep(50 * time.Millisecond)
	if clones, probes := eng.cloneCount(), eng.probeCount(); clones != 4 || probes != 2 {
		t.Errorf("stalled pipeline clones=%d probes=%d, want 4/2", clones, probes)
	}
	infos, _ := m.Info()
	if infos[0].Warm != 0 || infos[0].Refilling != 4 {
		t.Errorf("stalled pipeline info=%+v, want warm=0 refilling=4", infos[0])
	}

	close(eng.probeStall)
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Warm == 7 && infos[0].Refilling == 0
	})
}

func TestRefillCloneStageIsBounded(t *testing.T) {
	eng := newFakeEngine()
	eng.cloneStall = make(chan struct{})
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 5})
	m.refillSem = make(chan struct{}, 2)
	m.probeSem = make(chan struct{}, 2)
	m.pools[testKey].goldenDir = "/goldens/x"

	m.refillOnce(t.Context())
	waitFor(t, func() bool { return eng.cloneCount() == 2 })
	time.Sleep(50 * time.Millisecond)
	if got := eng.cloneCount(); got != 2 {
		t.Errorf("clones=%d while clone stage stalled, want 2", got)
	}
	close(eng.cloneStall)
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Warm == 5 && infos[0].Refilling == 0
	})
}

func TestRefillProbeFailureCleansUp(t *testing.T) {
	eng := newFakeEngine()
	eng.probeErr = errors.New("never ready")
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	m.pools[testKey].goldenDir = "/goldens/x"

	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Warm == 0 && infos[0].Refilling == 0 && len(eng.removedNames()) == 1
	})
	eng.mu.Lock()
	eng.probeErr = nil
	eng.mu.Unlock()
	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Warm == 1 && infos[0].Refilling == 0
	})
}

func TestRefillCanceledWhileWaitingForProbeCleansUp(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	m.refillSem = make(chan struct{}, 1)
	m.probeSem = make(chan struct{}, 1)
	m.probeSem <- struct{}{}
	m.pools[testKey].goldenDir = "/goldens/x"
	ctx, cancel := context.WithCancel(t.Context())

	m.refillOnce(ctx)
	waitFor(t, func() bool { return eng.cloneCount() == 1 && len(m.refillSem) == 0 })
	cancel()
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Refilling == 0 && len(eng.removedNames()) == 1
	})
	<-m.probeSem
}

func TestDirectClaimSkipsProbeGate(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	for range cap(m.probeSem) {
		m.probeSem <- struct{}{}
	}
	defer func() {
		for range cap(m.probeSem) {
			<-m.probeSem
		}
	}()
	if _, err := claimAny(t.Context(), m, testKey, 0); err != nil {
		t.Fatalf("direct claim waited on probe gate: %v", err)
	}
}

func TestRefillBudgetFollowsConfig(t *testing.T) {
	m, err := NewManager(t.Context(), &config.Config{DataDir: t.TempDir(), RefillConcurrency: 2}, newFakeEngine(), testSecrets(t))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if got := cap(m.refillSem); got != 2 {
		t.Errorf("budget %d, want the configured 2", got)
	}
	if got := cap(m.probeSem); got != 2 {
		t.Errorf("probe budget %d, want the configured 2", got)
	}
}

// TestRefillReactivelyFillsToTarget: one trigger fills the whole target (not
// just the concurrency budget), since a finished refill chains the next.
func TestRefillReactivelyFillsToTarget(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 10})
	m.pools[testKey].goldenDir = "/goldens/x"

	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Warm == 10 && infos[0].Refilling == 0
	})
	if n := eng.cloneCount(); n != 10 {
		t.Errorf("clones=%d, want 10", n)
	}
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
	if fi, err := os.Stat(golden); err != nil || !fi.IsDir() {
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

func TestReapBatchDoesNotStallTheLoop(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	// More expired claims than the concurrency budget (defaultRefill here) so
	// the batch would block the submit loop if it acquired the semaphore there
	// instead of per-goroutine.
	const n = defaultRefill + 3
	var sbs []*types.Sandbox
	for range n {
		sbs = append(sbs, mustClaim(t, m, testKey))
	}
	m.mu.Lock()
	for _, sb := range sbs {
		m.claimed[sb.ID].Deadline = time.Now().Add(-time.Second)
	}
	m.mu.Unlock()

	stall := make(chan struct{})
	eng.mu.Lock()
	eng.removeStall = stall
	eng.mu.Unlock()

	done := make(chan struct{})
	go func() { m.reapOnce(t.Context()); close(done) }()
	select {
	case <-done: // returned while every destroy is parked: the loop never blocked
	case <-time.After(2 * time.Second):
		t.Fatal("reapOnce blocked on a batch larger than the budget")
	}
	close(stall)
	waitFor(t, func() bool { return eng.removed(sbs[n-1].VMName) })
}

func TestProbeReadyWaitsForVsock(t *testing.T) {
	eng := newFakeEngine()
	eng.vsockLateN = 2 // cocoon reports the socket only once the VMM runs
	m := newTestManager(t, eng)

	sb, err := claimAny(t.Context(), m, testKey, 0)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if sb.VsockSocket == "" {
		t.Error("claim returned an empty vsock socket")
	}
}

func newTestManager(t *testing.T, eng *fakeEngine, pools ...config.PoolSpec) *Manager {
	t.Helper()
	return newTestManagerAt(t, eng, t.TempDir(), pools...)
}

// claimAny composes warm-then-provision the way the server does around the
// redirect decision; production has no single-call form.
func claimAny(ctx context.Context, m *Manager, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
	sb, err := m.ClaimWarm(ctx, key, ttl, "", "", nil)
	if errors.Is(err, ErrNoWarm) {
		return m.ClaimProvision(ctx, key, ttl, "", "", nil)
	}
	return sb, err
}

func mustClaim(t *testing.T, m *Manager, key types.PoolKey) *types.Sandbox {
	t.Helper()
	sb, err := claimAny(t.Context(), m, key, 0)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return sb
}

func newTestManagerAt(t *testing.T, eng *fakeEngine, dataDir string, pools ...config.PoolSpec) *Manager {
	t.Helper()
	m, err := NewManager(t.Context(), &config.Config{DataDir: dataDir, Pools: pools}, eng, testSecrets(t))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	return m
}

func testSecrets(t *testing.T, specs ...egress.SecretSpec) *egress.SecretStore {
	t.Helper()
	s, err := egress.NewSecretStore(specs)
	if err != nil {
		t.Fatalf("secrets: %v", err)
	}
	return s
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
	volumeSpecs   []engine.VolumeSpec
	volumeMounts  []types.Volume
	volumeOps     []string
	// attachDirty records whether an image's dirty marker was already down when
	// it was attached; removeSeenOps and removeSeenDirty snapshot the volume-op
	// log and the markers still on disk at each VM removal, so teardown ordering
	// is checkable on both sides of the removal.
	attachDirty     map[string]bool
	removeSeenOps   map[string][]string
	removeSeenDirty map[string][]string
	// dirtyPaths are the images removeSeenDirty samples; tests set it to the
	// catalog paths the claim under test uses.
	dirtyPaths []string
	syncs      []string // vsock sockets SyncGuest was called on

	hibernates, restores, snapRemoves []string
	snapSaves, exports, snapshots     []string
	exportContent                     []byte
	caInstalls                        []string // vsock sockets InstallCACert was called on
	staleReconciles                   []string // VM names ReconcileStaleCreate was called on
	installCAErr                      error
	diskAttachErr                     error
	diskAttachCancel                  context.CancelFunc
	mountVolumeErr                    error
	unmountVolumeErr                  error
	stopped                           map[string]bool
	creating                          map[string]bool // VMs List reports in the creating state
	staleOutcome                      engine.StaleCreateOutcome
	staleErr                          error
	pids                              map[string]int // VM name → PID, for Stats' resident-set lookup
	vsockLateN                        int            // List calls that report no socket yet

	cloneErr, runColdErr, probeErr, hibernateErr, restoreErr, snapSaveErr, snapListErr error
	removeErrFor                                                                       string // VM name whose Remove fails; "" = never
	cloneFailNth                                                                       int    // 1-based Clone call to fail; 0 = never
	hibernateErrCompletes                                                              bool   // hibernateErr fires after the snapshot lands (CLI timeout)
	tap                                                                                string // non-empty: lifecycle records carry this NIC tap
	listCount                                                                          int

	cloneStall      chan struct{} // non-nil: Clone blocks until closed
	probeStall      chan struct{} // non-nil: Probe blocks until closed
	hibernateStall  chan struct{} // non-nil: Hibernate blocks until closed
	removeStall     chan struct{} // non-nil: Remove blocks until closed
	snapRemoveStall chan struct{} // non-nil: SnapshotRemove blocks until closed
	snapListStall   chan struct{} // non-nil: SnapshotList blocks until closed
	snapListCount   int
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{
		vms: map[string]string{}, stopped: map[string]bool{}, creating: map[string]bool{}, pids: map[string]int{},
		attachDirty: map[string]bool{}, removeSeenOps: map[string][]string{}, removeSeenDirty: map[string][]string{},
	}
}

func (f *fakeEngine) Clone(_ context.Context, fromDir, name string, _ types.PoolKey) (types.VMRecord, error) {
	return f.clone(fromDir, name)
}

func (f *fakeEngine) CloneSnap(_ context.Context, snap, name string, _ types.PoolKey) (types.VMRecord, error) {
	return f.clone(snap, name)
}

func (f *fakeEngine) RunCold(_ context.Context, name string, _ types.PoolKey) (types.VMRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.colds = append(f.colds, name)
	f.volumeOps = append(f.volumeOps, "provision")
	if f.runColdErr != nil {
		return types.VMRecord{}, f.runColdErr
	}
	f.vms[name] = "/vsock/" + name
	return f.record(name), nil
}

func (f *fakeEngine) Remove(ctx context.Context, name string) error {
	// Models exec.CommandContext: a canceled ctx never runs cocoon at all.
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	stall := f.removeStall
	f.mu.Unlock()
	if stall != nil {
		<-stall
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if name == f.removeErrFor {
		return errors.New("remove failed")
	}
	f.removeSeenOps[name] = slices.Clone(f.volumeOps)
	for _, path := range f.dirtyPaths {
		if volumeDirty(path) {
			f.removeSeenDirty[name] = append(f.removeSeenDirty[name], path)
		}
	}
	f.removes = append(f.removes, name)
	delete(f.vms, name)
	return nil
}

func (f *fakeEngine) ReconcileStaleCreate(ctx context.Context, name string) (engine.StaleCreateOutcome, error) {
	// Models exec.CommandContext: a canceled ctx never runs cocoon at all.
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.staleReconciles = append(f.staleReconciles, name)
	if f.staleErr != nil {
		return "", f.staleErr
	}
	if f.staleOutcome == engine.StaleCreateCollected || f.staleOutcome == engine.StaleCreateNotFound {
		delete(f.vms, name)
		delete(f.creating, name)
	}
	return f.staleOutcome, nil
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
	content := slices.Clone(f.exportContent)
	f.mu.Unlock()
	if err := os.MkdirAll(toDir, 0o750); err != nil {
		return err
	}
	if content == nil {
		return nil
	}
	return os.WriteFile(filepath.Join(toDir, "state.bin"), content, 0o600)
}

func (f *fakeEngine) SnapshotList(_ context.Context) ([]string, error) {
	f.mu.Lock()
	f.snapListCount++
	stall := f.snapListStall
	err := f.snapListErr
	snaps := slices.Clone(f.snapshots)
	f.mu.Unlock()
	if stall != nil {
		<-stall
	}
	if err != nil {
		return nil, err
	}
	return snaps, nil
}

func (f *fakeEngine) SnapshotRemove(_ context.Context, snapName string) error {
	f.mu.Lock()
	stall := f.snapRemoveStall
	f.mu.Unlock()
	if stall != nil {
		<-stall
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapRemoves = append(f.snapRemoves, snapName)
	f.snapshots = slices.DeleteFunc(f.snapshots, func(s string) bool { return s == snapName })
	return nil
}

func (f *fakeEngine) Hibernate(_ context.Context, name, snapName string) error {
	f.mu.Lock()
	f.hibernates = append(f.hibernates, snapName)
	stall := f.hibernateStall
	err := f.hibernateErr
	completes := err == nil || f.hibernateErrCompletes
	f.mu.Unlock()
	if stall != nil {
		<-stall
	}
	if completes {
		f.mu.Lock()
		f.stopped[name] = true
		f.snapshots = append(f.snapshots, snapName)
		f.mu.Unlock()
	}
	return err
}

func (f *fakeEngine) Restore(_ context.Context, name, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restores = append(f.restores, name)
	if f.restoreErr != nil {
		return "", f.restoreErr
	}
	f.stopped[name] = false
	if f.vms[name] == "" {
		f.vms[name] = "/vsock/" + name // Restore rebuilds the VM from its snapshot
	}
	return f.lateVsock(f.vms[name]), nil
}

func (f *fakeEngine) List(_ context.Context, filters ...string) ([]types.VMRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCount++
	var vms []types.VMRecord
	for name, sock := range f.vms {
		if len(filters) > 0 && !slices.Contains(filters, name) {
			continue
		}
		sock = f.lateVsock(sock)
		state := vmStateRunning
		switch {
		case f.creating[name]:
			state = vmStateCreating
		case f.stopped[name]:
			state = "stopped"
		}
		rec := types.VMRecord{State: state, PID: f.pids[name], VsockSocket: sock, Config: types.VMConfig{Name: name}}
		if f.tap != "" {
			rec.NetworkConfigs = []types.VMNetConfig{{TAP: f.tap}}
		}
		vms = append(vms, rec)
	}
	return vms, nil
}

func (f *fakeEngine) Probe(_ context.Context, _ string, timeout time.Duration) error {
	f.mu.Lock()
	f.probeTimeouts = append(f.probeTimeouts, timeout)
	f.volumeOps = append(f.volumeOps, "probe")
	stall := f.probeStall
	err := f.probeErr
	f.mu.Unlock()
	if stall != nil {
		<-stall
	}
	return err
}

func (f *fakeEngine) DialGuestPort(context.Context, string, uint16) (net.Conn, error) {
	return nil, fmt.Errorf("fakeEngine has no guest port")
}

func (f *fakeEngine) InstallCACert(_ context.Context, vsockSocket string, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.caInstalls = append(f.caInstalls, vsockSocket)
	return f.installCAErr
}

func (f *fakeEngine) DiskAttach(_ context.Context, _ string, spec engine.VolumeSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volumeSpecs = append(f.volumeSpecs, spec)
	f.volumeOps = append(f.volumeOps, "attach:"+spec.Name)
	f.attachDirty[spec.Name] = volumeDirty(spec.Path)
	if f.diskAttachCancel != nil {
		f.diskAttachCancel()
	}
	return f.diskAttachErr
}

func (f *fakeEngine) MountVolume(_ context.Context, _, name, mount string, rw bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	mode := ""
	if rw {
		mode = types.VolumeModeRW
	}
	f.volumeMounts = append(f.volumeMounts, types.Volume{Name: name, Mount: mount, Mode: mode})
	f.volumeOps = append(f.volumeOps, "mount:"+name+":"+mount)
	return f.mountVolumeErr
}

func (f *fakeEngine) UnmountVolume(_ context.Context, _, mount string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volumeOps = append(f.volumeOps, "umount:"+mount)
	return f.unmountVolumeErr
}

func (f *fakeEngine) SyncGuest(_ context.Context, vsockSocket string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncs = append(f.syncs, vsockSocket)
	f.volumeOps = append(f.volumeOps, "sync")
	return nil
}

func (f *fakeEngine) clone(from, name string) (types.VMRecord, error) {
	f.mu.Lock()
	f.clones = append(f.clones, name)
	f.cloneFroms = append(f.cloneFroms, from)
	f.volumeOps = append(f.volumeOps, "provision")
	call := len(f.clones)
	stall, err, failNth := f.cloneStall, f.cloneErr, f.cloneFailNth
	f.mu.Unlock()
	if stall != nil {
		<-stall
	}
	if err != nil {
		return types.VMRecord{}, err
	}
	if failNth > 0 && call == failNth {
		return types.VMRecord{}, errors.New("clone failed")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.vms[name] = "/vsock/" + name
	return f.record(name), nil
}

// record builds a lifecycle result for name; callers hold f.mu.
func (f *fakeEngine) record(name string) types.VMRecord {
	rec := types.VMRecord{VsockSocket: f.lateVsock(f.vms[name]), Config: types.VMConfig{Name: name}}
	if f.tap != "" {
		rec.NetworkConfigs = []types.VMNetConfig{{TAP: f.tap}}
	}
	return rec
}

// lateVsock models cocoon's report-once-running lag: while vsockLateN ticks
// remain it reports no socket (consuming one), forcing the caller's poll.
func (f *fakeEngine) lateVsock(sock string) string {
	if f.vsockLateN > 0 {
		f.vsockLateN--
		return ""
	}
	return sock
}

// removed reports whether name was destroyed, under the fake's own lock —
// bounded batches append concurrently with test polls.
func (f *fakeEngine) removed(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.removes, name)
}

func (f *fakeEngine) dirtyAtAttach(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attachDirty[name]
}

func (f *fakeEngine) opsAtRemoval(vmName string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.removeSeenOps[vmName]
}

func (f *fakeEngine) dirtyAtRemoval(vmName string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.removeSeenDirty[vmName]
}

func (f *fakeEngine) syncCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.syncs)
}

func (f *fakeEngine) volumeOpsLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.volumeOps)
}

func (f *fakeEngine) listCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCount
}

func (f *fakeEngine) snapListCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapListCount
}

func (f *fakeEngine) cloneCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.clones)
}

func (f *fakeEngine) probeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.probeTimeouts)
}

func (f *fakeEngine) hibernateCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.hibernates)
}

func (f *fakeEngine) exportCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.exports)
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

func (f *fakeEngine) snapRemoveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.snapRemoves)
}

func (f *fakeEngine) snapRemoved(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.snapRemoves, name)
}
