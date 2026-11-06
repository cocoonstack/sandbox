package pool

import (
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/dir"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/tenants/file"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

const keepAnHour = 3600

var (
	testKey  = types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall}
	egKey    = types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeSmall}
	otherKey = types.PoolKey{Template: "other:1", Net: types.NetNone, Size: types.SizeSmall}
	plainKey = tplKey("plain:24.04")

	egPolicy = &egress.Policy{Allow: []egress.Rule{{Host: "example.com", Secret: "gh"}}}
	hostOnly = new(false)
)

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
	if got := journaled(t, m); len(got) != 1 {
		t.Errorf("claim not persisted: %d entries", len(got))
	}
}

func TestClaimClampsTTL(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})

	sb, err := claimAny(t.Context(), m, testKey, ClaimOptions{TTL: 48 * time.Hour})
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
	suffix, ok := strings.CutPrefix(sb.VMName, vmPrefix+testKey.Hash()+"-")
	if !ok {
		t.Fatalf("vm name %q missing pool prefix", sb.VMName)
	}
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

	mustClaim(t, m, testKey)
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

	if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{}); err == nil {
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

	if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{}); err == nil {
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

	if _, err := claimAny(ctx, m, testKey, ClaimOptions{}); err == nil {
		t.Fatal("Claim succeeded with failing probe")
	}
	if len(eng.removes) != 1 {
		t.Errorf("removes=%v, want destroy to survive the canceled request", eng.removes)
	}
}

func TestClaimEgressNeedsAttachment(t *testing.T) {
	m := newTestManager(t, newFakeEngine())

	if _, err := claimAny(t.Context(), m, egKey, ClaimOptions{}); !errors.Is(err, ErrNoEgress) {
		t.Errorf("got %v, want ErrNoEgress", err)
	}
}

func TestClaimRejectsBadKey(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	key := types.PoolKey{Template: "rt:24.04", Net: "lan", Size: types.SizeSmall}

	if _, err := claimAny(t.Context(), m, key, ClaimOptions{}); !errors.Is(err, ErrBadKey) {
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
	if got := journaled(t, m); len(got) != 0 {
		t.Errorf("release not persisted: %d entries", len(got))
	}
}

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
	if !slices.Equal(eng.removes, []string{sb.VMName}) {
		t.Errorf("removes=%v, want %s torn down exactly once", eng.removes, sb.VMName)
	}
}

func TestReleaseRecordsItsUsageEventBeforeTheTeardown(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	stall := make(chan struct{})
	eng.mu.Lock()
	eng.removeStall = stall
	eng.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
	waitFor(t, func() bool { return strings.HasSuffix(usageEventsOf(t, m, sb.ID), "release") })
	close(stall)
	if err := <-done; err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestReleaseDelayQueuesTeardown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		editView(m, func(v *configView) { v.releaseDelay = 2 * time.Second })
		sb := mustClaim(t, m, testKey)

		if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
			t.Fatalf("release: %v", err)
		}
		if _, g := m.Info(); g.Claimed != 0 {
			t.Fatalf("claimed=%d after release, want 0", g.Claimed)
		}
		m.retryRemovals(t.Context()).Wait()
		if removed := eng.removedNames(); len(removed) != 0 {
			t.Fatalf("removes=%v before the delay, want none", removed)
		}
		time.Sleep(2 * time.Second)
		m.retryRemovals(t.Context()).Wait()
		if removed := eng.removedNames(); len(removed) != 1 || removed[0] != sb.VMName {
			t.Fatalf("removes=%v after the delay, want %s", removed, sb.VMName)
		}
	})
}

func TestReleaseByOperatorCred(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)

	if err := m.Release(t.Context(), "sb_nope", Cred{Operator: true}); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("got %v, want ErrUnknownSandbox", err)
	}

	if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err != nil {
		t.Fatalf("Release operator: %v", err)
	}
	if !slices.Contains(eng.removes, sb.VMName) {
		t.Errorf("removes=%v, want %s", eng.removes, sb.VMName)
	}
	if got := journaled(t, m); len(got) != 0 {
		t.Errorf("release not persisted: %d entries", len(got))
	}
}

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
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		sb := mustClaim(t, m, testKey)
		expire(m, sb)

		m.reapOnce(t.Context())

		waitFor(t, func() bool { return eng.removed(sb.VMName) })
		if _, g := m.Info(); g.Claimed != 0 {
			t.Errorf("claimed=%d, want 0", g.Claimed)
		}
		if got := journaled(t, m); len(got) != 0 {
			t.Errorf("reap not persisted: %d entries", len(got))
		}
	})
}

func TestReconcileAdoptsLiveClaimsAndDestroysTheRest(t *testing.T) {
	eng := newFakeEngine()
	eng.vms["sbx-live-1"] = "/vsock/live-new"
	eng.vms["sbx-stale-1"] = "/vsock/stale"
	eng.vms["other-vm"] = "/vsock/other"
	dataDir := t.TempDir()
	claims := map[string]*types.Sandbox{
		"sb_live": {ID: "sb_live", VMName: "sbx-live-1", Key: testKey, Token: "tok", VsockSocket: "/vsock/live-old"},
		"sb_dead": {ID: "sb_dead", VMName: "sbx-dead-1", Key: testKey, Token: "tok"},
	}
	seedClaims(t, dataDir, claims)
	m := newTestManagerAt(t, eng, dataDir, config.PoolSpec{PoolKey: testKey, Warm: 1})
	goldenDir := seedGolden(t, m, testKey, "")

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
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
		m.pools[testKey].goldenDir = "/goldens/x"

		m.refillOnce(t.Context())
		waitWarm(t, m, 2)
		if n := eng.cloneCount(); n != 2 {
			t.Errorf("clones=%d, want 2", n)
		}
	})
}

func TestSetPoolsGrowShrinkAndDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		seedGolden(t, m, testKey, "")

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
	})
}

func TestRefillServicesEveryPoolUnderTightBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		waitFor(t, both)
	})
}

func TestSetPoolsRemovalShedsArchivePolicy(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 0, ArchiveAfterSeconds: 60, ArchiveDeleteAfterSeconds: 120})
	m.mu.Lock()
	m.pools[testKey].refilling = 1
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

func TestSetPoolsRemovalDeletesTheIdlePoolsGolden(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 0})
	g := seedGolden(t, m, testKey, "")
	if err := os.WriteFile(filepath.Join(g, "memory"), []byte("x"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := m.SetPools(t.Context(), nil); err != nil {
		t.Fatalf("SetPools: %v", err)
	}
	for _, path := range []string{g, g + goldenStampSuffix} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s survived the removal of its idle pool: %v", path, err)
		}
	}
	waitFor(t, func() bool {
		tmps, _ := filepath.Glob(filepath.Join(m.goldensDir(), "*.tmp"))
		return len(tmps) == 0
	})
}

func TestAReaddedPoolRebuildsTheGoldenItsRemovalDeleted(t *testing.T) {
	eng := newFakeEngine()
	spec := config.PoolSpec{PoolKey: testKey, Warm: 1}
	m := newTestManager(t, eng, spec)
	waitFor(t, func() bool { return refilledTo(t, m, testKey, 1) })

	if err := m.SetPools(t.Context(), nil); err != nil {
		t.Fatalf("SetPools remove: %v", err)
	}
	if err := m.SetPools(t.Context(), []config.PoolSpec{spec}); err != nil {
		t.Fatalf("SetPools re-add: %v", err)
	}
	waitFor(t, func() bool { return refilledTo(t, m, testKey, 1) })
	if n := eng.coldCount(); n != 2 {
		t.Errorf("cold boots=%d, want 2: the re-added key must rebuild, not adopt the deleted golden", n)
	}
	m.mu.Lock()
	golden := m.pools[testKey].goldenDir
	m.mu.Unlock()
	if !dirExists(golden) {
		t.Errorf("golden %q of the re-added pool is missing on disk", golden)
	}
}

func TestARemovedPoolKeepsItsGoldenUntilItSettles(t *testing.T) {
	for _, tt := range []struct {
		name string
		busy func(p *pool)
	}{
		{"build", func(p *pool) { p.building = true }},
		{"refill", func(p *pool) { p.refilling = 1 }},
		{"claim clone", func(p *pool) { p.claimClones.Store(1) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 0})
			g := seedGolden(t, m, testKey, "")
			m.mu.Lock()
			p := m.pools[testKey]
			tt.busy(p)
			m.mu.Unlock()

			if err := m.SetPools(t.Context(), nil); err != nil {
				t.Fatalf("SetPools: %v", err)
			}
			if !dirExists(g) {
				t.Fatalf("golden deleted while the removed pool's %s was in flight", tt.name)
			}
			m.mu.Lock()
			p.building, p.refilling = false, 0
			p.claimClones.Store(0)
			m.mu.Unlock()
			m.refillOnce(t.Context())
			for _, path := range []string{g, g + goldenStampSuffix} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("%s survived the sweep of its settled pool: %v", path, err)
				}
			}
		})
	}
}

func TestAClaimCloneHoldsTheGoldenOfItsRemovedPool(t *testing.T) {
	eng := newFakeEngine()
	eng.cloneStall = make(chan struct{})
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 0})
	g := seedGolden(t, m, testKey, "")
	m.mu.Lock()
	m.pools[testKey].goldenDir = g
	m.mu.Unlock()

	cloned := make(chan error, 1)
	go func() {
		_, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{})
		cloned <- err
	}()
	waitFor(t, func() bool { return eng.cloneCount() == 1 })
	if err := m.SetPools(t.Context(), nil); err != nil {
		t.Fatalf("SetPools: %v", err)
	}
	if !dirExists(g) {
		t.Fatal("golden deleted under a claim that is cloning from it")
	}
	close(eng.cloneStall)
	if err := <-cloned; err != nil {
		t.Fatalf("claim across the removal of its pool: %v", err)
	}

	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); err != nil {
		t.Fatalf("claim of the removed key: %v", err)
	}
	if clones, colds := eng.cloneCount(), eng.coldCount(); clones != 1 || colds != 1 {
		t.Errorf("clones=%d colds=%d, want a claim after the removal to cold-boot, not clone the retiring golden", clones, colds)
	}
	m.refillOnce(t.Context())
	if dirExists(g) {
		t.Error("golden survived the sweep of its settled pool")
	}
}

func TestSetPoolsWarmZeroKeepsTheGolden(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1})
	g := seedGolden(t, m, testKey, "")
	marker := filepath.Join(g, "memory")
	if err := os.WriteFile(marker, []byte("x"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: testKey, Warm: 0}}); err != nil {
		t.Fatalf("SetPools: %v", err)
	}
	for _, path := range []string{marker, g + goldenStampSuffix} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("%s of a key kept at warm 0 is gone: %v", path, err)
		}
	}
}

func TestSetPoolsRejectsInvalidSpec(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	err := m.SetPools(t.Context(), []config.PoolSpec{{
		Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall,
		Warm: -1,
	}})
	if !errors.Is(err, ErrBadConfig) || errors.Is(err, ErrBadCount) {
		t.Fatalf("got %v, want ErrBadConfig naming the pool", err)
	}
}

func TestSetPoolsRejectsConfigOwnedFields(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1})
	for _, tt := range []struct {
		field string
		spec  config.PoolSpec
	}{
		{"egress", config.PoolSpec{PoolKey: testKey, Egress: &egress.Policy{}}},
		{"warmup", config.PoolSpec{PoolKey: testKey, Warm: 1, Warmup: []string{"true"}}},
		{"capture_trim", config.PoolSpec{PoolKey: testKey, Warm: 1, CaptureTrim: true}},
		{"storage", config.PoolSpec{PoolKey: testKey, Warm: 1, Storage: "40G"}},
		{"replicate_seconds", config.PoolSpec{PoolKey: testKey, Warm: 1, ReplicateSeconds: 60}},
		{"reader", config.PoolSpec{PoolKey: testKey, Warm: 1, Reader: true}},
		{"egress_upstream_env", config.PoolSpec{PoolKey: testKey, Warm: 1, EgressUpstreamEnv: "UPSTREAM"}},
		{"tun", config.PoolSpec{PoolKey: testKey, Warm: 1, Tun: true}},
	} {
		err := m.SetPools(t.Context(), []config.PoolSpec{tt.spec})
		if !errors.Is(err, ErrBadKey) || !strings.Contains(err.Error(), tt.field+" is set in the config file") {
			t.Errorf("SetPools error = %v, want ErrBadKey naming %s as config-owned", err, tt.field)
		}
	}
}

func TestRefillRespectsSemaphore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		waitWarm(t, m, 7)
	})
}

func TestRefillCloneStageIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		waitWarm(t, m, 5)
	})
}

func TestRefillProbeFailureCleansUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		waitWarm(t, m, 1)
	})
}

func TestRefillCanceledWhileWaitingForProbeCleansUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
	})
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
	if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{}); err != nil {
		t.Fatalf("direct claim waited on probe gate: %v", err)
	}
}

func TestRefillBudgetFollowsConfig(t *testing.T) {
	cfg := &config.Config{DataDir: t.TempDir(), RefillConcurrency: 2}
	m := mustManager(t, cfg, newFakeEngine())
	if got := cap(m.refillSem); got != 2 {
		t.Errorf("budget %d, want the configured 2", got)
	}
	if got := cap(m.probeSem); got != 2 {
		t.Errorf("probe budget %d, want the configured 2", got)
	}
}

func TestRefillReactivelyFillsToTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 10})
		m.pools[testKey].goldenDir = "/goldens/x"

		m.refillOnce(t.Context())
		waitWarm(t, m, 10)
		if n := eng.cloneCount(); n != 10 {
			t.Errorf("clones=%d, want 10", n)
		}
	})
}

func TestGoldenBuildExportsAndDestroysItsBuilder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		waitWarm(t, m, 1)
	})
}

func TestGoldenBuildsDumpOneAtATime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.snapSaveStall = make(chan struct{})
		specs := make([]config.PoolSpec, 0, 3)
		for _, size := range []types.Size{types.SizeSmall, types.SizeMedium, types.SizeLarge} {
			specs = append(specs, config.PoolSpec{Template: testKey.Template, Net: testKey.Net, Size: size, Warm: 1})
		}
		m := newTestManager(t, eng, specs...)

		m.refillOnce(t.Context())
		synctest.Wait()
		eng.mu.Lock()
		entered := eng.snapSavesEntered
		eng.mu.Unlock()
		if entered != 1 {
			t.Errorf("memory dumps in flight = %d, want 1", entered)
		}
		close(eng.snapSaveStall)
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return !slices.ContainsFunc(infos, func(i PoolInfo) bool { return !i.Golden })
		})
	})
}

func TestGoldenBuildFailureBacksOff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
	})
}

func TestReapBatchDoesNotStallTheLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		const n = defaultRefill + 3
		var sbs []*types.Sandbox
		for range n {
			sbs = append(sbs, mustClaim(t, m, testKey))
		}
		for _, sb := range sbs {
			expire(m, sb)
		}

		stall := make(chan struct{})
		eng.mu.Lock()
		eng.removeStall = stall
		eng.mu.Unlock()

		done := make(chan struct{})
		go func() { m.reapOnce(t.Context()); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("reapOnce blocked on a batch larger than the budget")
		}
		close(stall)
		waitFor(t, func() bool { return eng.removed(sbs[n-1].VMName) })
	})
}

func TestProbeReadyWaitsForVsock(t *testing.T) {
	eng := newFakeEngine()
	eng.vsockLateN = 2
	m := newTestManager(t, eng)

	sb := mustClaim(t, m, testKey)
	if sb.VsockSocket == "" {
		t.Error("claim returned an empty vsock socket")
	}
}

func TestHeldConnForwardsCloseWrite(t *testing.T) {
	inner := &closeWriteConn{}
	held := &heldConn{Conn: inner, release: func() {}}

	if err := held.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	if !inner.closedWrite {
		t.Error("CloseWrite did not reach the wrapped connection")
	}
}

func TestHeldConnCloseWriteIsANoopWithoutSupport(t *testing.T) {
	held := &heldConn{Conn: &plainConn{}, release: func() {}}
	if err := held.CloseWrite(); err != nil {
		t.Errorf("CloseWrite on a conn without one: %v, want nil", err)
	}
}

func TestRenewExtendsTheLease(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	before := sb.Deadline

	got, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Hour, "")
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if !got.After(before) {
		t.Errorf("deadline %v, want after %v", got, before)
	}
	if sb.Deadline != got || sb.LeaseSeconds != int(time.Hour/time.Second) {
		t.Errorf("claim carries deadline %v lease %ds", sb.Deadline, sb.LeaseSeconds)
	}
	if list := m.Sandboxes("", "", nil); len(list) != 1 || !list[0].Deadline.Equal(got) {
		t.Errorf("index %+v does not report the granted deadline", list)
	}
}

func TestClaimedAtIsTheFirstGrant(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	before := time.Now()
	sb := mustClaim(t, m, testKey)
	if sb.ClaimedAt.Before(before) || sb.ClaimedAt.After(time.Now()) {
		t.Fatalf("claimed_at %v, want the moment of the claim", sb.ClaimedAt)
	}
	claimed := sb.ClaimedAt

	if _, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Hour, ""); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if !sb.ClaimedAt.Equal(claimed) {
		t.Errorf("renew moved claimed_at to %v, want %v", sb.ClaimedAt, claimed)
	}
	if list := m.Sandboxes("", "", nil); len(list) != 1 || !list[0].ClaimedAt.Equal(claimed) {
		t.Errorf("index %+v does not report claimed_at", list)
	}
}

func TestTheByIDReadCarriesTheTokenAndTheIndexDoesNot(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)

	if got, ok := m.Sandbox(sb.ID); !ok || got.Token != sb.Token {
		t.Errorf("Sandbox(%s) token %q, want the claim's %q", sb.ID, got.Token, sb.Token)
	}
	if list := m.Sandboxes("", "", nil); len(list) != 1 || list[0].Token != "" {
		t.Errorf("index %+v carries a token", list)
	}
}

func TestRenewClampsAndDefaults(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)

	if _, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Hour, ""); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if _, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, 0, ""); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if want := int(defaultTTL / time.Second); sb.LeaseSeconds != want {
		t.Errorf("zero ttl granted %ds, want the %ds default", sb.LeaseSeconds, want)
	}
	got, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, 999*time.Hour, "")
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if want := int(maxTTL / time.Second); sb.LeaseSeconds != want {
		t.Errorf("oversized ttl granted %ds, want the %ds cap", sb.LeaseSeconds, want)
	}
	if !got.Before(time.Now().Add(maxTTL + time.Minute)) {
		t.Errorf("deadline %v outruns the cap", got)
	}
}

func TestRenewRefusesAnArchivedClaim(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	m.mu.Lock()
	sb.ArchiveCk, sb.Deadline = "ck_1", time.Time{}
	m.store.set(sb)
	m.mu.Unlock()

	if _, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, 0, ""); !errors.Is(err, ErrArchived) {
		t.Fatalf("renew on an archived claim: %v, want ErrArchived", err)
	}
	if !sb.Deadline.IsZero() {
		t.Errorf("deadline %v; a kept-forever archive must not gain an expiry the reaper acts on", sb.Deadline)
	}
}

func TestRenewRefusesAClaimBeingArchived(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	before := sb.Deadline
	m.mu.Lock()
	m.archiving[sb.ID] = struct{}{}
	m.mu.Unlock()

	if _, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Hour, ""); !errors.Is(err, ErrArchived) {
		t.Fatalf("renew during an export: %v, want ErrArchived", err)
	}
	if !sb.Deadline.Equal(before) {
		t.Error("a renew the landing archive would overwrite was still reported as granted")
	}
}

func TestRenewRollsBackOnPersistFailure(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	before, beforeLease := sb.Deadline, sb.LeaseSeconds
	breakStore(t, m)

	if _, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Hour, ""); err == nil {
		t.Fatal("renew reported success while the claim store was unwritable")
	}
	if !sb.Deadline.Equal(before) || sb.LeaseSeconds != beforeLease {
		t.Errorf("deadline %v lease %ds after a failed persist, want %v / %ds", sb.Deadline, sb.LeaseSeconds, before, beforeLease)
	}
	if err := os.MkdirAll(filepath.Dir(m.store.path), 0o750); err != nil {
		t.Fatalf("heal store dir: %v", err)
	}
	granted, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Hour, "")
	if err != nil {
		t.Fatalf("renew after a rolled-back one: %v", err)
	}
	if !sb.Deadline.Equal(granted) {
		t.Errorf("deadline %v, want the %v just granted", sb.Deadline, granted)
	}
}

func TestRenewAuthorizesByToken(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	before := sb.Deadline

	for _, cred := range []Cred{{Token: "wrong"}, {}} {
		if _, err := m.Renew(t.Context(), sb.ID, cred, time.Hour, ""); !errors.Is(err, ErrUnknownSandbox) {
			t.Errorf("cred %+v: %v, want ErrUnknownSandbox", cred, err)
		}
	}
	if _, err := m.Renew(t.Context(), "sb_missing", Cred{Token: sb.Token}, time.Hour, ""); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("unknown id: %v, want ErrUnknownSandbox", err)
	}
	if sb.Deadline != before {
		t.Error("a rejected renew moved the deadline")
	}
}

func TestDialPortAuthorizesByToken(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)

	if _, err := m.DialPort(t.Context(), sb.ID, Cred{Token: "wrong"}, 49983); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("wrong token: %v, want ErrUnknownSandbox", err)
	}
	if _, err := m.DialPort(t.Context(), sb.ID, Cred{}, 49983); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("empty token: %v, want ErrUnknownSandbox", err)
	}
	if _, err := m.DialPort(t.Context(), "sb_missing", Cred{Token: sb.Token}, 49983); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("unknown id: %v, want ErrUnknownSandbox", err)
	}
	if _, err := m.DialPort(t.Context(), sb.ID, Cred{Token: sb.Token}, 49983); errors.Is(err, ErrUnknownSandbox) {
		t.Error("right token was rejected as an unknown sandbox")
	}
}

func TestOperatorPortDialNeverWakesAPausedSandbox(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	operator := Cred{Operator: true}

	paused := mustClaim(t, m, testKey)
	hibernateClaim(t, m, paused)
	restores := len(eng.restores)
	if _, err := m.DialPort(t.Context(), paused.ID, operator, 49983); !errors.Is(err, ErrPaused) {
		t.Errorf("hibernated: %v, want ErrPaused", err)
	}
	if len(eng.restores) != restores || paused.HibernateSnap == "" {
		t.Error("an operator dial restored a hibernated sandbox")
	}

	running := mustClaim(t, m, testKey)
	running.Transition.Lock()
	if _, err := m.DialPort(t.Context(), running.ID, operator, 49983); !errors.Is(err, ErrPaused) {
		t.Errorf("mid-transition: %v, want ErrPaused at once", err)
	}
	running.Transition.Unlock()
	for _, tt := range []struct{ name, pending, archive string }{
		{"pending hibernate", "snap", ""},
		{"archived", "", "ck"},
	} {
		m.mu.Lock()
		running.PendingSnap, running.ArchiveCk = tt.pending, tt.archive
		m.store.set(running)
		m.mu.Unlock()
		if _, err := m.DialPort(t.Context(), running.ID, operator, 49983); !errors.Is(err, ErrPaused) {
			t.Errorf("%s: %v, want ErrPaused", tt.name, err)
		}
	}
	if _, err := m.DialPort(t.Context(), "sb_missing", operator, 49983); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("unknown id: %v, want ErrUnknownSandbox", err)
	}
}

func TestOperatorPortDialStreamsWithoutStampingActivity(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	guest, host := net.Pipe()
	t.Cleanup(func() { _ = host.Close() })
	eng.guestDial = func() (net.Conn, error) { return guest, nil }
	stamp := time.Now().Add(-time.Hour).Truncate(time.Second)
	sb.TouchAt(stamp)

	conn, err := m.DialPort(t.Context(), sb.ID, Cred{Operator: true}, 49983)
	if err != nil {
		t.Fatalf("operator dial on a running sandbox: %v", err)
	}
	go func() { _, _ = host.Write([]byte("metrics")) }()
	buf := make([]byte, 7)
	if _, readErr := io.ReadFull(conn, buf); readErr != nil || string(buf) != "metrics" {
		t.Fatalf("stream read %q, %v", buf, readErr)
	}
	if !sb.Busy() {
		t.Error("an open operator stream must hold the sandbox against the idle sweep")
	}
	_ = conn.Close()
	if sb.Busy() || !sb.LastSeen().Equal(stamp) {
		t.Errorf("after close busy=%v lastSeen=%v, want released with the stamp %v unchanged", sb.Busy(), sb.LastSeen(), stamp)
	}

	guest2, host2 := net.Pipe()
	t.Cleanup(func() { _ = host2.Close() })
	eng.guestDial = func() (net.Conn, error) { return guest2, nil }
	tokenConn, err := m.DialPort(t.Context(), sb.ID, Cred{Token: sb.Token}, 49983)
	if err != nil {
		t.Fatalf("token dial: %v", err)
	}
	_ = tokenConn.Close()
	if !sb.LastSeen().After(stamp) {
		t.Error("a sandbox-token dial must keep stamping activity")
	}
}

func TestTokenPortDialStillWakesAHibernatedSandbox(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	restores := len(eng.restores)
	_, _ = m.DialPort(t.Context(), sb.ID, Cred{Token: sb.Token}, 49983)
	if len(eng.restores) != restores+1 || sb.HibernateSnap != "" {
		t.Errorf("restores %d -> %d, hibernateSnap %q: a token dial must wake the sandbox", restores, len(eng.restores), sb.HibernateSnap)
	}
}

func TestOperatorPortDialsOnOneSandboxRunConcurrently(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	var arrived atomic.Int32
	release := make(chan struct{})
	eng.guestDial = func() (net.Conn, error) {
		arrived.Add(1)
		<-release
		guest, host := net.Pipe()
		t.Cleanup(func() { _ = host.Close() })
		return guest, nil
	}
	errs := make(chan error, 2)
	for i := range int32(2) {
		go func() {
			conn, err := m.DialPort(t.Context(), sb.ID, Cred{Operator: true}, 49983)
			if err == nil {
				_ = conn.Close()
			}
			errs <- err
		}()
		waitFor(t, func() bool { return arrived.Load() == i+1 })
	}
	close(release)
	for range 2 {
		if err := <-errs; err != nil {
			t.Errorf("an operator dial while another is in flight on the same sandbox: %v, want a stream", err)
		}
	}
}

func TestAClaimForASlotAnotherClaimHoldsTakesNoVM(t *testing.T) {
	for name, tt := range map[string]struct {
		tenant string
		cap    func(*testing.T, *Manager)
	}{
		"node max_claims": {cap: func(_ *testing.T, m *Manager) { editView(m, func(v *configView) { v.maxClaims = 1 }) }},
		"tenant max_claims": {tenant: "acme", cap: func(t *testing.T, m *Manager) {
			setTenantCaps(t, m, map[string]int{"acme": 1})
		}},
	} {
		t.Run(name, func(t *testing.T) {
			eng := newFakeEngine()
			m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
			m.pools[testKey].goldenDir = "/goldens/x"
			m.refillOnce(t.Context())
			waitWarm(t, m, 2)
			tt.cap(t, m)
			hold, err := m.holdClaims(1, tt.tenant)
			if err != nil {
				t.Fatalf("hold the last slot: %v", err)
			}
			if _, err := m.ClaimWarm(t.Context(), testKey, ClaimOptions{Tenant: tt.tenant}); !errors.Is(err, ErrQuota) {
				t.Fatalf("claim behind a held slot: %v, want ErrQuota", err)
			}
			if infos, _ := m.Info(); infos[0].Warm != 2 || len(eng.removedNames()) != 0 {
				t.Errorf("warm %d removes %v, want the refused claim to leave both warm VMs", infos[0].Warm, eng.removedNames())
			}
			m.dropHold(hold)
			if _, err := m.ClaimWarm(t.Context(), testKey, ClaimOptions{Tenant: tt.tenant}); err != nil {
				t.Fatalf("claim once the slot is free: %v", err)
			}
		})
	}
}

func TestABurstForTheLastSlotDestroysNoWarmVM(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 8})
	m.pools[testKey].goldenDir = "/goldens/x"
	m.refillOnce(t.Context())
	waitWarm(t, m, 8)
	editView(m, func(v *configView) { v.maxClaims = 1 })
	start := make(chan struct{})
	results := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			<-start
			_, err := m.ClaimWarm(t.Context(), testKey, ClaimOptions{})
			results <- err
		})
	}
	close(start)
	wg.Wait()
	close(results)
	won := 0
	for err := range results {
		switch {
		case err == nil:
			won++
		case !errors.Is(err, ErrQuota):
			t.Errorf("a burst claim failed with %v, want ErrQuota", err)
		}
	}
	if won != 1 || len(eng.removedNames()) != 0 {
		t.Errorf("won %d removes %v, want one claim and no warm VM destroyed", won, eng.removedNames())
	}
}

type plainConn struct {
	net.Conn
}

type closeWriteConn struct {
	net.Conn

	closedWrite bool
}

func (c *closeWriteConn) CloseWrite() error {
	c.closedWrite = true
	return nil
}

type fakeVMEvent struct {
	sync   []engine.VMStatus
	change engine.VMChange
}

type fakeEngine struct {
	mu sync.Mutex

	vms        map[string]string
	stopped    map[string]bool
	creating   map[string]bool
	disks      map[string][]types.VMDisk
	pids       map[string]int
	sockRoot   string
	tap        string
	vsockLateN int

	clones          []string
	cloneFroms      []string
	colds           []string
	coldStorage     []string
	starts          []string
	stops           []string
	removes         []string
	probeTimeouts   []time.Duration
	syncs           []string
	laneMarks       []string
	caInstalls      []string
	trims           []string
	staleReconciles []string
	listCount       int
	inspects        int

	hibernates    []string
	restores      []string
	snapRemoves   []string
	snapSaves     []string
	exports       []string
	snapshots     []string
	exportContent []byte
	exportFiles   map[string]string
	snapListCount int

	cloneErr              error
	runColdErr            error
	probeErr              error
	hibernateErr          error
	captureWaits          []string
	vmStates              map[string]string
	startFails            int
	stopErr               error
	hibernateLeavesVMM    bool
	captureStall          chan struct{}
	captureHook           func()
	listFails             int
	restoreErr            error
	restoreStall          chan struct{}
	snapSaveStall         chan struct{}
	snapSavesEntered      int
	snapListErr           error
	snapSaveErr           error
	startErr              error
	startHook             func()
	syncErr               error
	trimErr               error
	installCAErr          error
	removeErrFor          string
	cloneFailNth          int
	hibernateErrCompletes bool
	staleOutcome          engine.StaleCreateOutcome
	staleErr              error

	cloneStall      chan struct{}
	probeStall      chan struct{}
	hibernateStall  chan struct{}
	removeStall     chan struct{}
	exportStall     chan struct{}
	snapRemoveStall chan struct{}
	snapListStall   chan struct{}
	vmEvents        chan fakeVMEvent
	guestDial       func() (net.Conn, error)

	volumeSpecs          []engine.VolumeSpec
	volumeMounts         []types.Volume
	volumeOps            []string
	attachDirty          map[string]bool
	removeSeenOps        map[string][]string
	removeSeenDirty      map[string][]string
	dirtyPaths           []string
	diskAttachErr        error
	diskAttachErrFor     string
	diskAttachCancel     context.CancelFunc
	attachRendezvous     *sync.WaitGroup
	mountVolumeErr       error
	unmountVolumeErr     error
	detachVolumeErr      error
	detachUnclean        bool
	liveVolumeSupportErr error
	liveVolumeHook       func(string) error
	inspectHook          func()
	inspectFails         int

	startedReaders   []string
	attachedCopies   []string
	attachedDirFiles []string
	attachStall      chan struct{}
	readerDials      []string
	startReaderErr   error
	readerDialErr    error
	readerPeers      chan net.Conn

	warmups           [][]string
	warmupSocks       []string
	warmupAfterSnap   bool
	warmupErr         error
	warmupHook        func()
	settles           int
	settleAfterSnap   bool
	settleAfterWarmup bool
	settleErr         error

	imageIDs   map[string]string
	imageErr   error
	imageLists int

	metadataDocs map[string]string
	metadataErr  error
	guestEnvs    map[string]string
	guestEnvErr  error
	tunVerbs     []string
	tunUpGate    chan struct{}

	migrateDir        string
	migrateSeries     map[string]string
	migrateRound      map[string]int
	migrateSizes      []int64
	paused            map[string]bool
	captures          []string
	receives          []string
	aborts            []string
	migrateSupportErr error
	captureErr        error
	finalCaptureErr   error
	receiveErr        error
	receiveStall      chan struct{}
	receiveEntered    chan struct{}
	abortErr          error
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{
		vms: map[string]string{}, stopped: map[string]bool{}, creating: map[string]bool{}, pids: map[string]int{},
		attachDirty: map[string]bool{}, removeSeenOps: map[string][]string{}, removeSeenDirty: map[string][]string{},
		metadataDocs: map[string]string{}, guestEnvs: map[string]string{}, sockRoot: "/vsock",
		migrateSeries: map[string]string{}, migrateRound: map[string]int{}, paused: map[string]bool{},
	}
}

func (f *fakeEngine) Clone(_ context.Context, fromDir, name string, _ types.PoolKey) (types.VMRecord, error) {
	return f.clone(fromDir, name)
}

func (f *fakeEngine) CloneSnap(_ context.Context, snap, name string, _ types.PoolKey) (types.VMRecord, error) {
	return f.clone(snap, name)
}

func (f *fakeEngine) RunCold(_ context.Context, name string, _ types.PoolKey, storage string) (types.VMRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.colds = append(f.colds, name)
	f.coldStorage = append(f.coldStorage, storage)
	f.volumeOps = append(f.volumeOps, "provision")
	if f.runColdErr != nil {
		return types.VMRecord{}, f.runColdErr
	}
	f.vms[name] = f.sockRoot + "/" + name
	return f.record(name), nil
}

func (f *fakeEngine) Remove(ctx context.Context, name string) error {
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

func (f *fakeEngine) Start(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if hook := f.startHook; hook != nil {
		f.startHook = nil
		hook()
	}
	f.starts = append(f.starts, name)
	if f.startFails > 0 {
		f.startFails--
		return errors.New("query vm.info: context deadline exceeded")
	}
	if f.startErr != nil {
		return f.startErr
	}
	if _, ok := f.vms[name]; !ok {
		return fmt.Errorf("vm %s not found", name)
	}
	f.stopped[name], f.paused[name] = false, false
	return nil
}

func (f *fakeEngine) Stop(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops = append(f.stops, name)
	if f.stopErr != nil {
		return f.stopErr
	}
	f.stopped[name] = true
	return nil
}

func (f *fakeEngine) WaitCapture(_ context.Context, snap string) error {
	f.mu.Lock()
	f.captureWaits = append(f.captureWaits, snap)
	stall, hook := f.captureStall, f.captureHook
	f.mu.Unlock()
	if stall != nil {
		<-stall
	}
	if hook != nil {
		hook()
	}
	return nil
}

func (f *fakeEngine) ReconcileStaleCreate(ctx context.Context, name string) (engine.StaleCreateOutcome, error) {
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
	f.snapSavesEntered++
	stall := f.snapSaveStall
	f.mu.Unlock()
	if stall != nil {
		<-stall
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snapSaveErr != nil {
		return f.snapSaveErr
	}
	f.snapSaves = append(f.snapSaves, snapName)
	return nil
}

func (f *fakeEngine) SnapshotExport(_ context.Context, snapName, toDir string) error {
	f.mu.Lock()
	f.exports = append(f.exports, snapName)
	content := slices.Clone(f.exportContent)
	stall := f.exportStall
	f.mu.Unlock()
	if stall != nil {
		<-stall
	}
	if err := os.MkdirAll(toDir, 0o750); err != nil {
		return err
	}
	f.mu.Lock()
	files := maps.Clone(f.exportFiles)
	f.mu.Unlock()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(toDir, name), []byte(body), 0o600); err != nil {
			return err
		}
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

func (f *fakeEngine) WriteInstanceMetadata(_ context.Context, vsockSocket string, doc []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.metadataErr != nil {
		return f.metadataErr
	}
	f.metadataDocs[vsockSocket] = string(doc)
	return nil
}

func (f *fakeEngine) WriteGuestEnv(_ context.Context, vsockSocket string, doc []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.guestEnvErr != nil {
		return f.guestEnvErr
	}
	f.guestEnvs[vsockSocket] = string(doc)
	return nil
}

func (f *fakeEngine) ImageIDs(context.Context) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.imageLists++
	if f.imageErr != nil {
		return nil, f.imageErr
	}
	return maps.Clone(f.imageIDs), nil
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
		f.stopped[name] = !f.hibernateLeavesVMM
		f.snapshots = append(f.snapshots, snapName)
		f.mu.Unlock()
	}
	return err
}

func (f *fakeEngine) Restore(_ context.Context, name, _ string) (string, error) {
	f.mu.Lock()
	stall := f.restoreStall
	f.mu.Unlock()
	if stall != nil {
		<-stall
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restores = append(f.restores, name)
	if f.restoreErr != nil {
		return "", f.restoreErr
	}
	if _, ok := f.vms[name]; !ok {
		return "", fmt.Errorf("vm %s not found", name)
	}
	f.stopped[name] = false
	return f.lateVsock(f.vms[name]), nil
}

func (f *fakeEngine) List(ctx context.Context) ([]types.VMRecord, error) {
	f.mu.Lock()
	f.listCount++
	f.mu.Unlock()
	return f.list(ctx)
}

func (f *fakeEngine) Inspect(ctx context.Context, name string) (types.VMRecord, bool, error) {
	f.mu.Lock()
	f.inspects++
	hook := f.inspectHook
	f.inspectHook = nil
	failing := f.inspectFails > 0
	if failing {
		f.inspectFails--
	}
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	if failing {
		return types.VMRecord{}, false, errors.New("cocoon unreachable")
	}
	vms, _ := f.list(ctx)
	i := slices.IndexFunc(vms, func(vm types.VMRecord) bool { return vm.Config.Name == name })
	if i < 0 {
		return types.VMRecord{}, false, nil
	}
	f.mu.Lock()
	vms[i].AttachedDevices.Disks = f.disks[name]
	f.mu.Unlock()
	return vms[i], true, nil
}

func (f *fakeEngine) VMEvents(ctx context.Context, _ string, onSync engine.VMSyncFunc, onChange engine.VMChangeFunc) error {
	f.mu.Lock()
	events := f.vmEvents
	f.mu.Unlock()
	if events == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return errors.New("stream closed")
			}
			if ev.sync != nil {
				onSync(ev.sync)
			} else {
				onChange(ev.change)
			}
		}
	}
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
	if f.guestDial != nil {
		return f.guestDial()
	}
	return nil, errors.New("fakeEngine has no guest port")
}

func (f *fakeEngine) InstallCACert(_ context.Context, vsockSocket string, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.caInstalls = append(f.caInstalls, vsockSocket)
	return f.installCAErr
}

func (f *fakeEngine) MarkLane(_ context.Context, _ string, lane types.NetRoute) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.laneMarks = append(f.laneMarks, string(lane))
	return nil
}

func (f *fakeEngine) TunUp(_ context.Context, sock string) error {
	if f.tunUpGate != nil {
		<-f.tunUpGate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tunVerbs = append(f.tunVerbs, "up "+sock)
	return nil
}

func (f *fakeEngine) TunDown(_ context.Context, sock string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tunVerbs = append(f.tunVerbs, "down "+sock)
	return nil
}

func (f *fakeEngine) SettleGuest(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settles++
	f.settleAfterSnap = f.settleAfterSnap || len(f.snapSaves) > 0
	f.settleAfterWarmup = len(f.warmups) > 0
	return f.settleErr
}

func (f *fakeEngine) Warmup(_ context.Context, sock string, argv []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.warmups = append(f.warmups, argv)
	f.warmupSocks = append(f.warmupSocks, sock)
	f.warmupAfterSnap = f.warmupAfterSnap || len(f.snapSaves) > 0
	if f.warmupHook != nil {
		f.warmupHook()
	}
	return f.warmupErr
}

func (f *fakeEngine) TrimCow(_ context.Context, sock string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.trims = append(f.trims, fmt.Sprintf("%s after %d snaps", filepath.Base(sock), len(f.snapSaves)))
	return f.trimErr
}

func (f *fakeEngine) CheckLiveVolumeSupport(context.Context) error { return f.liveVolumeSupportErr }

func (f *fakeEngine) DiskAttach(ctx context.Context, _ string, spec engine.VolumeSpec) error {
	if f.liveVolumeHook != nil {
		if err := f.liveVolumeHook("attach"); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	f.volumeSpecs = append(f.volumeSpecs, spec)
	f.volumeOps = append(f.volumeOps, "attach:"+spec.Name)
	f.attachDirty[spec.Name] = volumeDirty(spec.Path)
	cancel, rendezvous, err := f.diskAttachCancel, f.attachRendezvous, f.diskAttachErr
	if spec.Name == f.diskAttachErrFor {
		err = errors.New("attach failed")
	}
	f.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if rendezvous != nil {
		rendezvous.Done()
		rendezvous.Wait()
	}
	return err
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

func (f *fakeEngine) MountLiveVolume(ctx context.Context, sock, name, mount string, rw bool) error {
	return f.MountVolume(ctx, sock, name, mount, rw)
}

func (f *fakeEngine) DetachVolume(ctx context.Context, _, sock, name, mount string) (bool, error) {
	if f.liveVolumeHook != nil {
		if err := f.liveVolumeHook("detach"); err != nil {
			return false, err
		}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	clean := false
	if mount != "" {
		var err error
		if clean, err = f.UnmountVolume(ctx, sock, mount); err != nil {
			return false, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volumeOps = append(f.volumeOps, "detach:"+name)
	if f.detachVolumeErr != nil {
		return false, f.detachVolumeErr
	}
	return clean, nil
}

func (f *fakeEngine) UnmountVolume(_ context.Context, _, mount string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volumeOps = append(f.volumeOps, "umount:"+mount)
	if f.unmountVolumeErr != nil {
		return false, f.unmountVolumeErr
	}
	return !f.detachUnclean && slices.ContainsFunc(f.volumeMounts, func(v types.Volume) bool { return v.Mount == mount && v.RW() }), nil
}

func (f *fakeEngine) SyncGuest(_ context.Context, vsockSocket string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncs = append(f.syncs, vsockSocket)
	f.volumeOps = append(f.volumeOps, "sync")
	return f.syncErr
}

func (f *fakeEngine) DiskAttachSnapshot(_ context.Context, vmName, snapName string) (string, error) {
	f.mu.Lock()
	f.attachedCopies = append(f.attachedCopies, "snapshot:"+vmName+":"+snapName)
	stall, err := f.attachStall, f.diskAttachErr
	f.mu.Unlock()
	if stall != nil {
		<-stall
	}
	if err != nil {
		return "", err
	}
	return "snap-" + snapName, nil
}

func (f *fakeEngine) DiskAttachDir(_ context.Context, vmName, dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err = cmp.Or(err, f.diskAttachErr); err != nil {
		return "", err
	}
	f.attachedCopies = append(f.attachedCopies, "dir:"+vmName)
	f.attachedDirFiles = nil
	for _, entry := range entries {
		f.attachedDirFiles = append(f.attachedDirFiles, entry.Name())
	}
	return "snap-dir", nil
}

func (f *fakeEngine) StartReader(_ context.Context, vsockSocket, cowDisk string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startedReaders = append(f.startedReaders, cowDisk)
	f.volumeOps = append(f.volumeOps, "reader-start:"+vsockSocket)
	return f.startReaderErr
}

func (f *fakeEngine) DialReader(_ context.Context, vsockSocket string) (net.Conn, error) {
	f.mu.Lock()
	f.readerDials = append(f.readerDials, vsockSocket)
	err, peers := f.readerDialErr, f.readerPeers
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	conn, peer := net.Pipe()
	if peers == nil {
		_ = peer.Close()
	} else {
		peers <- peer
	}
	return conn, nil
}

func (f *fakeEngine) CheckMigrateSupport(context.Context) error { return f.migrateSupportErr }

func (f *fakeEngine) MigrateCapture(_ context.Context, vmName, series string, final bool) (types.MigrateRound, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.captures = append(f.captures, fmt.Sprintf("%s:%s:%v", vmName, series, final))
	switch {
	case f.captureErr != nil:
		return types.MigrateRound{}, f.captureErr
	case f.stopped[vmName]:
		return types.MigrateRound{}, fmt.Errorf("vm %s is not running", vmName)
	case final && f.finalCaptureErr != nil:
		f.paused[vmName] = true
		return types.MigrateRound{}, f.finalCaptureErr
	case series == "":
		series = "s-" + vmName + "-" + strconv.Itoa(len(f.captures))
		f.migrateSeries[vmName], f.migrateRound[vmName] = series, 0
	case f.migrateSeries[vmName] != series:
		return types.MigrateRound{}, fmt.Errorf("series %s is not live on %s", series, vmName)
	}
	round := f.migrateRound[vmName]
	f.migrateRound[vmName]++
	dir := filepath.Join(f.migrateDir, series, strconv.Itoa(round))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return types.MigrateRound{}, err
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(vmName+" round "+strconv.Itoa(round)), 0o600); err != nil {
		return types.MigrateRound{}, err
	}
	size := int64(1)
	if len(f.migrateSizes) > 0 {
		size, f.migrateSizes = f.migrateSizes[0], f.migrateSizes[1:]
	}
	f.paused[vmName] = final
	return types.MigrateRound{Series: series, Dir: dir, Files: []types.MigrateFile{{Name: "state.json", Size: size}}}, nil
}

func (f *fakeEngine) MigrateReceive(_ context.Context, fromDir, name string, _ types.PoolKey) (types.VMRecord, error) {
	if f.receiveEntered != nil {
		close(f.receiveEntered)
		<-f.receiveStall
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.receives = append(f.receives, name)
	if f.receiveErr != nil {
		f.vms[name], f.creating[name] = f.sockRoot+"/"+name, true
		return types.VMRecord{}, f.receiveErr
	}
	if _, err := os.Stat(filepath.Join(fromDir, "state.json")); err != nil {
		return types.VMRecord{}, err
	}
	f.vms[name], f.paused[name] = f.sockRoot+"/"+name, true
	return f.record(name), nil
}

func (f *fakeEngine) MigrateAbort(_ context.Context, vmName, series string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.aborts = append(f.aborts, vmName+":"+series)
	if f.abortErr != nil {
		return f.abortErr
	}
	if _, ok := f.vms[vmName]; !ok {
		return fmt.Errorf("vm %s not found", vmName)
	}
	if f.migrateSeries[vmName] == series {
		delete(f.migrateSeries, vmName)
		f.paused[vmName] = false
	}
	return nil
}

func (f *fakeEngine) list(_ context.Context) ([]types.VMRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listFails > 0 {
		f.listFails--
		return nil, errors.New("cocoon vm list: signal: killed")
	}
	var vms []types.VMRecord
	for name, sock := range f.vms {
		sock = f.lateVsock(sock)
		state := vmStateRunning
		switch {
		case f.vmStates[name] != "":
			state = f.vmStates[name]
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
	f.vms[name] = f.sockRoot + "/" + name
	return f.record(name), nil
}

func (f *fakeEngine) record(name string) types.VMRecord {
	rec := types.VMRecord{VsockSocket: f.lateVsock(f.vms[name]), Config: types.VMConfig{Name: name}}
	if f.tap != "" {
		rec.NetworkConfigs = []types.VMNetConfig{{TAP: f.tap}}
	}
	return rec
}

func (f *fakeEngine) lateVsock(sock string) string {
	if f.vsockLateN > 0 {
		f.vsockLateN--
		return ""
	}
	return sock
}

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

func (f *fakeEngine) unmountCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(slices.DeleteFunc(slices.Clone(f.volumeOps), func(op string) bool { return !strings.HasPrefix(op, "umount:") }))
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

func (f *fakeEngine) lookupCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCount + f.inspects
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

func (m *Manager) deadlineOf(sb *types.Sandbox) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sb.Deadline
}

func (m *Manager) epochOf(sb *types.Sandbox) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sb.ArchiveEpoch
}

func (m *Manager) tripFence() {
	ch := make(chan struct{})
	close(ch)
	m.fence.Store(&fenceState{ch: ch, fenced: true})
}

func (m *Manager) liftFence() {
	m.fence.Store(&fenceState{ch: make(chan struct{})})
}

func lockedVMName(sb *types.Sandbox) string {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	return sb.VMName
}

func newTestManager(t testing.TB, eng *fakeEngine, pools ...config.PoolSpec) *Manager {
	t.Helper()
	return newTestManagerAt(t, eng, t.TempDir(), pools...)
}

func claimAny(ctx context.Context, m *Manager, key types.PoolKey, o ClaimOptions) (*types.Sandbox, error) {
	sb, err := m.ClaimWarm(ctx, key, o)
	if errors.Is(err, ErrNoWarm) {
		return m.ClaimProvision(ctx, key, o)
	}
	return sb, err
}

func mustClaim(t *testing.T, m *Manager, key types.PoolKey) *types.Sandbox {
	t.Helper()
	sb, err := claimAny(t.Context(), m, key, ClaimOptions{})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return sb
}

func hibernateClaim(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
}

func auditLines(t *testing.T, dir string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
	if err != nil {
		t.Fatalf("read audit journal: %v", err)
	}
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

func reloadWith(t *testing.T, m *Manager, edit func(*config.Config)) ReloadResult {
	t.Helper()
	next := *m.cfg
	edit(&next)
	res, err := m.ReloadConfig(t.Context(), &next)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	return res
}

func hostPolicy() *egress.Policy {
	return &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}
}

func pendingOf(m *Manager, name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.pendingRemovals[name]
	return ok
}

func claimedCount(m *Manager) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.claimed)
}

func mustFlush(t *testing.T, m *Manager) {
	t.Helper()
	if err := m.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func mustAdopt(t *testing.T, m *Manager, sb *types.Sandbox, step string) {
	t.Helper()
	if adopted, err := m.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("%s: %v %v, want adopted", step, adopted, err)
	}
}

func mustNotAdopt(t *testing.T, m *Manager, sb *types.Sandbox, step string) {
	t.Helper()
	if adopted, err := m.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || adopted {
		t.Fatalf("%s: %v %v, want refused", step, adopted, err)
	}
}

func breakStore(t *testing.T, m *Manager) {
	t.Helper()
	m.store.path = filepath.Join(t.TempDir(), "gone", "claims.json")
}

func healStore(t *testing.T, m *Manager) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(m.store.path), 0o750); err != nil {
		t.Fatalf("heal store: %v", err)
	}
	waitFor(t, m.store.synced)
}

func startLease(t *testing.T, m *Manager) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go m.keepLease(ctx)
	return ctx
}

func startWatch(t *testing.T, m *Manager) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go m.watchVMMs(ctx)
}

func fakeOf(m *Manager) *fakeEngine {
	return m.eng.(*fakeEngine)
}

func seedClaims(t *testing.T, dataDir string, claims map[string]*types.Sandbox) {
	t.Helper()
	if err := newClaimStore(dataDir, false).save(claims); err != nil {
		t.Fatalf("setup: %v", err)
	}
}

func journaled(t *testing.T, m *Manager) map[string]*types.Sandbox {
	t.Helper()
	claims, err := newClaimStore(m.dataDir, false).load()
	if err != nil {
		t.Fatalf("load journal: %v", err)
	}
	return claims
}

func claimTenant(t *testing.T, m *Manager, tenant string) *types.Sandbox {
	t.Helper()
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour, Tenant: tenant})
	if err != nil {
		t.Fatalf("claim %q: %v", tenant, err)
	}
	return sb
}

func tplKey(template string) types.PoolKey {
	return types.PoolKey{Template: template, Net: testKey.Net, Size: testKey.Size}
}

func buildGoldenAt(t *testing.T, m *Manager, key types.PoolKey, imageID string) string {
	t.Helper()
	final := filepath.Join(m.goldensDir(), key.Hash())
	if err := m.buildGoldenSteps(t.Context(), m.view.Load(), key, "sbx-gb-"+key.Hash(), "snap", final, imageID); err != nil {
		t.Fatalf("buildGoldenSteps %s: %v", key.Template, err)
	}
	return final
}

func mustManager(t testing.TB, cfg *config.Config, eng Engine) *Manager {
	t.Helper()
	m, err := NewManager(t.Context(), cfg, eng, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	return m
}

func newTestManagerAt(t testing.TB, eng *fakeEngine, dataDir string, pools ...config.PoolSpec) *Manager {
	t.Helper()
	cfg := &config.Config{DataDir: dataDir, Pools: pools}
	m := mustManager(t, cfg, eng)
	t.Cleanup(func() { assertRunningCount(t, m) })
	t.Cleanup(func() { settleGoldenBuilds(m) })
	t.Cleanup(func() { settleCaptures(m) })
	t.Cleanup(func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.store.mu.Lock()
		defer m.store.mu.Unlock()
		for id, sb := range m.claimed {
			if m.store.rows[id] == nil || m.store.refs[id] != checkpointRefs(sb) {
				t.Errorf("claim %s references differ from its store row", id)
			}
		}
	})
	t.Cleanup(func() { assertClaimedVMs(t, m) })
	return m
}

func assertClaimedVMs(t testing.TB, m *Manager) {
	m.mu.Lock()
	defer m.mu.Unlock()
	named := 0
	for id, sb := range m.claimed {
		if sb.VMName == "" {
			continue
		}
		named++
		if m.claimedVMs[sb.VMName] != sb {
			t.Errorf("claim %s with VM %s is missing from the VM index", id, sb.VMName)
		}
	}
	if len(m.claimedVMs) != named {
		t.Errorf("VM index holds %d entries for %d named claims", len(m.claimedVMs), named)
	}
}

func settleCaptures(m *Manager) {
	start := time.Now()
	for deadline := start.Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		m.mu.Lock()
		claims := slices.Collect(maps.Values(m.claimed))
		intent := slices.ContainsFunc(claims, func(sb *types.Sandbox) bool { return sb.PendingSnap != "" || sb.SaveSnap != "" })
		m.mu.Unlock()
		busy := slices.ContainsFunc(claims, func(sb *types.Sandbox) bool {
			if !sb.Transition.TryLock() {
				return true
			}
			sb.Transition.Unlock()
			return false
		})
		if !busy && (!intent || time.Since(start) > 300*time.Millisecond) {
			return
		}
	}
}

func settleGoldenBuilds(m *Manager) {
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		m.mu.Lock()
		building := slices.ContainsFunc(slices.Collect(maps.Values(m.pools)), func(p *pool) bool { return p.building })
		m.mu.Unlock()
		if !building {
			return
		}
	}
}

func assertRunningCount(t testing.TB, m *Manager) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	want := 0
	for _, sb := range m.claimed {
		if sb.Waking || sb.HibernateSnap == "" && sb.ArchiveCk == "" && sb.Failed == "" {
			want++
		}
	}
	if got := m.store.runningCount(); got != want {
		t.Errorf("running count %d, want %d from the claims", got, want)
	}
}

func testBackends(t testing.TB, cfg *config.Config) Backends {
	t.Helper()
	root := cmp.Or(cfg.CheckpointDir, filepath.Join(cfg.DataDir, "checkpoints"))
	ckpts, err := dir.New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("checkpoint store: %v", err)
	}
	tpls, err := dir.New(root, store.TemplateIDRe)
	if err != nil {
		t.Fatalf("template store: %v", err)
	}
	var rootSum string
	if cfg.APIToken != "" {
		rootSum = tenants.TokenSHA256(cfg.APIToken)
	}
	src, err := file.Open(t.Context(), cfg.DataDir, rootSum)
	if err != nil {
		t.Fatalf("tenant set: %v", err)
	}
	return Backends{Checkpoints: ckpts, Templates: tpls, Tenants: src}
}

func testSecrets(t testing.TB, specs ...egress.SecretSpec) *egress.SecretStore {
	t.Helper()
	s, err := egress.NewSecretStore(specs)
	if err != nil {
		t.Fatalf("secrets: %v", err)
	}
	return s
}

func lockCount(m *Manager) int {
	m.recs.mu.Lock()
	defer m.recs.mu.Unlock()
	return len(m.recs.held)
}

func hasRecLock(m *Manager, id string) bool {
	m.recs.mu.Lock()
	defer m.recs.mu.Unlock()
	_, ok := m.recs.held[id]
	return ok
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

func waitWarm(t *testing.T, m *Manager, n int) {
	t.Helper()
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Warm == n && infos[0].Refilling == 0
	})
}

func refilledTo(t *testing.T, m *Manager, key types.PoolKey, n int) bool {
	m.refillOnce(t.Context())
	return warmLen(m, key) == n
}

func waitPoolSettled(t *testing.T, m *Manager, key types.PoolKey) {
	t.Helper()
	waitFor(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		p := m.pools[key]
		return p != nil && !p.building && p.refilling == 0
	})
}

func warmLen(m *Manager, key types.PoolKey) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p := m.pools[key]; p != nil {
		return len(p.warm)
	}
	return 0
}

func seedGolden(t *testing.T, m *Manager, key types.PoolKey, imageID string) string {
	t.Helper()
	g := filepath.Join(m.goldensDir(), key.Hash())
	if err := os.MkdirAll(g, 0o750); err != nil {
		t.Fatalf("seed golden: %v", err)
	}
	if err := os.WriteFile(g+goldenStampSuffix, []byte(m.goldenStamp(m.view.Load(), key, imageID)), 0o644); err != nil {
		t.Fatalf("seed golden stamp: %v", err)
	}
	return g
}

func ckExists(t *testing.T, m *Manager, ck string) bool {
	t.Helper()
	_, _, _, err := m.ckpts.Fetch(t.Context(), ck) //nolint:dogsled
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("fetch %s: %v", ck, err)
	}
	return err == nil
}

func mustArchive(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	hibernateClaim(t, m, sb)
	if err := m.archive(t.Context(), sb); err != nil {
		t.Fatalf("archive: %v", err)
	}
}

func archivePool(deleteAfter int) config.PoolSpec {
	return config.PoolSpec{
		PoolKey:                   testKey,
		Warm:                      1,
		IdleHibernateSeconds:      1,
		ArchiveAfterSeconds:       2,
		ArchiveDeleteAfterSeconds: deleteAfter,
	}
}

func backdate(m *Manager, sb *types.Sandbox, by time.Duration) {
	m.mu.Lock()
	sb.TouchAt(time.Now().Add(-by))
	m.mu.Unlock()
}

func hibernated(m *Manager) int {
	_, g := m.Info()
	return g.Hibernated
}

func sweepIdle(t *testing.T, m *Manager) {
	t.Helper()
	m.idleOnce(t.Context())
	waitFor(t, func() bool { return !m.sweeps.idle.Load() })
}

func expire(m *Manager, sb *types.Sandbox) {
	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Second)
	m.mu.Unlock()
}

func egressManager(t testing.TB, eng *fakeEngine, pools ...config.PoolSpec) *Manager {
	t.Helper()
	return egressManagerAt(t, eng, t.TempDir(), pools...)
}

func withOutbound(t *testing.T, m *Manager, edit func(*outbound.Options)) {
	t.Helper()
	ca, err := outbound.LoadCA(m.cfg)
	if err != nil {
		t.Fatalf("load egress ca: %v", err)
	}
	o := m.outboundOptions(ca)
	edit(&o)
	m.out = outbound.New(o)
}

func refillWarmVM(t *testing.T, m *Manager) *types.Sandbox {
	t.Helper()
	seedGolden(t, m, testKey, "")
	m.mu.Lock()
	m.adoptGolden(m.pools[testKey], "")
	m.mu.Unlock()
	m.refillOnce(t.Context())
	var warm *types.Sandbox
	waitFor(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		if p := m.pools[testKey]; p != nil && len(p.warm) > 0 {
			warm = p.warm[0]
		}
		return warm != nil
	})
	return warm
}

func usageEventsOf(t *testing.T, m *Manager, id string) string {
	t.Helper()
	var events []string
	for _, ev := range usageJournal(t, m) {
		if ev.ID == id {
			events = append(events, ev.Event)
		}
	}
	return strings.Join(events, ",")
}

func usageJournal(t *testing.T, m *Manager) []usage.Event {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(m.dataDir, usage.JournalFile))
	if err != nil {
		t.Fatalf("read usage journal: %v", err)
	}
	var events []usage.Event
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		var ev usage.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("bad journal line %q: %v", line, err)
		}
		events = append(events, ev)
	}
	return events
}

func setTenantCaps(t *testing.T, m *Manager, caps map[string]int) {
	t.Helper()
	records := make([]tenants.TenantRecord, 0, len(caps))
	for name, limit := range caps {
		records = append(records, tenants.TenantRecord{Name: name, TokenSHA256: tenants.TokenSHA256("tok-" + name), MaxClaims: limit})
	}
	src, err := file.Open(t.Context(), t.TempDir(), "")
	if err != nil {
		t.Fatalf("tenant set: %v", err)
	}
	if _, err = src.Replace(t.Context(), records); err != nil {
		t.Fatalf("tenant records: %v", err)
	}
	m.tenants = src
}

func putTenants(t *testing.T, m *Manager, specs ...tenants.TenantSpec) {
	t.Helper()
	if len(specs) == 0 {
		return
	}
	if err := m.SetTenants(t.Context(), specs); err != nil {
		t.Fatalf("set tenants: %v", err)
	}
}

func replicateNow(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	if err := m.replicate(t.Context(), sb); err != nil {
		t.Fatalf("replicate %s: %v", sb.ID, err)
	}
}

func newReplicaCluster(t *testing.T) *failoverCluster {
	t.Helper()
	c := newFailoverCluster(t)
	c.replicate = replicateEvery
	return c
}

func volumeHoldersOf(m *Manager, name string) volumeHolders {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.catalog.holders[name]
}

func newVolumeManagerAt(t *testing.T, eng *fakeEngine, dataDir string, volumes []config.VolumeSpec) *Manager {
	t.Helper()
	cfg := &config.Config{DataDir: dataDir, Volumes: volumes}
	return mustManager(t, cfg, eng)
}

func liveCatalog(t *testing.T) []config.VolumeSpec {
	t.Helper()
	return []config.VolumeSpec{
		{Name: "data", Path: writeVolumeImage(t, "data.img", "data"), Writable: true},
		{Name: "other", Path: writeVolumeImage(t, "other.img", "other"), Writable: true},
	}
}

func disableReplication(m *Manager) {
	view := *m.view.Load()
	view.poolReplicate = nil
	m.view.Store(&view)
}
