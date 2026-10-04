package pool

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
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
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var (
	egKey    = types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeSmall}
	egPolicy = &egress.Policy{Allow: []egress.Rule{{Host: "example.com", Secret: "gh"}}}
)

func TestLaneVerdictReachesGuestOnColdBoots(t *testing.T) {
	tests := []struct {
		name   string
		key    types.PoolKey
		locked bool
		cold   bool
		want   []string
	}{
		{"golden build on a locked egress lane", egKey, true, false, []string{"relay"}},
		{"golden build on an unlocked egress lane", egKey, false, false, []string{"direct"}},
		{"golden build on the none lane", testKey, true, false, nil},
		{"cold provision on a locked egress lane", egKey, true, true, []string{"relay"}},
		{"cold provision on an unlocked egress lane", egKey, false, true, []string{"direct"}},
		{"cold provision on the none lane", testKey, true, true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := egressManager(t, eng, config.PoolSpec{PoolKey: tt.key, Warm: 1, Egress: egPolicy})
			withOutbound(m, func(o *outbound.Options) { o.LockNIC = tt.locked })
			if tt.cold {
				sb, err := m.provision(t.Context(), tt.key, "")
				if err != nil {
					t.Fatalf("cold provision: %v", err)
				}
				m.destroy(t.Context(), sb.VMName)
			} else if err := m.buildGoldenSteps(t.Context(), m.view.Load(), tt.key, "sbx-gb", "snap", filepath.Join(m.goldensDir(), tt.key.Hash()), ""); err != nil {
				t.Fatalf("buildGoldenSteps: %v", err)
			}
			if !slices.Equal(eng.laneMarks, tt.want) {
				t.Errorf("MarkLane verdicts = %q, want %q", eng.laneMarks, tt.want)
			}
		})
	}
}

func TestGoldenStampGatesAdoptionOnLane(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: egKey, Warm: 1, Egress: egPolicy})
	p := m.pools[egKey]
	g := filepath.Join(m.goldensDir(), egKey.Hash())
	if err := os.MkdirAll(g, 0o750); err != nil {
		t.Fatalf("stage golden: %v", err)
	}
	if err := os.WriteFile(g+goldenStampSuffix, []byte(m.goldenStamp(m.view.Load(), testKey, "")), 0o644); err != nil {
		t.Fatalf("write stamp: %v", err)
	}
	m.adoptGolden(p, "")
	if p.goldenDir != "" {
		t.Error("adopted an egress-lane golden that never marked its guest; want rebuild")
	}
	if err := os.WriteFile(g+goldenStampSuffix, []byte(m.goldenStamp(m.view.Load(), egKey, "")), 0o644); err != nil {
		t.Fatalf("write stamp: %v", err)
	}
	m.adoptGolden(p, "")
	if p.goldenDir != g {
		t.Error("rejected an egress-lane golden whose stamp says the guest was marked")
	}
}

func TestEgressLaneDoesNotHibernate(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := &types.Sandbox{ID: "sb_eg_h", Key: types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeSmall}}
	sb.Transition.Lock()
	err := m.hibernateLocked(t.Context(), sb)
	sb.Transition.Unlock()
	if !errors.Is(err, ErrNoEgressHibernate) {
		t.Fatalf("hibernate egress lane: got %v, want ErrNoEgressHibernate", err)
	}
}

func TestEgressLaneWakeFailsClosed(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	cases := []struct {
		name string
		sb   *types.Sandbox
	}{
		{"hibernated", &types.Sandbox{ID: "sb_h", Key: egKey, VMName: "sbx-h", HibernateSnap: "sbx-hib-x"}},
		{"archived", &types.Sandbox{ID: "sb_a", Key: egKey, ArchiveCk: "ck_x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := m.wakeResolved(t.Context(), tc.sb); err == nil {
				t.Fatal("egress-lane wake must fail closed, not resume unguarded")
			}
		})
	}
}

func TestEffectivePolicyKeepsTheLayerPinnedAtClaim(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	editEgress(t, m, func(cfg *config.Config) {
		cfg.EgressClasses = []config.EgressClass{{Name: "desk", Egress: &egress.Policy{Allow: []egress.Rule{{Host: "b.test"}}}}}
	})

	m.pools = map[types.PoolKey]*pool{testKey: newPool(testKey)}
	if _, ok := m.policyOf(m.view.Load(), &types.Sandbox{Key: testKey, Tenant: "acme", EgressClass: "desk", Layer: types.LayerUnpooled}); !ok {
		t.Error("a claim made on an unpooled key lost its tenant policy when the key gained a pool")
	}
	m.pools = map[types.PoolKey]*pool{}
	if _, ok := m.policyOf(m.view.Load(), &types.Sandbox{Key: testKey, Tenant: "acme", EgressClass: "desk", Layer: types.LayerPooled}); ok {
		t.Error("a claim made on a policy-less pool gained the tenant policy when the pool was dropped")
	}
}

func TestClaimRecordsItsPolicyLayer(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1})
	if sb := mustClaim(t, m, testKey); sb.Layer != types.LayerPooled {
		t.Errorf("pooled claim layer %q, want %q", sb.Layer, types.LayerPooled)
	}
	unpooled := types.PoolKey{Template: "promoted-name", Net: types.NetNone, Size: types.SizeSmall}
	sb, err := m.ClaimProvision(t.Context(), unpooled, ClaimOptions{TTL: time.Hour, Tenant: "acme"})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if sb.Layer != types.LayerUnpooled {
		t.Errorf("unpooled claim layer %q, want %q", sb.Layer, types.LayerUnpooled)
	}
}

func TestEffectivePolicyComposition(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	both := &egress.Policy{Allow: []egress.Rule{{Host: "a.test"}, {Host: "b.test"}}}
	tenantOnly := &egress.Policy{Allow: []egress.Rule{{Host: "b.test"}, {Host: "c.test"}}}

	cases := []struct {
		name        string
		tenant      string
		pooled      bool
		removed     bool
		pool, tnPol *egress.Policy
		allow, deny string
		wantArmed   bool
	}{
		{"root takes the pool policy whole", "", true, false, both, nil, "a.test", "z.test", true},
		{"root without a pool policy", "", true, false, nil, nil, "", "", false},
		{"tenant intersects", "acme", true, false, both, tenantOnly, "b.test", "a.test", true},
		{"tenant declaring no policy", "acme", true, false, both, nil, "", "", false},
		{"tenant on a policyless pool", "acme", true, false, nil, tenantOnly, "", "", false},
		{"tenant on a policyless pool being removed", "acme", true, true, nil, tenantOnly, "b.test", "a.test", true},
		{"tenant on a promoted template", "acme", false, false, nil, tenantOnly, "b.test", "a.test", true},
		{"root on a promoted template", "", false, false, nil, nil, "", "", false},
		{"neither", "acme", false, false, nil, nil, "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m.pools = map[types.PoolKey]*pool{}
			if tc.pooled {
				m.pools[testKey] = newPool(testKey)
				m.pools[testKey].removed = tc.removed
			}
			editEgress(t, m, func(cfg *config.Config) {
				cfg.Pools = []config.PoolSpec{{PoolKey: testKey, Egress: tc.pool}}
				if tc.tnPol != nil {
					cfg.EgressClasses = []config.EgressClass{{Name: "desk", Egress: tc.tnPol}}
				}
			})
			sb := &types.Sandbox{Key: testKey, Tenant: tc.tenant}
			if tc.tenant != "" {
				sb.EgressClass = "desk"
			}
			eval, ok := m.policyOf(m.view.Load(), sb)
			if ok != tc.wantArmed {
				t.Fatalf("armed=%v, want %v", ok, tc.wantArmed)
			}
			if !ok {
				return
			}
			if _, d := eval.Eval(tc.allow, "GET", 443); d != egress.DecisionAllow {
				t.Errorf("%s should allow", tc.allow)
			}
			if _, d := eval.Eval(tc.deny, "GET", 443); d != egress.DecisionDeny {
				t.Errorf("%s should deny", tc.deny)
			}
		})
	}
}

func TestLockUsesProvisionedTapWithoutList(t *testing.T) {
	eng := newFakeEngine()
	eng.tap = "tap-fake0"
	eng.sockRoot = sockRoot(t)
	m := egressManager(t, eng, config.PoolSpec{PoolKey: egKey, Egress: egPolicy})
	var taps tapLog
	withOutbound(m, taps.record)

	sb, err := m.provision(t.Context(), egKey, "")
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	if sb.TAP != "tap-fake0" {
		t.Fatalf("provision carried tap %q, want tap-fake0", sb.TAP)
	}
	if err := m.out.Arm(t.Context(), sb); err != nil {
		t.Fatalf("arm egress: %v", err)
	}
	if calls := eng.lookupCalls(); calls != 0 {
		t.Errorf("claim-path lock looked the VM up %d times, want 0", calls)
	}
	m.out.Disarm(sb.ID, true)
	if locked, unlocked := taps.locked(), taps.unlocked(); !slices.Equal(locked, []string{"tap-fake0"}) || !slices.Equal(unlocked, locked) {
		t.Errorf("locked %v and unlocked %v at removal, want tap-fake0 recorded", locked, unlocked)
	}
}

func TestBatchArmFailureRecordsNoUsage(t *testing.T) {
	eng := newFakeEngine()
	m := egressManager(t, eng, config.PoolSpec{PoolKey: egKey, Egress: egPolicy})

	sbs := []*types.Sandbox{
		{VMName: "sbx-ok", Key: testKey},
		{VMName: "sbx-eg", Key: egKey},
	}
	if err := m.finalizeBatch(t.Context(), sbs, time.Minute, "", false); err == nil {
		t.Fatal("finalizeBatch must fail when a batch member cannot arm")
	}
	waitFor(t, m.store.synced)
	m.mu.Lock()
	claimed := len(m.claimed)
	m.mu.Unlock()
	if claimed != 0 {
		t.Errorf("rollback left %d claims", claimed)
	}
	raw, _ := os.ReadFile(filepath.Join(m.dataDir, "usage.jsonl"))
	if strings.Contains(string(raw), `"ev":"claim"`) {
		t.Errorf("rolled-back batch left claim usage events:\n%s", raw)
	}
	if !eng.removed("sbx-ok") || !eng.removed("sbx-eg") {
		t.Error("rollback must destroy every batch VM")
	}
}

func TestRollbackKeepsLockWhenRemoveFails(t *testing.T) {
	eng := newFakeEngine()
	eng.removeErrFor = "sbx-r1"

	eng.vms["sbx-r1"] = "/run/sbx-r1.sock"
	eng.vms["sbx-r2"] = "/run/sbx-r2.sock"
	m := egressManager(t, eng, config.PoolSpec{PoolKey: egKey, Egress: egPolicy})
	var taps tapLog
	withOutbound(m, taps.record)

	sbs := []*types.Sandbox{
		{ID: "sb_r1", VMName: "sbx-r1", Key: egKey},
		{ID: "sb_r2", VMName: "sbx-r2", Key: egKey},
	}
	m.mu.Lock()
	for _, sb := range sbs {
		m.claimed[sb.ID] = sb
	}
	m.mu.Unlock()
	m.out.KeepLock("sb_r1", "tap-r1")
	m.out.KeepLock("sb_r2", "tap-r2")

	m.rollbackClaim(t.Context(), sbs)
	waitFor(t, m.store.synced)

	m.mu.Lock()
	claimed := len(m.claimed)
	m.mu.Unlock()
	if claimed != 0 {
		t.Errorf("rollback left %d claims", claimed)
	}
	if slices.Contains(taps.unlocked(), "tap-r1") {
		t.Error("rollback unlocked a VM whose remove failed")
	}
	if !slices.Contains(taps.unlocked(), "tap-r2") {
		t.Error("rollback kept a confirmed-removed VM locked")
	}
	m.out.Disarm("sb_r1", true)
	if !slices.Contains(taps.unlocked(), "tap-r1") {
		t.Error("rollback dropped the lock record of a VM whose remove failed")
	}
}

func TestQuarantineFailedRemoveStaysUnswept(t *testing.T) {
	eng := newFakeEngine()
	eng.removeErrFor = "sbx-q1"

	eng.vms["sbx-q1"] = "/run/sbx-q1.sock"
	m := egressManager(t, eng, config.PoolSpec{PoolKey: egKey, Egress: egPolicy})
	sb := &types.Sandbox{ID: "sb_q1", VMName: "sbx-q1", Key: egKey, TAP: "tap-q1"}
	m.mu.Lock()
	m.claimed[sb.ID] = sb
	m.mu.Unlock()
	live := map[string]types.VMRecord{"sbx-q1": {Config: types.VMConfig{Name: "sbx-q1"}, State: vmStateRunning}}
	removed := map[string]bool{}

	var gotKeep map[string]bool
	m.sweep = func(keep map[string]bool) error { gotKeep = keep; return nil }
	m.resyncEgress(t.Context(), live, removed)

	if removed["sbx-q1"] {
		t.Error("failed remove marked the VM gone; the sweep would drop its lock table")
	}
	if !gotKeep["tap-q1"] {
		t.Error("failed-remove VM's journal tap not kept; the sweep would unlock a running guest")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.claimed["sb_q1"]; ok {
		t.Error("quarantined claim still in service")
	}
}

func TestSetPoolsPreservesEgressPolicy(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: egKey, Egress: egPolicy})
	gd := filepath.Join(m.goldensDir(), egKey.Hash())
	if err := os.MkdirAll(gd, 0o750); err != nil {
		t.Fatalf("golden dir: %v", err)
	}
	if err := os.WriteFile(gd+goldenStampSuffix, []byte(m.goldenStamp(m.view.Load(), egKey, "")), 0o644); err != nil {
		t.Fatalf("golden stamp: %v", err)
	}
	m.mu.Lock()
	m.pools[egKey].goldenDir = gd
	m.mu.Unlock()
	policyLive := func() bool {
		_, ok := m.policyOf(m.view.Load(), &types.Sandbox{Key: egKey})
		return ok
	}
	if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: egKey, WarmMax: 3}}); err != nil {
		t.Fatalf("SetPools warm change: %v", err)
	}
	if !policyLive() {
		t.Fatal("a warm change wiped the pool egress policy")
	}
	if err := m.SetPools(t.Context(), nil); err != nil {
		t.Fatalf("SetPools drain: %v", err)
	}
	if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: egKey}}); err != nil {
		t.Fatalf("SetPools re-add: %v", err)
	}
	if !policyLive() {
		t.Fatal("drain + re-add wiped the pool egress policy")
	}
}

func TestEgressLaneCannotForkOrCheckpoint(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: egKey, Egress: egPolicy})
	sb := &types.Sandbox{ID: "sb_egb", Key: egKey, Token: "tok", VMName: "sbx-egb"}
	m.mu.Lock()
	m.claimed[sb.ID] = sb
	m.mu.Unlock()
	if _, err := m.Fork(t.Context(), sb.ID, Cred{Token: "tok"}, 1, time.Minute, "", ""); !errors.Is(err, ErrNoEgressFork) {
		t.Errorf("Fork on egress lane: got %v, want ErrNoEgressFork", err)
	}
	if _, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: "tok"}, "", ""); !errors.Is(err, ErrNoEgressFork) {
		t.Errorf("Checkpoint on egress lane: got %v, want ErrNoEgressFork", err)
	}
	if _, _, err := m.Promote(t.Context(), sb.ID, Cred{Token: "tok"}, "tpl", ""); !errors.Is(err, ErrNoEgressFork) {
		t.Errorf("Promote on egress lane: got %v, want ErrNoEgressFork", err)
	}
}

func TestWarmClaimServesTheDoorsRefillBound(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = sockRoot(t)
	pol := &egress.Policy{Socks5: true, Allow: []egress.Rule{{Host: "example.com"}}}
	m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: pol})
	warm := refillWarmVM(t, m)
	pins := pinDoors(t, warm)
	if m.NetRoute(warm) != types.NetRouteNone {
		t.Fatal("refill served the doors it bound, want them bound and nothing served")
	}

	sb := mustClaim(t, m, testKey)
	m.out.ClosePrebound(sb.VMName)
	if !servesPinned(t, sb, pins) || m.NetRoute(sb) != types.NetRouteRelay {
		t.Fatalf("claim route %q, want the refill-bound pair served with none left prebound", m.NetRoute(sb))
	}
	dialDoors(t, sb)
	m.out.Disarm(sb.ID, true)
	if _, err := net.Dial("unix", engine.EgressSocketPath(sb.VsockSocket)); err == nil {
		t.Error("egress socket still accepts after disarm")
	}
}

func TestWarmClaimRebindsADoorTheGuestReachedEarly(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = sockRoot(t)
	pol := &egress.Policy{Socks5: true, Allow: []egress.Rule{{Host: "example.com"}}}
	m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: pol})
	warm := refillWarmVM(t, m)
	pins := pinDoors(t, warm)
	early, err := net.Dial("unix", engine.SocksSocketPath(warm.VsockSocket))
	if err != nil {
		t.Fatalf("pre-claim dial: %v", err)
	}
	defer early.Close()

	sb := mustClaim(t, m, testKey)
	if servesPinned(t, sb, pins) {
		t.Fatal("claim served the pair a guest had already reached")
	}
	_ = early.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := early.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("a connection queued before the claim was kept: read err=%v, want closed", err)
	}
	dialDoors(t, sb)
	m.out.Disarm(sb.ID, true)
}

func TestTrimmedWarmVMClosesItsDoors(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = sockRoot(t)
	m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: egPolicy})
	warm := refillWarmVM(t, m)
	path := engine.EgressSocketPath(warm.VsockSocket)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("refill did not bind the door: %v", err)
	}
	pin := pinDoor(t, path)

	if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: testKey}}); err != nil {
		t.Fatalf("SetPools: %v", err)
	}
	waitFor(t, func() bool { return eng.removed(warm.VMName) })
	if conn, err := net.Dial("unix", pin); err == nil {
		_ = conn.Close()
		t.Error("prebound door survives its VM")
	}
	if _, err := os.Stat(path); err == nil {
		t.Error("door socket survives its VM")
	}
}

func TestAPromotedTemplatesCloneEgressesAsItsSourcePool(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = sockRoot(t)
	pol := &egress.Policy{Socks5: true, Allow: []egress.Rule{{Host: "example.com"}}}
	m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: pol})
	parent := mustClaim(t, m, testKey)
	key, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:egress", "")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}

	clone, err := m.ClaimProvision(t.Context(), key, ClaimOptions{RequirePromoted: true})
	if err != nil {
		t.Fatalf("claim the template: %v", err)
	}
	if clone.PolicySource != testKey || clone.Layer != types.LayerPooled || m.NetRoute(clone) != types.NetRouteRelay {
		t.Fatalf("clone policy source %+v layer %q, want the source pool %+v and its pooled layer", clone.PolicySource, clone.Layer, testKey)
	}
	dialDoors(t, clone)
	eval, ok := m.policyOf(m.view.Load(), clone)
	if !ok {
		t.Fatal("the clone resolved no policy")
	}
	if _, d := eval.Eval("example.com", "GET", 443); d != egress.DecisionAllow {
		t.Error("the clone denies what its source pool allows")
	}
	if _, d := eval.Eval("other.test", "GET", 443); d != egress.DecisionDeny {
		t.Error("the clone allows what its source pool denies")
	}

	children, err := m.Fork(t.Context(), clone.ID, Cred{Token: clone.Token}, 1, 0, "", "")
	if err != nil {
		t.Fatalf("fork the clone: %v", err)
	}
	ckpt, err := m.Checkpoint(t.Context(), clone.ID, Cred{Token: clone.Token}, "c1", "")
	if err != nil {
		t.Fatalf("checkpoint the clone: %v", err)
	}
	branch, err := m.ClaimCheckpoint(t.Context(), ckpt.ID, ClaimOptions{})
	if err != nil {
		t.Fatalf("branch the checkpoint: %v", err)
	}
	for _, sb := range []*types.Sandbox{children[0], branch} {
		if sb.PolicySource != testKey {
			t.Errorf("%s policy source %+v, want %+v", sb.ID, sb.PolicySource, testKey)
		}
		dialDoors(t, sb)
	}
}

func TestAPromotedTemplatesCloneInterceptsAsItsSourcePool(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = sockRoot(t)
	m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: interceptPolicy()})
	parent := mustClaim(t, m, testKey)
	key, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:intercept", "")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	clone, err := m.ClaimProvision(t.Context(), key, ClaimOptions{RequirePromoted: true})
	if err != nil {
		t.Fatalf("claim the template: %v", err)
	}
	conn := connectDoor(t, engine.EgressSocketPath(clone.VsockSocket), "api.github.com:443")
	tc := tls.Client(conn, &tls.Config{ServerName: "api.github.com", InsecureSkipVerify: true})
	if err = tc.Handshake(); err != nil {
		t.Fatalf("handshake through the door: %v", err)
	}
	if issuer := tc.ConnectionState().PeerCertificates[0].Issuer.CommonName; !strings.HasSuffix(issuer, "node-test") {
		t.Errorf("leaf issued by %q, want the node intermediate: the clone must intercept as its source pool does", issuer)
	}
}

func TestAClaimWithoutEgressGetsNoDoorsWhateverItsPolicy(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = sockRoot(t)
	pol := &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}
	m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: pol})
	pin := pinDoor(t, engine.EgressSocketPath(refillWarmVM(t, m).VsockSocket))
	warm, err := m.ClaimWarm(t.Context(), testKey, ClaimOptions{NoEgress: true})
	if err != nil {
		t.Fatalf("warm claim: %v", err)
	}
	parent := mustClaim(t, m, testKey)
	key, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:closed", "")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	clone, err := m.ClaimProvision(t.Context(), key, ClaimOptions{RequirePromoted: true, NoEgress: true})
	if err != nil {
		t.Fatalf("claim the template: %v", err)
	}
	if conn, err := net.Dial("unix", pin); err == nil {
		_ = conn.Close()
		t.Error("the warm VM's prebound door accepts")
	}
	for _, sb := range []*types.Sandbox{warm, clone} {
		if !sb.NoEgress || m.NetRoute(sb) != types.NetRouteNone {
			t.Errorf("%s: route %q no_egress %v, want no door and the opt-out recorded", sb.ID, m.NetRoute(sb), sb.NoEgress)
		}
		if conn, err := net.Dial("unix", engine.EgressSocketPath(sb.VsockSocket)); err == nil {
			_ = conn.Close()
			t.Errorf("%s: the egress door accepts", sb.ID)
		}
	}
}

func TestARepromoteCarriesItsNewSourcePool(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = sockRoot(t)
	other := types.PoolKey{Template: "py:3.12", Net: testKey.Net, Size: testKey.Size}
	m := egressManager(t, eng,
		config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "a.test"}}}},
		config.PoolSpec{PoolKey: other, Warm: 1, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "b.test"}}}})
	for _, source := range []types.PoolKey{testKey, other} {
		parent := mustClaim(t, m, source)
		key, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:moved", "")
		if err != nil {
			t.Fatalf("promote from %s: %v", source.Template, err)
		}
		clone, err := m.ClaimProvision(t.Context(), key, ClaimOptions{RequirePromoted: true})
		if err != nil {
			t.Fatalf("claim the template: %v", err)
		}
		if clone.PolicySource != source {
			t.Errorf("clone after a promote from %s inherits %+v", source.Template, clone.PolicySource)
		}
	}
}

func dialDoors(t *testing.T, sb *types.Sandbox) {
	t.Helper()
	for _, path := range doorPaths(sb) {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("door %s: %v", path, err)
		}
		_ = conn.Close()
	}
}

func doorPaths(sb *types.Sandbox) []string {
	return []string{engine.EgressSocketPath(sb.VsockSocket), engine.SocksSocketPath(sb.VsockSocket)}
}

func pinDoors(t *testing.T, sb *types.Sandbox) []string {
	t.Helper()
	var pins []string
	for _, path := range doorPaths(sb) {
		pins = append(pins, pinDoor(t, path))
	}
	return pins
}

// pinDoor hard-links a door's socket file, so a rebind cannot reuse its inode and a leaked listener still answers there.
func pinDoor(t *testing.T, path string) string {
	t.Helper()
	pin := path + ".pin"
	if err := os.Link(path, pin); err != nil {
		t.Fatalf("pin door %s: %v", path, err)
	}
	return pin
}

func servesPinned(t *testing.T, sb *types.Sandbox, pins []string) bool {
	t.Helper()
	for i, path := range doorPaths(sb) {
		door, err := os.Stat(path)
		if err != nil {
			t.Fatalf("door %s: %v", path, err)
		}
		pin, err := os.Stat(pins[i])
		if err != nil {
			t.Fatalf("pin %s: %v", pins[i], err)
		}
		if !os.SameFile(door, pin) {
			return false
		}
	}
	return true
}

func connectDoor(t *testing.T, path, target string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		t.Fatalf("dial the door: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
		t.Fatalf("send CONNECT: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read the CONNECT answer: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT answered %s", resp.Status)
	}
	return conn
}

func refillWarmVM(t *testing.T, m *Manager) *types.Sandbox {
	t.Helper()
	seedGolden(t, m, "")
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

func egressClient(path string) *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyURL(&url.URL{Scheme: "http", Host: "proxy.internal:3128"}),
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", path)
			},
		},
	}
}

func egressManager(t *testing.T, eng *fakeEngine, pools ...config.PoolSpec) *Manager {
	t.Helper()
	return egressManagerAt(t, eng, t.TempDir(), pools...)
}

func egressManagerAt(t *testing.T, eng *fakeEngine, dataDir string, pools ...config.PoolSpec) *Manager {
	t.Helper()
	t.Setenv("GH_TOKEN", "s3cr3t")
	secrets := testSecrets(t, egress.SecretSpec{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"})
	cfg := &config.Config{DataDir: dataDir, Bridges: []string{"sbxbr0"}, EgressCA: writeTestEgressCA(t), Pools: pools}
	m, err := NewManager(t.Context(), cfg, eng, secrets)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	return m
}

func writeTestEgressCA(t *testing.T) *config.EgressCAConfig {
	t.Helper()
	rootCert, rootKey, err := egress.GenerateRoot("test cluster ca")
	if err != nil {
		t.Fatalf("generate root: %v", err)
	}
	interCert, interKey, err := egress.IssueIntermediate(rootCert, rootKey, "node-test")
	if err != nil {
		t.Fatalf("issue intermediate: %v", err)
	}
	dir := t.TempDir()
	ca := &config.EgressCAConfig{
		RootCert:         filepath.Join(dir, "root.crt"),
		IntermediateCert: filepath.Join(dir, "node.crt"),
		IntermediateKey:  filepath.Join(dir, "node.key"),
	}
	for path, data := range map[string][]byte{ca.RootCert: rootCert, ca.IntermediateCert: interCert, ca.IntermediateKey: interKey} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	return ca
}

func vsockSandbox(t *testing.T, id string) *types.Sandbox {
	t.Helper()
	return &types.Sandbox{ID: id, Key: testKey, VsockSocket: filepath.Join(sockRoot(t), "v")}
}

func sockRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "eg")
	if err != nil {
		t.Fatalf("sockdir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func mustHostname(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u.Hostname()
}

func withOutbound(m *Manager, edit func(*outbound.Options)) {
	o := m.outboundOptions(m.out.CA())
	edit(&o)
	m.out = outbound.New(o)
}

type tapLog struct {
	mu      sync.Mutex
	locks   []string
	unlocks []string
}

func (l *tapLog) record(o *outbound.Options) {
	o.Lock, o.Unlock = l.lock, l.unlock
}

func (l *tapLog) lock(tap string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.locks = append(l.locks, tap)
	return nil
}

func (l *tapLog) unlock(tap string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.unlocks = append(l.unlocks, tap)
	return nil
}

func (l *tapLog) locked() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.locks)
}

func (l *tapLog) unlocked() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.unlocks)
}
