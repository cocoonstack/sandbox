package pool

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var (
	egKey    = types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeSmall}
	egPolicy = &egress.Policy{Allow: []egress.Rule{{Host: "example.com", Secret: "gh"}}}
)

func TestEgressProxyInjectsAndGates(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Seen-Auth", r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(origin.Close)
	host := mustHostname(t, origin.URL)

	pol := &egress.Policy{Allow: []egress.Rule{{Host: host, Secret: "gh"}}}
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: pol})
	m.dial = (&net.Dialer{}).DialContext

	sb := vsockSandbox(t, "sb_egress")
	if armErr := m.armEgress(t.Context(), sb); armErr != nil {
		t.Fatalf("arm egress: %v", armErr)
	}
	path := engine.EgressSocketPath(sb.VsockSocket)
	client := egressClient(path)

	resp, err := client.Get(origin.URL + "/")
	if err != nil {
		t.Fatalf("allowed request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "ok" || resp.Header.Get("X-Seen-Auth") != "s3cr3t" {
		t.Errorf("allowed request body=%q injected=%q, want ok/s3cr3t", body, resp.Header.Get("X-Seen-Auth"))
	}

	req, _ := http.NewRequest(http.MethodGet, "http://blocked.example/", nil)
	deny, err := client.Do(req)
	if err != nil {
		t.Fatalf("denied request: %v", err)
	}
	deny.Body.Close()
	if deny.StatusCode != http.StatusForbidden {
		t.Errorf("denied status %d, want 403", deny.StatusCode)
	}

	m.disarmEgress(sb.ID, true)
	if _, err := net.Dial("unix", path); err == nil {
		t.Error("egress socket still accepts after disarm")
	}
}

func TestArmEgressBindsSocksOnlyWhenOptedIn(t *testing.T) {
	tests := []struct {
		name   string
		policy *egress.Policy
		want   bool
	}{
		{"opted in with a bare host rule", &egress.Policy{Socks5: true, Allow: []egress.Rule{{Host: "example.com"}}}, true},
		{"bare host rule without opting in", &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: tt.policy})
			sb := vsockSandbox(t, "sb_socks")
			if armErr := m.armEgressProxy(t.Context(), sb); armErr != nil {
				t.Fatalf("arm proxy: %v", armErr)
			}
			path := engine.SocksSocketPath(sb.VsockSocket)
			conn, err := net.Dial("unix", path)
			if (err == nil) != tt.want {
				t.Fatalf("socks listener bound = %v, want %v", err == nil, tt.want)
			}
			if conn != nil {
				_ = conn.Close()
			}
			m.disarmEgress(sb.ID, true)
			if _, err := net.Dial("unix", path); err == nil {
				t.Error("socks socket still accepts after disarm")
			}
		})
	}
}

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
			m.lockEgress = tt.locked
			if tt.cold {
				sb, err := m.provision(t.Context(), tt.key, "")
				if err != nil {
					t.Fatalf("cold provision: %v", err)
				}
				m.destroy(t.Context(), sb.VMName)
			} else if err := m.buildGoldenSteps(t.Context(), tt.key, "sbx-gb", "snap", filepath.Join(m.goldensDir(), tt.key.Hash())); err != nil {
				t.Fatalf("buildGoldenSteps: %v", err)
			}
			if !slices.Equal(eng.laneMarks, tt.want) {
				t.Errorf("MarkLane verdicts = %q, want %q", eng.laneMarks, tt.want)
			}
		})
	}
}

func TestGoldenNICSidecarGatesAdoption(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: egKey, Warm: 1, Egress: egPolicy})
	p := m.pools[egKey]
	g := filepath.Join(m.goldensDir(), egKey.Hash())
	if err := os.MkdirAll(g, 0o750); err != nil {
		t.Fatalf("stage golden: %v", err)
	}
	m.adoptGolden(p)
	if p.goldenDir != "" {
		t.Error("adopted an egress-lane golden that never marked its guest; want rebuild")
	}
	if err := writeGoldenSidecar(g+nicSidecarSuffix, string(engine.LaneRelay)); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}
	m.adoptGolden(p)
	if p.goldenDir != g {
		t.Error("rejected an egress-lane golden whose sidecar says the guest was marked")
	}
}

func TestArmEgressFailsClosedWhenNICUnlockable(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: egKey, Egress: egPolicy})

	sb := &types.Sandbox{ID: "sb_eg_no_tap", Key: egKey, VMName: "sbx-no-tap-1"}
	if armErr := m.armEgress(t.Context(), sb); armErr == nil {
		t.Fatal("armEgress must fail closed when the egress-lane NIC cannot be locked")
	}
}

func TestArmEgressLocksEgressLaneWithoutPolicy(t *testing.T) {
	m := egressManager(t, newFakeEngine(),
		config.PoolSpec{PoolKey: testKey, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "a.test"}}}},
		config.PoolSpec{PoolKey: egKey},
	)
	sb := &types.Sandbox{ID: "sb_eg_np", Key: egKey, VMName: "sbx-np-1"}
	if armErr := m.armEgress(t.Context(), sb); armErr == nil {
		t.Fatal("policyless egress-lane claim must still lock the NIC, not skip it")
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
			m.poolEgress = map[types.PoolKey]*egress.Policy{}
			if tc.pool != nil {
				m.poolEgress[testKey] = tc.pool
			}
			m.tenantEgress = map[string]*egress.Policy{}
			if tc.tnPol != nil {
				m.tenantEgress["acme"] = tc.tnPol
			}
			sb := &types.Sandbox{Key: testKey, Tenant: tc.tenant}
			eval, ok := m.effectivePolicy(sb)
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
	m := egressManager(t, eng, config.PoolSpec{PoolKey: egKey, Egress: egPolicy})

	sb, err := m.provision(t.Context(), egKey, "")
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	if sb.TAP != "tap-fake0" {
		t.Fatalf("provision carried tap %q, want tap-fake0", sb.TAP)
	}
	lockErr := m.lockEgressNIC(t.Context(), sb)
	if calls := eng.listCalls(); calls != 0 {
		t.Errorf("claim-path lock consulted vm list %d times, want 0", calls)
	}
	switch {
	case lockErr != nil && !strings.Contains(lockErr.Error(), "tap-fake0"):
		t.Errorf("lock failed outside nft: %v", lockErr)
	case lockErr == nil:
		m.mu.Lock()
		got := m.egressTaps[sb.ID]
		m.mu.Unlock()
		if got != "tap-fake0" {
			t.Errorf("recorded tap %q, want tap-fake0", got)
		}
	}
}

func TestLockFallsBackToListForPreTapClaims(t *testing.T) {
	eng := newFakeEngine()
	eng.tap = "tap-fake1"
	m := egressManager(t, eng, config.PoolSpec{PoolKey: egKey, Egress: egPolicy})
	if _, err := eng.RunCold(t.Context(), "sbx-old", egKey); err != nil {
		t.Fatalf("seed vm: %v", err)
	}

	sb := &types.Sandbox{ID: "sb_old", Key: egKey, VMName: "sbx-old"}
	lockErr := m.lockEgressNIC(t.Context(), sb)
	if calls := eng.listCalls(); calls != 1 {
		t.Errorf("fallback consulted vm list %d times, want 1", calls)
	}
	if lockErr != nil && !strings.Contains(lockErr.Error(), "tap-fake1") {
		t.Errorf("fallback resolved no tap: %v", lockErr)
	}
}

func TestBatchArmFailureRecordsNoUsage(t *testing.T) {
	eng := newFakeEngine()
	m := egressManager(t, eng, config.PoolSpec{PoolKey: egKey, Egress: egPolicy})

	sbs := []*types.Sandbox{
		{VMName: "sbx-ok", Key: testKey},
		{VMName: "sbx-eg", Key: egKey},
	}
	if err := m.finalizeBatch(t.Context(), sbs, time.Minute); err == nil {
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

	sbs := []*types.Sandbox{
		{ID: "sb_r1", VMName: "sbx-r1", Key: egKey},
		{ID: "sb_r2", VMName: "sbx-r2", Key: egKey},
	}
	m.mu.Lock()
	for _, sb := range sbs {
		m.claimed[sb.ID] = sb
	}
	m.egressTaps["sb_r1"] = "tap-r1"
	m.egressTaps["sb_r2"] = "tap-r2"
	m.mu.Unlock()

	m.rollbackClaim(t.Context(), sbs)
	waitFor(t, m.store.synced)

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.claimed) != 0 {
		t.Errorf("rollback left %d claims", len(m.claimed))
	}
	if _, locked := m.egressTaps["sb_r1"]; !locked {
		t.Error("rollback unlocked a VM whose remove failed")
	}
	if _, locked := m.egressTaps["sb_r2"]; locked {
		t.Error("rollback kept a confirmed-removed VM locked")
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

func TestEgressLaneLocksWithNoPolicyAnywhere(t *testing.T) {
	eng := newFakeEngine()
	eng.tap = "tap-nopol"
	m := egressManager(t, eng, config.PoolSpec{PoolKey: egKey})
	if m.guardedEgress {
		t.Fatal("no policy configured; guardedEgress should be false")
	}
	sb := &types.Sandbox{ID: "sb_np", Key: egKey, VMName: "sbx-np", TAP: "tap-nopol"}
	lockErr := m.lockEgressNIC(t.Context(), sb)
	m.mu.Lock()
	attempted := m.egressTaps[sb.ID] == "tap-nopol"
	m.mu.Unlock()
	if lockErr != nil {
		attempted = strings.Contains(lockErr.Error(), "tap-nopol")
	}
	if !attempted {
		t.Fatalf("policyless egress lane skipped the NIC lock: err=%v", lockErr)
	}
}

func TestDisarmKeepsLockWhenRemoveFailed(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Egress: egPolicy})
	m.dial = (&net.Dialer{}).DialContext
	sb := vsockSandbox(t, "sb_dz")
	if armErr := m.armEgressProxy(t.Context(), sb); armErr != nil {
		t.Fatalf("arm proxy: %v", armErr)
	}
	m.mu.Lock()
	m.egressTaps[sb.ID] = "tap-dz"
	m.mu.Unlock()
	path := engine.EgressSocketPath(sb.VsockSocket)

	m.disarmEgress(sb.ID, false)
	if _, err := net.Dial("unix", path); err == nil {
		t.Error("proxy listener still accepts after a failed-remove disarm")
	}
	m.mu.Lock()
	tap := m.egressTaps[sb.ID]
	m.mu.Unlock()
	if tap != "tap-dz" {
		t.Error("failed remove dropped the NIC lock; a still-running VM must stay locked")
	}
}

func TestSetPoolsPreservesEgressPolicy(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: egKey, Egress: egPolicy})
	gd := filepath.Join(m.goldensDir(), egKey.Hash())
	if err := os.MkdirAll(gd, 0o750); err != nil {
		t.Fatalf("golden dir: %v", err)
	}
	m.mu.Lock()
	m.pools[egKey].goldenDir = gd
	m.mu.Unlock()
	policyLive := func() bool {
		_, ok := m.effectivePolicy(&types.Sandbox{Key: egKey})
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

func TestEgressDialerBlocksInternal(t *testing.T) {
	blocked := []string{
		"127.0.0.1:80", "169.254.169.254:80", "10.0.0.5:443", "192.168.1.1:22",
		"172.16.0.1:80", "255.255.255.255:80", "[::1]:80", "[::ffff:127.0.0.1]:80",
		"[fe80::1]:80", "[fc00::1]:80", "[ff02::1]:80",

		"0.6.6.6:80", "100.100.100.200:80", "192.0.0.1:80", "192.0.2.1:80",
		"192.88.99.2:80", "198.18.0.1:80", "198.19.255.255:80", "198.51.100.1:80",
		"203.0.113.1:80", "240.1.2.3:80",

		"[100::1]:80", "[100:0:0:1::1]:80", "[2001:db8::1]:80", "[3fff::1]:80",
		"[5f00::1]:80", "[::127.0.0.1]:80",
		"[2001::a9fe:a9fe]:80",
		"[2001:2::1]:80",
		"[2001:10::1]:80",
		"[64:ff9b:1::8.8.8.8]:80",
		"[2002:a9fe:a9fe::]:80",
		"[64:ff9b::a9fe:a9fe]:80",
		"[64:ff9b::a00:1]:80",
	}
	for _, addr := range blocked {
		if err := newEgressDialer(nil).Control("tcp", addr, nil); err == nil {
			t.Errorf("dial to internal %s allowed; SSRF not blocked", addr)
		}
	}

	for _, addr := range []string{"93.184.216.34:443", "[2606:4700:4700::1111]:443", "[64:ff9b::5db8:d822]:443"} {
		if err := newEgressDialer(nil).Control("tcp", addr, nil); err != nil {
			t.Errorf("dial to public %s blocked: %v", addr, err)
		}
	}
}

func TestEgressLaneCannotForkOrCheckpoint(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: egKey, Egress: egPolicy})
	sb := &types.Sandbox{ID: "sb_egb", Key: egKey, Token: "tok", VMName: "sbx-egb"}
	m.mu.Lock()
	m.claimed[sb.ID] = sb
	m.mu.Unlock()
	if _, err := m.Fork(t.Context(), sb.ID, Cred{Token: "tok"}, 1, time.Minute); !errors.Is(err, ErrNoEgressFork) {
		t.Errorf("Fork on egress lane: got %v, want ErrNoEgressFork", err)
	}
	if _, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: "tok"}, "", ""); !errors.Is(err, ErrNoEgressFork) {
		t.Errorf("Checkpoint on egress lane: got %v, want ErrNoEgressFork", err)
	}
	if _, _, err := m.Promote(t.Context(), sb.ID, Cred{Token: "tok"}, "tpl", ""); !errors.Is(err, ErrNoEgressFork) {
		t.Errorf("Promote on egress lane: got %v, want ErrNoEgressFork", err)
	}
}

func TestEgressDialerAdmitsOnlyNamedInternalPrefixes(t *testing.T) {
	d := newEgressDialer(parsePrefixes([]string{"fdc8::/16", "10.8.0.0/16"}))
	check := func(addr string) error {
		return d.Control("tcp", addr, nil)
	}
	for name, tc := range map[string]struct {
		addr    string
		allowed bool
	}{
		"public v4":    {"93.184.216.34:443", true},
		"public v6":    {"[2606:2800:220:1:248:1893:25c8:1946]:443", true},
		"corporate v6": {"[fdc8:17:9:200f::1]:443", true},
		"corporate v4": {"10.8.7.149:443", true},

		"another sandbox on the bridge": {"[fd00:c0c0:38::5]:443", false},
		"the host's bridge gateway":     {"[fd00:c0c0:38::1]:443", false},
		"other private v4":              {"192.168.1.1:443", false},
		"loopback":                      {"127.0.0.1:443", false},
		"cloud metadata":                {"169.254.169.254:80", false},
		"CGNAT metadata":                {"100.100.100.200:80", false},
	} {
		t.Run(name, func(t *testing.T) {
			err := check(tc.addr)
			if tc.allowed && err != nil {
				t.Errorf("%s was blocked: %v", tc.addr, err)
			}
			if !tc.allowed && err == nil {
				t.Errorf("%s was allowed; it must not be reachable from a guest", tc.addr)
			}
		})
	}
}

func TestEgressDialerWildcardAllowsEverything(t *testing.T) {
	d := newEgressDialer(parsePrefixes([]string{"0.0.0.0/0", "::/0"}))
	for _, addr := range []string{
		"93.184.216.34:443",
		"[fdc8:17:9:200f::1]:443",
		"10.8.7.149:443",
		"[fd00:c0c0:38::5]:443",
		"127.0.0.1:7777",
		"169.254.169.254:80",
	} {
		if err := d.Control("tcp", addr, nil); err != nil {
			t.Errorf("%s blocked under a wildcard allow-list: %v", addr, err)
		}
	}
}

func TestWarmClaimServesTheDoorsRefillBound(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = sockRoot(t)
	pol := &egress.Policy{Socks5: true, Allow: []egress.Rule{{Host: "example.com"}}}
	m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: pol})
	warm := refillWarmVM(t, m)
	m.mu.Lock()
	el := m.egressPrebound[warm.VMName]
	m.mu.Unlock()
	if el == nil || el.socks == nil || el.srv != nil {
		t.Fatalf("refill left prebound=%+v, want both doors bound and nothing served", el)
	}

	sb := mustClaim(t, m, testKey)
	m.mu.Lock()
	armed, left := m.egressListeners[sb.ID], len(m.egressPrebound)
	m.mu.Unlock()
	if armed != el || left != 0 {
		t.Fatalf("claim armed %p from prebound %p with %d left, want the refill-bound pair", armed, el, left)
	}
	dialDoors(t, sb)
	m.disarmEgress(sb.ID, true)
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
	m.mu.Lock()
	prebound := m.egressPrebound[warm.VMName]
	m.mu.Unlock()
	early, err := net.Dial("unix", engine.SocksSocketPath(warm.VsockSocket))
	if err != nil {
		t.Fatalf("pre-claim dial: %v", err)
	}
	defer early.Close()

	sb := mustClaim(t, m, testKey)
	m.mu.Lock()
	armed := m.egressListeners[sb.ID]
	m.mu.Unlock()
	if armed == prebound {
		t.Fatal("claim served the pair a guest had already reached")
	}
	_ = early.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := early.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("a connection queued before the claim was kept: read err=%v, want closed", err)
	}
	dialDoors(t, sb)
	m.disarmEgress(sb.ID, true)
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

	if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: testKey}}); err != nil {
		t.Fatalf("SetPools: %v", err)
	}
	waitFor(t, func() bool { return eng.removed(warm.VMName) })
	m.mu.Lock()
	left := len(m.egressPrebound)
	m.mu.Unlock()
	if left != 0 {
		t.Errorf("%d prebound doors survive their VM", left)
	}
	if _, err := os.Stat(path); err == nil {
		t.Error("door socket survives its VM")
	}
}

func TestEgressDoorHalfCloseReachesTheGuest(t *testing.T) {
	origin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("origin: %v", err)
	}
	t.Cleanup(func() { _ = origin.Close() })
	go func() {
		conn, acceptErr := origin.Accept()
		if acceptErr != nil {
			return
		}
		_, _ = io.WriteString(conn, "hello")
		_ = conn.Close()
	}()
	pol := &egress.Policy{Allow: []egress.Rule{{Host: "127.0.0.1"}}}
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: pol})
	m.dial = (&net.Dialer{}).DialContext
	sb := vsockSandbox(t, "sb_halfclose")
	if err := m.armEgress(t.Context(), sb); err != nil {
		t.Fatalf("arm egress: %v", err)
	}
	t.Cleanup(func() { m.disarmEgress(sb.ID, true) })

	door, dialErr := net.Dial("unix", engine.EgressSocketPath(sb.VsockSocket))
	if dialErr != nil {
		t.Fatalf("dial door: %v", dialErr)
	}
	t.Cleanup(func() { _ = door.Close() })
	_ = door.SetDeadline(time.Now().Add(5 * time.Second))
	target := origin.Addr().String()
	if _, writeErr := fmt.Fprintf(door, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); writeErr != nil {
		t.Fatalf("connect: %v", writeErr)
	}
	resp, readErr := http.ReadResponse(bufio.NewReader(door), nil)
	if readErr != nil {
		t.Fatalf("read connect reply: %v", readErr)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("connect status %d, want 200", resp.StatusCode)
	}
	body, bodyErr := io.ReadAll(resp.Body)
	if bodyErr != nil {
		t.Fatalf("tunnel did not end with EOF after the origin closed: %v", bodyErr)
	}
	if string(body) != "hello" {
		t.Errorf("tunnel body %q, want hello", body)
	}
}

func TestEgressDoorCapsConcurrentConnections(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(origin.Close)
	pol := &egress.Policy{Allow: []egress.Rule{{Host: mustHostname(t, origin.URL)}}}
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: pol})
	m.dial = (&net.Dialer{}).DialContext
	sb := vsockSandbox(t, "sb_cap")
	if err := m.armEgress(t.Context(), sb); err != nil {
		t.Fatalf("arm egress: %v", err)
	}
	path := engine.EgressSocketPath(sb.VsockSocket)

	idle := make([]net.Conn, 0, egressDoorConns)
	for len(idle) < egressDoorConns {
		conn, err := net.Dial("unix", path)
		if errors.Is(err, syscall.ECONNREFUSED) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("idle dial %d: %v", len(idle), err)
		}
		idle = append(idle, conn)
	}
	t.Cleanup(func() {
		for _, conn := range idle {
			_ = conn.Close()
		}
	})

	client := egressClient(path)
	client.Timeout = 500 * time.Millisecond
	if resp, err := client.Get(origin.URL + "/"); err == nil {
		resp.Body.Close()
		t.Fatal("a request past the door cap was served")
	}

	_ = idle[0].Close()
	client.Timeout = 5 * time.Second
	resp, err := client.Get(origin.URL + "/")
	if err != nil {
		t.Fatalf("request after a slot freed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d after a slot freed, want 200", resp.StatusCode)
	}
	m.disarmEgress(sb.ID, true)
}

func dialDoors(t *testing.T, sb *types.Sandbox) {
	t.Helper()
	for _, path := range []string{engine.EgressSocketPath(sb.VsockSocket), engine.SocksSocketPath(sb.VsockSocket)} {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("door %s: %v", path, err)
		}
		_ = conn.Close()
	}
}

func refillWarmVM(t *testing.T, m *Manager) *types.Sandbox {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(m.goldensDir(), testKey.Hash()), 0o750); err != nil {
		t.Fatalf("setup golden: %v", err)
	}
	m.mu.Lock()
	m.adoptGolden(m.pools[testKey])
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
	t.Setenv("GH_TOKEN", "s3cr3t")
	secrets := testSecrets(t, egress.SecretSpec{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"})
	cfg := &config.Config{DataDir: t.TempDir(), Bridges: []string{"sbxbr0"}, EgressCA: writeTestEgressCA(t), Pools: pools}
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
