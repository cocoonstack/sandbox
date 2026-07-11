package pool

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
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

// egressClient builds an HTTP client that reaches the origin through the
// per-sandbox egress proxy UDS, exactly as the guest's proxy client would over
// vsock. Playing the VMM's role lets the host-side path run without a real VM.
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

func TestEgressProxyInjectsAndGates(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Seen-Auth", r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(origin.Close)
	host := mustHostname(t, origin.URL)

	pol := &egress.Policy{Allow: []egress.Rule{{Host: host, Secret: "gh"}}}
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: pol})

	// A short socket dir: the UDS path + "_2049" must fit the OS sun_path cap.
	sockDir, err := os.MkdirTemp("/tmp", "eg")
	if err != nil {
		t.Fatalf("sockdir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sb := &types.Sandbox{ID: "sb_egress", Key: testKey, VsockSocket: filepath.Join(sockDir, "v")}
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

	m.disarmEgress(sb.ID)
	if _, err := net.Dial("unix", path); err == nil {
		t.Error("egress socket still accepts after disarm")
	}
}

func TestArmEgressFailsClosedWhenNICUnlockable(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: egKey, Egress: egPolicy})
	// The engine reports no NIC tap, so the nft lock cannot apply; arming must
	// fail rather than hand out an egress-lane NIC that bypasses the proxy.
	sb := &types.Sandbox{ID: "sb_eg_fc", Key: egKey, VMName: "sbx-fc-1"}
	if armErr := m.armEgress(t.Context(), sb); armErr == nil {
		t.Fatal("armEgress must fail closed when the egress-lane NIC cannot be locked")
	}
}

func TestArmEgressLocksEgressLaneWithoutPolicy(t *testing.T) {
	// guardedEgress on (one pool carries a policy) plus a policyless egress-lane
	// pool: that lane must still lock the NIC (default-deny), so arming a claim
	// whose NIC cannot be locked fails rather than handing out a free NIC.
	m := egressManager(t, newFakeEngine(),
		config.PoolSpec{PoolKey: testKey, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "a.test"}}}},
		config.PoolSpec{PoolKey: egKey}, // egress lane, no egress policy
	)
	sb := &types.Sandbox{ID: "sb_eg_np", Key: egKey, VMName: "sbx-np-1"}
	if armErr := m.armEgress(t.Context(), sb); armErr == nil {
		t.Fatal("policyless egress-lane claim must still lock the NIC, not skip it")
	}
}

func TestEgressLaneDoesNotHibernate(t *testing.T) {
	// cocoon resumes a guest before its fresh tap can be re-locked, so an
	// egress-lane sandbox must refuse to hibernate rather than wake unlocked.
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
	// #25 forbids egress hibernation/archive; a claim that reached such a state
	// (corrupt or pre-#25 journal) must fail closed on wake, never resume with
	// an unlockable fresh tap. Drive wakeResolved directly, past hibernateLocked.
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
		name         string
		pool, tenant *egress.Policy
		allow, deny  string
		wantArmed    bool
	}{
		{"pool only", both, nil, "a.test", "z.test", true},
		{"tenant only", nil, tenantOnly, "c.test", "a.test", true},
		{"intersection", both, tenantOnly, "b.test", "a.test", true}, // a.test allowed by pool, denied by tenant
		{"neither", nil, nil, "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m.pools[testKey] = &pool{key: testKey, egressPolicy: tc.pool}
			m.tenantEgress = map[string]*egress.Policy{}
			if tc.tenant != nil {
				m.tenantEgress["acme"] = tc.tenant
			}
			sb := &types.Sandbox{Key: testKey, Tenant: "acme"}
			eval, ok := m.effectivePolicy(sb)
			if ok != tc.wantArmed {
				t.Fatalf("armed=%v, want %v", ok, tc.wantArmed)
			}
			if !ok {
				return
			}
			if _, d := eval.Eval(tc.allow, "GET"); d != egress.DecisionAllow {
				t.Errorf("%s should allow", tc.allow)
			}
			if _, d := eval.Eval(tc.deny, "GET"); d != egress.DecisionDeny {
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
		t.Errorf("lock failed outside nft: %v", lockErr) // nft itself may fail off-linux/unprivileged
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
	// A claim adopted from a pre-tap journal has no TAP; the lock must resolve
	// it through the engine instead of failing.
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
	// First member arms trivially (none lane), second fails (egress lane, no
	// such VM): the committed-then-rolled-back batch must leave no claim event
	// in the billing stream — a claim with no terminal release/reap would stay
	// open forever for the collector.
	sbs := []*types.Sandbox{
		{VMName: "sbx-ok", Key: testKey},
		{VMName: "sbx-eg", Key: egKey},
	}
	if err := m.finalizeBatch(t.Context(), sbs, time.Minute); err == nil {
		t.Fatal("finalizeBatch must fail when a batch member cannot arm")
	}
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

func egressManager(t *testing.T, eng *fakeEngine, pools ...config.PoolSpec) *Manager {
	t.Helper()
	t.Setenv("GH_TOKEN", "s3cr3t")
	secrets := testSecrets(t, egress.SecretSpec{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"})
	m, err := NewManager(t.Context(), &config.Config{DataDir: t.TempDir(), Pools: pools}, eng, secrets)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	return m
}

func mustHostname(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u.Hostname()
}
