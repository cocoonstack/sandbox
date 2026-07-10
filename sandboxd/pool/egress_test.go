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
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
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

	t.Setenv("GH_TOKEN", "s3cr3t")
	secrets := testSecrets(t, egress.SecretSpec{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"})
	pol := &egress.Policy{Allow: []egress.Rule{{Host: host, Secret: "gh"}}}
	m, err := NewManager(t.Context(), testEgressConfig(t, pol), newFakeEngine(), secrets)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

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
	t.Setenv("GH_TOKEN", "s3cr3t")
	secrets := testSecrets(t, egress.SecretSpec{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"})
	egKey := types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeSmall}
	pol := &egress.Policy{Allow: []egress.Rule{{Host: "example.com", Secret: "gh"}}}
	cfg := &config.Config{DataDir: t.TempDir(), Pools: []config.PoolSpec{{PoolKey: egKey, Egress: pol}}}
	m, err := NewManager(t.Context(), cfg, newFakeEngine(), secrets)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	sockDir, err := os.MkdirTemp("/tmp", "eg")
	if err != nil {
		t.Fatalf("sockdir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	// The engine reports no NIC tap, so the nft lock cannot apply; arming must
	// fail rather than hand out an egress-lane NIC that bypasses the proxy.
	sb := &types.Sandbox{ID: "sb_eg_fc", Key: egKey, VMName: "sbx-fc-1", VsockSocket: filepath.Join(sockDir, "v")}
	if armErr := m.armEgress(t.Context(), sb); armErr == nil {
		t.Fatal("armEgress must fail closed when the egress-lane NIC cannot be locked")
	}
}

func TestArmEgressLocksEgressLaneWithoutPolicy(t *testing.T) {
	// guardedEgress on (one pool carries a policy) plus a policyless egress-lane
	// pool: that lane must still lock the NIC (default-deny), so arming a claim
	// whose NIC cannot be locked fails rather than handing out a free NIC.
	t.Setenv("GH_TOKEN", "s3cr3t")
	secrets := testSecrets(t, egress.SecretSpec{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"})
	egKey := types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeSmall}
	cfg := &config.Config{DataDir: t.TempDir(), Pools: []config.PoolSpec{
		{PoolKey: testKey, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "a.test"}}}},
		{PoolKey: egKey}, // egress lane, no egress policy
	}}
	m, err := NewManager(t.Context(), cfg, newFakeEngine(), secrets)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	sockDir, err := os.MkdirTemp("/tmp", "eg")
	if err != nil {
		t.Fatalf("sockdir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sb := &types.Sandbox{ID: "sb_eg_np", Key: egKey, VMName: "sbx-np-1", VsockSocket: filepath.Join(sockDir, "v")}
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

func testEgressConfig(t *testing.T, pool *egress.Policy) *config.Config {
	t.Helper()
	return &config.Config{
		DataDir: t.TempDir(),
		Pools:   []config.PoolSpec{{PoolKey: testKey, Warm: 1, Egress: pool}},
	}
}

func mustHostname(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u.Hostname()
}
