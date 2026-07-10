package pool

import (
	"context"
	"io"
	"net"
	"net/http"
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
	origin := newEchoOrigin(t)
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
	m.armEgress(sb)
	path := engine.EgressSocketPath(sb.VsockSocket)
	client := egressClient(path)

	// Allowed host: reaches the origin, and the origin sees the injected header
	// the guest never supplied.
	resp, err := client.Get(origin.URL + "/")
	if err != nil {
		t.Fatalf("allowed request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "ok" || resp.Header.Get("X-Seen-Auth") != "s3cr3t" {
		t.Errorf("allowed request body=%q injected=%q, want ok/s3cr3t", body, resp.Header.Get("X-Seen-Auth"))
	}

	// Denied host: default-deny returns a typed 403 without reaching any origin.
	req, _ := http.NewRequest(http.MethodGet, "http://blocked.example/", nil)
	deny, err := client.Do(req)
	if err != nil {
		t.Fatalf("denied request: %v", err)
	}
	deny.Body.Close()
	if deny.StatusCode != http.StatusForbidden {
		t.Errorf("denied status %d, want 403", deny.StatusCode)
	}

	// Disarm removes the accept point.
	m.disarmEgress(sb.ID)
	if _, err := net.Dial("unix", path); err == nil {
		t.Error("egress socket still accepts after disarm")
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

func newEchoOrigin(t *testing.T) *echoOrigin {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("origin listen: %v", err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Seen-Auth", r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, "ok")
	})}
	go func() { _ = srv.Serve(ln) }()
	o := &echoOrigin{URL: "http://" + ln.Addr().String(), srv: srv}
	t.Cleanup(func() { _ = srv.Close() })
	return o
}

type echoOrigin struct {
	URL string
	srv *http.Server
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
