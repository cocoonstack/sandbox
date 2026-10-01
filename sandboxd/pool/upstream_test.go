package pool

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestRouteTakesTheClaimThenTenantThenPoolUpstream(t *testing.T) {
	m := upstreamManager(t, "127.0.0.1")
	m.poolUpstream[testKey] = "http://127.0.0.1:3001"
	m.tenantUpstream["acme"] = "http://127.0.0.1:3002"
	for _, tc := range []struct {
		name, tenant string
		env          types.Env
		want         string
	}{
		{"pool default", "", nil, "http://127.0.0.1:3001"},
		{"tenant default", "acme", nil, "http://127.0.0.1:3002"},
		{"claim value", "acme", types.Env{"EGRESS_UPSTREAM": {Value: "socks5://127.0.0.1:3003", Guest: hostOnly}}, "socks5://127.0.0.1:3003"},
		{"claim direct", "acme", types.Env{"EGRESS_UPSTREAM": {Value: "direct", Guest: hostOnly}}, ""},
	} {
		sb := &types.Sandbox{ID: "sb_route", Key: testKey, Tenant: tc.tenant, Env: tc.env}
		if got := (claimRouter{m: m, sb: sb}).Route(); got != tc.want {
			t.Errorf("%s: route %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestClaimsOnOnePoolLeaveThroughTheirOwnUpstreams(t *testing.T) {
	m := upstreamManager(t, "127.0.0.1")
	m.destVerdict = func(netip.Addr, uint16) (bool, error) { return false, nil }
	direct := countDial(m)
	origin := echoListener(t)
	up1, up2 := tunnelProxy(t), tunnelProxy(t)
	claim := func(upstream string) *types.Sandbox {
		sb := &types.Sandbox{ID: "sb_" + upstream, Key: testKey}
		if upstream != "" {
			sb.Env = types.Env{"EGRESS_UPSTREAM": {Value: "http://user:pw@" + upstream, Guest: hostOnly}}
		}
		return sb
	}
	for _, sb := range []*types.Sandbox{claim(up1.addr), claim(up2.addr), claim("")} {
		r := claimRouter{m: m, sb: sb}
		conn, err := r.Dial(t.Context(), r.Route(), "tcp", origin)
		if err != nil {
			t.Fatalf("%s: dial: %v", sb.ID, err)
		}
		pingPong(t, conn)
	}
	if got := up1.targets(); len(got) != 1 || got[0] != origin {
		t.Errorf("upstream 1 tunnels %v, want only the first claim's %s", got, origin)
	}
	if got := up2.targets(); len(got) != 1 || got[0] != origin {
		t.Errorf("upstream 2 tunnels %v, want only the second claim's %s", got, origin)
	}
	if direct.Load() != 1 {
		t.Errorf("direct dials %d, want 1: only the claim without an upstream", direct.Load())
	}
}

func TestBlockedDestinationNeverReachesTheUpstream(t *testing.T) {
	m := upstreamManager(t, "127.0.0.1")
	up := tunnelProxy(t)
	r := claimRouter{m: m}
	_, err := r.Dial(t.Context(), "http://"+up.addr, "tcp", "169.254.169.254:80")
	if err == nil || !strings.Contains(err.Error(), "blocked internal address") {
		t.Fatalf("metadata dial: %v, want the SSRF block", err)
	}
	if n := up.accepted.Load(); n != 0 {
		t.Errorf("the upstream saw %d connections before the block", n)
	}
}

func TestInternallyAllowedDestinationDialsDirect(t *testing.T) {
	m := upstreamManager(t, "127.0.0.1")
	m.destVerdict = func(netip.Addr, uint16) (bool, error) { return true, nil }
	direct := countDial(m)
	up := tunnelProxy(t)
	origin := echoListener(t)
	conn, err := (claimRouter{m: m}).Dial(t.Context(), "http://"+up.addr, "tcp", origin)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	pingPong(t, conn)
	if direct.Load() != 1 || up.accepted.Load() != 0 {
		t.Errorf("direct %d, upstream %d: a re-admitted internal service must dial direct", direct.Load(), up.accepted.Load())
	}
}

func TestDeadOrUnlistedUpstreamNeverFallsBackToDirect(t *testing.T) {
	m := upstreamManager(t, "127.0.0.1")
	m.destVerdict = func(netip.Addr, uint16) (bool, error) { return false, nil }
	direct := countDial(m)
	dead := closedPort(t)
	origin := echoListener(t)
	for route, want := range map[string]string{
		"http://" + dead:              "dial upstream",
		"socks5://192.0.2.1:1080":     "not allowed",
		"http://upstream.example:443": "not allowed",
	} {
		if _, err := (claimRouter{m: m}).Dial(t.Context(), route, "tcp", origin); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", route, err, want)
		}
	}
	if direct.Load() != 0 {
		t.Errorf("direct dials %d after upstream failures, want none", direct.Load())
	}
}

func TestUpstreamEnvIsAdmittedHostOnlyAndAllowed(t *testing.T) {
	m := upstreamManager(t, "127.0.0.1")
	for name, tc := range map[string]struct {
		v  types.EnvVar
		ok bool
	}{
		"allowed url": {types.EnvVar{Value: "http://u:p@127.0.0.1:3128", Guest: hostOnly}, true},
		"direct":      {types.EnvVar{Value: "direct", Guest: hostOnly}, true},
		"guest entry": {types.EnvVar{Value: "http://127.0.0.1:3128"}, false},
		"unlisted":    {types.EnvVar{Value: "http://10.0.0.1:3128", Guest: hostOnly}, false},
		"malformed":   {types.EnvVar{Value: "ftp://127.0.0.1:21", Guest: hostOnly}, false},
	} {
		err := m.checkUpstreamEnv(types.Env{"EGRESS_UPSTREAM": tc.v})
		if tc.ok != (err == nil) || (err != nil && !errors.Is(err, ErrBadEnv)) {
			t.Errorf("%s: %v, want ok=%v", name, err, tc.ok)
		}
	}
	m.upstreamEnv = ""
	if err := m.checkUpstreamEnv(types.Env{"EGRESS_UPSTREAM": {Value: "anything"}}); err != nil {
		t.Errorf("with the feature off the name is an ordinary env entry: %v", err)
	}
}

func TestCheckpointClaimAdmitsTheUpstreamEnvLikeAnyClaim(t *testing.T) {
	m := upstreamManager(t, "127.0.0.1")
	src := mustClaim(t, m, testKey)
	ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "", "")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	for name, v := range map[string]types.EnvVar{
		"guest entry": {Value: "http://u:p@127.0.0.1:3128"},
		"unlisted":    {Value: "http://10.0.0.1:3128", Guest: hostOnly},
	} {
		o := ClaimOptions{TTL: time.Hour, Env: types.Env{"EGRESS_UPSTREAM": v}}
		if _, err := m.ClaimCheckpoint(t.Context(), ckpt.ID, o); !errors.Is(err, ErrBadEnv) {
			t.Errorf("%s: branch claim %v, want ErrBadEnv", name, err)
		}
	}
}

func TestPatchSwitchesTheUpstreamForNewConnections(t *testing.T) {
	m := upstreamManager(t, "127.0.0.1")
	sb := mustClaim(t, m, testKey)
	if err := m.PatchEnv(t.Context(), sb.ID, types.EnvPatch{"EGRESS_UPSTREAM": {Value: "http://127.0.0.1:3001", Guest: hostOnly}}, ""); err != nil {
		t.Fatalf("patch: %v", err)
	}
	r := claimRouter{m: m, sb: sb}
	if got := r.Route(); got != "http://127.0.0.1:3001" {
		t.Fatalf("route %q after the first patch", got)
	}
	if err := m.PatchEnv(t.Context(), sb.ID, types.EnvPatch{"EGRESS_UPSTREAM": nil}, ""); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if got := r.Route(); got != "" {
		t.Errorf("route %q after clearing, want direct", got)
	}
	if err := m.PatchEnv(t.Context(), sb.ID, types.EnvPatch{"EGRESS_UPSTREAM": {Value: "http://10.9.9.9:1", Guest: hostOnly}}, ""); !errors.Is(err, ErrBadEnv) {
		t.Errorf("patch to an unlisted upstream: %v, want ErrBadEnv", err)
	}
	if got := sb.Env.Redacted()["EGRESS_UPSTREAM"].Value; got != "" {
		t.Errorf("GET env would show %q", got)
	}
}

func upstreamManager(t *testing.T, allow ...string) *Manager {
	t.Helper()
	m := newTestManager(t, newFakeEngine())
	parsed, err := egress.ParseUpstreamAllow(allow)
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	m.upstreamEnv, m.upstreamAllow = "EGRESS_UPSTREAM", parsed
	return m
}

func countDial(m *Manager) *atomic.Int32 {
	var n atomic.Int32
	m.dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		n.Add(1)
		var d net.Dialer
		return d.DialContext(ctx, network, addr)
	}
	return &n
}

func echoListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(conn, conn); _ = conn.Close() }()
		}
	}()
	return ln.Addr().String()
}

func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func pingPong(t *testing.T, conn net.Conn) {
	t.Helper()
	defer func() { _ = conn.Close() }()
	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("echo %q, %v", buf, err)
	}
}

type tunnel struct {
	addr     string
	accepted atomic.Int32
	mu       sync.Mutex
	seen     []string
}

func tunnelProxy(t *testing.T) *tunnel {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	p := &tunnel{addr: ln.Addr().String()}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			p.accepted.Add(1)
			go p.serve(conn)
		}
	}()
	return p
}

func (p *tunnel) targets() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.seen)
}

func (p *tunnel) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	br := bufio.NewReader(conn)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}
	p.mu.Lock()
	p.seen = append(p.seen, req.Host)
	p.mu.Unlock()
	target, err := net.Dial("tcp", req.Host)
	if err != nil {
		_, _ = io.WriteString(conn, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
		return
	}
	defer func() { _ = target.Close() }()
	_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\n")
	go func() { _, _ = io.Copy(target, br) }()
	_, _ = io.Copy(conn, target)
}
