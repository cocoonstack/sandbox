package outbound

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var hostOnly = new(false)

func TestRouteTakesTheClaimThenClassThenPoolUpstream(t *testing.T) {
	t.Setenv("POOL_UPSTREAM", "http://127.0.0.1:3001")
	t.Setenv("CLASS_UPSTREAM", "http://127.0.0.1:3002")
	h := upstreamHost(t, &config.Config{
		Pools:         []config.PoolSpec{{PoolKey: testKey, EgressUpstreamEnv: "POOL_UPSTREAM"}},
		EgressClasses: []config.EgressClass{{Name: "desk", EgressUpstreamEnv: "CLASS_UPSTREAM"}},
	}, nil)
	for _, tc := range []struct {
		name, class string
		env         types.Env
		want        string
	}{
		{"pool default", "", nil, "http://127.0.0.1:3001"},
		{"class default", "desk", nil, "http://127.0.0.1:3002"},
		{"claim value", "desk", types.Env{"EGRESS_UPSTREAM": {Value: "socks5://127.0.0.1:3003", Guest: hostOnly}}, "socks5://127.0.0.1:3003"},
		{"claim direct", "desk", types.Env{"EGRESS_UPSTREAM": {Value: "direct", Guest: hostOnly}}, ""},
	} {
		sb := &types.Sandbox{ID: "sb_route", Key: testKey, Tenant: "acme", EgressClass: tc.class}
		sb.SetEnv(tc.env)
		if got := newClaim(h, sb, nil, nil).Route(); got != tc.want {
			t.Errorf("%s: route %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestClaimsOnOnePoolLeaveThroughTheirOwnUpstreams(t *testing.T) {
	var direct atomic.Int32
	h := upstreamHost(t, &config.Config{}, func(o *Options) {
		o.Verdict = func(netip.Addr, uint16) (bool, error) { return false, nil }
		o.Dial = countDial(&direct)
	})
	origin := echoListener(t)
	up1, up2 := tunnelProxy(t), tunnelProxy(t)
	claim := func(upstream string) *types.Sandbox {
		sb := &types.Sandbox{ID: "sb_" + upstream, Key: testKey}
		if upstream != "" {
			sb.SetEnv(types.Env{"EGRESS_UPSTREAM": {Value: "http://user:pw@" + upstream, Guest: hostOnly}})
		}
		return sb
	}
	for _, sb := range []*types.Sandbox{claim(up1.addr), claim(up2.addr), claim("")} {
		r := newClaim(h, sb, nil, nil)
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

func TestATenantClaimUnderAClassDialsItsOwnUpstreamLikeARootClaim(t *testing.T) {
	origin := echoListener(t)
	own, classDefault := tunnelProxy(t), tunnelProxy(t)
	t.Setenv("CLASS_UPSTREAM", "http://"+classDefault.addr)
	var direct atomic.Int32
	h := upstreamHost(t, &config.Config{EgressClasses: []config.EgressClass{{Name: "desk", EgressUpstreamEnv: "CLASS_UPSTREAM"}}}, func(o *Options) {
		o.Verdict = func(netip.Addr, uint16) (bool, error) { return false, nil }
		o.Dial = countDial(&direct)
	})
	tenantClaim := func(upstream string) *types.Sandbox {
		sb := &types.Sandbox{ID: "sb_desk", Key: testKey, Tenant: "acme", EgressClass: "desk"}
		sb.SetEnv(types.Env{"EGRESS_UPSTREAM": {Value: upstream, Guest: hostOnly}})
		return sb
	}
	r := newClaim(h, tenantClaim("http://user:pw@"+own.addr), nil, nil)
	conn, err := r.Dial(t.Context(), r.Route(), "tcp", origin)
	if err != nil {
		t.Fatalf("dial through the claim's own upstream: %v", err)
	}
	pingPong(t, conn)
	if got := own.targets(); len(got) != 1 || got[0] != origin || classDefault.accepted.Load() != 0 {
		t.Errorf("own upstream tunnels %v, class default saw %d: the claim value must win over the class default", got, classDefault.accepted.Load())
	}
	r = newClaim(h, tenantClaim("socks5://192.0.2.1:1080"), nil, nil)
	if _, err := r.Dial(t.Context(), r.Route(), "tcp", origin); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Errorf("a tenant claim's unlisted upstream: %v, want the allow-list refusal", err)
	}
	if direct.Load() != 0 {
		t.Errorf("direct dials %d, want none", direct.Load())
	}
}

func TestBlockedDestinationNeverReachesTheUpstream(t *testing.T) {
	h := upstreamHost(t, &config.Config{}, nil)
	up := tunnelProxy(t)
	r := newClaim(h, nil, nil, nil)
	_, err := r.Dial(t.Context(), "http://"+up.addr, "tcp", "169.254.169.254:80")
	if err == nil || !strings.Contains(err.Error(), "blocked internal address") {
		t.Fatalf("metadata dial: %v, want the SSRF block", err)
	}
	if n := up.accepted.Load(); n != 0 {
		t.Errorf("the upstream saw %d connections before the block", n)
	}
}

func TestInternallyAllowedDestinationDialsDirect(t *testing.T) {
	var direct atomic.Int32
	h := upstreamHost(t, &config.Config{}, func(o *Options) {
		o.Verdict = func(netip.Addr, uint16) (bool, error) { return true, nil }
		o.Dial = countDial(&direct)
	})
	up := tunnelProxy(t)
	origin := echoListener(t)
	conn, err := newClaim(h, nil, nil, nil).Dial(t.Context(), "http://"+up.addr, "tcp", origin)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	pingPong(t, conn)
	if direct.Load() != 1 || up.accepted.Load() != 0 {
		t.Errorf("direct %d, upstream %d: a re-admitted internal service must dial direct", direct.Load(), up.accepted.Load())
	}
}

func TestDeadOrUnlistedUpstreamNeverFallsBackToDirect(t *testing.T) {
	var direct atomic.Int32
	h := upstreamHost(t, &config.Config{}, func(o *Options) {
		o.Verdict = func(netip.Addr, uint16) (bool, error) { return false, nil }
		o.Dial = countDial(&direct)
	})
	dead := closedPort(t)
	origin := echoListener(t)
	for route, want := range map[string]string{
		"http://" + dead:              "dial upstream",
		"socks5://192.0.2.1:1080":     "not allowed",
		"http://upstream.example:443": "not allowed",
	} {
		if _, err := newClaim(h, nil, nil, nil).Dial(t.Context(), route, "tcp", origin); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", route, err, want)
		}
	}
	if direct.Load() != 0 {
		t.Errorf("direct dials %d after upstream failures, want none", direct.Load())
	}
}

func TestAnInjectShapeRidesIntoTheCredential(t *testing.T) {
	h := newHost(t, &fakeEngine{}, &config.Config{}, nil)
	for _, tt := range []struct {
		name   string
		host   string
		inject types.EnvInject
	}{
		{"header with a placeholder", "api.github.com", types.EnvInject{Header: "X-Api-Key", Placeholder: "@KEY@"}},
		{"query with body", "graph.example.com", types.EnvInject{Query: "access_token", Body: true, Placeholder: "@FB@"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			inject := tt.inject
			inject.Hosts = []string{tt.host}
			sb := &types.Sandbox{}
			sb.SetEnv(types.Env{"KEY": {Value: "k", Guest: hostOnly, Inject: &inject}})
			want := egress.Credential{Name: "KEY", Header: inject.Header, Query: inject.Query, Body: inject.Body, Value: "k", Placeholder: inject.Placeholder}
			if creds := newClaim(h, sb, nil, nil).Credentials(tt.host); len(creds) != 1 || creds[0] != want {
				t.Errorf("credentials %+v, want [%+v]", creds, want)
			}
		})
	}
}

func BenchmarkClaimSecretsHeader(b *testing.B) {
	b.Setenv("GH_TOKEN", "node-gh")
	store, err := egress.NewSecretStore([]egress.SecretSpec{{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"}})
	if err != nil {
		b.Fatalf("secrets: %v", err)
	}
	v := &View{secrets: store}
	h := &Host{o: Options{View: func() *View { return v }}}
	b.Run("node-store-only", func(b *testing.B) {
		for b.Loop() {
			_, _, _ = store.Header("gh")
		}
	})
	for _, arm := range []string{"no-claim-env", "claim-env"} {
		sb := &types.Sandbox{}
		if arm == "claim-env" {
			sb.SetEnv(types.Env{"GH_TOKEN": {Value: "Bearer claim", Guest: hostOnly}, "MODE": {Value: "on"}})
		}
		b.Run(arm, func(b *testing.B) {
			for b.Loop() {
				_, _, _ = newClaim(h, sb, nil, nil).Header("gh")
			}
		})
		b.Run(arm+"/credentials", func(b *testing.B) {
			for b.Loop() {
				_ = newClaim(h, sb, nil, nil).Credentials("api.example.com")
			}
		})
	}
	sb := &types.Sandbox{}
	sb.SetEnv(types.Env{"KEY": {Value: "k", Guest: hostOnly, Inject: &types.EnvInject{Hosts: []string{"api.example.com"}, Header: "X-Key"}}})
	b.Run("inject/credentials", func(b *testing.B) {
		for b.Loop() {
			_ = newClaim(h, sb, nil, nil).Credentials("api.example.com")
		}
	})
	many := types.Env{}
	for i := range 64 {
		many[fmt.Sprintf("KEY_%d", i)] = types.EnvVar{Value: "k", Guest: hostOnly, Inject: &types.EnvInject{Hosts: []string{fmt.Sprintf("api%d.example.com", i), "*.vendor.example"}, Header: "X-Key"}}
	}
	sb = &types.Sandbox{}
	sb.SetEnv(many)
	b.Run("64-injects/miss", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = newClaim(h, sb, nil, nil).Credentials("other.example.com")
		}
	})
}

func upstreamHost(t *testing.T, cfg *config.Config, edit func(*Options)) *Host {
	t.Helper()
	cfg.EgressUpstream = &config.EgressUpstreamConfig{ClaimEnv: "EGRESS_UPSTREAM", Allow: []string{"127.0.0.1"}}
	return newHost(t, &fakeEngine{}, cfg, edit)
}

func countDial(n *atomic.Int32) egress.DialFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		n.Add(1)
		var d net.Dialer
		return d.DialContext(ctx, network, addr)
	}
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
