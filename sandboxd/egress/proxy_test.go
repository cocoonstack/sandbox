package egress

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestForwardAllowInjectsSecretAndOverwritesGuestHeader(t *testing.T) {
	var gotAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, "hello")
	}))
	defer upstream.Close()

	policy := Policy{Allow: []Rule{{Host: "api.internal", Secret: "gh"}}}
	secrets := fakeSecrets{"gh": {"Authorization", "Bearer SECRET"}}
	events := make(chan Event, 4)
	p := New("sb_1", "acme", policy, secrets, nil, fixedDial(upstream.Listener.Addr().String()), func(ev Event) { events <- ev })
	front := httptest.NewServer(p)
	defer front.Close()

	client := proxyClient(t, front.URL)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://api.internal/x", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer GUEST")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("proxied GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK || string(body) != "hello" {
		t.Fatalf("got %d %q, want 200 hello", resp.StatusCode, body)
	}
	if gotAuth != "Bearer SECRET" {
		t.Errorf("upstream saw Authorization %q, want the injected secret (guest value overwritten)", gotAuth)
	}
	ev := recvEvent(t, events)
	if ev.Decision != DecisionAllow || ev.Injected != "gh" || ev.Sandbox != "sb_1" || ev.Tenant != "acme" {
		t.Errorf("audit event = %+v, want allow/gh/sb_1/acme", ev)
	}
}

func TestForwardNeverInjectsInterceptSecret(t *testing.T) {
	reached := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
	}))
	defer upstream.Close()

	policy := Policy{Allow: []Rule{{Host: "api.internal", Secret: "gh", Intercept: true}}}
	secrets := fakeSecrets{"gh": {"Authorization", "Bearer SECRET"}}
	p := New("sb_1", "acme", policy, secrets, nil, fixedDial(upstream.Listener.Addr().String()), nil)
	front := httptest.NewServer(p)
	defer front.Close()

	resp, err := proxyClient(t, front.URL).Get("http://api.internal/x")
	if err != nil {
		t.Fatalf("proxied GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("plaintext forward matching only an intercept rule got %d, want 403", resp.StatusCode)
	}
	if reached {
		t.Error("request reached upstream; the intercept rule's secret was exposed to a cleartext scheme")
	}
}

// TestForwardStripsHopHeaders: fixed and Connection-named hop headers die at
// this hop, in both directions.
func TestForwardStripsHopHeaders(t *testing.T) {
	var gotHop, gotConn string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHop = r.Header.Get("X-Guest-Hop")
		gotConn = r.Header.Get("Connection")
		w.Header().Set("Connection", "X-Origin-Hop")
		w.Header().Set("X-Origin-Hop", "leak")
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	policy := Policy{Allow: []Rule{{Host: "api.internal"}}}
	p := New("sb_1", "", policy, nil, nil, fixedDial(upstream.Listener.Addr().String()), nil)
	front := httptest.NewServer(p)
	defer front.Close()

	client := proxyClient(t, front.URL)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://api.internal/x", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Connection", "X-Guest-Hop")
	req.Header.Set("X-Guest-Hop", "leak")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("proxied GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if gotHop != "" || gotConn != "" {
		t.Errorf("upstream saw hop headers: X-Guest-Hop=%q Connection=%q", gotHop, gotConn)
	}
	if got := resp.Header.Get("X-Origin-Hop"); got != "" {
		t.Errorf("client saw origin hop header: %q", got)
	}
}

func TestForwardDeniedIsTyped(t *testing.T) {
	policy := Policy{Allow: []Rule{{Host: "api.internal"}}}
	events := make(chan Event, 4)
	p := New("sb_1", "acme", policy, nil, nil, fixedDial("127.0.0.1:1"), func(ev Event) { events <- ev })
	front := httptest.NewServer(p)
	defer front.Close()

	resp, err := proxyClient(t, front.URL).Get("http://blocked.internal/")
	if err != nil {
		t.Fatalf("proxied GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("denied request got %d, want 403", resp.StatusCode)
	}
	if ev := recvEvent(t, events); ev.Decision != DecisionDeny || ev.Host != "blocked.internal" {
		t.Errorf("audit event = %+v, want deny/blocked.internal", ev)
	}
}

func TestConnectAllowTunnels(t *testing.T) {
	echo := echoServer(t)
	policy := Policy{Allow: []Rule{{Host: "echo.internal"}}}
	events := make(chan Event, 4)
	p := New("sb_1", "acme", policy, nil, nil, fixedDial(echo), func(ev Event) { events <- ev })
	front := httptest.NewServer(p)
	defer front.Close()

	conn := dialConnect(t, front.Listener.Addr().String(), "echo.internal:443")
	defer func() { _ = conn.Close() }()
	br := bufio.NewReader(conn)
	if status := readStatus(t, br); status != "HTTP/1.1 200 Connection Established" {
		t.Fatalf("CONNECT status = %q, want 200 Connection Established", status)
	}
	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatalf("write through tunnel: %v", err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(br, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(got) != "ping" {
		t.Errorf("tunnel echoed %q, want ping", got)
	}
	if ev := recvEvent(t, events); ev.Decision != DecisionAllow {
		t.Errorf("audit event = %+v, want allow", ev)
	}
}

func TestCloseEndsSplicedTunnel(t *testing.T) {
	echo := echoServer(t)
	p := New("sb_1", "", Policy{Allow: []Rule{{Host: "echo.internal"}}}, nil, nil, fixedDial(echo), nil)
	front := httptest.NewServer(p)
	defer front.Close()

	conn := dialConnect(t, front.Listener.Addr().String(), "echo.internal:443")
	defer func() { _ = conn.Close() }()
	br := bufio.NewReader(conn)
	if status := readStatus(t, br); status != "HTTP/1.1 200 Connection Established" {
		t.Fatalf("CONNECT status = %q, want 200 Connection Established", status)
	}
	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatalf("write through tunnel: %v", err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(br, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}

	p.Close()

	_, _ = io.WriteString(conn, "ping")
	if _, err := io.ReadFull(br, got); err == nil {
		t.Error("established spliced tunnel survived proxy Close")
	}
}

func TestConnectDeniedIsTyped(t *testing.T) {
	policy := Policy{Allow: []Rule{{Host: "echo.internal"}}}
	p := New("sb_1", "acme", policy, nil, nil, fixedDial("127.0.0.1:1"), nil)
	front := httptest.NewServer(p)
	defer front.Close()

	conn := dialConnect(t, front.Listener.Addr().String(), "blocked.internal:443")
	defer func() { _ = conn.Close() }()
	if status := readStatus(t, bufio.NewReader(conn)); status != "HTTP/1.1 403 Forbidden" {
		t.Fatalf("denied CONNECT status = %q, want 403 Forbidden", status)
	}
}

type fakeSecrets map[string][2]string

func (f fakeSecrets) Header(name string) (string, string, bool) {
	hv, ok := f[name]
	return hv[0], hv[1], ok
}

// fixedDial ignores the requested address and connects to target, so a test
// policy can name any host while the bytes land on a local backend.
func fixedDial(target string) DialFunc {
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, target)
	}
}

func proxyClient(t *testing.T, proxyURL string) *http.Client {
	t.Helper()
	u, err := url.Parse(proxyURL)
	if err != nil {
		t.Fatalf("parse proxy url: %v", err)
	}
	return &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(u)}}
}

func dialConnect(t *testing.T, proxyAddr, target string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	if _, err := io.WriteString(conn, "CONNECT "+target+" HTTP/1.1\r\nHost: "+target+"\r\n\r\n"); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	return conn
}

func readStatus(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read status line: %v", err)
	}
	for {
		next, err := br.ReadString('\n')
		if err != nil || next == "\r\n" || next == "\n" {
			break
		}
	}
	if len(line) >= 2 {
		line = line[:len(line)-2]
	}
	return line
}

// echoServer starts a one-shot TCP echo backend and returns its address.
func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen echo: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = io.Copy(conn, conn)
	}()
	return ln.Addr().String()
}

func recvEvent(t *testing.T, events <-chan Event) Event {
	t.Helper()
	select {
	case ev := <-events:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatalf("no audit event recorded")
		return Event{}
	}
}
