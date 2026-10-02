package egress

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestParseUpstream(t *testing.T) {
	for _, raw := range []string{"http://res.example.com:8080", "socks5://u:p@10.0.0.1:1080", "http://u:p%40x@h:3128/", "http://h:1", "socks5://h:65535"} {
		if _, err := ParseUpstream(raw); err != nil {
			t.Errorf("ParseUpstream(%q): %v", raw, err)
		}
	}
	for _, raw := range []string{"ftp://h:21", "http://u:secret@h", "http://:80", "http://h:80/path", "http://h:80?x=1", "http://h:80#f", "%zz", "h:80", "http://h:0", "http://h:70000", "socks5://h:65536"} {
		_, err := ParseUpstream(raw)
		if err == nil {
			t.Errorf("ParseUpstream(%q) accepted", raw)
			continue
		}
		if strings.Contains(err.Error(), "secret") {
			t.Errorf("ParseUpstream(%q) leaked the password: %v", raw, err)
		}
	}
}

func TestUpstreamAllow(t *testing.T) {
	allow, err := ParseUpstreamAllow([]string{"Res.example.com", "10.0.0.0/8", "192.0.2.7"})
	if err != nil {
		t.Fatalf("ParseUpstreamAllow: %v", err)
	}
	for raw, want := range map[string]bool{
		"http://res.EXAMPLE.com:1":   true,
		"http://other.example.com:1": false,
		"socks5://10.1.2.3:1080":     true,
		"http://192.0.2.7:1":         true,
		"http://192.0.2.8:1":         false,
		"http://[::ffff:10.0.0.1]:1": true,
	} {
		u, err := ParseUpstream(raw)
		if err != nil {
			t.Fatalf("ParseUpstream(%q): %v", raw, err)
		}
		if got := allow.Allows(u); got != want {
			t.Errorf("Allows(%q) = %v, want %v", raw, got, want)
		}
	}
	if _, err := ParseUpstreamAllow([]string{"res example"}); err == nil {
		t.Error("a malformed allow entry was accepted")
	}
}

func TestDialUpstreamThroughHTTPConnect(t *testing.T) {
	const greeting = "SSH-2.0-test\r\n"
	addr, seen := fakeConnectProxy(t, http.StatusOK, greeting)
	u, _ := ParseUpstream("http://alice:pw@" + addr)
	conn, err := DialUpstream(t.Context(), u, "203.0.113.9:443", plainDial)
	if err != nil {
		t.Fatalf("DialUpstream: %v", err)
	}
	defer func() { _ = conn.Close() }()
	buf := make([]byte, len(greeting))
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != greeting {
		t.Fatalf("greeting %q, %v", buf, err)
	}
	assertEcho(t, conn)
	if err := conn.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatalf("half-close: %v", err)
	}
	if tail, err := io.ReadAll(conn); err != nil || len(tail) != 0 {
		t.Fatalf("after half-close: %q, %v", tail, err)
	}
	got := <-seen
	want := "CONNECT 203.0.113.9:443 Basic " + base64.StdEncoding.EncodeToString([]byte("alice:pw"))
	if got != want {
		t.Errorf("upstream saw %q, want %q", got, want)
	}
}

func TestUpstreamTunnelKeepsTheZeroCopyPathAndBufferedBytes(t *testing.T) {
	const greeting = "220 ready\r\n"
	addr, _ := fakeConnectProxy(t, http.StatusOK, greeting)
	u, _ := ParseUpstream("http://" + addr)
	conn, err := DialUpstream(t.Context(), u, "203.0.113.9:25", plainDial)
	if err != nil {
		t.Fatalf("DialUpstream: %v", err)
	}
	defer func() { _ = conn.Close() }()
	rf, okFrom := conn.(io.ReaderFrom)
	wt, okTo := conn.(io.WriterTo)
	if !okFrom || !okTo {
		t.Fatalf("tunnel %T hides the TCP conn's ReadFrom/WriteTo, so splice cannot run", conn)
	}
	if _, err := rf.ReadFrom(strings.NewReader("ping")); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if err := conn.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatalf("half-close: %v", err)
	}
	var got strings.Builder
	if _, err := wt.WriteTo(&got); err != nil || got.String() != greeting+"ping" {
		t.Fatalf("WriteTo %q, %v; want the buffered greeting before the echo", got.String(), err)
	}
}

func TestDialUpstreamRefusedNamesNoCredentials(t *testing.T) {
	addr, _ := fakeConnectProxy(t, http.StatusProxyAuthRequired, "")
	u, _ := ParseUpstream("http://alice:pw-secret@" + addr)
	_, err := DialUpstream(t.Context(), u, "203.0.113.9:443", plainDial)
	if err == nil || !strings.Contains(err.Error(), "407") || strings.Contains(err.Error(), "pw-secret") {
		t.Fatalf("refused tunnel: %v, want a 407 error without the password", err)
	}
}

func TestDialUpstreamThroughSOCKS5WithAuth(t *testing.T) {
	addr, seen := fakeSOCKS5(t)
	u, _ := ParseUpstream("socks5://bob:hunter2@" + addr)
	conn, err := DialUpstream(t.Context(), u, "203.0.113.9:443", plainDial)
	if err != nil {
		t.Fatalf("DialUpstream: %v", err)
	}
	defer func() { _ = conn.Close() }()
	assertEcho(t, conn)
	if got, want := <-seen, "bob:hunter2 203.0.113.9:443"; got != want {
		t.Errorf("socks5 upstream saw %q, want %q", got, want)
	}
	if _, ok := conn.(interface{ CloseWrite() error }); !ok {
		t.Errorf("tunnel %T lost CloseWrite, so splice cannot half-close it", conn)
	}
}

func TestDialUpstreamTimesOutAndCloses(t *testing.T) {
	for _, stage := range []string{"dial", "http", "socks5"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				u, _ := ParseUpstream("http://proxy.example:80")
				if stage == "socks5" {
					u.Scheme = stage
				}
				closed := make(chan struct{})
				dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
					if stage == "dial" {
						<-ctx.Done()
						close(closed)
						return nil, ctx.Err()
					}
					client, server := net.Pipe()
					go func() {
						_, _ = io.Copy(io.Discard, server)
						_ = server.Close()
						close(closed)
					}()
					return client, nil
				}
				start := time.Now()
				if _, err := DialUpstream(t.Context(), u, "203.0.113.9:443", dial); err == nil {
					t.Fatal("silent upstream did not time out")
				}
				if elapsed := time.Since(start); elapsed != upstreamTimeout {
					t.Errorf("timeout after %v, want %v", elapsed, upstreamTimeout)
				}
				<-closed
			})
		})
	}
}

func TestProxyDialsEachConnectionOnTheCurrentRoute(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("direct=%t", direct), func(t *testing.T) {
			origin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, "ok")
			}))
			t.Cleanup(origin.Close)
			router := &switchRouter{route: "http://user:pw@up1.example:3128", target: origin.Listener.Addr().String(), direct: direct}
			events := make(chan Event, 8)
			p := New(Policy{Allow: []Rule{{Host: "*"}}}, nil, nil, router, func(ev Event) { events <- ev }, nil)
			t.Cleanup(p.Close)
			p.pools.Load().tr.TLSClientConfig = &tls.Config{RootCAs: trustUpstream(origin), MinVersion: tls.VersionTLS12}
			p.OnTransfer(func(ev Event, _, _ int64) { events <- ev })
			get := func(want string) {
				t.Helper()
				w := httptest.NewRecorder()
				p.ServeHTTP(w, httptest.NewRequest(http.MethodGet, origin.URL, nil))
				if w.Code != http.StatusOK || w.Body.String() != "ok" {
					t.Fatalf("get: %d %q", w.Code, w.Body.String())
				}
				if direct {
					want = ""
				}
				for range 2 {
					if ev := recvEvent(t, events); ev.Upstream != want {
						t.Errorf("event upstream %q, want %q", ev.Upstream, want)
					}
				}
			}
			get("up1.example:3128")
			get("up1.example:3128")
			router.set("socks5://up2.example:1080")
			get("up2.example:1080")
			if got := router.dialed(); strings.Join(got, ",") != "http://user:pw@up1.example:3128,socks5://up2.example:1080" {
				t.Errorf("dials %v, want one per route: a route change must not reuse the old path's conn", got)
			}
		})
	}
}

func TestTransferCountsTunnelAndRequestBytes(t *testing.T) {
	echo := echoServer(t)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/fail" {
			conn, _, err := http.NewResponseController(w).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		_, _ = io.WriteString(w, "seven!!")
	}))
	t.Cleanup(origin.Close)
	type transfer struct{ sent, received int64 }
	got := make(chan transfer, 2)
	router := DialFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
		var d net.Dialer
		if strings.HasSuffix(addr, ":7") {
			return d.DialContext(ctx, network, echo)
		}
		return d.DialContext(ctx, network, origin.Listener.Addr().String())
	})
	p := New(Policy{Allow: []Rule{{Host: "*"}}}, nil, nil, router, nil, nil)
	t.Cleanup(p.Close)
	p.OnTransfer(func(_ Event, sent, received int64) { got <- transfer{sent, received} })
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)

	conn := dialConnect(t, srv.Listener.Addr().String(), "tunnel.example:7")
	br := bufio.NewReader(conn)
	if status := readStatus(t, br); !strings.Contains(status, "200") {
		t.Fatalf("CONNECT: %s", status)
	}
	_, _ = io.WriteString(conn, "hello")
	buf := make([]byte, 5)
	if _, err := io.ReadFull(br, buf); err != nil {
		t.Fatalf("echo: %v", err)
	}
	_ = conn.(*net.TCPConn).CloseWrite()
	_, _ = io.Copy(io.Discard, br)
	_ = conn.Close()
	if tr := <-got; tr != (transfer{5, 5}) {
		t.Errorf("tunnel transfer %+v, want 5 sent and 5 received", tr)
	}

	resp, err := proxyClient(t, srv.URL).Post(origin.URL, "text/plain", strings.NewReader("eleven char"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if tr := <-got; tr != (transfer{11, 7}) {
		t.Errorf("request transfer %+v, want 11 sent and 7 received", tr)
	}
	resp, err = proxyClient(t, srv.URL).Post(origin.URL+"/fail", "text/plain", strings.NewReader("eleven char"))
	if err != nil {
		t.Fatalf("failed post: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("failed post: %d, want 502", resp.StatusCode)
	}
	if tr := <-got; tr != (transfer{11, 0}) {
		t.Errorf("failed request transfer %+v, want 11 sent and 0 received", tr)
	}
}

func TestADeadUpstreamIsNamedInTheAudit(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dead := ln.Addr().String()
	_ = ln.Close()
	events := make(chan Event, 4)
	p := New(Policy{Allow: []Rule{{Host: "*"}}}, nil, nil, upstreamRouter{route: "http://u:pw@" + dead}, func(ev Event) { events <- ev }, nil)
	t.Cleanup(p.Close)
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)

	conn := dialConnect(t, srv.Listener.Addr().String(), "203.0.113.9:443")
	if status := readStatus(t, bufio.NewReader(conn)); !strings.Contains(status, "502") {
		t.Errorf("CONNECT through a dead upstream: %s, want 502", status)
	}
	_ = conn.Close()
	if ev := recvEvent(t, events); ev.Upstream != dead {
		t.Errorf("CONNECT audit names upstream %q, want %q", ev.Upstream, dead)
	}
	resp, err := proxyClient(t, srv.URL).Get("http://203.0.113.9/")
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("forward through a dead upstream: %d, want 502", resp.StatusCode)
	}
	if ev := recvEvent(t, events); ev.Upstream != dead {
		t.Errorf("forward audit names upstream %q, want %q", ev.Upstream, dead)
	}
}

type upstreamRouter struct{ route string }

func (r upstreamRouter) Route() string { return r.route }

func (r upstreamRouter) Dial(ctx context.Context, route, _, addr string) (net.Conn, error) {
	u, err := ParseUpstream(route)
	if err != nil {
		return nil, err
	}
	return DialUpstream(ctx, u, addr, plainDial)
}

type switchRouter struct {
	mu     sync.Mutex
	route  string
	target string
	direct bool
	dials  []string
}

func (r *switchRouter) Route() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.route
}

func (r *switchRouter) Dial(ctx context.Context, route, network, _ string) (net.Conn, error) {
	r.mu.Lock()
	r.dials = append(r.dials, route)
	r.mu.Unlock()
	var d net.Dialer
	conn, err := d.DialContext(ctx, network, r.target)
	if err != nil || r.direct || route == "" {
		return conn, err
	}
	u, _ := ParseUpstream(route)
	return &upstreamConn{Conn: conn, host: u.Host}, nil
}

func (r *switchRouter) set(route string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.route = route
}

func (r *switchRouter) dialed() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.dials)
}

func plainDial(ctx context.Context, network, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, addr)
}

func assertEcho(t *testing.T, conn net.Conn) {
	t.Helper()
	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("echo %q, %v", buf, err)
	}
}

// fakeConnectProxy answers one CONNECT with status and, on 200, echoes the tunnel.
func fakeConnectProxy(t *testing.T, status int, greeting string) (string, <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	seen := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		br := bufio.NewReader(conn)
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		seen <- req.Method + " " + req.Host + " " + req.Header.Get("Proxy-Authorization")
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 %d %s\r\n\r\n%s", status, http.StatusText(status), greeting)
		if status == http.StatusOK {
			_, _ = io.Copy(conn, br)
		}
	}()
	return ln.Addr().String(), seen
}

// fakeSOCKS5 accepts one user/password CONNECT and echoes the tunnel.
func fakeSOCKS5(t *testing.T) (string, <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	seen := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		br := bufio.NewReader(conn)
		read := func(n int) []byte {
			b := make([]byte, n)
			_, _ = io.ReadFull(br, b)
			return b
		}
		read(int(read(2)[1]))
		_, _ = conn.Write([]byte{5, 2})
		read(1)
		user := string(read(int(read(1)[0])))
		pass := string(read(int(read(1)[0])))
		_, _ = conn.Write([]byte{1, 0})
		hdr := read(4)
		var host string
		switch hdr[3] {
		case 1:
			host = net.IP(read(4)).String()
		case 3:
			host = string(read(int(read(1)[0])))
		}
		port := binary.BigEndian.Uint16(read(2))
		seen <- fmt.Sprintf("%s:%s %s", user, pass, net.JoinHostPort(host, fmt.Sprint(port)))
		_, _ = conn.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
		_, _ = io.Copy(conn, br)
	}()
	return ln.Addr().String(), seen
}
