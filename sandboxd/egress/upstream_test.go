package egress

import (
	"bufio"
	"context"
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
)

func TestParseUpstream(t *testing.T) {
	for _, raw := range []string{"http://res.example.com:8080", "socks5://u:p@10.0.0.1:1080", "http://u:p%40x@h:3128/"} {
		if _, err := ParseUpstream(raw); err != nil {
			t.Errorf("ParseUpstream(%q): %v", raw, err)
		}
	}
	for _, raw := range []string{"ftp://h:21", "http://u:secret@h", "http://:80", "http://h:80/path", "http://h:80?x=1", "http://h:80#f", "%zz", "h:80"} {
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
	addr, seen := fakeConnectProxy(t, http.StatusOK)
	u, _ := ParseUpstream("http://alice:pw@" + addr)
	conn, err := DialUpstream(t.Context(), u, "203.0.113.9:443", plainDial)
	if err != nil {
		t.Fatalf("DialUpstream: %v", err)
	}
	defer func() { _ = conn.Close() }()
	assertEcho(t, conn)
	got := <-seen
	want := "CONNECT 203.0.113.9:443 Basic " + base64.StdEncoding.EncodeToString([]byte("alice:pw"))
	if got != want {
		t.Errorf("upstream saw %q, want %q", got, want)
	}
}

func TestDialUpstreamRefusedNamesNoCredentials(t *testing.T) {
	addr, _ := fakeConnectProxy(t, http.StatusProxyAuthRequired)
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

func TestProxyDialsEachConnectionOnTheCurrentRoute(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(origin.Close)
	router := &switchRouter{route: "http://user:pw@up1.example:3128", target: origin.Listener.Addr().String()}
	events := make(chan Event, 8)
	p := New(Policy{Allow: []Rule{{Host: "*"}}}, nil, nil, router, func(ev Event) { events <- ev }, nil)
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	client := proxyClient(t, srv.URL)
	get := func() {
		t.Helper()
		resp, err := client.Get(origin.URL)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	get()
	get()
	if ev := recvEvent(t, events); ev.Upstream != "up1.example:3128" {
		t.Errorf("event upstream %q, want the host without credentials", ev.Upstream)
	}
	router.set("socks5://up2.example:1080")
	get()
	if got := router.dialed(); strings.Join(got, ",") != "http://user:pw@up1.example:3128,socks5://up2.example:1080" {
		t.Errorf("dials %v, want one per route: a route change must not reuse the old path's conn", got)
	}
}

func TestTransferCountsTunnelAndRequestBytes(t *testing.T) {
	echo := echoServer(t)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
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
}

type switchRouter struct {
	mu     sync.Mutex
	route  string
	target string
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
	return d.DialContext(ctx, network, r.target)
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
func fakeConnectProxy(t *testing.T, status int) (string, <-chan string) {
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
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 %d %s\r\n\r\n", status, http.StatusText(status))
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
