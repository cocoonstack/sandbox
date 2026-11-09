package outbound

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

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
	_, path := armDialingDoor(t, "sb_halfclose", &egress.Policy{Allow: []egress.Rule{{Host: "127.0.0.1"}}})
	_, tunnel := outboundtest.ConnectDoor(t, path, origin.Addr().String())
	body, bodyErr := io.ReadAll(tunnel)
	if bodyErr != nil {
		t.Fatalf("tunnel did not end with EOF after the origin closed: %v", bodyErr)
	}
	if string(body) != "hello" {
		t.Errorf("tunnel body %q, want hello", body)
	}
}

func TestDoorConnHandsSpliceItsUnixConn(t *testing.T) {
	ln, err := listenUnix(filepath.Join(outboundtest.SockRoot(t), "d"))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	door := limitDoor(ln)
	t.Cleanup(func() { _ = door.Close() })
	peer, err := net.Dial("unix", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	conn, err := door.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	inner, ok := conn.(interface{ NetConn() net.Conn })
	if !ok {
		t.Fatalf("door conn %T hides its socket from splice", conn)
	}
	if _, ok := inner.NetConn().(*net.UnixConn); !ok {
		t.Fatalf("NetConn is %T, want the *net.UnixConn the kernel copy needs", inner.NetConn())
	}
	_ = conn.Close()
	if n := len(door.slots); n != 0 {
		t.Errorf("%d slots held after close", n)
	}
}

func TestEgressDoorCapsConcurrentConnections(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(origin.Close)
	h, path := armDialingDoor(t, "sb_cap", &egress.Policy{Allow: []egress.Rule{{Host: outboundtest.Hostname(t, origin.URL)}}})

	idle := make([]net.Conn, 0, doorConns)
	deadline := time.Now().Add(10 * time.Second)
	for len(idle) < doorConns {
		conn, err := net.Dial("unix", path)
		if (errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EAGAIN)) && time.Now().Before(deadline) {
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

	waitHeld(t, h, "sb_cap", doorConns)

	client := outboundtest.EgressClient(path)
	client.Timeout = 500 * time.Millisecond
	resp, err := client.Get(origin.URL + "/")
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("a request past the door cap was served")
	}
	if netErr, ok := errors.AsType[net.Error](err); !ok || !netErr.Timeout() {
		t.Fatalf("request past the door cap: %v, want it to wait out the client timeout", err)
	}

	_ = idle[0].Close()
	client.Timeout = 5 * time.Second
	resp, err = client.Get(origin.URL + "/")
	if err != nil {
		t.Fatalf("request after a slot freed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d after a slot freed, want 200", resp.StatusCode)
	}
}

func TestReachedSeesAGuestInTheBacklog(t *testing.T) {
	path := filepath.Join(outboundtest.SockRoot(t), "d.sock")
	ln, err := listenUnix(path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	door := limitDoor(ln)
	if reached(door) {
		t.Fatal("a door no guest dialed reads as reached")
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if !reached(door) {
		t.Error("a door with a pending connection reads as unreached")
	}
	_ = conn.Close()
	if !reached(door) {
		t.Error("a door whose guest hung up reads as unreached")
	}
}

func TestARearmedClaimKeepsADoorItsGuestCanDial(t *testing.T) {
	pol := &egress.Policy{Socks5: true, Allow: []egress.Rule{{Host: "example.com"}}}
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, pol), nil)
	sb := outboundtest.VsockSandbox(t, "sb_rearm", testKey)
	t.Cleanup(func() { h.Disarm(sb.ID, true) })
	for range 2 {
		if err := h.ArmProxy(t.Context(), sb); err != nil {
			t.Fatalf("arm proxy: %v", err)
		}
	}
	for _, path := range []string{engine.EgressSocketPath(sb.VsockSocket), engine.SocksSocketPath(sb.VsockSocket)} {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("dial after the second arm: %v", err)
		}
		_ = conn.Close()
	}
}

func TestARearmWithNoPolicyLeftClosesTheDoor(t *testing.T) {
	var cur atomic.Pointer[View]
	cur.Store(NewView(poolConfig(testKey, &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}), nil))
	h := newHost(t, &fakeEngine{}, &config.Config{}, func(o *Options) { o.View = cur.Load })
	sb := outboundtest.VsockSandbox(t, "sb_rearm_none", testKey)
	if err := h.ArmProxy(t.Context(), sb); err != nil {
		t.Fatalf("arm proxy: %v", err)
	}
	cur.Store(NewView(&config.Config{}, nil))
	if err := h.ArmProxy(t.Context(), sb); err != nil {
		t.Fatalf("second arm: %v", err)
	}
	if route := h.Route(sb); route != types.NetRouteNone {
		t.Errorf("route %q, want none", route)
	}
	if _, err := net.Dial("unix", engine.EgressSocketPath(sb.VsockSocket)); err == nil {
		t.Error("the door still accepts with no policy left")
	}
}

func armDialingDoor(t *testing.T, id string, pol *egress.Policy) (*Host, string) {
	t.Helper()
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, pol), func(o *Options) { o.Dial = (&net.Dialer{}).DialContext })
	sb := outboundtest.VsockSandbox(t, id, testKey)
	if err := h.Arm(t.Context(), sb); err != nil {
		t.Fatalf("arm egress: %v", err)
	}
	t.Cleanup(func() { h.Disarm(sb.ID, true) })
	return h, engine.EgressSocketPath(sb.VsockSocket)
}

func waitHeld(t *testing.T, h *Host, id string, want int) {
	t.Helper()
	h.mu.Lock()
	door := h.doors[id].ln.(*doorListener)
	h.mu.Unlock()
	for range 500 {
		if len(door.slots) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("door holds %d connections, want %d", len(door.slots), want)
}
