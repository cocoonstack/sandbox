package outbound

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
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
	pol := &egress.Policy{Allow: []egress.Rule{{Host: "127.0.0.1"}}}
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, pol), func(o *Options) { o.Dial = (&net.Dialer{}).DialContext })
	sb := vsockSandbox(t, "sb_halfclose")
	if err := h.Arm(t.Context(), sb); err != nil {
		t.Fatalf("arm egress: %v", err)
	}
	t.Cleanup(func() { h.Disarm(sb.ID, true) })

	tunnel := connectDoor(t, engine.EgressSocketPath(sb.VsockSocket), origin.Addr().String())
	body, bodyErr := io.ReadAll(tunnel)
	if bodyErr != nil {
		t.Fatalf("tunnel did not end with EOF after the origin closed: %v", bodyErr)
	}
	if string(body) != "hello" {
		t.Errorf("tunnel body %q, want hello", body)
	}
}

func TestDoorConnHandsSpliceItsUnixConn(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "door")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	ln, err := listenUnix(filepath.Join(dir, "d"))
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
	pol := &egress.Policy{Allow: []egress.Rule{{Host: mustHostname(t, origin.URL)}}}
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, pol), func(o *Options) { o.Dial = (&net.Dialer{}).DialContext })
	sb := vsockSandbox(t, "sb_cap")
	if err := h.Arm(t.Context(), sb); err != nil {
		t.Fatalf("arm egress: %v", err)
	}
	path := engine.EgressSocketPath(sb.VsockSocket)

	idle := make([]net.Conn, 0, doorConns)
	for len(idle) < doorConns {
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
	h.Disarm(sb.ID, true)
}

func connectDoor(t *testing.T, path, target string) io.ReadCloser {
	t.Helper()
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		t.Fatalf("dial the door: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
		t.Fatalf("send CONNECT: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read the CONNECT answer: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT answered %s", resp.Status)
	}
	return resp.Body
}
