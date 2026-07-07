package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const (
	execFrame = `{"v":1,"op":"exec","argv":["echo","42"],"detach":false}` + "\n"

	agentRequest = "GET /v1/sandboxes/sb_1/agent HTTP/1.1\r\n" +
		"Host: sandboxd\r\n" +
		"Authorization: Bearer tok\r\n" +
		"Upgrade: silkd\r\n" +
		"Connection: Upgrade\r\n\r\n"
)

func TestCloseRelaysRefusesLateRelays(t *testing.T) {
	ts, srv := newRelayServer(t, func(c net.Conn) {
		_, _ = io.Copy(io.Discard, c)
	})
	srv.CloseRelays()

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(conn, agentRequest); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if _, err := io.Copy(io.Discard, conn); err != nil {
		t.Errorf("late relay not refused promptly: %v", err)
	}
}

func TestRelayRoundTrip(t *testing.T) {
	frames := []string{
		`{"type":"started","pid":7}` + "\n",
		`{"type":"stdout","data":"NDIK"}` + "\n",
		`{"type":"exit","code":0}` + "\n",
	}
	ts, _ := newRelayServer(t, func(c net.Conn) {
		defer c.Close()
		r := bufio.NewReader(c)
		if _, err := r.ReadString('\n'); err != nil {
			return
		}
		for _, f := range frames {
			if _, err := io.WriteString(c, f); err != nil {
				return
			}
		}
	})

	conn, r := upgradeConn(t, ts)
	if _, err := io.WriteString(conn, execFrame); err != nil {
		t.Fatalf("write frame: %v", err)
	}
	for i, want := range frames {
		got, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if got != want {
			t.Errorf("frame %d: got %q, want %q", i, got, want)
		}
	}
	if _, err := r.ReadString('\n'); err != io.EOF {
		t.Errorf("got %v after terminal frame, want EOF", err)
	}
}

func TestRelayDeliversPipelinedBytes(t *testing.T) {
	// The client sends the request head and the first frame in one write,
	// before reading the 101 — the frame lands in the server's bufio reader
	// and must still reach the guest.
	ts, _ := newRelayServer(t, func(c net.Conn) {
		defer c.Close()
		r := bufio.NewReader(c)
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		_, _ = io.WriteString(c, line) // echo proves receipt
	})

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err = io.WriteString(conn, agentRequest+execFrame); err != nil {
		t.Fatalf("write: %v", err)
	}
	r := bufio.NewReader(conn)
	readStatus101(t, r)
	got, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if got != execFrame {
		t.Errorf("echo %q, want %q", got, execFrame)
	}
}

func TestRelayClientDisconnectClosesGuest(t *testing.T) {
	guestClosed := make(chan struct{})
	ts, _ := newRelayServer(t, func(c net.Conn) {
		defer close(guestClosed)
		_, _ = io.Copy(io.Discard, c) // block until the relay closes our side
	})

	conn, _ := upgradeConn(t, ts)
	_ = conn.Close()
	select {
	case <-guestClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("guest side not closed after client disconnect")
	}
}

func TestCloseRelaysDrains(t *testing.T) {
	ts, srv := newRelayServer(t, func(c net.Conn) {
		_, _ = io.Copy(io.Discard, c) // idle guest: keeps the relay open
	})

	_, r := upgradeConn(t, ts)
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		_, _ = io.Copy(io.Discard, r) // returns when the relay is torn down
	}()
	srv.CloseRelays()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("client conn still open after CloseRelays")
	}
}

// newRelayServer wires a Server whose dialer hands each relay one end of a
// net.Pipe and runs guest on the other end.
func newRelayServer(t *testing.T, guest func(net.Conn)) (*httptest.Server, *Server) {
	t.Helper()
	dialer := &fakeDialer{dial: func(context.Context, string) (net.Conn, error) {
		relayEnd, guestEnd := net.Pipe()
		go guest(guestEnd)
		return relayEnd, nil
	}}
	srv := New("", nil, "node:7777", &fakeManager{}, dialer, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		srv.CloseRelays()
		ts.Close()
	})
	return ts, srv
}

// upgradeConn performs the raw HTTP upgrade handshake; further reads must go
// through the returned reader — it may have buffered bytes past the 101.
func upgradeConn(t *testing.T, ts *httptest.Server) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(conn, agentRequest); err != nil {
		t.Fatalf("write request: %v", err)
	}
	r := bufio.NewReader(conn)
	readStatus101(t, r)
	return conn, r
}

// readStatus101 consumes exactly the 101 response head; a 1xx response has
// no body, so http.ReadResponse stops right where the guest bytes begin.
func readStatus101(t *testing.T, r *bufio.Reader) {
	t.Helper()
	resp, err := http.ReadResponse(r, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status %d, want 101", resp.StatusCode)
	}
}
