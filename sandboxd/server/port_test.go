package server

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
)

const portRequest = "GET /v1/sandboxes/sb_1/ports/49983 HTTP/1.1\r\n" +
	"Host: sandboxd\r\n" +
	"Authorization: Bearer tok\r\n" +
	"Upgrade: tcp\r\n" +
	"Connection: Upgrade\r\n\r\n"

func TestPortRelayRoundTrip(t *testing.T) {
	ts, _ := newPortServer(t, func(c net.Conn) {
		defer c.Close()
		_, _ = io.Copy(c, c)
	})

	conn := dialPortConn(t, ts, portRequest)
	r := bufio.NewReader(conn)
	readStatus101(t, r)
	if _, err := io.WriteString(conn, "GET /health HTTP/1.1\r\n\r\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, len("GET /health HTTP/1.1\r\n\r\n"))
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(got) != "GET /health HTTP/1.1\r\n\r\n" {
		t.Errorf("echo %q", got)
	}
}

func TestPortRelayPassesRequestedPort(t *testing.T) {
	var got uint16
	mgr := &fakeManager{dialPort: func(_, _ string, port uint16) (net.Conn, error) {
		got = port
		guestEnd, relayEnd := net.Pipe()
		go func() { _, _ = io.Copy(io.Discard, guestEnd); _ = guestEnd.Close() }()
		return relayEnd, nil
	}}
	ts := newTestServer(t, "", mgr, nil)
	conn := dialPortConn(t, ts, portRequest)
	readStatus101(t, bufio.NewReader(conn))
	if got != 49983 {
		t.Errorf("dialed port %d, want 49983", got)
	}
}

func TestPortRelayRejectsNonUpgradeWithoutDialing(t *testing.T) {
	dialed := false
	mgr := &fakeManager{dialPort: func(string, string, uint16) (net.Conn, error) {
		dialed = true
		return nil, pool.ErrUnknownSandbox
	}}
	ts := newTestServer(t, "", mgr, nil)
	resp := getPort(t, ts, "/v1/sandboxes/sb_1/ports/49983", "tok", false)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUpgradeRequired {
		t.Errorf("status %d, want 426", resp.StatusCode)
	}
	if dialed {
		t.Error("a non-upgrade GET dialed the guest; it must not wake a hibernated sandbox")
	}
}

func TestPortRelayRequiresToken(t *testing.T) {
	ts := newTestServer(t, "", &fakeManager{}, nil)
	resp := getPort(t, ts, "/v1/sandboxes/sb_1/ports/49983", "", true)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status %d, want 401", resp.StatusCode)
	}
}

func TestPortRelayRejectsBadPort(t *testing.T) {
	ts := newTestServer(t, "", &fakeManager{}, nil)
	for _, port := range []string{"0", "65536", "http", "-1", "+22"} {
		t.Run(port, func(t *testing.T) {
			resp := getPort(t, ts, "/v1/sandboxes/sb_1/ports/"+port, "tok", true)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("status %d, want 400", resp.StatusCode)
			}
		})
	}
}

func TestPortRelayMapsDialErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"unknown sandbox or wrong token", pool.ErrUnknownSandbox, http.StatusNotFound},
		{"no guest listener", errors.New("port_forward 49983: not_found"), http.StatusBadGateway},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{dialPort: func(string, string, uint16) (net.Conn, error) { return nil, tt.err }}
			ts := newTestServer(t, "", mgr, nil)
			resp := getPort(t, ts, "/v1/sandboxes/sb_1/ports/49983", "tok", true)
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestPortRelayEndsOnGuestClose(t *testing.T) {
	ts, _ := newPortServer(t, func(c net.Conn) {
		_, _ = io.WriteString(c, "bye")
		_ = c.Close()
	})

	conn := dialPortConn(t, ts, portRequest)
	r := bufio.NewReader(conn)
	readStatus101(t, r)
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(rest) != "bye" {
		t.Errorf("got %q, want %q", rest, "bye")
	}
}

func TestPortRelayRefusedAfterCloseRelays(t *testing.T) {
	ts, srv := newPortServer(t, func(c net.Conn) { _, _ = io.Copy(io.Discard, c) })
	srv.CloseRelays()

	conn := dialPortConn(t, ts, portRequest)
	if _, err := io.Copy(io.Discard, conn); err != nil {
		t.Errorf("late relay not refused promptly: %v", err)
	}
}

func newPortServer(t *testing.T, guest func(net.Conn)) (*httptest.Server, *Server) {
	t.Helper()
	mgr := &fakeManager{dialPort: func(string, string, uint16) (net.Conn, error) {
		relayEnd, guestEnd := net.Pipe()
		go guest(guestEnd)
		return relayEnd, nil
	}}
	srv := New("", nil, "node:7777", mgr, &fakeDialer{}, nil, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		srv.CloseRelays()
		ts.Close()
	})
	return ts, srv
}

func dialPortConn(t *testing.T, ts *httptest.Server, req string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(conn, req); err != nil {
		t.Fatalf("write request: %v", err)
	}
	return conn
}

func getPort(t *testing.T, ts *httptest.Server, path, token string, upgrade bool) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+path, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if upgrade {
		req.Header.Set("Upgrade", "tcp")
		req.Header.Set("Connection", "Upgrade")
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}
