package server

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
)

const agentRequest = "GET /v1/sandboxes/sb_1/agent HTTP/1.1\r\n" +
	"Host: sandboxd\r\n" +
	"Authorization: Bearer tok\r\n" +
	"Upgrade: silkd\r\n" +
	"Connection: Upgrade\r\n\r\n"

var (
	execFrame       = line(wire.Exec{Argv: []string{"echo", "42"}})
	statFrame       = line(wire.FsStat{Path: "/"})
	dataFrame       = line(&wire.Data{Data: []byte("hi")})
	dataEndFrame    = line(wire.DataEnd{})
	stdinFrame      = line(&wire.Stdin{Data: []byte("hi")})
	stdinCloseFrame = line(wire.StdinClose{})
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
	ts, _ := newRelayServer(t, func(c net.Conn) {
		defer c.Close()
		r := bufio.NewReader(c)
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		_, _ = io.WriteString(c, line)
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
		_, _ = io.Copy(io.Discard, c)
	})

	conn, _ := upgradeConn(t, ts)
	_ = conn.Close()
	select {
	case <-guestClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("guest side not closed after client disconnect")
	}
}

func TestAuditTeeRecordsRequestLines(t *testing.T) {
	big := line(wire.Exec{Argv: []string{strings.Repeat("x", pool.AuditLineCap)}})
	chunk := line(&wire.Data{Data: make([]byte, 3*pool.AuditLineCap)})
	exec, stat := strings.TrimSuffix(execFrame, "\n"), strings.TrimSuffix(statFrame, "\n")
	for _, tt := range []struct {
		name string
		in   string
		want []string
	}{
		{"requests back to back", execFrame + statFrame, []string{exec, stat}},
		{"upload input skipped", statFrame + dataFrame + chunk + dataEndFrame + execFrame, []string{stat, exec}},
		{"stdin skipped", execFrame + stdinFrame + stdinCloseFrame, []string{exec}},
		{"oversized request recorded once", big + statFrame, []string{"oversized", stat}},
		{"partial tail never recorded", execFrame + "partial", []string{exec}},
	} {
		for _, rd := range []struct {
			name string
			wrap func(io.Reader) io.Reader
		}{
			{"whole", func(r io.Reader) io.Reader { return r }},
			{"byte at a time", iotest.OneByteReader},
		} {
			t.Run(tt.name+"/"+rd.name, func(t *testing.T) {
				var got []string
				tee := &auditTee{r: rd.wrap(strings.NewReader(tt.in)), record: func(line []byte) {
					if len(line) > pool.AuditLineCap {
						got = append(got, "oversized")
						return
					}
					got = append(got, string(line))
				}}
				var out bytes.Buffer
				if _, err := io.Copy(&out, tee); err != nil || out.String() != tt.in {
					t.Fatalf("copy: %v, relayed %d bytes of %d", err, out.Len(), len(tt.in))
				}
				if !slices.Equal(got, tt.want) {
					t.Errorf("recorded %q, want %q", got, tt.want)
				}
			})
		}
	}
}

func TestCloseRelaysDrains(t *testing.T) {
	ts, srv := newRelayServer(t, func(c net.Conn) {
		_, _ = io.Copy(io.Discard, c)
	})

	_, r := upgradeConn(t, ts)
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		_, _ = io.Copy(io.Discard, r)
	}()
	srv.CloseRelays()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("client conn still open after CloseRelays")
	}
}

func newRelayServer(t *testing.T, guest func(net.Conn)) (*httptest.Server, *Server) {
	t.Helper()
	dialer := &fakeDialer{dial: func(context.Context, string) (net.Conn, error) {
		relayEnd, guestEnd := net.Pipe()
		go guest(guestEnd)
		return relayEnd, nil
	}}
	srv := New("", nil, "node:7777", &fakeManager{}, dialer, nil, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		srv.CloseRelays()
		ts.Close()
	})
	return ts, srv
}

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

func line(r wire.Request) string {
	frame, err := wire.EncodeRequest(r)
	if err != nil {
		panic(err)
	}
	return string(frame) + "\n"
}

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
