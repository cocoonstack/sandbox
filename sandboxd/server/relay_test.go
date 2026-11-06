package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/projecteru2/core/log"
	"github.com/rs/zerolog"

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
	for _, tt := range []struct {
		name    string
		serve   func(*testing.T, guestFunc) (*httptest.Server, *Server)
		request string
	}{
		{"agent", newRelayServer, agentRequest},
		{"port", newPortServer, portRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ts, srv := tt.serve(t, func(c net.Conn) { _, _ = io.Copy(io.Discard, c) })
			srv.CloseRelays()
			if _, err := io.Copy(io.Discard, dialPortConn(t, ts, tt.request)); err != nil {
				t.Errorf("late relay not refused promptly: %v", err)
			}
		})
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
	if _, err := r.ReadString('\n'); !errors.Is(err, io.EOF) {
		t.Errorf("got %v after terminal frame, want EOF", err)
	}
}

func TestRelayDeliversPipelinedBytes(t *testing.T) {
	ts, _ := newRelayServer(t, func(c net.Conn) {
		defer c.Close()
		r := bufio.NewReader(c)
		frame, err := r.ReadString('\n')
		if err != nil {
			return
		}
		_, _ = io.WriteString(c, frame)
	})

	r := bufio.NewReader(dialPortConn(t, ts, agentRequest+execFrame))
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
	await(t, guestClosed, "guest side not closed after client disconnect")
}

func TestRelayEndsWhenItsClaimIsFenced(t *testing.T) {
	relayWithFence := func(fenced <-chan struct{}) (net.Conn, *bufio.Reader) {
		guest := pipeDialer(func(c net.Conn) { _, _ = io.Copy(io.Discard, c) })
		return upgradeConn(t, newTestServer(t, "", &fakeManager{fenced: fenced}, guest))
	}
	fenced := make(chan struct{})
	conn, r := relayWithFence(fenced)
	plainConn, plainR := relayWithFence(nil)
	close(fenced)

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Errorf("fenced relay read: %v, want EOF", err)
	}
	_ = plainConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, err := plainR.ReadByte(); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("unreplicated relay read: %v, want it still open", err)
	}
}

func TestWakeGuestLogsAClientCancelBelowError(t *testing.T) {
	logs := captureLogs(t)
	global := log.GetGlobalLogger()
	*global = global.Level(zerolog.InfoLevel)
	dialer := &fakeDialer{dial: func(ctx context.Context, _ string) (net.Conn, error) { return nil, ctx.Err() }}
	srv := New("", "node:7777", &fakeManager{}, dialer, nil, nil, nil, nil)

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	w := httptest.NewRecorder()
	if _, _, _, ok := srv.wakeGuest(canceled, w, "sb_1", "tok", srv.mgr.WakeAgentSocket); ok || w.Code != http.StatusBadGateway {
		t.Fatalf("canceled dial: ok %v status %d, want a refused 502", ok, w.Code)
	}
	if logs.String() != "" {
		t.Errorf("a client cancel logged %q, want nothing at info level", logs.String())
	}

	dialer.dial = func(context.Context, string) (net.Conn, error) { return nil, errors.New("connection refused") }
	if _, _, _, ok := srv.wakeGuest(t.Context(), httptest.NewRecorder(), "sb_1", "tok", srv.mgr.WakeAgentSocket); ok {
		t.Fatal("a failing dial succeeded")
	}
	if !strings.Contains(logs.String(), `"level":"error"`) {
		t.Errorf("a real dial failure logged %q, want an error record", logs.String())
	}

	logs.Reset()
	dialer.dial = func(context.Context, string) (net.Conn, error) { return nil, context.Canceled }
	if _, _, _, ok := srv.wakeGuest(t.Context(), httptest.NewRecorder(), "sb_1", "tok", srv.mgr.WakeAgentSocket); ok {
		t.Fatal("a failing dial succeeded")
	}
	if !strings.Contains(logs.String(), `"level":"error"`) {
		t.Errorf("a cancel that did not come from the client logged %q, want an error record", logs.String())
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
				tee := &auditTee{r: rd.wrap(strings.NewReader(tt.in)), record: func(frame []byte) {
					if len(frame) > pool.AuditLineCap {
						got = append(got, "oversized")
						return
					}
					got = append(got, string(frame))
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

func TestCloseRelaysClosesAnOpenClientConn(t *testing.T) {
	ts, srv := newRelayServer(t, func(c net.Conn) {
		_, _ = io.Copy(io.Discard, c)
	})

	conn, r := upgradeConn(t, ts)
	srv.CloseRelays()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Errorf("client after CloseRelays: %v, want EOF", err)
	}
}

func newRelayServer(t *testing.T, guest guestFunc) (*httptest.Server, *Server) {
	t.Helper()
	srv := New("", "node:7777", &fakeManager{}, pipeDialer(guest), nil, nil, nil, nil)
	return serveTestServer(t, srv), srv
}

func upgradeConn(t *testing.T, ts *httptest.Server) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn := dialPortConn(t, ts, agentRequest)
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
