package sandbox

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sdk/go/silkd/silkdtest"
)

const upgrade101 = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: silkd\r\nConnection: Upgrade\r\n\r\n"

func TestExecCapturesStdout(t *testing.T) {
	sb := testSandbox(t, newAgentServer(t, silkdtest.ServeConn))

	out, err := sb.Exec(t.Context(), "echo", "42")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if out != "42\n" {
		t.Errorf("stdout %q, want 42\\n", out)
	}
}

func TestExecNonZeroExit(t *testing.T) {
	sb := testSandbox(t, newAgentServer(t, silkdtest.ServeConn))

	_, err := sb.Exec(t.Context(), "false")
	exitErr, ok := errors.AsType[*ExitError](err)
	if !ok || exitErr.Code != 1 {
		t.Errorf("got %v, want ExitError code 1", err)
	}
}

func TestExecSurfacesErrorFrame(t *testing.T) {
	sb := testSandbox(t, newAgentServer(t, silkdtest.ServeConn))

	_, err := sb.Exec(t.Context(), "no-such-binary")
	silkdErr, ok := errors.AsType[*wire.ErrorResp](err)
	if !ok || silkdErr.Kind != wire.KindNotFound {
		t.Errorf("got %v, want silkd not_found error", err)
	}
}

func TestRunStreamsStdin(t *testing.T) {
	sb := testSandbox(t, newAgentServer(t, silkdtest.ServeConn))

	var out strings.Builder
	code, err := sb.Run(t.Context(), Cmd{
		Argv:   []string{"cat"},
		Stdin:  strings.NewReader("through the relay"),
		Stdout: &out,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if code != 0 || out.String() != "through the relay" {
		t.Errorf("code=%d out=%q", code, out.String())
	}
}

func TestRunContextCancel(t *testing.T) {
	sb := testSandbox(t, newAgentServer(t, silkdtest.ServeConn))

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := sb.Run(ctx, Cmd{Argv: []string{"sleep", "300"}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want deadline exceeded", err)
	}
}

func TestRunCancelKillsTheCommandItStarted(t *testing.T) {
	killed := make(chan uint32, 1)
	sb := testSandbox(t, newAgentServer(t, func(conn net.Conn) {
		defer conn.Close()
		r := bufio.NewReader(conn)
		for {
			line, err := r.ReadBytes('\n')
			if err != nil {
				return
			}
			req, err := wire.DecodeRequest(bytes.TrimSpace(line))
			if err != nil {
				return
			}
			switch req := req.(type) {
			case *wire.Info:
				_, _ = io.WriteString(conn, `{"type":"info","version":"test","proto":2}`+"\n")
			case *wire.Exec:
				_, _ = io.WriteString(conn, `{"type":"started","pid":7}`+"\n")
				_, _ = io.Copy(io.Discard, r)
				return
			case *wire.Kill:
				killed <- req.PID
				_, _ = io.WriteString(conn, `{"type":"done"}`+"\n")
			}
		}
	}))

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := sb.Run(ctx, Cmd{Argv: []string{"sleep", "300"}}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline exceeded", err)
	}
	select {
	case pid := <-killed:
		if pid != 7 {
			t.Errorf("killed pid %d, want 7", pid)
		}
	default:
		t.Fatal("no kill reached the guest before Run returned")
	}
}

func TestUpgradeKeepsCoalescedBytes(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sandboxes/{id}/agent", func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer conn.Close()
		blob := upgrade101 +
			`{"type":"started","pid":1}` + "\n" +
			`{"type":"stdout","data":"NDIK"}` + "\n" +
			`{"type":"exit","code":0}` + "\n"
		if _, err := io.WriteString(conn, blob); err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, conn)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	var out strings.Builder
	code, err := legacySandbox(t, ts).Run(t.Context(), Cmd{Argv: []string{"echo", "42"}, Stdout: &out})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if code != 0 || out.String() != "42\n" {
		t.Errorf("code=%d out=%q", code, out.String())
	}
}

func TestRunRejectedUpgrade(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sandboxes/{id}/agent", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":"unknown sandbox"}`)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	_, err := testSandbox(t, ts).Run(t.Context(), Cmd{Argv: []string{"echo"}})
	if err == nil || !strings.Contains(err.Error(), "unknown sandbox") {
		t.Errorf("got %v, want rejected upgrade", err)
	}
}

func TestDialAgentRejectsControlChars(t *testing.T) {
	ts := newAgentServer(t, func(conn net.Conn) { _ = conn.Close() })
	c := testClient(t, ts)
	for _, bad := range []struct{ id, token string }{
		{"sb\r\nInjected: 1", "tok"},
		{"sb_1", "tok\r\nInjected: 1"},
	} {
		if _, err := c.dialAgent(t.Context(), c.addr, bad.id, bad.token); err == nil {
			t.Errorf("dialAgent(%q, %q) succeeded, want control-character error", bad.id, bad.token)
		}
	}
}

func TestRenewSendsTheSandboxTokenAndRecordsTheGrant(t *testing.T) {
	granted := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		ttl  time.Duration
		body string
	}{
		{"explicit ttl", 90 * time.Second, `{"ttl_seconds":90}`},
		{"server default", 0, `{}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/sandboxes/sb_1/renew" {
					t.Errorf("got %s %s", r.Method, r.URL.Path)
				}
				if got := r.Header.Get("Authorization"); got != "Bearer tok" {
					t.Errorf("authorization %q, want the sandbox's own token", got)
				}
				if body, _ := io.ReadAll(r.Body); string(body) != tt.body {
					t.Errorf("body %s, want %s", body, tt.body)
				}
				_, _ = io.WriteString(w, `{"deadline":"2026-09-23T12:00:00Z"}`)
			}))
			t.Cleanup(ts.Close)
			c := testClient(t, ts, WithAPIToken("api"))
			sb := &Sandbox{ID: "sb_1", token: "tok", owner: c.addr, c: c}

			got, err := sb.Renew(t.Context(), tt.ttl)
			if err != nil {
				t.Fatalf("Renew: %v", err)
			}
			if !got.Equal(granted) || !sb.Deadline.Equal(granted) {
				t.Errorf("returned %v, handle %v, want %v", got, sb.Deadline, granted)
			}
		})
	}
}

func TestRenewRefusalLeavesTheDeadline(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"error":"sandbox archived"}`)
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	before := time.Date(2026, 9, 23, 11, 0, 0, 0, time.UTC)
	sb := &Sandbox{ID: "sb_1", Deadline: before, token: "tok", owner: c.addr, c: c}

	_, err := sb.Renew(t.Context(), time.Minute)
	if apiErr, ok := errors.AsType[*APIError](err); !ok || apiErr.Status != http.StatusConflict {
		t.Fatalf("Renew error %v, want a 409 APIError", err)
	}
	if !sb.Deadline.Equal(before) {
		t.Errorf("a refused renew moved the handle to %v", sb.Deadline)
	}
}

func newAgentServer(t *testing.T, serve func(net.Conn)) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sandboxes/{id}/agent", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") != "silkd" || r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		conn, err := silkdtest.Upgrade(w)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		serve(conn)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts
}

func testSandbox(t *testing.T, ts *httptest.Server, opts ...ClientOption) *Sandbox {
	t.Helper()
	c := testClient(t, ts, opts...)
	sb := &Sandbox{ID: "sb_1", c: c, token: "tok", owner: c.addr}
	t.Cleanup(sb.pool.drain)
	return sb
}

func legacySandbox(t *testing.T, ts *httptest.Server) *Sandbox {
	t.Helper()
	sb := testSandbox(t, ts)
	sb.proto.Store(1)
	return sb
}
