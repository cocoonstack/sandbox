package sandbox

import (
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
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != 1 {
		t.Errorf("got %v, want ExitError code 1", err)
	}
}

func TestExecSurfacesErrorFrame(t *testing.T) {
	sb := testSandbox(t, newAgentServer(t, silkdtest.ServeConn))

	_, err := sb.Exec(t.Context(), "no-such-binary")
	var silkdErr *wire.ErrorResp
	if !errors.As(err, &silkdErr) || silkdErr.Kind != wire.KindNotFound {
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

func TestUpgradeKeepsCoalescedBytes(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sandboxes/{id}/agent", func(w http.ResponseWriter, r *http.Request) {
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
	code, err := testSandbox(t, ts).Run(t.Context(), Cmd{Argv: []string{"echo", "42"}, Stdout: &out})
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

func newAgentServer(t *testing.T, serve func(net.Conn)) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sandboxes/{id}/agent", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") != "silkd" || r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		if _, err := io.WriteString(conn, upgrade101); err != nil {
			_ = conn.Close()
			return
		}
		serve(conn)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts
}

func testSandbox(t *testing.T, ts *httptest.Server) *Sandbox {
	t.Helper()
	c := testClient(t, ts)
	return &Sandbox{ID: "sb_1", c: c, token: "tok", owner: c.addr}
}
