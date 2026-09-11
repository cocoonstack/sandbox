package server

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

func TestExecReturnsBufferedOutput(t *testing.T) {
	var gotExec, gotStdinClose wire.Request
	ts, _ := newRelayServer(t, func(conn net.Conn) {
		defer conn.Close()
		r := bufio.NewReader(conn)
		line, err := r.ReadBytes('\n')
		if err != nil {
			return
		}
		gotExec, _ = wire.DecodeRequest(bytes.TrimSpace(line))
		line, err = r.ReadBytes('\n')
		if err != nil {
			return
		}
		gotStdinClose, _ = wire.DecodeRequest(bytes.TrimSpace(line))
		_, _ = io.WriteString(conn, `{"type":"started","pid":7}`+"\n")
		_, _ = io.WriteString(conn, `{"type":"stdout","data":"`+base64.StdEncoding.EncodeToString([]byte("v22\n"))+`"}`+"\n")
		_, _ = io.WriteString(conn, `{"type":"stderr","data":"`+base64.StdEncoding.EncodeToString([]byte("warn\n"))+`"}`+"\n")
		_, _ = io.WriteString(conn, `{"type":"exit","code":3}`+"\n")
	})
	status, body := postExec(t, ts, `{"argv":["node","-v"],"cwd":"/work","env":{"A":"1"},"timeout_seconds":5}`)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, body)
	}
	var out ExecResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.ExitCode != 3 || out.Stdout != "v22\n" || out.Stderr != "warn\n" {
		t.Errorf("response = %+v", out)
	}
	exec, ok := gotExec.(*wire.Exec)
	if !ok || strings.Join(exec.Argv, " ") != "node -v" || exec.Cwd != "/work" || exec.Env["A"] != "1" || exec.Detach {
		t.Errorf("guest request = %#v", gotExec)
	}
	if _, ok := gotStdinClose.(*wire.StdinClose); !ok {
		t.Errorf("guest stdin request = %#v, want stdin_close", gotStdinClose)
	}
}

func TestExecMapsSilkdErrors(t *testing.T) {
	for _, tt := range []struct {
		kind string
		want int
	}{
		{"bad_request", http.StatusBadRequest},
		{"internal", http.StatusBadGateway},
	} {
		t.Run(tt.kind, func(t *testing.T) {
			ts, _ := newRelayServer(t, func(conn net.Conn) {
				defer conn.Close()
				_, _ = bufio.NewReader(conn).ReadBytes('\n')
				_, _ = io.WriteString(conn, `{"type":"error","kind":"`+tt.kind+`","message":"spawn failed"}`+"\n")
			})
			if status, _ := postExec(t, ts, `{"argv":["true"]}`); status != tt.want {
				t.Errorf("status %d, want %d", status, tt.want)
			}
		})
	}
}

func TestExecTimesOutAndClosesGuest(t *testing.T) {
	closed := make(chan struct{})
	ts, _ := newRelayServer(t, func(conn net.Conn) {
		defer close(closed)
		r := bufio.NewReader(conn)
		_, _ = r.ReadBytes('\n')
		_, _ = io.WriteString(conn, `{"type":"started","pid":7}`+"\n")
		_, _ = r.ReadBytes('\n')
		_, _ = r.ReadBytes('\n')
	})
	if status, _ := postExec(t, ts, `{"argv":["sleep","60"],"timeout_seconds":1}`); status != http.StatusGatewayTimeout {
		t.Fatalf("status %d, want 504", status)
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("guest conn still open after the timeout")
	}
}

func TestExecRejectsEmptyArgv(t *testing.T) {
	ts, _ := newRelayServer(t, func(conn net.Conn) { _ = conn.Close() })
	if status, _ := postExec(t, ts, `{"argv":[]}`); status != http.StatusBadRequest {
		t.Errorf("status %d, want 400", status)
	}
}

func TestExecGuestClosingEarlyIs502(t *testing.T) {
	ts, _ := newRelayServer(t, func(conn net.Conn) {
		_, _ = bufio.NewReader(conn).ReadBytes('\n')
		_ = conn.Close()
	})
	if status, _ := postExec(t, ts, `{"argv":["true"]}`); status != http.StatusBadGateway {
		t.Errorf("status %d, want 502", status)
	}
}

func TestExecTimeoutStartsAfterWake(t *testing.T) {
	wakeHadCommandDeadline := make(chan bool, 1)
	mgr := &execManager{
		fakeManager: &fakeManager{},
		wake: func(ctx context.Context, _, _ string) (string, error) {
			deadline, ok := ctx.Deadline()
			wakeHadCommandDeadline <- ok && time.Until(deadline) < 2*time.Second
			return "/v/sock", nil
		},
	}
	dialer := &fakeDialer{dial: func(context.Context, string) (net.Conn, error) {
		relayEnd, guestEnd := net.Pipe()
		go func() {
			defer guestEnd.Close()
			r := bufio.NewReader(guestEnd)
			_, _ = r.ReadBytes('\n')
			_, _ = r.ReadBytes('\n')
			_, _ = io.WriteString(guestEnd, `{"type":"exit","code":0}`+"\n")
		}()
		return relayEnd, nil
	}}
	srv := New("", nil, "node:7777", mgr, dialer, nil, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

	if status, body := postExec(t, ts, `{"argv":["true"],"timeout_seconds":1}`); status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, body)
	}
	if <-wakeHadCommandDeadline {
		t.Error("command timeout was active during wake")
	}
}

type execManager struct {
	*fakeManager
	wake func(context.Context, string, string) (string, error)
}

func (m *execManager) WakeAgentSocket(ctx context.Context, id, token string) (string, error) {
	return m.wake(ctx, id, token)
}

func postExec(t *testing.T, ts *httptest.Server, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/v1/sandboxes/sb_1/exec", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("post exec: %v", err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, out
}
