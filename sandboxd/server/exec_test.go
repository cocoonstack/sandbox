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
	var got wire.Request
	ts, _ := newRelayServer(t, func(conn net.Conn) {
		defer conn.Close()
		line, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			return
		}
		got, _ = wire.DecodeRequest(bytes.TrimSpace(line))
		_, _ = io.WriteString(conn, `{"type":"started","pid":7}`+"\n")
		_, _ = io.WriteString(conn, `{"type":"stdout","data":"`+base64.StdEncoding.EncodeToString([]byte("v22\n"))+`"}`+"\n")
		_, _ = io.WriteString(conn, `{"type":"stderr","data":"`+base64.StdEncoding.EncodeToString([]byte("warn\n"))+`"}`+"\n")
		_, _ = io.WriteString(conn, `{"type":"exit","code":3}`+"\n")
	})
	status, body := postExec(t, ts, `{"argv":["node","-v"],"cwd":"/work","env":{"A":"1"}}`)
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
	exec, ok := got.(*wire.Exec)
	if !ok || strings.Join(exec.Argv, " ") != "node -v" || exec.Cwd != "/work" || exec.Env["A"] != "1" || exec.Detach {
		t.Errorf("guest request = %#v", got)
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
