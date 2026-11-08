package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
)

const startedFrame = `{"type":"started","pid":7}` + "\n"

func TestExecReturnsBufferedOutput(t *testing.T) {
	var gotExec, gotStdinClose wire.Request
	ts, _ := newRelayServer(t, func(conn net.Conn) {
		defer conn.Close()
		r := bufio.NewReader(conn)
		frame, err := r.ReadBytes('\n')
		if err != nil {
			return
		}
		gotExec, _ = wire.DecodeRequest(bytes.TrimSpace(frame))
		frame, err = r.ReadBytes('\n')
		if err != nil {
			return
		}
		gotStdinClose, _ = wire.DecodeRequest(bytes.TrimSpace(frame))
		_, _ = io.WriteString(conn, startedFrame)
		_, _ = io.WriteString(conn, stdoutFrame("v22\n"))
		_, _ = io.WriteString(conn, `{"type":"stderr","data":"`+base64.StdEncoding.EncodeToString([]byte("warn\n"))+`"}`+"\n")
		_, _ = io.WriteString(conn, exitFrame(3))
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

func TestExecKeepsInvalidUTF8OutputAsReplacementCharacters(t *testing.T) {
	ts, _ := newRelayServer(t, func(conn net.Conn) {
		defer conn.Close()
		r := bufio.NewReader(conn)
		_, _ = r.ReadBytes('\n')
		_, _ = r.ReadBytes('\n')
		_, _ = io.WriteString(conn, startedFrame)
		_, _ = io.WriteString(conn, stdoutFrame("ok\xff\n"))
		_, _ = io.WriteString(conn, exitFrame(0))
	})
	status, body := postExec(t, ts, `{"argv":["cat","blob"],"timeout_seconds":5}`)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, body)
	}
	var out ExecResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	if out.Stdout != "ok\uFFFD\n" {
		t.Errorf("stdout %q, want the invalid byte as U+FFFD", out.Stdout)
	}
}

func TestExecAuditsTheFrameWithoutItsDelimiter(t *testing.T) {
	ts, srv := newRelayServer(t, okGuest)
	audited := make(chan []byte, 1)
	srv.mgr.(*fakeManager).audited = func(_ string, frame []byte) { audited <- bytes.Clone(frame) }
	if status, body := postExec(t, ts, `{"argv":["true"]}`); status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, body)
	}
	want, _ := wire.EncodeRequest(wire.Exec{Argv: []string{"true"}})
	if got := <-audited; !bytes.Equal(got, want) {
		t.Errorf("audited %q, want the bare frame %q", got, want)
	}
}

func TestExecRunsUnderAnOverflowingTimeout(t *testing.T) {
	ts, _ := newRelayServer(t, okGuest)
	status, body := postExec(t, ts, `{"argv":["true"],"timeout_seconds":10000000000}`)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200: a huge timeout_seconds must not wrap into an expired deadline: %s", status, body)
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

func TestExecTimeoutKillsTheCommand(t *testing.T) {
	killed := make(chan uint32, 1)
	closed := make(chan struct{})
	ts, _ := newRelayServer(t, killWatchGuest(killed, func(conn net.Conn, r *bufio.Reader) {
		defer close(closed)
		_, _ = r.ReadBytes('\n')
		_, _ = io.WriteString(conn, startedFrame)
		_, _ = r.ReadBytes('\n')
		_, _ = r.ReadBytes('\n')
	}))
	if status, _ := postExec(t, ts, `{"argv":["sleep","60"],"timeout_seconds":1}`); status != http.StatusGatewayTimeout {
		t.Fatalf("status %d, want 504", status)
	}
	await(t, closed, "guest conn still open after the timeout")
	expectKill(t, killed, "timeout")
}

func TestExecRejectsBadRequests(t *testing.T) {
	for _, tt := range []struct{ name, body string }{
		{"unknown fields", `{"argv":["whoami"],"user":"nobody"}`},
		{"empty argv", `{"argv":[]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ts, _ := newRelayServer(t, func(conn net.Conn) { _ = conn.Close() })
			if status, _ := postExec(t, ts, tt.body); status != http.StatusBadRequest {
				t.Errorf("status %d, want 400", status)
			}
		})
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

func TestExecGuestClosingAfterTheStartWakesOnce(t *testing.T) {
	var wakes atomic.Int32
	mgr := &execManager{
		fakeManager: &fakeManager{},
		wake: func(context.Context, string, string) (string, error) {
			wakes.Add(1)
			return "/v/sock", nil
		},
	}
	ts := newTestServer(t, "", mgr, pipeDialer(func(guestEnd net.Conn) {
		defer guestEnd.Close()
		r := bufio.NewReader(guestEnd)
		_, _ = r.ReadBytes('\n')
		_, _ = r.ReadBytes('\n')
		_, _ = io.WriteString(guestEnd, startedFrame)
	}))

	if status, body := postExec(t, ts, `{"argv":["sleep","60"]}`); status != http.StatusBadGateway {
		t.Fatalf("status %d, want 502: %s", status, body)
	}
	if n := wakes.Load(); n != 1 {
		t.Errorf("%d wakes, want 1: a guest that hung up was stopped by its own node, not by this exec", n)
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
	ts := newTestServer(t, "", mgr, pipeDialer(func(guestEnd net.Conn) {
		defer guestEnd.Close()
		r := bufio.NewReader(guestEnd)
		_, _ = r.ReadBytes('\n')
		_, _ = r.ReadBytes('\n')
		_, _ = io.WriteString(guestEnd, exitFrame(0))
	}))

	if status, body := postExec(t, ts, `{"argv":["true"],"timeout_seconds":1}`); status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, body)
	}
	if <-wakeHadCommandDeadline {
		t.Error("command timeout was active during wake")
	}
}

func TestExecOverTheNodeBudgetIs503AndKillsTheCommand(t *testing.T) {
	killed := make(chan uint32, 1)
	ts, srv := newRelayServer(t, killWatchGuest(killed, func(conn net.Conn, r *bufio.Reader) {
		_, _ = r.ReadBytes('\n')
		_, _ = io.WriteString(conn, startedFrame)
		_, _ = io.WriteString(conn, stdoutFrame("x"))
		_, _ = io.Copy(io.Discard, r)
	}))
	if !srv.execBudget.TryAcquire(execBufferBudget) {
		t.Fatal("budget not empty at start")
	}
	resp := postJSON(t, ts.URL+"/v1/sandboxes/sb_1/exec", "tok", `{"argv":["yes"]}`)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("status %d Retry-After %q, want 503 and 1", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	expectKill(t, killed, "budget refusal")
}

func TestExecOverTheOutputCapIs413AndKillsTheCommand(t *testing.T) {
	killed := make(chan uint32, 1)
	chunk := stdoutFrame(strings.Repeat("x", 1<<20))
	ts, _ := newRelayServer(t, killWatchGuest(killed, func(conn net.Conn, r *bufio.Reader) {
		_, _ = r.ReadBytes('\n')
		_, _ = io.WriteString(conn, startedFrame)
		for range execOutputCap>>20 + 1 {
			if _, err := io.WriteString(conn, chunk); err != nil {
				return
			}
		}
		_, _ = io.Copy(io.Discard, r)
	}))
	if status, _ := postExec(t, ts, `{"argv":["yes"]}`); status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", status)
	}
	expectKill(t, killed, "output cap")
}

func TestExecReturnsItsBudgetAfterAnswering(t *testing.T) {
	ts, srv := newRelayServer(t, func(conn net.Conn) {
		defer conn.Close()
		r := bufio.NewReader(conn)
		_, _ = r.ReadBytes('\n')
		_, _ = io.WriteString(conn, startedFrame)
		_, _ = io.WriteString(conn, stdoutFrame("v22\n"))
		_, _ = io.WriteString(conn, exitFrame(0))
	})
	if status, body := postExec(t, ts, `{"argv":["node","-v"]}`); status != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", status, body)
	}
	if !srv.execBudget.TryAcquire(execBufferBudget) {
		t.Error("the answered exec kept part of the node budget")
	}
}

func TestExecTakesTheRootToken(t *testing.T) {
	tests := []struct {
		name, token string
		want        int
		wantWake    bool
		wantCred    pool.Cred
	}{
		{"root", "root", http.StatusOK, true, pool.Cred{Operator: true}},
		{"the claim token", "tok", http.StatusOK, true, pool.Cred{Token: "tok"}},
		{"a tenant token", "acme-token", http.StatusNotFound, true, pool.Cred{Token: "acme-token"}},
		{"a wrong token", "wrong", http.StatusNotFound, true, pool.Cred{Token: "wrong"}},
		{"no token", "", http.StatusUnauthorized, false, pool.Cred{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			woke := make(chan pool.Cred, 2)
			audited := make(chan string, 1)
			mgr := &execManager{
				fakeManager: &fakeManager{audited: func(id string, _ []byte) { audited <- id }},
				wake: func(_ context.Context, _, token string) (string, error) {
					if token == "" || token == "tok" {
						return "/v/sock", nil
					}
					return "", pool.ErrUnknownSandbox
				},
				creds: woke,
			}
			ts := newTenantTestServer(t, "root", []tenants.TenantSpec{{Name: "acme", Token: "acme-token"}}, mgr, pipeDialer(okGuest))

			code, body := bodyOf(t, http.MethodPost, ts.URL+"/v1/sandboxes/sb_1/exec", tt.token, `{"argv":["true"]}`)
			if code != tt.want {
				t.Fatalf("status %d, want %d: %s", code, tt.want, body)
			}
			if !tt.wantWake {
				if len(woke) != 0 {
					t.Errorf("woke with %+v, want no wake", <-woke)
				}
				return
			}
			if got := <-woke; got != tt.wantCred {
				t.Errorf("woke with %+v, want %+v", got, tt.wantCred)
			}
			if wantAudit := code == http.StatusOK; (len(audited) == 1) != wantAudit {
				t.Errorf("audited %d records, want one %v", len(audited), wantAudit)
			}
		})
	}
}

type execManager struct {
	*fakeManager
	wake  func(context.Context, string, string) (string, error)
	creds chan<- pool.Cred
}

func (m *execManager) WakeAgentSocket(ctx context.Context, id string, cred pool.Cred) (string, <-chan struct{}, func(), error) {
	if m.creds != nil {
		m.creds <- cred
	}
	sock, err := m.wake(ctx, id, credToken(cred))
	return sock, nil, func() {}, err
}

func postExec(t *testing.T, ts *httptest.Server, body string) (int, []byte) {
	t.Helper()
	return bodyOf(t, http.MethodPost, ts.URL+"/v1/sandboxes/sb_1/exec", "tok", body)
}

func killWatchGuest(killed chan<- uint32, first func(net.Conn, *bufio.Reader)) guestFunc {
	var conns atomic.Int32
	return func(conn net.Conn) {
		r := bufio.NewReader(conn)
		if conns.Add(1) == 1 {
			first(conn, r)
			return
		}
		defer conn.Close()
		frame, _ := r.ReadBytes('\n')
		if req, _ := wire.DecodeRequest(bytes.TrimSpace(frame)); req != nil {
			if kill, ok := req.(*wire.Kill); ok {
				killed <- kill.PID
			}
		}
		_, _ = io.WriteString(conn, `{"type":"done"}`+"\n")
	}
}

func expectKill(t *testing.T, killed <-chan uint32, after string) {
	t.Helper()
	if pid := await(t, killed, "no kill reached the guest after the "+after); pid != 7 {
		t.Errorf("killed pid %d, want 7", pid)
	}
}

func okGuest(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for range 2 {
		if _, err := r.ReadBytes('\n'); err != nil {
			return
		}
	}
	_, _ = io.WriteString(conn, startedFrame+exitFrame(0))
}

func exitFrame(code int) string {
	return `{"type":"exit","code":` + strconv.Itoa(code) + `}` + "\n"
}

func stdoutFrame(data string) string {
	return `{"type":"stdout","data":"` + base64.StdEncoding.EncodeToString([]byte(data)) + `"}` + "\n"
}
