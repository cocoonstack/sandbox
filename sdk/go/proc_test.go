package sandbox

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net"
	"strings"
	"testing"
)

func TestProcVerbs(t *testing.T) {
	script := map[string][]string{
		"exec": {`{"type":"started","pid":41}`},
		"ps":   {`{"type":"procs","procs":[{"pid":41,"argv":["sleep","30"],"detached":true,"state":"running","started_at_epoch_secs":1}]}`},
		"kill": {`{"type":"done"}`},
		"logs": {
			`{"type":"stdout","data":"aGVsbG8="}`, // "hello"
			`{"type":"stderr","data":"b29wcw=="}`, // "oops"
			`{"type":"done"}`,                     // still running: no exit
		},
		"attach": {
			`{"type":"stdout","data":"bGF0ZQ=="}`, // "late"
			`{"type":"exit","code":7}`,
		},
	}
	sb := testSandbox(t, newAgentServer(t, procServe(script)))
	ctx := t.Context()

	pid, err := sb.Spawn(ctx, Cmd{Argv: []string{"sleep", "30"}})
	if err != nil || pid != 41 {
		t.Fatalf("Spawn: pid %d, %v", pid, err)
	}

	if _, err = sb.Spawn(ctx, Cmd{Argv: []string{"true"}, Session: "s1"}); err == nil || !strings.Contains(err.Error(), "session") {
		t.Fatalf("Spawn with session: want session error, got %v", err)
	}

	procs, err := sb.Ps(ctx)
	if err != nil || len(procs) != 1 || procs[0].PID != 41 || !procs[0].Detached || procs[0].State != "running" {
		t.Fatalf("Ps: %+v, %v", procs, err)
	}

	if err = sb.Kill(ctx, 41, 0); err != nil {
		t.Fatalf("Kill: %v", err)
	}

	var out, errs bytes.Buffer
	code, exited, err := sb.Logs(ctx, 41, &out, &errs)
	if err != nil || exited || code != 0 {
		t.Fatalf("Logs: code=%d exited=%v, %v", code, exited, err)
	}
	if out.String() != "hello" || errs.String() != "oops" {
		t.Errorf("Logs streams %q / %q", out.String(), errs.String())
	}

	out.Reset()
	code, exited, err = sb.Attach(ctx, 41, &out, nil)
	if err != nil || !exited || code != 7 {
		t.Fatalf("Attach: code=%d exited=%v, %v", code, exited, err)
	}
	if out.String() != "late" {
		t.Errorf("Attach stream %q", out.String())
	}
}

func TestProcVerbsUnknownPid(t *testing.T) {
	script := map[string][]string{
		"logs": {`{"type":"error","kind":"not_found","message":"no such pid"}`},
	}
	sb := testSandbox(t, newAgentServer(t, procServe(script)))
	_, _, err := sb.Logs(t.Context(), 999, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "not_found") {
		t.Fatalf("Logs unknown pid: %v, want typed not_found", err)
	}
}

// procServe answers each RPC with a canned frame script keyed by op — the
// proc verbs are pure frame plumbing, so a scripted fake covers them.
func procServe(script map[string][]string) func(net.Conn) {
	return func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		line, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			return
		}
		var req struct {
			Op string `json:"op"`
		}
		if json.Unmarshal([]byte(line), &req) != nil {
			return
		}
		for _, frame := range script[req.Op] {
			if _, err := conn.Write([]byte(frame + "\n")); err != nil {
				return
			}
		}
	}
}
