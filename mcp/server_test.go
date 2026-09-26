package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sdk/go/silkd/silkdtest"
)

var infoFrame = func() string {
	frame, err := wire.EncodeResponse(&wire.InfoResp{Version: "test", Proto: wire.KeepAliveProto})
	if err != nil {
		panic(err)
	}
	return string(frame) + "\n"
}()

func TestServeSpeaksMCP(t *testing.T) {
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/checkpoint") {
			return false
		}
		_ = json.MarshalWrite(w, map[string]any{
			"checkpoint": map[string]any{"id": "ck_0011223344556677", "name": "s1", "sandbox_id": "sb_1"},
		})
		return true
	})
	replies := serveLines(t, srv,
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"create_sandbox","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"checkpoint","arguments":{"sandbox_id":"sb_1","name":"s1"}}}`,
		`{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"release","arguments":{"sandbox_id":"sb_1"}}}`,
		`{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"exec","arguments":{"sandbox_id":"sb_1","command":"true"}}}`,
		`{"jsonrpc":"2.0","id":7,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":8,"method":"ping","params":{"note":"\ud800"}}`,
	)
	if len(replies) != 8 {
		t.Fatalf("got %d replies, want 8 (notification unanswered)", len(replies))
	}

	initResult := replies[1]["result"].(map[string]any)
	if initResult["protocolVersion"] != protocolVersion {
		t.Errorf("initialize protocolVersion = %v", initResult["protocolVersion"])
	}
	tools := replies[2]["result"].(map[string]any)["tools"].([]any)
	if len(tools) != len(toolSpecs()) {
		t.Errorf("tools/list: %d tools, want %d", len(tools), len(toolSpecs()))
	}
	created := toolText(t, replies[3])
	if !strings.Contains(created, "sb_1") {
		t.Errorf("create_sandbox: %q", created)
	}
	ckpt := toolText(t, replies[4])
	if !strings.Contains(ckpt, "ck_0011223344556677") {
		t.Errorf("checkpoint: %q", ckpt)
	}
	if got := toolText(t, replies[5]); got != "released" {
		t.Errorf("release: %q", got)
	}

	execReply := replies[6]["result"].(map[string]any)
	if execReply["isError"] != true {
		t.Errorf("exec after release: %+v, want isError", execReply)
	}
	if !strings.Contains(toolText(t, replies[6]), "sb_1") {
		t.Errorf("exec error should name the released id: %q", toolText(t, replies[6]))
	}
	if pong, ok := replies[7]["result"].(map[string]any); !ok || len(pong) != 0 {
		t.Errorf("ping reply %+v, want an empty result object", replies[7])
	}
	if _, ok := replies[8]["result"].(map[string]any); !ok {
		t.Errorf("ping with a lone surrogate %+v, want a reply", replies[8])
	}
}

func TestRepliesEncodeMapsInSortedOrder(t *testing.T) {
	var out bytes.Buffer
	in := bufio.NewReader(strings.NewReader(strings.Repeat(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`+"\n", 20)))
	if err := newTestServer(t, nil).serve(t.Context(), in, &out); err != nil {
		t.Fatalf("serve: %v", err)
	}
	lines := slices.Collect(bytes.Lines(out.Bytes()))
	if len(lines) != 20 || slices.ContainsFunc(lines, func(l []byte) bool { return !bytes.Equal(l, lines[0]) }) {
		t.Errorf("20 tools/list replies are not byte-identical (%d lines)", len(lines))
	}
	if got := jsonText(map[string]int{"b": 2, "a": 1, "d": 4, "c": 3, "f": 6, "e": 5, "h": 8, "g": 7}); got != `{"a":1,"b":2,"c":3,"d":4,"e":5,"f":6,"g":7,"h":8}` {
		t.Errorf("jsonText = %s, want sorted keys", got)
	}
}

func TestExecKeepsOutputWhenTheGuestDrops(t *testing.T) {
	srv := newTestServer(t, agentRoute(t, func(string) (string, bool) {
		return `{"type":"started","pid":7}` + "\n" + `{"type":"stdout","data":"cGFydGlhbA=="}` + "\n", true
	}))
	replies := serveLines(t, srv,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_sandbox","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"exec","arguments":{"sandbox_id":"sb_1","command":"yes"}}}`,
	)
	execResult := replies[2]["result"].(map[string]any)
	text := toolText(t, replies[2])
	if execResult["isError"] != true || !strings.Contains(text, `"stdout":"partial"`) || !strings.Contains(text, `"error"`) {
		t.Errorf("exec reply %v: want isError with the partial stdout and an error field", text)
	}
}

func TestReadFileStopsAtTheCap(t *testing.T) {
	chunk := `{"type":"data","data":"` + base64.StdEncoding.EncodeToString(make([]byte, 256<<10)) + `"}` + "\n"
	srv := newTestServer(t, agentRoute(t, func(req string) (string, bool) {
		if strings.Contains(req, `"op":"fs_stat"`) {
			return `{"type":"stat","info":{"kind":"file","size":0}}` + "\n", false
		}
		return strings.Repeat(chunk, 8) + `{"type":"done"}` + "\n", false
	}))
	replies := serveLines(t, srv,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_sandbox","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"read_file","arguments":{"sandbox_id":"sb_1","path":"/dev/zero"}}}`,
	)
	if text := toolText(t, replies[2]); !strings.Contains(text, "read_file cap") {
		t.Errorf("read_file past the cap answered %q, want the cap error", text)
	}
}

func TestLogsKeepsAtMostTheCap(t *testing.T) {
	chunk := `{"type":"stdout","data":"` + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("x"), 256<<10)) + `"}` + "\n"
	srv := newTestServer(t, agentRoute(t, func(string) (string, bool) {
		return strings.Repeat(chunk, 8) + `{"type":"done"}` + "\n", false
	}))
	replies := serveLines(t, srv,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_sandbox","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"logs","arguments":{"sandbox_id":"sb_1","pid":7}}}`,
	)
	if n := strings.Count(toolText(t, replies[2]), "x"); n != execOutputCap {
		t.Errorf("logs kept %d bytes of a 2 MiB replay, want the %d cap", n, execOutputCap)
	}
}

func TestToolRepliesReplaceInvalidUTF8(t *testing.T) {
	lossy := base64.StdEncoding.EncodeToString([]byte("a\xffb"))
	srv := newTestServer(t, agentRoute(t, func(req string) (string, bool) {
		switch {
		case strings.Contains(req, `"op":"fs_stat"`):
			return `{"type":"stat","info":{"kind":"file","size":3}}` + "\n", false
		case strings.Contains(req, `"op":"logs"`):
			return `{"type":"stdout","data":"` + lossy + `"}` + "\n" + `{"type":"done"}` + "\n", false
		}
		return `{"type":"data","data":"` + lossy + `"}` + "\n" + `{"type":"done"}` + "\n", false
	}))
	replies := serveLines(t, srv,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_sandbox","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"read_file","arguments":{"sandbox_id":"sb_1","path":"/bin/x"}}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"logs","arguments":{"sandbox_id":"sb_1","pid":7}}}`,
	)
	if text := toolText(t, replies[2]); text != "a\uFFFDb" {
		t.Errorf("read_file answered %q, want the invalid byte replaced", text)
	}
	var logs map[string]any
	if err := json.Unmarshal([]byte(toolText(t, replies[3])), &logs); err != nil || logs["stdout"] != "a\uFFFDb" {
		t.Errorf("logs answered %v (%v), want JSON with the invalid byte replaced", logs, err)
	}
}

func TestTrackBoxAfterCloseReleasesTheClaim(t *testing.T) {
	released := make(chan string, 1)
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/release") {
			released <- r.URL.Path
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(node.Close)
	addr := strings.TrimPrefix(node.URL, "http://")
	srv, err := newServer(addr, "", "rt:24.04")
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	srv.closeBoxes()
	srv.trackBox(srv.client.Attach(addr, "sb_late", "tok"))
	select {
	case path := <-released:
		if !strings.Contains(path, "sb_late") {
			t.Errorf("released %s, want sb_late", path)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a claim tracked after closeBoxes was never released")
	}
	if len(srv.boxes) != 0 {
		t.Errorf("boxes = %d after a late track, want 0", len(srv.boxes))
	}
}

func TestCloseBoxesWaitsForEveryReleaseOnce(t *testing.T) {
	var releases atomic.Int32
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/release") {
			time.Sleep(200 * time.Millisecond)
			releases.Add(1)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(node.Close)
	addr := strings.TrimPrefix(node.URL, "http://")
	srv, err := newServer(addr, "", "rt:24.04")
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	for _, id := range []string{"sb_a", "sb_b", "sb_c"} {
		srv.trackBox(srv.client.Attach(addr, id, "tok"))
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(srv.closeBoxes)
	}
	wg.Wait()
	if got := releases.Load(); got != 3 {
		t.Errorf("releases = %d after closeBoxes returned, want 3", got)
	}
}

func TestExecRejectsAnEmptyCommand(t *testing.T) {
	replies := serveLines(t, newTestServer(t, nil),
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_sandbox","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"exec","arguments":{"sandbox_id":"sb_1"}}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"spawn","arguments":{"sandbox_id":"sb_1","command":""}}}`)
	for _, id := range []int{2, 3} {
		if !strings.Contains(toolText(t, replies[id]), "command must not be empty") {
			t.Errorf("reply %d accepted an empty command: %q", id, toolText(t, replies[id]))
		}
	}
}

func TestCreateSandboxRejectsNegativeTTL(t *testing.T) {
	replies := serveLines(t, newTestServer(t, nil),
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_sandbox","arguments":{"ttl_seconds":-5}}}`)
	if !strings.Contains(toolText(t, replies[1]), "ttl_seconds must not be negative") {
		t.Errorf("negative ttl accepted: %q", toolText(t, replies[1]))
	}
}

func TestCappedOutputMarksTruncation(t *testing.T) {
	var o cappedOutput
	chunk := bytes.Repeat([]byte("x"), execOutputCap/2+1)
	for range 3 {
		if n, err := o.Write(chunk); n != len(chunk) || err != nil {
			t.Fatalf("Write = %d, %v; want the full length accepted", n, err)
		}
	}
	if o.Len() != execOutputCap || !o.truncated {
		t.Errorf("kept %d bytes, truncated=%v; want the cap and the flag", o.Len(), o.truncated)
	}
}

func toolText(t *testing.T, resp map[string]any) string {
	t.Helper()
	res, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("reply without result: %+v", resp)
	}
	content := res["content"].([]any)
	return fmt.Sprintf("%v", content[0].(map[string]any)["text"])
}

func newTestServer(t *testing.T, extra func(http.ResponseWriter, *http.Request) bool) *server {
	t.Helper()
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/claim":
			_ = json.MarshalWrite(w, map[string]any{"id": "sb_1", "token": "tok"})
		case strings.HasSuffix(r.URL.Path, "/release"):
			w.WriteHeader(http.StatusNoContent)
		case extra != nil && extra(w, r):
		default:
			http.Error(w, `{"error":"no route"}`, http.StatusNotFound)
		}
	}))
	t.Cleanup(node.Close)
	srv, err := newServer(strings.TrimPrefix(node.URL, "http://"), "", "rt:24.04")
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}
	return srv
}

func agentRoute(t *testing.T, reply func(req string) (frames string, drop bool)) func(http.ResponseWriter, *http.Request) bool {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/agent") {
			return false
		}
		conn, err := silkdtest.Upgrade(w)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return true
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		for {
			req, err := br.ReadString('\n')
			if err != nil {
				return true
			}
			frames, drop := infoFrame, false
			if !strings.Contains(req, `"op":"info"`) {
				frames, drop = reply(req)
			}
			if _, err := io.WriteString(conn, frames); err != nil || drop {
				return true
			}
		}
	}
}

func serveLines(t *testing.T, srv *server, lines ...string) map[int]map[string]any {
	t.Helper()
	var out bytes.Buffer
	in := bufio.NewReader(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err := srv.serve(t.Context(), in, &out); err != nil {
		t.Fatalf("serve: %v", err)
	}
	replies := map[int]map[string]any{}
	for line := range bytes.Lines(out.Bytes()) {
		var resp map[string]any
		if err := json.Unmarshal(line, &resp); err != nil {
			t.Fatalf("decode reply %q: %v", line, err)
		}
		replies[int(resp["id"].(float64))] = resp
	}
	return replies
}
