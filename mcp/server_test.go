package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServeSpeaksMCP(t *testing.T) {
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/claim":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "sb_1", "token": "tok"})
		case strings.HasSuffix(r.URL.Path, "/checkpoint"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"checkpoint": map[string]any{"id": "ck_0011223344556677", "name": "s1", "sandbox_id": "sb_1"},
			})
		case strings.HasSuffix(r.URL.Path, "/release"):
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, `{"error":"no route"}`, http.StatusNotFound)
		}
	}))
	t.Cleanup(node.Close)

	srv, err := newServer(strings.TrimPrefix(node.URL, "http://"), "", "rt:24.04")
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}

	lines := []string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"create_sandbox","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"checkpoint","arguments":{"sandbox_id":"sb_1","name":"s1"}}}`,
		`{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"release","arguments":{"sandbox_id":"sb_1"}}}`,
		`{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"exec","arguments":{"sandbox_id":"sb_1","command":"true"}}}`,
	}
	var out bytes.Buffer
	in := bufio.NewReader(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err := srv.serve(t.Context(), in, &out); err != nil {
		t.Fatalf("serve: %v", err)
	}

	replies := map[int]map[string]any{}
	dec := json.NewDecoder(&out)
	for dec.More() {
		var resp map[string]any
		if err := dec.Decode(&resp); err != nil {
			t.Fatalf("decode reply: %v", err)
		}
		id := int(resp["id"].(float64))
		replies[id] = resp
	}
	if len(replies) != 6 {
		t.Fatalf("got %d replies, want 6 (notification unanswered)", len(replies))
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
