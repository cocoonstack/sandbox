package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNodeInfoReportsCapacityState(t *testing.T) {
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"pools":              []any{},
			"claimed":            2,
			"hibernated":         1,
			"archived":           0,
			"at_capacity":        true,
			"at_capacity_reason": "not enough memory",
		})
	}))
	t.Cleanup(node.Close)

	srv, err := newServer(strings.TrimPrefix(node.URL, "http://"), "", "rt:24.04")
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}
	text, err := toolNodeInfo(t.Context(), srv, nil)
	if err != nil {
		t.Fatalf("toolNodeInfo: %v", err)
	}
	var info map[string]any
	if err := json.Unmarshal([]byte(text), &info); err != nil {
		t.Fatalf("decode node info: %v", err)
	}
	if info["at_capacity"] != true || info["at_capacity_reason"] != "not enough memory" {
		t.Errorf("capacity = %v/%v, want true/not enough memory", info["at_capacity"], info["at_capacity_reason"])
	}
}
