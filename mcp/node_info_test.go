package main

import (
	"encoding/json/v2"
	"net/http"
	"testing"
)

func TestNodeInfoReportsCapacityState(t *testing.T) {
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/v1/info" {
			return false
		}
		_ = json.MarshalWrite(w, map[string]any{
			"pools":              []any{},
			"claimed":            2,
			"hibernated":         1,
			"archived":           0,
			"at_capacity":        true,
			"at_capacity_reason": "not enough memory",
		})
		return true
	})
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
