package sandbox

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInfoReportsCapacityState(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"pools":              []any{},
			"claimed":            2,
			"hibernated":         1,
			"archived":           0,
			"at_capacity":        true,
			"at_capacity_reason": "not enough memory",
		})
	}))
	t.Cleanup(ts.Close)

	info, err := testClient(t, ts).Info(t.Context())
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if !info.AtCapacity || info.AtCapacityReason != "not enough memory" {
		t.Errorf("capacity = %t/%q, want true/not enough memory", info.AtCapacity, info.AtCapacityReason)
	}
}
