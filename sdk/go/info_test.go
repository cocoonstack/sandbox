package sandbox

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInfoReportsCapacityState(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, map[string]any{
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

func TestSandboxesDecodeMetadataAndResources(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, map[string]any{"sandboxes": []any{map[string]any{
			"id": "sb_1", "metadata": map[string]string{"team": "a"}, "cpu_count": 2, "mem_total_bytes": 1 << 30,
		}}})
	}))
	t.Cleanup(ts.Close)

	list, err := testClient(t, ts).Sandboxes(t.Context())
	if err != nil {
		t.Fatalf("Sandboxes: %v", err)
	}
	if len(list) != 1 || list[0].Metadata["team"] != "a" || list[0].CPUCount != 2 || list[0].MemTotalBytes != 1<<30 {
		t.Errorf("summaries %+v, want metadata team=a, 2 CPUs, 1 GiB", list)
	}
}
