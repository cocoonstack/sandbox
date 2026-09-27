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
			"templates":          []any{map[string]any{"key": map[string]any{"template": "app:v1", "net": "none", "size": "small"}, "content_digest": "sha256:aa", "tenant": "acme"}},
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
	want := TemplateStatus{Key: PoolKey{Template: "app:v1", Net: "none", Size: "small"}, ContentDigest: "sha256:aa", Tenant: "acme"}
	if len(info.Templates) != 1 || info.Templates[0] != want {
		t.Errorf("templates = %+v, want [%+v]", info.Templates, want)
	}
}

func TestSandboxesDecodeMetadataAndResources(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, map[string]any{"sandboxes": []any{map[string]any{
			"id": "sb_1", "metadata": map[string]string{"team": "a"}, "cpu_count": 2, "mem_total_bytes": 1 << 30, "on_expire": "archive",
		}}})
	}))
	t.Cleanup(ts.Close)

	list, err := testClient(t, ts).Sandboxes(t.Context())
	if err != nil {
		t.Fatalf("Sandboxes: %v", err)
	}
	if len(list) != 1 || list[0].Metadata["team"] != "a" || list[0].CPUCount != 2 || list[0].MemTotalBytes != 1<<30 || list[0].OnExpire != "archive" {
		t.Errorf("summaries %+v, want metadata team=a, 2 CPUs, 1 GiB", list)
	}
}
