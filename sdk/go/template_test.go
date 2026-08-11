package sandbox

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPromoteReturnsContentDigest(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/sandboxes/sb_1/promote" {
			t.Errorf("got %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"key":            map[string]string{"template": "task:v1", "net": "none", "size": "small"},
			"content_digest": "sha256:promoted",
		})
	}))
	t.Cleanup(ts.Close)

	c := testClient(t, ts)
	tpl, err := (&Sandbox{ID: "sb_1", token: "tok", owner: c.addr, c: c}).Promote(t.Context(), "task:v1")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if tpl.Name != "task:v1" || tpl.ContentDigest != "sha256:promoted" {
		t.Errorf("template %+v, want name and content digest", tpl)
	}
}
