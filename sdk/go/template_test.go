package sandbox

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPromoteReturnsContentDigest(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/sandboxes/sb_1/promote" {
			t.Errorf("got %s %s", r.Method, r.URL.Path)
		}
		_ = json.MarshalWrite(w, map[string]any{
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

func TestTemplateNewNeverColdBootsAnImageNamedLikeTheTemplate(t *testing.T) {
	var got claimRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.UnmarshalRead(r.Body, &got); err != nil {
			t.Errorf("decode claim: %v", err)
		}
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_2", Token: "tok"})
	}))
	t.Cleanup(ts.Close)

	c := testClient(t, ts)
	tpl := &Template{Name: "task:v1", c: c, addr: c.addr, net: "none", size: "small"}
	if _, err := tpl.New(t.Context()); err != nil {
		t.Fatalf("New: %v", err)
	}
	if !got.RequirePromoted || !got.NoRedirect {
		t.Errorf("claim require_promoted=%v no_redirect=%v, want both: a template handle names a promoted template and dials its owner", got.RequirePromoted, got.NoRedirect)
	}
}
