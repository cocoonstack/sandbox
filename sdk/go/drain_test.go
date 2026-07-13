package sandbox

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDrainAndUncordon(t *testing.T) {
	var methods []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/drain" {
			t.Errorf("path = %s, want /v1/drain", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer root" {
			t.Errorf("auth = %q, want Bearer root", auth)
		}
		methods = append(methods, r.Method)
		_ = json.NewEncoder(w).Encode(NodeInfo{Claimed: 3, Draining: r.Method == http.MethodPost})
	}))
	defer ts.Close()

	c := testClient(t, ts, WithAPIToken("root"))
	info, err := c.Drain(t.Context())
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if !info.Draining || info.Claimed != 3 {
		t.Errorf("info = %+v, want Draining true, Claimed 3", info)
	}
	info, err = c.Uncordon(t.Context())
	if err != nil {
		t.Fatalf("Uncordon: %v", err)
	}
	if info.Draining {
		t.Errorf("info = %+v, want Draining false", info)
	}
	if len(methods) != 2 || methods[0] != http.MethodPost || methods[1] != http.MethodDelete {
		t.Errorf("methods = %v, want [POST DELETE]", methods)
	}
}
