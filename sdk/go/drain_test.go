package sandbox

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
)

func TestDrainCordonVerbs(t *testing.T) {
	var calls []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "Bearer root" {
			t.Errorf("auth = %q, want Bearer root", auth)
		}
		calls = append(calls, r.Method+" "+r.URL.Path)
		post := r.Method == http.MethodPost
		_ = json.MarshalWrite(w, NodeInfo{Claimed: 3, Draining: post && r.URL.Path == "/v1/drain", Cordoned: post && r.URL.Path == "/v1/cordon"})
	}))
	defer ts.Close()

	c := testClient(t, ts, WithAPIToken("root"))
	for _, tt := range []struct {
		name               string
		verb               func() (*NodeInfo, error)
		draining, cordoned bool
	}{
		{"Drain", func() (*NodeInfo, error) { return c.Drain(t.Context()) }, true, false},
		{"Undrain", func() (*NodeInfo, error) { return c.Undrain(t.Context()) }, false, false},
		{"Cordon", func() (*NodeInfo, error) { return c.Cordon(t.Context()) }, false, true},
		{"Uncordon", func() (*NodeInfo, error) { return c.Uncordon(t.Context()) }, false, false},
	} {
		info, err := tt.verb()
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if info.Draining != tt.draining || info.Cordoned != tt.cordoned || info.Claimed != 3 {
			t.Errorf("%s: info = %+v, want draining %v cordoned %v", tt.name, info, tt.draining, tt.cordoned)
		}
	}
	want := []string{"POST /v1/drain", "DELETE /v1/drain", "POST /v1/cordon", "DELETE /v1/cordon"}
	if !slices.Equal(calls, want) {
		t.Errorf("calls = %v, want %v", calls, want)
	}
}
