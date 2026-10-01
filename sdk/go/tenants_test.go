package sandbox

import (
	"encoding/json/v2"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestTenantVerbsSendTheOperatorTokenAndEscapeTheName(t *testing.T) {
	type request struct{ method, path, auth, body string }
	got := make(chan request, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got <- request{r.Method, r.URL.EscapedPath(), r.Header.Get("Authorization"), string(body)}
		switch r.Method {
		case http.MethodGet, http.MethodPut:
			if r.URL.Path == "/v1/tenants" {
				_, _ = io.WriteString(w, `{"tenants":[{"name":"acme","max_claims":2,"claims":1},{"name":"gone","claims":1,"removed":true}],"digest":"d"}`)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts, WithAPIToken("root"))

	list, err := c.Tenants(t.Context())
	if err != nil || list.Digest != "d" || len(list.Tenants) != 2 || !list.Tenants[1].Removed || list.Tenants[0].MaxClaims != 2 {
		t.Fatalf("Tenants = %+v, %v", list, err)
	}
	if r := <-got; r != (request{http.MethodGet, "/v1/tenants", "Bearer root", ""}) {
		t.Errorf("list sent %+v", r)
	}
	for _, tt := range []struct {
		name string
		call func() error
		want request
	}{
		{"put", func() error { return c.PutTenant(t.Context(), TenantSpec{Name: "u:42/west", Token: "w", MaxClaims: 3}) }, request{http.MethodPut, "/v1/tenants/u:42%2Fwest", "Bearer root", `{"token":"w","max_claims":3}`}},
		{"patch cap", func() error { return c.PutTenant(t.Context(), TenantSpec{Name: "acme", MaxClaims: 1}) }, request{http.MethodPut, "/v1/tenants/acme", "Bearer root", `{"max_claims":1}`}},
		{"delete", func() error { return c.DeleteTenant(t.Context(), "acme") }, request{http.MethodDelete, "/v1/tenants/acme", "Bearer root", ""}},
		{"set", func() error {
			_, err := c.SetTenants(t.Context(), []TenantSpec{{Name: "acme"}, {Name: "beta", Token: "b"}})
			return err
		}, request{http.MethodPut, "/v1/tenants", "Bearer root", `{"tenants":[{"name":"acme"},{"name":"beta","token":"b"}]}`}},
	} {
		if err := tt.call(); err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if r := <-got; r != tt.want {
			t.Errorf("%s sent %+v, want %+v", tt.name, r, tt.want)
		}
	}
}

func TestDeleteTenantClusterCountsAMissingTenantAsDone(t *testing.T) {
	var mu sync.Mutex
	deletes := map[string]int{}
	answer := func(status int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			deletes[r.Host]++
			mu.Unlock()
			w.WriteHeader(status)
			if status == http.StatusNotFound {
				_, _ = io.WriteString(w, `{"error":"unknown tenant"}`)
			}
		}
	}
	gone := httptest.NewServer(answer(http.StatusNotFound))
	defer gone.Close()
	broken := httptest.NewServer(answer(http.StatusInternalServerError))
	defer broken.Close()
	old := httptest.NewServer(http.NotFoundHandler())
	defer old.Close()
	peers := []string{strings.TrimPrefix(gone.URL, "http://"), strings.TrimPrefix(broken.URL, "http://"), strings.TrimPrefix(old.URL, "http://")}
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peers" {
			_ = json.MarshalWrite(w, map[string][]string{"peers": peers})
			return
		}
		answer(http.StatusNoContent)(w, r)
	}))
	defer entry.Close()

	results, err := testClient(t, entry, WithAPIToken("root")).DeleteTenantCluster(t.Context(), "acme")
	if err != nil {
		t.Fatalf("DeleteTenantCluster: %v", err)
	}
	failed := map[string]int{}
	for _, res := range results {
		if apiErr, ok := errors.AsType[*APIError](res.Err); ok {
			failed[res.Addr] = apiErr.Status
		} else if res.Err != nil {
			t.Errorf("node %s: %v", res.Addr, res.Err)
		}
	}
	want := map[string]int{peers[1]: http.StatusInternalServerError, peers[2]: http.StatusNotFound}
	if len(results) != 4 || !maps.Equal(failed, want) {
		t.Errorf("failures %v over %d nodes, want the broken peer and the node without the route to retry: %v", failed, len(results), want)
	}
}
