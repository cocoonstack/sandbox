package server

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type httpReply struct {
	status     int
	retryAfter string
	body       string
}

func sendVolumeRequest(t *testing.T, ts, method, path, token, match, body string) httpReply {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if match != "" {
		req.Header.Set("If-Match", match)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return httpReply{status: resp.StatusCode, retryAfter: resp.Header.Get("Retry-After"), body: string(raw)}
}

func TestManagedVolumeRoutesUseRequireToken(t *testing.T) {
	routes := []struct{ method, path, match, body string }{
		{http.MethodPut, "/v1/volumes/x", "", `{"size_gib":1}`},
		{http.MethodGet, "/v1/volumes/x", "", ""},
		{http.MethodDelete, "/v1/volumes/x", `"vol_123"`, ""},
		{http.MethodPost, "/v1/volumes/x/grow", `"vol_123"`, growBody},
	}
	fm := &fakeManager{tenants: []tenants.TenantSpec{{Name: "t1", Token: "t1-token"}}}
	mgr := &volumeGrowFake{volumeManagerFake: &volumeManagerFake{fakeManager: fm, err: pool.ErrTenantStoreDown, get: func(string, string) (types.ManagedVolumeInfo, error) {
		t.Error("an unauthenticated request reached the volume manager")
		return types.ManagedVolumeInfo{}, nil
	}}, grow: func(context.Context, string, string, int64, string) (types.ManagedVolumeInfo, error) {
		t.Error("an unauthenticated request reached growth")
		return types.ManagedVolumeInfo{}, nil
	}}
	ts := newTestServer(t, "root-token", mgr, nil)

	for _, scope := range []struct {
		name, token string
		tenantErr   error
		status      int
	}{
		{"sandbox token", "sandbox-token", nil, http.StatusUnauthorized},
		{"tenant store outage", "t1-token", pool.ErrTenantStoreDown, http.StatusServiceUnavailable},
	} {
		fm.tenantErr = scope.tenantErr
		want := sendVolumeRequest(t, ts.URL, http.MethodGet, "/v1/sandboxes", scope.token, "", "")
		if want.status != scope.status || scope.status == http.StatusServiceUnavailable && want.retryAfter == "" {
			t.Fatalf("%s: GET /v1/sandboxes = %+v, want %d", scope.name, want, scope.status)
		}
		for _, r := range routes {
			if got := sendVolumeRequest(t, ts.URL, r.method, r.path, scope.token, r.match, r.body); got != want {
				t.Errorf("%s: %s %s = %+v, want the GET /v1/sandboxes reply %+v", scope.name, r.method, r.path, got, want)
			}
		}
	}

	fm.tenantErr = nil
	created := sendVolumeRequest(t, ts.URL, http.MethodPut, "/v1/volumes/x", "root-token", "", `{"size_gib":1,"tenant":"t1"}`)
	if created.status != http.StatusServiceUnavailable || created.retryAfter == "" {
		t.Fatalf("create during a tenant store outage = %+v, want 503 with Retry-After", created)
	}
}
