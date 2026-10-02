package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestTenantRoutesAreRootOnlyAndNeverServeATokenBack(t *testing.T) {
	var put config.TenantSpec
	var set []config.TenantSpec
	mgr := &fakeManager{
		tenantList: []pool.TenantInfo{{Name: "acme", MaxClaims: 2, Claims: 1}},
		putTenant:  func(spec config.TenantSpec) error { put = spec; return nil },
		setTenants: func(specs []config.TenantSpec) error { set = specs; return nil },
		deleteTenant: func(name string) error {
			if name != "acme" {
				return pool.ErrUnknownTenant
			}
			return nil
		},
	}
	ts := newTenantTestServer(t, "root", []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}, mgr, nil)
	for _, route := range []struct{ method, path, body string }{
		{http.MethodGet, "/v1/tenants", ""},
		{http.MethodPut, "/v1/tenants", `{"tenants":[]}`},
		{http.MethodPut, "/v1/tenants/beta", `{"token":"b"}`},
		{http.MethodDelete, "/v1/tenants/acme", ""},
	} {
		resp := doReq(t, route.method, ts.URL+route.path, "acme-tok", route.body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s %s with a tenant token: %d, want 403", route.method, route.path, resp.StatusCode)
		}
	}

	resp := doReq(t, http.MethodGet, ts.URL+"/v1/tenants", "root", "")
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != `{"tenants":[{"name":"acme","max_claims":2,"claims":1}],"digest":"set-digest"}` {
		t.Errorf("GET /v1/tenants: %d %s", resp.StatusCode, body)
	}

	resp = doReq(t, http.MethodPut, ts.URL+"/v1/tenants/beta", "root", `{"token":"beta-tok","max_claims":3}`)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent || put != (config.TenantSpec{Name: "beta", Token: "beta-tok", MaxClaims: 3}) {
		t.Errorf("PUT /v1/tenants/beta: %d, manager got %+v", resp.StatusCode, put)
	}
	resp = doReq(t, http.MethodPut, ts.URL+"/v1/tenants/u:42%2Fwest", "root", `{"token":"w"}`)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent || put.Name != "u:42/west" {
		t.Errorf("PUT an escaped name: %d, manager got %q", resp.StatusCode, put.Name)
	}
	resp = doReq(t, http.MethodPut, ts.URL+"/v1/tenants", "root", `{"tenants":[{"name":"acme","max_claims":1}]}`)
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || len(set) != 1 || set[0].Name != "acme" || strings.Contains(string(body), "tok") {
		t.Errorf("PUT /v1/tenants: %d %s, manager got %+v", resp.StatusCode, body, set)
	}
	for path, want := range map[string]int{"/v1/tenants/acme": http.StatusNoContent, "/v1/tenants/nobody": http.StatusNotFound} {
		resp = doReq(t, http.MethodDelete, ts.URL+path, "root", "")
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("DELETE %s: %d, want %d", path, resp.StatusCode, want)
		}
	}
	resp = doReq(t, http.MethodPut, ts.URL+"/v1/tenants/beta", "root", `{"token":"b","egress":{}}`)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("PUT a tenant with an unknown field: %d, want 400", resp.StatusCode)
	}
}

func TestRootListsOneTenantsClaimsAndATenantCannotWiden(t *testing.T) {
	mgr := &fakeManager{}
	ts := newTenantTestServer(t, "root", []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}, mgr, nil)
	for _, tt := range []struct{ token, query, want string }{
		{"root", "", ""},
		{"root", "?tenant=gone", "gone"},
		{"acme-tok", "?tenant=gone", "acme"},
	} {
		resp := doReq(t, http.MethodGet, ts.URL+"/v1/sandboxes"+tt.query, tt.token, "")
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || mgr.gotTenant != tt.want {
			t.Errorf("%s %s: %d scope %q, want %q", tt.token, tt.query, resp.StatusCode, mgr.gotTenant, tt.want)
		}
	}
}

func TestRenewOfARemovedTenantsClaimIsForbidden(t *testing.T) {
	mgr := &fakeManager{renew: func(string, string, time.Duration) (time.Time, error) { return time.Time{}, pool.ErrTenantRemoved }}
	ts := newTestServer(t, "root", mgr, nil)
	resp := doReq(t, http.MethodPost, ts.URL+"/v1/sandboxes/sb_1/renew", "sandbox-tok", "")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("renew of a removed tenant's claim: %d, want 403", resp.StatusCode)
	}
}

func TestRootFindsARemovedTenantsRemoteTemplate(t *testing.T) {
	hash := types.ClaimRequest{Template: "tpl"}.Key().Hash()
	placer := &fakePlacer{ownersByProbe: map[string][]string{
		types.TemplateGossipHash(hash, "gone"):                  {"peer:7777"},
		types.TemplateGossipHash(hash, types.TemplateRootScope): {"peer:7777"},
	}}
	srv := New("root", "node:7777", &fakeManager{}, &fakeDialer{}, placer, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	for _, tt := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/claim", `{"template":"tpl"}`},
		{http.MethodPost, "/v1/claim", `{"template":"tpl","volumes":[{"name":"imagenet"}]}`},
		{http.MethodDelete, "/v1/templates?template=tpl", ""},
	} {
		resp := doReq(t, tt.method, ts.URL+tt.path, "root", tt.body)
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"redirect":["peer:7777"]`) {
			t.Errorf("%s %s: %d %s, want the retained template's owner", tt.method, tt.path, resp.StatusCode, body)
		}
	}
}

func TestConfigReloadIsRootOnlyAndNamesWhatChanged(t *testing.T) {
	calls := 0
	reload := func(context.Context) (pool.ReloadResult, error) {
		calls++
		switch calls {
		case 1:
			return pool.ReloadResult{Changed: []string{"egress_internal_allow"}}, nil
		case 2:
			return pool.ReloadResult{}, fmt.Errorf("%w: listen change only at a restart", pool.ErrReloadRefused)
		default:
			return pool.ReloadResult{}, fmt.Errorf("%w: parse config", pool.ErrBadConfig)
		}
	}
	mgr := &fakeManager{tenants: []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}}
	srv := New("root", "node:7777", mgr, &fakeDialer{}, nil, nil, nil, nil)
	srv.SetConfigReload(reload)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	resp := doReq(t, http.MethodPost, ts.URL+"/v1/config/reload", "acme-tok", "")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden || calls != 0 {
		t.Errorf("tenant token: %d after %d reloads, want 403 and none", resp.StatusCode, calls)
	}
	for _, want := range []struct {
		code int
		body string
	}{
		{http.StatusOK, `{"changed":["egress_internal_allow"]}`},
		{http.StatusConflict, "listen change only at a restart"},
		{http.StatusBadRequest, "parse config"},
	} {
		resp = doReq(t, http.MethodPost, ts.URL+"/v1/config/reload", "root", "")
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != want.code || !strings.Contains(string(body), want.body) {
			t.Errorf("reload: %d %s, want %d with %q", resp.StatusCode, body, want.code, want.body)
		}
	}
}
