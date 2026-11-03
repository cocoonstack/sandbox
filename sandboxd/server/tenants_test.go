package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestTenantRoutesAreRootOnlyAndNeverServeATokenBack(t *testing.T) {
	var put tenants.TenantSpec
	var set []tenants.TenantSpec
	mgr := &fakeManager{
		tenantList: []pool.TenantInfo{{Name: "acme", MaxClaims: 2, Claims: 1}},
		putTenant:  func(spec tenants.TenantSpec) error { put = spec; return nil },
		setTenants: func(specs []tenants.TenantSpec) error { set = specs; return nil },
		deleteTenant: func(name string) error {
			if name != "acme" {
				return pool.ErrUnknownTenant
			}
			return nil
		},
	}
	ts := newTenantTestServer(t, "root", []tenants.TenantSpec{{Name: "acme", Token: "acme-tok"}}, mgr, nil)
	for _, route := range []struct{ method, path, body string }{
		{http.MethodGet, "/v1/tenants", ""},
		{http.MethodPut, "/v1/tenants", `{"tenants":[]}`},
		{http.MethodPut, "/v1/tenants/beta", `{"token":"b"}`},
		{http.MethodDelete, "/v1/tenants/acme", ""},
	} {
		if code := statusOf(t, route.method, ts.URL+route.path, "acme-tok", route.body); code != http.StatusForbidden {
			t.Errorf("%s %s with a tenant token: %d, want 403", route.method, route.path, code)
		}
	}

	status, body := bodyOf(t, http.MethodGet, ts.URL+"/v1/tenants", "root", "")
	if status != http.StatusOK || string(body) != `{"tenants":[{"name":"acme","max_claims":2,"claims":1}],"digest":"set-digest"}` {
		t.Errorf("GET /v1/tenants: %d %s", status, body)
	}

	if code := statusOf(t, http.MethodPut, ts.URL+"/v1/tenants/beta", "root", `{"token":"beta-tok","max_claims":3,"egress_class":"desk"}`); code != http.StatusNoContent || put != (tenants.TenantSpec{Name: "beta", Token: "beta-tok", MaxClaims: 3, EgressClass: "desk"}) {
		t.Errorf("PUT /v1/tenants/beta: %d, manager got %+v", code, put)
	}
	if code := statusOf(t, http.MethodPut, ts.URL+"/v1/tenants/u:42%2Fwest", "root", `{"token":"w"}`); code != http.StatusNoContent || put.Name != "u:42/west" {
		t.Errorf("PUT an escaped name: %d, manager got %q", code, put.Name)
	}
	status, body = bodyOf(t, http.MethodPut, ts.URL+"/v1/tenants", "root", `{"tenants":[{"name":"acme","max_claims":1}]}`)
	if status != http.StatusOK || len(set) != 1 || set[0].Name != "acme" || strings.Contains(string(body), "tok") {
		t.Errorf("PUT /v1/tenants: %d %s, manager got %+v", status, body, set)
	}
	for path, want := range map[string]int{"/v1/tenants/acme": http.StatusNoContent, "/v1/tenants/nobody": http.StatusNotFound} {
		if code := statusOf(t, http.MethodDelete, ts.URL+path, "root", ""); code != want {
			t.Errorf("DELETE %s: %d, want %d", path, code, want)
		}
	}
	if code := statusOf(t, http.MethodPut, ts.URL+"/v1/tenants/beta", "root", `{"token":"b","egress":{}}`); code != http.StatusBadRequest {
		t.Errorf("PUT a tenant with an unknown field: %d, want 400", code)
	}
}

func TestRootListsOneTenantsClaimsAndATenantCannotWiden(t *testing.T) {
	mgr := &fakeManager{}
	ts := newTenantTestServer(t, "root", []tenants.TenantSpec{{Name: "acme", Token: "acme-tok"}}, mgr, nil)
	for _, tt := range []struct{ token, query, want string }{
		{"root", "", ""},
		{"root", "?tenant=gone", "gone"},
		{"acme-tok", "?tenant=gone", "acme"},
	} {
		if code := statusOf(t, http.MethodGet, ts.URL+"/v1/sandboxes"+tt.query, tt.token, ""); code != http.StatusOK || mgr.gotTenant != tt.want {
			t.Errorf("%s %s: %d scope %q, want %q", tt.token, tt.query, code, mgr.gotTenant, tt.want)
		}
	}
}

func TestRenewOfARemovedTenantsClaimIsForbidden(t *testing.T) {
	mgr := &fakeManager{renew: func(string, string, time.Duration) (time.Time, error) { return time.Time{}, pool.ErrTenantRemoved }}
	ts := newTestServer(t, "root", mgr, nil)
	if code := statusOf(t, http.MethodPost, ts.URL+"/v1/sandboxes/sb_1/renew", "sandbox-tok", ""); code != http.StatusForbidden {
		t.Errorf("renew of a removed tenant's claim: %d, want 403", code)
	}
}

func TestRootFindsARemovedTenantsRemoteTemplate(t *testing.T) {
	hash := ClaimRequest{Template: "tpl"}.Key().Hash()
	placer := &fakePlacer{ownersByProbe: map[string][]string{
		types.TemplateGossipHash(hash, "gone"):                  {"peer:7777"},
		types.TemplateGossipHash(hash, types.TemplateRootScope): {"peer:7777"},
	}}
	ts := newMeshTestServer(t, "root", &fakeManager{}, placer, nil)
	for _, tt := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/claim", `{"template":"tpl"}`},
		{http.MethodPost, "/v1/claim", `{"template":"tpl","volumes":[{"name":"imagenet"}]}`},
		{http.MethodDelete, "/v1/templates?template=tpl", ""},
	} {
		code, body := bodyOf(t, tt.method, ts.URL+tt.path, "root", tt.body)
		if code != http.StatusOK || !strings.Contains(string(body), `"redirect":["peer:7777"]`) {
			t.Errorf("%s %s: %d %s, want the retained template's owner", tt.method, tt.path, code, body)
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
	mgr := &fakeManager{tenants: []tenants.TenantSpec{{Name: "acme", Token: "acme-tok"}}}
	srv := New("root", "node:7777", mgr, &fakeDialer{}, nil, nil, nil, nil)
	srv.SetConfigReload(reload)
	ts := serveTestServer(t, srv)

	if code := statusOf(t, http.MethodPost, ts.URL+"/v1/config/reload", "acme-tok", ""); code != http.StatusForbidden || calls != 0 {
		t.Errorf("tenant token: %d after %d reloads, want 403 and none", code, calls)
	}
	for _, want := range []struct {
		code int
		body string
	}{
		{http.StatusOK, `{"changed":["egress_internal_allow"]}`},
		{http.StatusConflict, "listen change only at a restart"},
		{http.StatusBadRequest, "parse config"},
	} {
		code, body := bodyOf(t, http.MethodPost, ts.URL+"/v1/config/reload", "root", "")
		if code != want.code || !strings.Contains(string(body), want.body) {
			t.Errorf("reload: %d %s, want %d with %q", code, body, want.code, want.body)
		}
	}
}

func TestATenantClaimCarriesItsTokensEgressClassAndADownSourceIs503(t *testing.T) {
	mgr := &fakeManager{warmClaim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
		return &types.Sandbox{ID: "sb_1", Token: "sb-tok"}, nil
	}}
	ts := newTenantTestServer(t, "root", []tenants.TenantSpec{{Name: "acme", Token: "acme-tok", EgressClass: "desk"}}, mgr, nil)
	claim := `{"template":"rt:24.04"}`
	if code := statusOf(t, http.MethodPost, ts.URL+"/v1/claim", "acme-tok", claim); code != http.StatusOK || mgr.gotTenant != "acme" || mgr.gotEgressClass != "desk" {
		t.Errorf("tenant claim: %d tenant %q class %q, want acme under desk", code, mgr.gotTenant, mgr.gotEgressClass)
	}
	if code := statusOf(t, http.MethodPost, ts.URL+"/v1/claim", "root", claim); code != http.StatusOK || mgr.gotEgressClass != "" {
		t.Errorf("root claim: %d class %q, want none", code, mgr.gotEgressClass)
	}
	mgr.tenantErr = fmt.Errorf("%w: connection refused", pool.ErrTenantStoreDown)
	if code := statusOf(t, http.MethodPost, ts.URL+"/v1/claim", "acme-tok", claim); code != http.StatusServiceUnavailable {
		t.Errorf("claim with the tenant source down: %d, want 503", code)
	}
	mgr.tenantErr = pool.ErrUnknownTenant
	if code := statusOf(t, http.MethodPost, ts.URL+"/v1/claim", "acme-tok", claim); code != http.StatusUnauthorized {
		t.Errorf("claim with an unknown token: %d, want 401", code)
	}
}

func TestTenantListPagesByCursor(t *testing.T) {
	mgr := &fakeManager{tenantList: []pool.TenantInfo{{Name: "u-2"}}}
	ts := newTenantTestServer(t, "root", nil, mgr, nil)
	if code := statusOf(t, http.MethodGet, ts.URL+"/v1/tenants?after=u-1&limit=2", "root", ""); code != http.StatusOK || mgr.gotTenantPage != [2]string{"u-1", "2"} {
		t.Errorf("paged list: %d, manager got %v", code, mgr.gotTenantPage)
	}
	statusOf(t, http.MethodGet, ts.URL+"/v1/tenants", "root", "")
	if mgr.gotTenantPage != [2]string{"", "1000"} {
		t.Errorf("default page: manager got %v, want the first 1000", mgr.gotTenantPage)
	}
	for _, q := range []string{"limit=0", "limit=10001", "limit=x"} {
		if code := statusOf(t, http.MethodGet, ts.URL+"/v1/tenants?"+q, "root", ""); code != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", q, code)
		}
	}
}
