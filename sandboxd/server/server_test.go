package server

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/store/peer"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestOwnerAddrOmitsAnUnspecifiedHost(t *testing.T) {
	for advertise, want := range map[string]string{":7777": "", "0.0.0.0:7777": "", "[::]:7777": "", "10.0.0.5:7777": "10.0.0.5:7777"} {
		srv := New("", advertise, &fakeManager{}, &fakeDialer{}, nil, nil, nil, nil)
		if got := srv.claimResponse(&types.Sandbox{ID: "sb_1"}).OwnerAddr; got != want {
			t.Errorf("advertise %q: owner_addr %q, want %q", advertise, got, want)
		}
	}
}

func TestClaimHappyPath(t *testing.T) {
	var gotKey types.PoolKey
	var gotTTL time.Duration
	mgr := &fakeManager{
		claim: func(_ context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
			gotKey, gotTTL = key, ttl
			return &types.Sandbox{
				ID: "sb_1", Token: "tok", Deadline: time.Unix(42, 0).UTC(),
				TemplateDigest: "sha256:claim-digest",
			}, nil
		},
	}
	ts := newTestServer(t, "", mgr, nil)

	resp, err := http.Post(ts.URL+"/v1/claim", "application/json",
		strings.NewReader(`{"template":"rt:24.04","ttl_seconds":60}`))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	var cr types.ClaimResponse
	if err := json.UnmarshalRead(resp.Body, &cr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cr.ID != "sb_1" || cr.Token != "tok" || !cr.Deadline.Equal(time.Unix(42, 0)) {
		t.Errorf("got %+v", cr)
	}
	if cr.TemplateDigest != "sha256:claim-digest" {
		t.Errorf("template digest %q, want sha256:claim-digest", cr.TemplateDigest)
	}
	want := types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall}
	if gotKey != want {
		t.Errorf("key %+v, want defaults %+v", gotKey, want)
	}
	if gotTTL != time.Minute {
		t.Errorf("ttl %v, want 1m", gotTTL)
	}
}

func TestClaimCarriesAnEgressOptOut(t *testing.T) {
	for body, want := range map[string]bool{
		`{"template":"rt:24.04"}`:                false,
		`{"template":"rt:24.04","egress":true}`:  false,
		`{"template":"rt:24.04","egress":false}`: true,
	} {
		mgr := &fakeManager{netRoute: types.NetRouteRelay}
		ts := newTestServer(t, "", mgr, nil)
		resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		var cr types.ClaimResponse
		decodeErr := json.UnmarshalRead(resp.Body, &cr)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || decodeErr != nil || mgr.gotNoEgress != want || cr.NetRoute != types.NetRouteRelay {
			t.Errorf("%s: status %d no_egress %v route %q (%v), want 200, %v and the manager's route", body, resp.StatusCode, mgr.gotNoEgress, cr.NetRoute, decodeErr, want)
		}
	}
}

func TestClaimErrorMapping(t *testing.T) {
	tests := []struct {
		name string
		body string
		err  error
		want int
	}{
		{"bad json", `{oops`, nil, http.StatusBadRequest},
		{"bad key", `{"template":"rt:24.04","net":"lan"}`, fmt.Errorf("%w: unknown net", pool.ErrBadKey), http.StatusBadRequest},
		{"bad volume", `{"template":"rt:24.04","volumes":[{"name":"data"}]}`, fmt.Errorf("%w: unknown volume", pool.ErrBadVolume), http.StatusBadRequest},
		{"no egress", `{"template":"rt:24.04","net":"egress"}`, pool.ErrNoEgress, http.StatusConflict},
		{"volume busy", `{"template":"rt:24.04","volumes":[{"name":"data","mode":"rw"}]}`, fmt.Errorf("%w: volume %q", pool.ErrVolumeBusy, "data"), http.StatusConflict},
		{"volume needs recovery", `{"template":"rt:24.04","volumes":[{"name":"data"}]}`, fmt.Errorf("%w: volume %q", pool.ErrVolumeNeedsRecovery, "data"), http.StatusConflict},
		{"engine failure", `{"template":"rt:24.04"}`, errors.New("cocoon vm run: boom"), http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{
				claim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
					return nil, tt.err
				},
			}
			ts := newTestServer(t, "", mgr, nil)
			resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestHibernateVolumeCaptureMapsConflict(t *testing.T) {
	mgr := &fakeManager{hibernate: func(string, string) error { return pool.ErrVolumeCapture }}
	ts := newTestServer(t, "", mgr, nil)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/v1/sandboxes/sb_1/hibernate", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status %d, want 409", resp.StatusCode)
	}
}

func TestRenewGrantsAndReportsTheDeadline(t *testing.T) {
	want := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	var gotTTL time.Duration
	mgr := &fakeManager{renew: func(_, token string, ttl time.Duration) (time.Time, error) {
		gotTTL = ttl
		if token != "tok" {
			return time.Time{}, pool.ErrUnknownSandbox
		}
		return want, nil
	}}
	ts := newTestServer(t, "", mgr, nil)

	resp := postJSON(t, ts.URL+"/v1/sandboxes/sb_1/renew", "tok", `{"ttl_seconds":3600}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	var got types.RenewResponse
	if err := json.UnmarshalRead(resp.Body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.Deadline.Equal(want) {
		t.Errorf("deadline %v, want %v", got.Deadline, want)
	}
	if gotTTL != time.Hour {
		t.Errorf("ttl %v, want 1h", gotTTL)
	}
}

func TestRenewWithoutABodyTakesTheNodeDefault(t *testing.T) {
	var gotTTL time.Duration
	called := false
	mgr := &fakeManager{renew: func(_, _ string, ttl time.Duration) (time.Time, error) {
		gotTTL, called = ttl, true
		return time.Now().Add(time.Minute), nil
	}}
	ts := newTestServer(t, "", mgr, nil)

	resp := postJSON(t, ts.URL+"/v1/sandboxes/sb_1/renew", "tok", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200: an absent body is the documented way to ask for the default", resp.StatusCode)
	}
	if !called || gotTTL != 0 {
		t.Errorf("manager saw ttl %v (called=%v), want the zero that means the node default", gotTTL, called)
	}
}

func TestRenewMatchesBodyKeysCaseInsensitively(t *testing.T) {
	var gotTTL time.Duration
	mgr := &fakeManager{renew: func(_, _ string, ttl time.Duration) (time.Time, error) {
		gotTTL = ttl
		return time.Now(), nil
	}}
	ts := newTestServer(t, "", mgr, nil)

	resp := postJSON(t, ts.URL+"/v1/sandboxes/sb_1/renew", "tok", `{"TTL_Seconds":3600}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || gotTTL != time.Hour {
		t.Errorf("status %d ttl %v, want 200 and 1h", resp.StatusCode, gotTTL)
	}
}

func TestClaimMatchesBodyKeysCaseInsensitively(t *testing.T) {
	var gotKey types.PoolKey
	mgr := &fakeManager{claim: func(_ context.Context, key types.PoolKey, _ time.Duration) (*types.Sandbox, error) {
		gotKey = key
		return &types.Sandbox{ID: "sb_1"}, nil
	}}
	ts := newTestServer(t, "", mgr, nil)

	resp := postJSON(t, ts.URL+"/v1/claim", "", `{"Template":"rt:24.04"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || gotKey.Template != "rt:24.04" {
		t.Errorf("status %d template %q, want 200 and rt:24.04", resp.StatusCode, gotKey.Template)
	}
}

func TestRenewRejectsWrongToken(t *testing.T) {
	mgr := &fakeManager{renew: func(string, string, time.Duration) (time.Time, error) {
		return time.Time{}, pool.ErrUnknownSandbox
	}}
	ts := newTestServer(t, "", mgr, nil)

	resp := postJSON(t, ts.URL+"/v1/sandboxes/sb_1/renew", "wrong", `{"ttl_seconds":60}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status %d, want 404", resp.StatusCode)
	}
}

func TestInfoReportsTheNodeAdvertiseAddr(t *testing.T) {
	ts := newTestServer(t, "sekret", &fakeManager{}, nil)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/v1/info", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer sekret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	var info InfoResponse
	if err := json.UnmarshalRead(resp.Body, &info); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if info.AdvertiseAddr != "node:7777" {
		t.Errorf("advertise_addr = %q, want node:7777", info.AdvertiseAddr)
	}
}

func TestInfoListsThePromotedTemplatesOnTheWire(t *testing.T) {
	created := time.Date(2026, 9, 28, 1, 2, 3, 0, time.UTC)
	mgr := &fakeManager{infoTemplates: []pool.TemplateInfo{
		{Key: types.PoolKey{Template: "app:v1", Net: types.NetNone, Size: types.SizeSmall}, ContentDigest: "sha256:aa", CreatedAt: created, CPUCount: 1, MemTotalBytes: 512 << 20},
		{Key: types.PoolKey{Template: "app:v2", Net: types.NetNone, Size: types.SizeMedium}, ContentDigest: "sha256:bb", Tenant: "acme", CreatedAt: created, CPUCount: 2, MemTotalBytes: 1 << 30},
	}}
	ts := newTestServer(t, "sekret", mgr, nil)

	resp := doReq(t, http.MethodGet, ts.URL+"/v1/info", "sekret", "")
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := `"templates":[{"key":{"template":"app:v1","net":"none","size":"small"},"content_digest":"sha256:aa","cpu_count":1,"mem_total_bytes":536870912,"created_at":"2026-09-28T01:02:03Z"},` +
		`{"key":{"template":"app:v2","net":"none","size":"medium"},"content_digest":"sha256:bb","tenant":"acme","cpu_count":2,"mem_total_bytes":1073741824,"created_at":"2026-09-28T01:02:03Z"}]`
	if !strings.Contains(string(body), want) {
		t.Errorf("info body %s, want it to carry %s", body, want)
	}
}

func TestSetTemplateLabelsReplacesTheMapForRootOrTheTenant(t *testing.T) {
	mgr := &fakeManager{}
	ts := newTenantTestServer(t, "root", []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}, mgr, nil)
	put := func(t *testing.T, query, token, body string) int {
		t.Helper()
		resp := doReq(t, http.MethodPut, ts.URL+"/v1/templates/labels?"+query, token, body)
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	if code := put(t, "template=app:v1&size=medium", "root", `{"labels":{"v1":"sha256:aa"}}`); code != http.StatusNoContent {
		t.Fatalf("root set: %d, want 204", code)
	}
	if code := put(t, "template=app:v1", "acme-tok", `{"labels":{}}`); code != http.StatusNoContent {
		t.Fatalf("tenant clear: %d, want 204", code)
	}
	want := []string{`app:v1 medium map[v1:sha256:aa] ""`, `app:v1 small map[] "acme"`}
	if !slices.Equal(mgr.labeled, want) {
		t.Errorf("SetTemplateLabels calls %q, want %q", mgr.labeled, want)
	}
	if code := put(t, "template=app:v1", "root", `{"labels":{"a=b":"x"}}`); code != http.StatusBadRequest {
		t.Errorf("a key with '=': %d, want 400", code)
	}
	if code := put(t, "template=app:v1", "root", `{"tags":{}}`); code != http.StatusBadRequest {
		t.Errorf("an unknown field: %d, want 400", code)
	}
	for err, want := range map[error]int{pool.ErrUnknownTemplate: http.StatusNotFound, pool.ErrPooledTemplate: http.StatusConflict} {
		mgr.labelErr = err
		if code := put(t, "template=app:v1", "root", `{"labels":{}}`); code != want {
			t.Errorf("%v: %d, want %d", err, code, want)
		}
	}
	if code := put(t, "template=app:v1", "nope", `{"labels":{}}`); code != http.StatusUnauthorized {
		t.Errorf("bad token: %d, want 401", code)
	}
}

func TestEnvVerbsForRootOrTheTenant(t *testing.T) {
	mgr := &fakeManager{env: types.Env{"G": {Value: "g"}, "H": {Guest: new(false)}}}
	ts := newTenantTestServer(t, "root", []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}, mgr, nil)
	call := func(t *testing.T, method, token, body string) (int, string) {
		t.Helper()
		resp := doReq(t, method, ts.URL+"/v1/sandboxes/sb_1/env", token, body)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(raw)
	}

	if code, _ := call(t, http.MethodPut, "root", `{"env":{"GW":{"value":"Bearer a","guest":false},"G":{"value":"g"}}}`); code != http.StatusNoContent {
		t.Fatalf("root set: %d, want 204", code)
	}
	if code, _ := call(t, http.MethodDelete, "acme-tok", ""); code != http.StatusNoContent {
		t.Fatalf("tenant clear: %d, want 204", code)
	}
	want := []string{`sb_1 2 ""`, `sb_1 0 "acme"`}
	if !slices.Equal(mgr.envSet, want) {
		t.Errorf("SetEnv calls %q, want %q", mgr.envSet, want)
	}
	code, body := call(t, http.MethodGet, "acme-tok", "")
	var read SandboxEnv
	if err := json.Unmarshal([]byte(body), &read); code != http.StatusOK || err != nil || !read.Env.Equal(mgr.env) || mgr.gotTenant != "acme" {
		t.Errorf("read: %d %s as %q", code, body, mgr.gotTenant)
	}
	for body, want := range map[string]int{
		`{"vars":{}}`:                       http.StatusBadRequest,
		`{"env":{"A-B":{"value":"x"}}}`:     http.StatusBadRequest,
		`{"env":{"A":{"value":"a\nb"}}}`:    http.StatusBadRequest,
		`{"env":{"A":{"value":"a","x":1}}}`: http.StatusBadRequest,
	} {
		if code, _ := call(t, http.MethodPut, "root", body); code != want {
			t.Errorf("%s: %d, want %d", body, code, want)
		}
	}
	for err, want := range map[error]int{pool.ErrPaused: http.StatusConflict, pool.ErrUnknownSandbox: http.StatusNotFound} {
		mgr.envErr = err
		if code, _ := call(t, http.MethodPut, "root", `{"env":{}}`); code != want {
			t.Errorf("%v: %d, want %d", err, code, want)
		}
	}
	if code, _ := call(t, http.MethodPut, "sb-token", `{"env":{}}`); code != http.StatusUnauthorized {
		t.Errorf("a sandbox token: %d, want 401", code)
	}
}

func TestEnvPatchVerb(t *testing.T) {
	mgr := &fakeManager{}
	ts := newTenantTestServer(t, "root", []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}, mgr, nil)
	call := func(t *testing.T, token, body string) int {
		t.Helper()
		resp := doReq(t, http.MethodPatch, ts.URL+"/v1/sandboxes/sb_1/env", token, body)
		resp.Body.Close()
		return resp.StatusCode
	}

	if code := call(t, "root", `{"env":{"GW":{"value":"Bearer a","guest":false},"OLD":null}}`); code != http.StatusNoContent {
		t.Fatalf("root patch: %d, want 204", code)
	}
	if gw := mgr.gotPatch["GW"]; gw == nil || gw.Value != "Bearer a" || gw.InGuest() || mgr.gotPatch["OLD"] != nil || len(mgr.gotPatch) != 2 {
		t.Errorf("manager saw %v", mgr.gotPatch)
	}
	if code := call(t, "acme-tok", `{"env":{"M":{"value":"x"}}}`); code != http.StatusNoContent {
		t.Fatalf("tenant patch: %d, want 204", code)
	}
	if want := []string{`sb_1 [GW OLD] ""`, `sb_1 [M] "acme"`}; !slices.Equal(mgr.envPatched, want) {
		t.Errorf("PatchEnv calls %q, want %q", mgr.envPatched, want)
	}
	for body, want := range map[string]int{
		`{"vars":{}}`:                              http.StatusBadRequest,
		`{"env":{"A-B":null}}`:                     http.StatusBadRequest,
		`{"env":{"A":{"value":"a\nb"}}}`:           http.StatusBadRequest,
		`{"env":{"A":{"value":"a","x":1}}}`:        http.StatusBadRequest,
		`{"env":{"A":{"value":"s","guest":null}}}`: http.StatusBadRequest,
	} {
		if code := call(t, "root", body); code != want {
			t.Errorf("%s: %d, want %d", body, code, want)
		}
	}
	if len(mgr.envPatched) != 2 {
		t.Errorf("refused bodies reached the manager: %q", mgr.envPatched)
	}
	for err, want := range map[error]int{pool.ErrPaused: http.StatusConflict, pool.ErrUnknownSandbox: http.StatusNotFound, pool.ErrBadEnv: http.StatusBadRequest} {
		mgr.envErr = err
		if code := call(t, "root", `{"env":{}}`); code != want {
			t.Errorf("%v: %d, want %d", err, code, want)
		}
	}
	if code := call(t, "sb-token", `{"env":{}}`); code != http.StatusUnauthorized {
		t.Errorf("a sandbox token: %d, want 401", code)
	}
}

func TestClaimRefusesAnAmbiguousHostOnlyFlag(t *testing.T) {
	for _, entry := range []string{`{"value":"s","guset":false}`, `{"value":"s","host_only":true}`, `{"value":"s","guest":null}`} {
		for _, path := range []string{"/v1/claim", "/v1/checkpoints/ck_00000000000000aa/claim"} {
			mgr := &fakeManager{claimCheckpoint: func(string) (*types.Sandbox, error) { return &types.Sandbox{ID: "sb_1"}, nil }}
			ts := newTestServer(t, "sekret", mgr, nil)
			resp := postJSON(t, ts.URL+path, "sekret", `{"template":"rt:24.04","env":{"KEY":`+entry+`}}`)
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest || mgr.warmCalls+mgr.provisionCalls != 0 || mgr.gotEnv != nil {
				t.Errorf("%s %s: %d, manager called %d times, want 400 and none", path, entry, resp.StatusCode, mgr.warmCalls+mgr.provisionCalls)
			}
		}
	}
}

func TestAPITokenGuard(t *testing.T) {
	ts := newTestServer(t, "sekret", &fakeManager{}, nil)

	tests := []struct {
		name   string
		path   string
		method string
		auth   string
		want   int
	}{
		{"claim no token", "/v1/claim", http.MethodPost, "", http.StatusUnauthorized},
		{"claim wrong token", "/v1/claim", http.MethodPost, "Bearer nope", http.StatusUnauthorized},
		{"claim right token", "/v1/claim", http.MethodPost, "Bearer sekret", http.StatusOK},
		{"volumes no token", "/v1/volumes", http.MethodGet, "", http.StatusUnauthorized},
		{"volumes right token", "/v1/volumes", http.MethodGet, "Bearer sekret", http.StatusOK},
		{"sandboxes no token", "/v1/sandboxes", http.MethodGet, "", http.StatusUnauthorized},
		{"sandboxes right token", "/v1/sandboxes", http.MethodGet, "Bearer sekret", http.StatusOK},
		{"info no token", "/v1/info", http.MethodGet, "", http.StatusUnauthorized},
		{"info right token", "/v1/info", http.MethodGet, "Bearer sekret", http.StatusOK},
		{"put pools no token", "/v1/pools", http.MethodPut, "", http.StatusUnauthorized},
		{"put pools right token", "/v1/pools", http.MethodPut, "Bearer sekret", http.StatusOK},
		{"healthz open", "/healthz", http.MethodGet, "", http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body io.Reader
			switch tt.method {
			case http.MethodPost:
				body = strings.NewReader(`{"template":"rt:24.04"}`)
			case http.MethodPut:
				body = strings.NewReader(`{"pools":[{"template":"rt:24.04","net":"none","size":"small","warm":1}]}`)
			}
			req, err := http.NewRequestWithContext(t.Context(), tt.method, ts.URL+tt.path, body)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestVolumeCatalogReturnsScopedFleetProjection(t *testing.T) {
	mgr := &fakeManager{volumeCatalog: func(tenant string, holders map[string]int) []types.VolumeInfo {
		if tenant != "acme" {
			t.Errorf("tenant = %q, want acme", tenant)
		}
		if holders["imagenet"] != 3 {
			t.Errorf("imagenet holders = %d, want 3", holders["imagenet"])
		}
		return []types.VolumeInfo{{
			Name: "imagenet", DefaultMount: "/volumes/imagenet", SizeBytes: 42, Available: true, Nodes: 3,
		}}
	}}
	placer := &fakePlacer{volumeHolders: map[string]int{"imagenet": 3}}
	mgr.tenants = []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}
	srv := New("root", "node:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/v1/volumes", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer acme-tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("volumes: %v", err)
	}
	defer resp.Body.Close()
	var got types.VolumeListResponse
	if err := json.UnmarshalRead(resp.Body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := []types.VolumeInfo{{
		Name: "imagenet", DefaultMount: "/volumes/imagenet", SizeBytes: 42, Available: true, Nodes: 3,
	}}
	if !slices.Equal(got.Volumes, want) {
		t.Errorf("volumes = %+v, want %+v", got.Volumes, want)
	}
}

func TestSandboxIndexReturnsScopedAppliedVolumes(t *testing.T) {
	mgr := &fakeManager{sandboxIndex: func(tenant, _ string) []pool.SandboxSummary {
		if tenant != "acme" {
			t.Errorf("tenant = %q, want acme", tenant)
		}
		return []pool.SandboxSummary{{
			ID: "sb_1", Volumes: []types.Volume{{Name: "imagenet", Mount: "/datasets/imagenet"}},
		}}
	}}
	ts := newTenantTestServer(t, "root", []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}, mgr, nil)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/v1/sandboxes", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer acme-tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("sandboxes: %v", err)
	}
	defer resp.Body.Close()
	var got SandboxListResponse
	if err := json.UnmarshalRead(resp.Body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := []types.Volume{{Name: "imagenet", Mount: "/datasets/imagenet"}}
	if len(got.Sandboxes) != 1 || got.Sandboxes[0].ID != "sb_1" || !slices.Equal(got.Sandboxes[0].Volumes, want) {
		t.Errorf("sandboxes = %+v, want sb_1 with %+v", got.Sandboxes, want)
	}
}

func TestSandboxesReportClaimedAt(t *testing.T) {
	claimed := time.Date(2026, 9, 23, 1, 2, 3, 0, time.UTC)
	mgr := &fakeManager{sandboxIndex: func(string, string) []pool.SandboxSummary {
		return []pool.SandboxSummary{{ID: "sb_1", ClaimedAt: claimed}}
	}}
	ts := newTenantTestServer(t, "root", nil, mgr, nil)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/v1/sandboxes", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer root")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("sandboxes: %v", err)
	}
	defer resp.Body.Close()
	var got SandboxListResponse
	if err := json.UnmarshalRead(resp.Body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Sandboxes) != 1 || !got.Sandboxes[0].ClaimedAt.Equal(claimed) {
		t.Errorf("sandboxes = %+v, want claimed_at %v", got.Sandboxes, claimed)
	}
}

func TestSandboxByIDCarriesTheTokenOnlyForTheRootCredential(t *testing.T) {
	mgr := &fakeManager{sandboxByID: func(string) (pool.SandboxSummary, bool) {
		return pool.SandboxSummary{ID: "sb_1", Token: "victim"}, true
	}}
	for _, tt := range []struct {
		name, apiToken, bearer, want string
	}{
		{"open node", "", "", ""},
		{"root", "root", "root", "victim"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ts := newTestServer(t, tt.apiToken, mgr, nil)
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/v1/sandboxes/sb_1", nil)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			if tt.bearer != "" {
				req.Header.Set("Authorization", "Bearer "+tt.bearer)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("sandbox: %v", err)
			}
			defer resp.Body.Close()
			var got pool.SandboxSummary
			if err := json.UnmarshalRead(resp.Body, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if resp.StatusCode != http.StatusOK || got.Token != tt.want {
				t.Errorf("status %d token %q, want 200 and %q", resp.StatusCode, got.Token, tt.want)
			}
		})
	}
}

func TestSandboxesNarrowToAClaimRef(t *testing.T) {
	for _, tt := range []struct {
		path string
		want string
	}{
		{"/v1/sandboxes?claim_ref=team-a%2Fdemo", "team-a/demo"},
		{"/v1/sandboxes", ""},
	} {
		got := "unset"
		mgr := &fakeManager{sandboxIndex: func(_, claimRef string) []pool.SandboxSummary {
			got = claimRef
			return nil
		}}
		ts := newTenantTestServer(t, "root", nil, mgr, nil)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+tt.path, nil)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer root")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("sandboxes: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || got != tt.want {
			t.Errorf("%s: status %d, manager saw claim_ref %q, want %q", tt.path, resp.StatusCode, got, tt.want)
		}
	}
}

func TestTenantAuthMatrix(t *testing.T) {
	mgr := &fakeManager{tenantClaims: map[string]int{"acme": 2}}
	tenants := []config.TenantSpec{{Name: "acme", Token: "acme-tok"}, {Name: "beta", Token: "beta-tok"}}
	ts := newTenantTestServer(t, "sekret", tenants, mgr, nil)

	do := func(t *testing.T, method, path, auth string) *http.Response {
		t.Helper()
		var body io.Reader
		switch {
		case path == "/v1/claim":
			body = strings.NewReader(`{"template":"rt:24.04"}`)
		case method == http.MethodPut:
			body = strings.NewReader(`{"pools":[]}`)
		}
		req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, body)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		if auth != "" {
			req.Header.Set("Authorization", "Bearer "+auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		return resp
	}

	for _, tt := range []struct {
		name, method, path, auth string
		want                     int
		wantTenant               string
	}{
		{"root claims", http.MethodPost, "/v1/claim", "sekret", http.StatusOK, ""},
		{"tenant claims", http.MethodPost, "/v1/claim", "acme-tok", http.StatusOK, "acme"},
		{"second tenant resolves its own name", http.MethodPost, "/v1/claim", "beta-tok", http.StatusOK, "beta"},
		{"wrong token", http.MethodPost, "/v1/claim", "nope", http.StatusUnauthorized, ""},
		{"missing token", http.MethodPost, "/v1/claim", "", http.StatusUnauthorized, ""},
		{"tenant lists own checkpoints", http.MethodGet, "/v1/checkpoints", "acme-tok", http.StatusOK, "acme"},
		{"root lists all checkpoints", http.MethodGet, "/v1/checkpoints", "sekret", http.StatusOK, ""},
		{"tenant lists visible volumes", http.MethodGet, "/v1/volumes", "acme-tok", http.StatusOK, "acme"},
		{"root lists all volumes", http.MethodGet, "/v1/volumes", "sekret", http.StatusOK, ""},
		{"tenant lists own sandboxes", http.MethodGet, "/v1/sandboxes", "acme-tok", http.StatusOK, "acme"},
		{"root lists all sandboxes", http.MethodGet, "/v1/sandboxes", "sekret", http.StatusOK, ""},
		{"tenant forbidden on info", http.MethodGet, "/v1/info", "acme-tok", http.StatusForbidden, ""},
		{"tenant forbidden on metrics", http.MethodGet, "/metrics", "acme-tok", http.StatusForbidden, ""},
		{"tenant forbidden on pools", http.MethodPut, "/v1/pools", "acme-tok", http.StatusForbidden, ""},
		{"tenant forbidden on checkpoint blob", http.MethodGet, "/v1/checkpoints/ck_00000000000000aa/blob", "acme-tok", http.StatusForbidden, ""},
		{"wrong token on info stays 401", http.MethodGet, "/v1/info", "nope", http.StatusUnauthorized, ""},
		{"root reads info", http.MethodGet, "/v1/info", "sekret", http.StatusOK, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mgr.gotTenant = "unset"
			resp := do(t, tt.method, tt.path, tt.auth)
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status %d, want %d", resp.StatusCode, tt.want)
			}
			reachedManager := resp.StatusCode == http.StatusOK && slices.Contains(
				[]string{"/v1/claim", "/v1/checkpoints", "/v1/volumes", "/v1/sandboxes"}, tt.path)
			if reachedManager && mgr.gotTenant != tt.wantTenant {
				t.Errorf("manager saw tenant %q, want %q", mgr.gotTenant, tt.wantTenant)
			}
		})
	}
}

func TestMetricsTenantGauge(t *testing.T) {
	mgr := &fakeManager{tenantClaims: map[string]int{"acme": 2, "beta": 0}}
	ts := newTestServer(t, "", mgr, nil)

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	body := string(raw)
	if !strings.Contains(body, `sandboxd_tenant_claims{tenant="acme"} 2`) ||
		!strings.Contains(body, `sandboxd_tenant_claims{tenant="beta"} 0`) {
		t.Errorf("tenant gauge missing:\n%s", body)
	}
}

func TestPutPoolsRejectsUnknownFields(t *testing.T) {
	ts := newTestServer(t, "sekret", &fakeManager{}, nil)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, ts.URL+"/v1/pools",
		strings.NewReader(`{"pools":[{"template":"rt:24.04","net":"none","size":"small","egres":{"allow":[]}}]}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer sekret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", resp.StatusCode)
	}
}

func TestInstanceMetadataVerb(t *testing.T) {
	var got []byte
	var fail error
	mgr := &fakeManager{setInstanceMetadata: func(id string, doc []byte) error {
		if id != "sb_1" {
			return pool.ErrUnknownSandbox
		}
		got = doc
		return fail
	}}
	ts := newTenantTestServer(t, "sekret", []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}, mgr, nil)
	put := func(token, id, body string) int {
		t.Helper()
		resp := doReq(t, http.MethodPut, ts.URL+"/v1/sandboxes/"+id+"/instance-metadata", token, body)
		resp.Body.Close()
		return resp.StatusCode
	}
	valid := `{"region": "local", "tags": ["a", "b"]}`
	if code := put("sekret", "sb_1", valid); code != http.StatusNoContent {
		t.Fatalf("status %d, want 204", code)
	}
	if string(got) != valid {
		t.Errorf("manager got %s, want the body verbatim", got)
	}
	for _, tt := range []struct {
		name, token, id, body string
		want                  int
	}{
		{"tenant token", "acme-tok", "sb_1", valid, http.StatusForbidden},
		{"array", "sekret", "sb_1", `[1]`, http.StatusBadRequest},
		{"null", "sekret", "sb_1", `null`, http.StatusBadRequest},
		{"not JSON", "sekret", "sb_1", `{"a":`, http.StatusBadRequest},
		{"over 4 KiB", "sekret", "sb_1", `{"a":"` + strings.Repeat("x", 4<<10) + `"}`, http.StatusBadRequest},
		{"unknown sandbox", "sekret", "sb_2", valid, http.StatusNotFound},
	} {
		if code := put(tt.token, tt.id, tt.body); code != tt.want {
			t.Errorf("%s: status %d, want %d", tt.name, code, tt.want)
		}
	}
	for _, err := range []error{pool.ErrPaused, pool.ErrArchived, pool.ErrNoInstanceMetadata} {
		fail = err
		if code := put("sekret", "sb_1", valid); code != http.StatusConflict {
			t.Errorf("%v: status %d, want 409", err, code)
		}
	}
}

func TestDrainEndpoints(t *testing.T) {
	mgr := &fakeManager{}
	ts := newTestServer(t, "sekret", mgr, nil)
	for _, tt := range []struct {
		method   string
		draining bool
	}{
		{http.MethodPost, true},
		{http.MethodDelete, false},
	} {
		req, err := http.NewRequestWithContext(t.Context(), tt.method, ts.URL+"/v1/drain", nil)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer sekret")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		var info InfoResponse
		if err := json.UnmarshalRead(resp.Body, &info); err != nil {
			t.Fatalf("decode: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || info.Draining != tt.draining || mgr.draining != tt.draining {
			t.Fatalf("%s: status=%d draining=%v mgr=%v, want 200/%v", tt.method, resp.StatusCode, info.Draining, mgr.draining, tt.draining)
		}
	}
}

func TestPutPoolsUpdatesTargets(t *testing.T) {
	var got []config.PoolSpec
	mgr := &fakeManager{
		setPools: func(pools []config.PoolSpec) error {
			got = pools
			return nil
		},
		infoPools: []pool.PoolInfo{{
			Key:    types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall},
			Target: 2,
			Warm:   1,
			Golden: true,
		}},
	}
	ts := newTestServer(t, "sekret", mgr, nil)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, ts.URL+"/v1/pools",
		strings.NewReader(`{"pools":[{"template":"rt:24.04","net":"none","size":"small","warm":2}]}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer sekret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	if len(got) != 1 || got[0].Template != "rt:24.04" || got[0].Warm != 2 {
		t.Fatalf("SetPools got %+v", got)
	}
	var out InfoResponse
	if err := json.UnmarshalRead(resp.Body, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Pools) != 1 || out.Pools[0].Target != 2 || !out.Pools[0].Golden {
		t.Fatalf("info response %+v, want updated pool", out)
	}
}

func TestSandboxVerbFlows(t *testing.T) {
	verbs := []struct {
		name string
		hook func(f *fakeManager, h func(id, token string) error)
	}{
		{"release", func(f *fakeManager, h func(id, token string) error) { f.release = h }},
		{"hibernate", func(f *fakeManager, h func(id, token string) error) { f.hibernate = h }},
	}
	tests := []struct {
		name string
		auth string
		err  error
		want int
	}{
		{"ok", "Bearer tok", nil, http.StatusNoContent},
		{"unknown or bad token", "Bearer bad", pool.ErrUnknownSandbox, http.StatusNotFound},
		{"missing bearer", "", nil, http.StatusUnauthorized},
		{"engine failure", "Bearer tok", errors.New("engine failed"), http.StatusInternalServerError},
	}
	for _, v := range verbs {
		for _, tt := range tests {
			t.Run(v.name+"/"+tt.name, func(t *testing.T) {
				var gotID, gotToken string
				mgr := &fakeManager{}
				v.hook(mgr, func(id, token string) error {
					gotID, gotToken = id, token
					return tt.err
				})
				ts := newTestServer(t, "", mgr, nil)
				req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/v1/sandboxes/sb_1/"+v.name, nil)
				if err != nil {
					t.Fatalf("request: %v", err)
				}
				if tt.auth != "" {
					req.Header.Set("Authorization", tt.auth)
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("do: %v", err)
				}
				defer resp.Body.Close()
				if resp.StatusCode != tt.want {
					t.Errorf("status %d, want %d", resp.StatusCode, tt.want)
				}
				wantToken := strings.TrimPrefix(tt.auth, "Bearer ")
				if tt.auth != "" && (gotID != "sb_1" || gotToken != wantToken) {
					t.Errorf("%s called with (%q, %q), want (sb_1, %q)", v.name, gotID, gotToken, wantToken)
				}
			})
		}
	}
}

func TestReleaseOperatorToken(t *testing.T) {
	const rootTok, sbTok = "sekret", "sb-secret"
	tenants := []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}
	tests := []struct {
		name        string
		auth        string
		wantOp      bool
		wantRelease bool
		wantToken   string
		releaseErr  error
		want        int
	}{
		{"root token releases by id", "Bearer " + rootTok, true, false, "", nil, http.StatusNoContent},
		{"sandbox token releases self", "Bearer " + sbTok, false, true, sbTok, nil, http.StatusNoContent},
		{"tenant token gets no operator release", "Bearer acme-tok", false, true, "acme-tok", pool.ErrUnknownSandbox, http.StatusNotFound},
		{"wrong token 404s", "Bearer nope", false, true, "nope", pool.ErrUnknownSandbox, http.StatusNotFound},
		{"missing bearer", "", false, false, "", nil, http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opID, relID, relToken string
			var opCalled, relCalled bool
			mgr := &fakeManager{
				releaseOp: func(id string) error {
					opCalled, opID = true, id
					return nil
				},
				release: func(id, token string) error {
					relCalled, relID, relToken = true, id, token
					return tt.releaseErr
				},
			}
			ts := newTenantTestServer(t, rootTok, tenants, mgr, nil)
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/v1/sandboxes/sb_1/release", nil)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status %d, want %d", resp.StatusCode, tt.want)
			}
			if opCalled != tt.wantOp {
				t.Errorf("operator release called=%v, want %v", opCalled, tt.wantOp)
			}
			if relCalled != tt.wantRelease {
				t.Errorf("Release called=%v, want %v", relCalled, tt.wantRelease)
			}
			if tt.wantOp && opID != "sb_1" {
				t.Errorf("operator release id=%q, want sb_1", opID)
			}
			if tt.wantRelease && (relID != "sb_1" || relToken != tt.wantToken) {
				t.Errorf("Release(%q, %q), want (sb_1, %q)", relID, relToken, tt.wantToken)
			}
		})
	}
}

func TestAgentErrorPaths(t *testing.T) {
	tests := []struct {
		name    string
		auth    string
		upgrade string
		sockErr error
		dialErr error
		want    int
	}{
		{"missing bearer", "", "silkd", nil, nil, http.StatusUnauthorized},
		{"unknown sandbox", "Bearer tok", "silkd", pool.ErrUnknownSandbox, nil, http.StatusNotFound},
		{"no upgrade header", "Bearer tok", "", nil, nil, http.StatusUpgradeRequired},
		{"guest unreachable", "Bearer tok", "silkd", nil, errors.New("connection refused"), http.StatusBadGateway},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{socket: func(string, string) (string, error) { return "/v/sock", tt.sockErr }}
			dialer := &fakeDialer{dial: func(context.Context, string) (net.Conn, error) {
				if tt.dialErr != nil {
					return nil, tt.dialErr
				}
				c, _ := net.Pipe()
				return c, nil
			}}
			ts := newTestServer(t, "", mgr, dialer)
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/v1/sandboxes/sb_1/agent", nil)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			if tt.upgrade != "" {
				req.Header.Set("Upgrade", tt.upgrade)
				req.Header.Set("Connection", "Upgrade")
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestForkFlow(t *testing.T) {
	mgr := &fakeManager{fork: func(_, token string, count int, ttl time.Duration) ([]*types.Sandbox, error) {
		switch {
		case token != "tok":
			return nil, pool.ErrUnknownSandbox
		case count > 16:
			return nil, fmt.Errorf("%w: %d", pool.ErrBadCount, count)
		}
		children := make([]*types.Sandbox, count)
		for i := range children {
			children[i] = &types.Sandbox{ID: fmt.Sprintf("sb_c%d", i), Token: "ct", Deadline: time.Unix(42, 0).UTC()}
		}
		if ttl != time.Minute {
			t.Errorf("ttl %v, want 1m", ttl)
		}
		return children, nil
	}}

	ts := newTestServer(t, "sekret", mgr, nil)

	post := func(auth, body string) *http.Response {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/v1/sandboxes/sb_1/fork", strings.NewReader(body))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		return resp
	}

	resp := post("Bearer sekret", `{"token":"tok","count":2,"ttl_seconds":60}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	var fr types.ForkResponse
	if err := json.UnmarshalRead(resp.Body, &fr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(fr.Children) != 2 || fr.Children[0].ID != "sb_c0" || fr.Children[0].OwnerAddr != "node:7777" {
		t.Errorf("children %+v, want two with this node as owner", fr.Children)
	}

	for _, tt := range []struct {
		name, auth, body string
		want             int
	}{
		{"bad count", "Bearer sekret", `{"token":"tok","count":17,"ttl_seconds":60}`, http.StatusBadRequest},
		{"bad body", "Bearer sekret", `{oops`, http.StatusBadRequest},
		{"unknown or bad sandbox token", "Bearer sekret", `{"token":"bad","count":1}`, http.StatusNotFound},
		{"missing api token", "", `{"token":"tok","count":1}`, http.StatusUnauthorized},
		{"sandbox token is no api token", "Bearer tok", `{"token":"tok","count":1}`, http.StatusUnauthorized},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resp := post(tt.auth, tt.body)
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestPromoteAndDeleteTemplateFlow(t *testing.T) {
	var gotKey types.PoolKey
	mgr := &fakeManager{
		promoteContentDigest: "sha256:promoted-digest",
		promote: func(_, token, template string) error {
			switch {
			case token != "tok":
				return pool.ErrUnknownSandbox
			case template == "_bad":
				return fmt.Errorf("%w: bad template", pool.ErrBadKey)
			case template == "pooled":
				return pool.ErrPooledTemplate
			}
			return nil
		},
		deleteGolden: func(key types.PoolKey) error {
			gotKey = key
			if key.Template == "nope" {
				return pool.ErrUnknownTemplate
			}
			return nil
		},
	}
	ts := newTestServer(t, "sekret", mgr, nil)

	promote := func(auth, body string) (int, types.PromoteResponse) {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/v1/sandboxes/sb_1/promote", strings.NewReader(body))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		defer resp.Body.Close()
		var result types.PromoteResponse
		if resp.StatusCode == http.StatusOK {
			if err := json.UnmarshalRead(resp.Body, &result); err != nil {
				t.Fatalf("decode promote response: %v", err)
			}
		}
		return resp.StatusCode, result
	}
	for _, tt := range []struct {
		name, auth, body string
		want             int
	}{
		{"ok", "Bearer sekret", `{"token":"tok","template":"tpl:x"}`, http.StatusOK},
		{"bad name", "Bearer sekret", `{"token":"tok","template":"_bad"}`, http.StatusBadRequest},
		{"pooled", "Bearer sekret", `{"token":"tok","template":"pooled"}`, http.StatusConflict},
		{"bad sandbox token", "Bearer sekret", `{"token":"bad","template":"tpl:x"}`, http.StatusNotFound},
		{"missing api token", "", `{"token":"tok","template":"tpl:x"}`, http.StatusUnauthorized},
	} {
		t.Run("promote/"+tt.name, func(t *testing.T) {
			got, result := promote(tt.auth, tt.body)
			if got != tt.want {
				t.Errorf("status %d, want %d", got, tt.want)
			}
			if got == http.StatusOK && result.ContentDigest != "sha256:promoted-digest" {
				t.Errorf("content digest %q, want sha256:promoted-digest", result.ContentDigest)
			}
		})
	}

	del := func(auth, query string) int {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodDelete, ts.URL+"/v1/templates?"+query, nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if got := del("Bearer sekret", "template=tpl:x&net=none&size=small"); got != http.StatusNoContent {
		t.Errorf("delete status %d, want 204", got)
	}
	want := types.PoolKey{Template: "tpl:x", Net: types.NetNone, Size: types.SizeSmall}
	if gotKey != want {
		t.Errorf("delete key %+v, want %+v (claim defaults applied)", gotKey, want)
	}
	if got := del("Bearer sekret", "template=nope"); got != http.StatusNotFound {
		t.Errorf("unknown delete status %d, want 404", got)
	}

	if got := del("", "template=tpl:x"); got != http.StatusUnauthorized {
		t.Errorf("unauthenticated delete status %d, want 401", got)
	}
}

func TestCheckpointFlow(t *testing.T) {
	mgr := &fakeManager{
		checkpoint: func(id, token, name string) (types.Checkpoint, error) {
			if id != "sb_1" || token != "tok" {
				return types.Checkpoint{}, pool.ErrUnknownSandbox
			}
			return types.Checkpoint{ID: "ck_0011223344556677", Name: name, SandboxID: id}, nil
		},
		claimCheckpoint: func(ckptID string) (*types.Sandbox, error) {
			if ckptID != "ck_0011223344556677" {
				return nil, pool.ErrUnknownCheckpoint
			}
			return &types.Sandbox{ID: "sb_branch", Token: "btok", FromCheckpoint: ckptID}, nil
		},
		deleteCheckpoint: func(ckptID string) error {
			if ckptID != "ck_0011223344556677" {
				return pool.ErrUnknownCheckpoint
			}
			return nil
		},
	}
	ts := newTestServer(t, "api", mgr, &fakeDialer{})

	post := func(path, body string) *http.Response {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer api")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		return resp
	}

	resp := post("/v1/sandboxes/sb_1/checkpoint", `{"token":"tok","name":"step-1"}`)
	defer resp.Body.Close()
	var cr types.CheckpointResponse
	if err := json.UnmarshalRead(resp.Body, &cr); err != nil || cr.Checkpoint.ID != "ck_0011223344556677" {
		t.Fatalf("checkpoint: status %d, %+v, %v", resp.StatusCode, cr, err)
	}
	if cr.Checkpoint.Name != "step-1" {
		t.Errorf("name %q, want step-1", cr.Checkpoint.Name)
	}

	resp2 := post("/v1/checkpoints/"+cr.Checkpoint.ID+"/claim", `{}`)
	defer resp2.Body.Close()
	var claim types.ClaimResponse
	if err := json.UnmarshalRead(resp2.Body, &claim); err != nil || claim.ID != "sb_branch" {
		t.Fatalf("claim from checkpoint: status %d, %+v, %v", resp2.StatusCode, claim, err)
	}

	resp3 := post("/v1/checkpoints/ck_ffffffffffffffff/claim", `{}`)
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusNotFound {
		t.Errorf("unknown checkpoint claim: status %d, want 404", resp3.StatusCode)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, ts.URL+"/v1/checkpoints/"+cr.Checkpoint.ID, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer api")
	resp4, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	defer resp4.Body.Close()
	if resp4.StatusCode != http.StatusNoContent {
		t.Errorf("delete checkpoint: status %d, want 204", resp4.StatusCode)
	}
}

func TestDeleteCheckpointNoForwardQueryParam(t *testing.T) {
	mgr := &fakeManager{deleteCheckpoint: func(string) error { return nil }}
	ts := newTestServer(t, "api", mgr, &fakeDialer{})

	del := func(path string) int {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, ts.URL+path, nil)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer api")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if got := del("/v1/checkpoints/ck_0011223344556677"); got != http.StatusNoContent {
		t.Fatalf("delete: status %d, want 204", got)
	}
	if mgr.gotNoForward {
		t.Error("gotNoForward = true without the query param")
	}
	if got := del("/v1/checkpoints/ck_0011223344556677?no_forward=1"); got != http.StatusNoContent {
		t.Fatalf("delete with no_forward: status %d, want 204", got)
	}
	if !mgr.gotNoForward {
		t.Error("gotNoForward = false with no_forward=1 set")
	}
}

func TestDeleteCheckpointForgetsProbeCache(t *testing.T) {
	prober := &fakeProber{}

	t.Run("original delete", func(t *testing.T) {
		prober.forgotten = nil
		mgr := &fakeManager{deleteCheckpoint: func(string) error { return nil }}
		ts := newPlacerTestServer(t, "api", mgr, prober)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, ts.URL+"/v1/checkpoints/ck_0011223344556677", nil)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer api")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
		defer resp.Body.Close()
		if len(prober.forgotten) != 1 || prober.forgotten[0] != "ck_0011223344556677" {
			t.Errorf("forgotten = %v, want [ck_0011223344556677]", prober.forgotten)
		}
	})

	t.Run("forwarded delete finding nothing locally", func(t *testing.T) {
		prober.forgotten = nil
		mgr := &fakeManager{deleteCheckpoint: func(string) error { return pool.ErrUnknownCheckpoint }}
		ts := newPlacerTestServer(t, "api", mgr, prober)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, ts.URL+"/v1/checkpoints/ck_0011223344556677?no_forward=1", nil)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer api")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
		defer resp.Body.Close()
		if len(prober.forgotten) != 1 || prober.forgotten[0] != "ck_0011223344556677" {
			t.Errorf("forgotten = %v, want [ck_0011223344556677] even though nothing was held locally", prober.forgotten)
		}
	})
}

func TestClaimRedirectsOnWarmMiss(t *testing.T) {
	provisioned := false
	mgr := &fakeManager{claim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
		provisioned = true
		return &types.Sandbox{ID: "sb_local"}, nil
	}}
	srv := New("", "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{addrs: []string{"node-b:7777", "node-c:7777"}}, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

	resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(`{"template":"rt:24.04"}`))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer resp.Body.Close()
	var cr types.ClaimResponse
	if err := json.UnmarshalRead(resp.Body, &cr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(cr.Redirect) != 2 || cr.Redirect[0] != "node-b:7777" {
		t.Errorf("redirect %v, want [node-b:7777 node-c:7777]", cr.Redirect)
	}
	if cr.ID != "" {
		t.Errorf("redirect carried a sandbox id %q", cr.ID)
	}
	if provisioned {
		t.Error("provisioned locally despite an available peer")
	}
}

func TestClaimRedirectsToTemplateOwner(t *testing.T) {
	for _, tt := range []struct {
		name         string
		hasGolden    bool
		wantRedirect bool
	}{
		{"no local golden redirects", false, true},
		{"local golden provisions", true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			provisioned := false
			mgr := &fakeManager{
				hasGolden: tt.hasGolden,
				claim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
					provisioned = true
					return &types.Sandbox{ID: "sb_local", Token: "tok"}, nil
				},
			}
			srv := New("", "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{owners: []string{"node-b:7777"}}, nil, nil, nil)
			ts := httptest.NewServer(srv.Handler())
			t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

			resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(`{"template":"tpl"}`))
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			defer resp.Body.Close()
			var cr types.ClaimResponse
			if err := json.UnmarshalRead(resp.Body, &cr); err != nil {
				t.Fatalf("decode: %v", err)
			}
			gotRedirect := len(cr.Redirect) > 0
			if gotRedirect != tt.wantRedirect {
				t.Errorf("redirect %v, want redirect=%v", cr.Redirect, tt.wantRedirect)
			}
			if provisioned == tt.wantRedirect {
				t.Errorf("provisioned=%v with wantRedirect=%v", provisioned, tt.wantRedirect)
			}
		})
	}
}

func TestATemplateDigestGatesDeleteAndLabelWrites(t *testing.T) {
	mgr := &fakeManager{deleteGolden: func(types.PoolKey) error { return pool.ErrTemplateReplaced }}
	ts := newTestServer(t, "sekret", mgr, nil)

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/templates?template=ns%2Fapp&digest=sha256%3Aold", nil)
	req.Header.Set("Authorization", "Bearer sekret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPreconditionFailed || mgr.gotDigest != "sha256:old" {
		t.Errorf("status=%d digest=%q, want 412 for a replaced generation and the digest passed through", resp.StatusCode, mgr.gotDigest)
	}

	mgr.labelErr, mgr.gotDigest = pool.ErrTemplateReplaced, ""
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/templates/labels?template=ns%2Fapp&digest=sha256%3Aold", strings.NewReader(`{"labels":{"v1":"sha256:new"}}`))
	req.Header.Set("Authorization", "Bearer sekret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPreconditionFailed || mgr.gotDigest != "sha256:old" {
		t.Errorf("labels: status=%d digest=%q, want 412 for a replaced generation and the digest passed through", resp.StatusCode, mgr.gotDigest)
	}
}

func TestDeleteTemplateRedirectsToOwner(t *testing.T) {
	mgr := &fakeManager{}
	srv := New("", "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{owners: []string{"node-b:7777"}}, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, ts.URL+"/v1/templates?template=tpl", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200 redirect", resp.StatusCode)
	}
	var cr types.ClaimResponse
	if decodeErr := json.UnmarshalRead(resp.Body, &cr); decodeErr != nil {
		t.Fatalf("decode: %v", decodeErr)
	}
	if len(cr.Redirect) != 1 || cr.Redirect[0] != "node-b:7777" {
		t.Errorf("redirect %v, want [node-b:7777]", cr.Redirect)
	}

	req2, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, ts.URL+"/v1/templates?template=tpl&no_redirect=1", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("status %d, want 404 with no_redirect despite known owners", resp2.StatusCode)
	}

	srvNoOwner := New("", "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{}, nil, nil, nil)
	ts2 := httptest.NewServer(srvNoOwner.Handler())
	t.Cleanup(func() { ts2.Close(); srvNoOwner.CloseRelays() })
	req3, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, ts2.URL+"/v1/templates?template=tpl", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusNotFound {
		t.Errorf("status %d, want 404 when no owner known", resp3.StatusCode)
	}
}

func TestClaimProvisionsWhenNoCandidate(t *testing.T) {
	mgr := &fakeManager{claim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
		return &types.Sandbox{ID: "sb_local", Token: "tok"}, nil
	}}
	srv := New("", "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{addrs: nil}, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

	resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(`{"template":"rt:24.04","claim_ref":"ns1/w1"}`))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer resp.Body.Close()
	var cr types.ClaimResponse
	_ = json.UnmarshalRead(resp.Body, &cr)
	if cr.ID != "sb_local" || len(cr.Redirect) != 0 {
		t.Errorf("got %+v, want local sandbox", cr)
	}

	if mgr.gotClaimRef != "ns1/w1" {
		t.Errorf("claim_ref not threaded: got %q, want %q", mgr.gotClaimRef, "ns1/w1")
	}
}

func TestVolumeClaimUsesWarmBeforeRedirectOrProvision(t *testing.T) {
	wantVolumes := []types.Volume{{Name: "imagenet", Mount: "/volumes/imagenet"}}
	for _, tt := range []struct {
		name               string
		warmHit            bool
		volumeCandidates   []string
		wantID             string
		wantRedirect       []string
		wantProvisionCalls int
		wantCandidateCalls int
	}{
		{name: "local warm hit", warmHit: true, wantID: "sb_warm"},
		{
			name:             "warm miss redirects to volume-aware warm candidate",
			volumeCandidates: []string{"warm-volume:7777"}, wantRedirect: []string{"warm-volume:7777"},
			wantCandidateCalls: 1,
		},
		{
			name: "warm miss without candidate provisions locally", wantID: "sb_provisioned",
			wantProvisionCalls: 1, wantCandidateCalls: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{claim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
				return &types.Sandbox{ID: "sb_provisioned", Token: "tok", Volumes: slices.Clone(wantVolumes)}, nil
			}}
			if tt.warmHit {
				mgr.warmClaim = func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
					return &types.Sandbox{ID: "sb_warm", Token: "tok", Volumes: slices.Clone(wantVolumes)}, nil
				}
			}
			placer := &fakePlacer{
				addrs: []string{"wrong-general-candidate:7777"}, volumeCandidates: tt.volumeCandidates,
			}
			srv := New("", "node-a:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
			ts := httptest.NewServer(srv.Handler())
			t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

			resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(`{"template":"rt:24.04","volumes":[{"name":"imagenet"}]}`))
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			defer resp.Body.Close()
			var got types.ClaimResponse
			if err := json.UnmarshalRead(resp.Body, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if resp.StatusCode != http.StatusOK || got.ID != tt.wantID || !slices.Equal(got.Redirect, tt.wantRedirect) {
				t.Errorf("status=%d response=%+v", resp.StatusCode, got)
			}
			if tt.wantID != "" && !slices.Equal(got.Volumes, wantVolumes) {
				t.Errorf("volumes=%+v, want %+v", got.Volumes, wantVolumes)
			}
			if mgr.warmCalls != 1 || mgr.provisionCalls != tt.wantProvisionCalls ||
				!slices.Equal(mgr.gotWarmVolumes, wantVolumes) {
				t.Errorf("warm=%d warm volumes=%+v provision=%d", mgr.warmCalls, mgr.gotWarmVolumes, mgr.provisionCalls)
			}
			badCandidateNames := tt.wantCandidateCalls > 0 &&
				!slices.Equal(placer.volumeCandidateNames, []string{"imagenet"})
			if placer.candidateCalls != 0 || placer.volumeCandidateCalls != tt.wantCandidateCalls || badCandidateNames {
				t.Errorf("candidates=%d volume candidates=%d names=%v",
					placer.candidateCalls, placer.volumeCandidateCalls, placer.volumeCandidateNames)
			}
		})
	}
}

func TestVolumeClaimUsesVolumeAndTemplateIntersections(t *testing.T) {
	for _, tt := range []struct {
		name                string
		localVolumes        bool
		localPromoted       bool
		hasGolden           bool
		templateOwners      []string
		volumeOwners        []string
		templateVolumeOwner []string
		wantRedirect        string
		wantStatus          int
		wantVolumeCalls     int
		wantTemplateCalls   int
		wantWarmCalls       int
		wantCandidateCalls  int
		noRedirect          bool
		requirePromoted     bool
		wantPromoted        bool
	}{
		{
			name: "ordinary local volume warm-misses then provisions", localVolumes: true,
			wantStatus: http.StatusOK, wantWarmCalls: 1, wantCandidateCalls: 1,
		},
		{
			name: "configured golden with remote volume uses volume owner", hasGolden: true,
			volumeOwners: []string{"volume:7777"}, wantRedirect: "volume:7777",
			wantStatus: http.StatusOK, wantVolumeCalls: 1, wantCandidateCalls: 1,
		},
		{
			name:       "unknown fleet volume fails without provisioning",
			wantStatus: http.StatusBadRequest, wantVolumeCalls: 1, wantCandidateCalls: 1,
		},
		{
			name:         "redirect target resolves locally without another hop",
			volumeOwners: []string{"volume:7777"}, wantStatus: http.StatusBadRequest, noRedirect: true,
		},
		{
			name: "remote promoted template uses true intersection", localVolumes: true,
			templateOwners: []string{"template:7777"}, templateVolumeOwner: []string{"both:7777"},
			wantRedirect: "both:7777", wantStatus: http.StatusOK, wantTemplateCalls: 1, wantPromoted: true,
		},
		{
			name: "redirect target provisions the required promoted template", localVolumes: true,
			localPromoted: true, noRedirect: true, requirePromoted: true,
			wantStatus: http.StatusOK, wantPromoted: true,
		},
		{
			name:          "local promoted template with remote volume uses intersection",
			localPromoted: true, volumeOwners: []string{"volume-only:7777"},
			templateVolumeOwner: []string{"both:7777"}, wantRedirect: "both:7777",
			wantStatus: http.StatusOK, wantTemplateCalls: 1, wantPromoted: true,
		},
		{
			name:          "shared-store template falls back to a volume holder",
			localPromoted: true, volumeOwners: []string{"volume-only:7777"},
			wantRedirect: "volume-only:7777", wantStatus: http.StatusOK,
			wantVolumeCalls: 1, wantTemplateCalls: 1, wantPromoted: true,
		},
		{
			name:         "redirect target refuses missing promoted template before provisioning",
			localVolumes: true, noRedirect: true, requirePromoted: true,
			wantStatus: http.StatusBadRequest, wantPromoted: true,
		},
		{
			name:          "promoted template refuses when no volume holder exists",
			localPromoted: true, wantStatus: http.StatusBadRequest,
			wantVolumeCalls: 1, wantTemplateCalls: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{
				hasGolden: tt.hasGolden, hasPromoted: tt.localPromoted,
				volumePlacement: func(types.PoolKey, string, []string) (bool, error) { return tt.localVolumes, nil },
			}
			placer := &fakePlacer{
				addrs: []string{"warm-peer:7777"}, owners: tt.templateOwners,
				volumeOwners: tt.volumeOwners, templateVolumeOwners: tt.templateVolumeOwner,
			}
			srv := New("", "node-a:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
			ts := httptest.NewServer(srv.Handler())
			t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

			request := types.ClaimRequest{
				Template: "tpl", Volumes: []types.Volume{{Name: "imagenet"}},
				NoRedirect: tt.noRedirect, RequirePromoted: tt.requirePromoted,
			}
			body, err := json.Marshal(request)
			if err != nil {
				t.Fatalf("encode request: %v", err)
			}
			resp, err := http.Post(ts.URL+"/v1/claim", "application/json", bytes.NewReader(body))
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status=%d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusOK {
				assertVolumeClaimResponse(t, resp.Body, tt.wantRedirect, tt.wantPromoted)
			}
			wantProvision := 0
			if tt.wantStatus == http.StatusOK && tt.wantRedirect == "" {
				wantProvision = 1
			}
			if mgr.provisionCalls != wantProvision {
				t.Errorf("provision calls=%d, want %d", mgr.provisionCalls, wantProvision)
			}
			if mgr.gotRequirePromoted != (wantProvision == 1 && tt.wantPromoted) {
				t.Errorf("promoted provision=%v", mgr.gotRequirePromoted)
			}
			if mgr.warmCalls != tt.wantWarmCalls || placer.candidateCalls != 0 ||
				placer.volumeCandidateCalls != tt.wantCandidateCalls ||
				placer.volumeOwnerCalls != tt.wantVolumeCalls || placer.templateVolumeCalls != tt.wantTemplateCalls {
				t.Errorf("warm=%d candidates=%d volume candidates=%d volume owners=%d template-volume owners=%d",
					mgr.warmCalls, placer.candidateCalls, placer.volumeCandidateCalls,
					placer.volumeOwnerCalls, placer.templateVolumeCalls)
			}
		})
	}
}

func TestVolumeClaimKeepsPoolContentAgainstPeerTemplates(t *testing.T) {
	for _, tt := range []struct {
		name              string
		poolGolden        bool
		requirePromoted   bool
		claimErr          error
		wantStatus        int
		wantRedirect      string
		wantPromoted      bool
		wantProvisions    int
		wantWarmCalls     int
		wantTemplateCalls int
	}{
		{
			name: "pool golden outranks the peer's template", poolGolden: true,
			wantStatus: http.StatusOK, wantProvisions: 1, wantWarmCalls: 1,
		},
		{
			name:       "without a pool golden the peer's template still escalates",
			wantStatus: http.StatusOK, wantRedirect: "both:7777", wantPromoted: true,
			wantTemplateCalls: 1,
		},
		{
			name:       "caller-required promoted is refused, never served from the pool golden",
			poolGolden: true, requirePromoted: true, claimErr: pool.ErrUnknownTemplate,
			wantStatus: http.StatusNotFound, wantPromoted: true, wantProvisions: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{
				hasPoolGolden:   tt.poolGolden,
				volumePlacement: func(types.PoolKey, string, []string) (bool, error) { return true, nil },
			}
			if tt.claimErr != nil {
				mgr.claim = func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
					return nil, tt.claimErr
				}
			}
			placer := &fakePlacer{owners: []string{"template:7777"}, templateVolumeOwners: []string{"both:7777"}}
			srv := New("", "node-a:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
			ts := httptest.NewServer(srv.Handler())
			t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

			body, err := json.Marshal(types.ClaimRequest{
				Template: "tpl", Volumes: []types.Volume{{Name: "imagenet"}},
				RequirePromoted: tt.requirePromoted,
			})
			if err != nil {
				t.Fatalf("encode request: %v", err)
			}
			resp, err := http.Post(ts.URL+"/v1/claim", "application/json", bytes.NewReader(body))
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status=%d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusOK {
				assertVolumeClaimResponse(t, resp.Body, tt.wantRedirect, tt.wantPromoted)
			}
			if mgr.provisionCalls != tt.wantProvisions || mgr.warmCalls != tt.wantWarmCalls {
				t.Errorf("provision=%d warm=%d, want %d/%d",
					mgr.provisionCalls, mgr.warmCalls, tt.wantProvisions, tt.wantWarmCalls)
			}
			if mgr.gotRequirePromoted != (tt.wantProvisions == 1 && tt.wantPromoted) {
				t.Errorf("promoted provision=%v, want %v", mgr.gotRequirePromoted, tt.wantPromoted)
			}
			if placer.templateOwnerCalls != tt.wantTemplateCalls || placer.templateVolumeCalls != tt.wantTemplateCalls {
				t.Errorf("template owner calls=%d template-volume calls=%d, want %d each",
					placer.templateOwnerCalls, placer.templateVolumeCalls, tt.wantTemplateCalls)
			}
		})
	}
}

func TestForeignTemplateGossipNeverEscalates(t *testing.T) {
	hash := types.ClaimRequest{Template: "tpl"}.Key().Hash()
	tenants := []config.TenantSpec{{Name: "acme", Token: "acme-tok"}, {Name: "beta", Token: "beta-tok"}}
	for _, tt := range []struct {
		name, token   string
		volumes       []types.Volume
		wantRedirect  bool
		wantProvision int
	}{
		{"foreign volume claim cold-boots locally", "beta-tok", []types.Volume{{Name: "imagenet"}}, false, 1},
		{"owner volume claim escalates to its template", "acme-tok", []types.Volume{{Name: "imagenet"}}, true, 0},
		{"foreign plain claim stays local, no existence signal", "beta-tok", nil, false, 1},
		{"owner plain claim follows its template", "acme-tok", nil, true, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{
				volumePlacement: func(types.PoolKey, string, []string) (bool, error) { return true, nil },
			}
			placer := &fakePlacer{ownersByProbe: map[string][]string{
				types.TemplateGossipHash(hash, "acme"):                  {"peer:7777"},
				types.TemplateGossipHash(hash, types.TemplateRootScope): {"peer:7777"},
			}}
			mgr.tenants = tenants
			srv := New("root-tok", "node-a:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
			ts := httptest.NewServer(srv.Handler())
			t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

			body, _ := json.Marshal(types.ClaimRequest{Template: "tpl", Volumes: tt.volumes})
			req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/claim", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+tt.token)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status=%d, want 200", resp.StatusCode)
			}
			var cr types.ClaimResponse
			if err := json.UnmarshalRead(resp.Body, &cr); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got := len(cr.Redirect) > 0; got != tt.wantRedirect {
				t.Errorf("redirect=%v (%v), want %v", got, cr.Redirect, tt.wantRedirect)
			}
			if cr.RequirePromoted != tt.wantRedirect {
				t.Errorf("require_promoted=%v, want %v: a redirect to a template's owner pins it", cr.RequirePromoted, tt.wantRedirect)
			}
			if mgr.provisionCalls != tt.wantProvision {
				t.Errorf("provisions=%d, want %d", mgr.provisionCalls, tt.wantProvision)
			}
			if mgr.gotRequirePromoted {
				t.Error("a local resolution must not be forced onto the promoted path")
			}
		})
	}
}

func TestARequiredPromotedClaimIsNeverSentToAWarmPeer(t *testing.T) {
	for _, tt := range []struct {
		name       string
		require    bool
		provision  error
		wantStatus int
		wantWarm   bool
	}{
		{"a plain claim follows the warm peer", false, pool.ErrUnknownTemplate, http.StatusOK, true},
		{"a required promoted claim answers 404 rather than a warm peer", true, pool.ErrUnknownTemplate, http.StatusNotFound, false},
		{"a plain claim at quota follows the warm peer", false, pool.ErrQuota, http.StatusOK, true},
		{"a required promoted claim at quota answers 429 rather than a warm peer", true, pool.ErrQuota, http.StatusTooManyRequests, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{claim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
				return nil, tt.provision
			}}
			srv := New("sekret", "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{addrs: []string{"warm:7777"}}, nil, nil, nil)
			ts := httptest.NewServer(srv.Handler())
			t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

			body, _ := json.Marshal(types.ClaimRequest{Template: "ns/app", RequirePromoted: tt.require})
			resp := postJSON(t, ts.URL+"/v1/claim", "sekret", string(body))
			defer resp.Body.Close()
			var cr types.ClaimResponse
			_ = json.UnmarshalRead(resp.Body, &cr)
			if resp.StatusCode != tt.wantStatus || (len(cr.Redirect) > 0) != tt.wantWarm {
				t.Errorf("status=%d redirect=%v, want %d and warm redirect %v", resp.StatusCode, cr.Redirect, tt.wantStatus, tt.wantWarm)
			}
		})
	}
}

func TestAPlainClaimRequiringAPromotedTemplateNeverColdBoots(t *testing.T) {
	for _, tt := range []struct {
		name         string
		require      bool
		held         bool
		warm         bool
		wantStatus   int
		wantPromoted bool
	}{
		{"required and held clones", true, true, false, http.StatusOK, true},
		{"required and absent is 404", true, false, false, http.StatusNotFound, true},
		{"required with a pool golden still clones the template", true, true, true, http.StatusOK, true},
		{"not required provisions as before", false, false, false, http.StatusOK, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{claim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
				return &types.Sandbox{ID: "sb_1", Token: "tok"}, nil
			}}
			if !tt.held && tt.require {
				mgr.claim = func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
					return nil, pool.ErrUnknownTemplate
				}
			}
			if tt.warm {
				mgr.warmClaim = func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
					return &types.Sandbox{ID: "sb_warm", Token: "tok"}, nil
				}
			}
			ts := newTestServer(t, "sekret", mgr, nil)

			body, _ := json.Marshal(types.ClaimRequest{Template: "ns/app", RequirePromoted: tt.require})
			resp := postJSON(t, ts.URL+"/v1/claim", "sekret", string(body))
			defer resp.Body.Close()
			if resp.StatusCode != tt.wantStatus || mgr.provisionCalls != 1 || mgr.gotRequirePromoted != tt.wantPromoted {
				t.Errorf("status=%d provisions=%d promoted=%v, want %d 1 %v", resp.StatusCode, mgr.provisionCalls, mgr.gotRequirePromoted, tt.wantStatus, tt.wantPromoted)
			}
			if tt.require && mgr.warmCalls != 0 {
				t.Errorf("a claim requiring the promoted template asked the warm pool %d times", mgr.warmCalls)
			}
		})
	}
}

func TestVolumeClaimValidatesKeyBeforePlacement(t *testing.T) {
	mgr := &fakeManager{
		volumePlacement: func(types.PoolKey, string, []string) (bool, error) {
			return false, pool.ErrNoEgress
		},
	}
	placer := &fakePlacer{volumeOwners: []string{"volume:7777"}}
	srv := New("", "node-a:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

	resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(
		`{"template":"rt:24.04","net":"egress","volumes":[{"name":"imagenet"}]}`))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status=%d, want 409", resp.StatusCode)
	}
	if mgr.provisionCalls != 0 || placer.volumeOwnerCalls != 0 || placer.templateVolumeCalls != 0 {
		t.Errorf("provision=%d volume owners=%d template-volume owners=%d",
			mgr.provisionCalls, placer.volumeOwnerCalls, placer.templateVolumeCalls)
	}
}

func TestVolumeClaimQuotaDoesNotRedirect(t *testing.T) {
	mgr := &fakeManager{warmClaim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
		return nil, pool.ErrQuota
	}}
	placer := &fakePlacer{
		addrs: []string{"wrong-general-candidate:7777"}, volumeCandidates: []string{"wrong-volume-candidate:7777"},
	}
	srv := New("", "node-a:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

	resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(`{"template":"rt:24.04","volumes":[{"name":"imagenet"}]}`))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status=%d, want 429", resp.StatusCode)
	}
	if mgr.warmCalls != 1 || mgr.provisionCalls != 0 ||
		placer.candidateCalls != 0 || placer.volumeCandidateCalls != 0 {
		t.Errorf("warm=%d provision=%d candidates=%d volume candidates=%d",
			mgr.warmCalls, mgr.provisionCalls, placer.candidateCalls, placer.volumeCandidateCalls)
	}
}

func TestVolumeClaimRejectsShapeBeforePlacement(t *testing.T) {
	for _, body := range []string{
		`{"template":"rt:24.04","volumes":["data"]}`,
		`{"template":"rt:24.04","volumes":[{"name":"data"},{"name":"data"}]}`,
		`{"template":"rt:24.04","volumes":[{"name":"cocoon-data"}]}`,
		`{"template":"rt:24.04","volumes":[{"name":"data","mount":"relative"}]}`,
		`{"template":"rt:24.04","volumes":[{"name":"data","mount":"/datasets"},{"name":"other","mount":"/datasets/nested"}]}`,
		`{"template":"rt:24.04","volumes":[{"name":"a"},{"name":"b"},{"name":"c"},{"name":"d"},{"name":"e"},{"name":"f"},{"name":"g"},{"name":"h"},{"name":"i"}]}`,
		`{"template":"rt:24.04","volumes_attach_only":true}`,
		`{"template":"rt:24.04","volumes_attach_only":true,"volumes":[{"name":"data","mount":"/datasets"}]}`,
		`{"template":"rt:24.04","volumes_attach_only":true,"volumes":[{"name":"data"},{"name":"other","mount":"/datasets"}]}`,
	} {
		mgr := &fakeManager{}
		placer := &fakePlacer{addrs: []string{"warm-peer:7777"}, owners: []string{"owner:7777"}}
		srv := New("", "node-a:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
		ts := httptest.NewServer(srv.Handler())

		resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		resp.Body.Close()
		ts.Close()
		srv.CloseRelays()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("body=%s status=%d, want 400", body, resp.StatusCode)
		}
		if mgr.provisionCalls != 0 || placer.candidateCalls != 0 {
			t.Errorf("body=%s provision=%d candidates=%d", body, mgr.provisionCalls, placer.candidateCalls)
		}
	}
}

func TestOwnerEndpoint(t *testing.T) {
	mgr := &fakeManager{socket: func(_, token string) (string, error) {
		if token == "good" {
			return "/v/sock", nil
		}
		return "", pool.ErrUnknownSandbox
	}}
	srv := New("", "node-b:7777", mgr, &fakeDialer{}, nil, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/v1/sandboxes/sb_1/owner", nil)
	req.Header.Set("Authorization", "Bearer good")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	var body struct {
		OwnerAddr string `json:"owner_addr"`
	}
	_ = json.UnmarshalRead(resp.Body, &body)
	if body.OwnerAddr != "node-b:7777" {
		t.Errorf("owner %q, want node-b:7777", body.OwnerAddr)
	}

	req2, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/v1/sandboxes/sb_1/owner", nil)
	req2.Header.Set("Authorization", "Bearer wrong")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("status %d, want 404", resp2.StatusCode)
	}
}

func TestPreviewHandlerZeroDeadlineMintsLiveToken(t *testing.T) {
	mgr := &fakeManager{claimDeadline: func(string, string) (time.Time, error) {
		return time.Time{}, nil
	}}
	ps := NewPreviewServer("secret", "node:7777", "node:7777", &fakePreviewMgr{})
	srv := New("", "node:7777", mgr, &fakeDialer{}, nil, nil, nil, ps)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

	resp, err := http.Post(ts.URL+"/v1/sandboxes/sb_1/preview", "application/json",
		strings.NewReader(`{"token":"tok","port":8080,"ttl_seconds":300}`))
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	var out types.PreviewResponse
	if err := json.UnmarshalRead(resp.Body, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	_, token, _ := strings.Cut(out.URL, "/p/")
	token = strings.TrimSuffix(token, "/")
	claims, ok := ps.verify(token)
	if !ok {
		t.Fatalf("minted token %q does not verify", token)
	}
	if claims.Exp <= time.Now().Unix() {
		t.Errorf("exp %d, want in the future", claims.Exp)
	}
}

func TestPreviewHandlerRejectsPortZero(t *testing.T) {
	mgr := &fakeManager{claimDeadline: func(string, string) (time.Time, error) {
		return time.Now().Add(time.Hour), nil
	}}
	ps := NewPreviewServer("secret", "node:7777", "node:7777", &fakePreviewMgr{})
	srv := New("", "node:7777", mgr, &fakeDialer{}, nil, nil, nil, ps)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

	resp, err := http.Post(ts.URL+"/v1/sandboxes/sb_1/preview", "application/json",
		strings.NewReader(`{"token":"tok","port":0}`))
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status %d, want 400 for port 0", resp.StatusCode)
	}
}

func TestCheckpointClaimRedirectsToOwner(t *testing.T) {
	mgr := &fakeManager{}
	prober := &fakeProber{owners: []string{"owner-a:7777"}}
	ts := newPlacerTestServer(t, "sekret", mgr, prober)

	resp := postJSON(t, ts.URL+"/v1/checkpoints/ck_00000000000000aa/claim", "sekret", `{}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 carrying a redirect (the mesh redirect is a 200, not a 3xx)", resp.StatusCode)
	}
	var got types.ClaimResponse
	if err := json.UnmarshalRead(resp.Body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Redirect) != 1 || got.Redirect[0] != "owner-a:7777" {
		t.Errorf("redirect = %v, want [owner-a:7777]", got.Redirect)
	}
	if got.ID != "" {
		t.Errorf("id = %q, want empty: a redirect and a delivered sandbox are mutually exclusive", got.ID)
	}
}

func TestCheckpointClaimNoRedirectResolvesLocally(t *testing.T) {
	prober := &fakeProber{owners: []string{"owner-a:7777"}}
	ts := newPlacerTestServer(t, "sekret", &fakeManager{}, prober)

	resp := postJSON(t, ts.URL+"/v1/checkpoints/ck_00000000000000aa/claim", "sekret", `{"no_redirect":true}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: a no_redirect retry must not bounce again", resp.StatusCode)
	}
}

func TestCheckpointClaimNoOwnersIs404(t *testing.T) {
	mgr := &fakeManager{}
	ts := newPlacerTestServer(t, "sekret", mgr, nil)

	resp := postJSON(t, ts.URL+"/v1/checkpoints/ck_00000000000000aa/claim", "sekret", `{}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	if mgr.healCalls != 1 {
		t.Errorf("heal called %d times, want 1: a miss with no owner must still try heal", mgr.healCalls)
	}
}

func TestCheckpointClaimRedirectBeatsHeal(t *testing.T) {
	mgr := &fakeManager{}
	prober := &fakeProber{owners: []string{"owner-a:7777"}}
	ts := newPlacerTestServer(t, "sekret", mgr, prober)

	resp := postJSON(t, ts.URL+"/v1/checkpoints/ck_00000000000000aa/claim", "sekret", `{}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 carrying a redirect", resp.StatusCode)
	}
	var got types.ClaimResponse
	if err := json.UnmarshalRead(resp.Body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Redirect) != 1 || got.Redirect[0] != "owner-a:7777" {
		t.Errorf("redirect = %v, want [owner-a:7777]", got.Redirect)
	}
	if mgr.healCalls != 0 {
		t.Errorf("heal called %d times, want 0: a known owner must redirect, not heal", mgr.healCalls)
	}
}

func TestCheckpointClaimFallsBackToHealWhenNoOwner(t *testing.T) {
	mgr := &fakeManager{
		healCheckpoint: func(ckptID string) (*types.Sandbox, error) {
			return &types.Sandbox{ID: "sb_healed", Token: "htok", FromCheckpoint: ckptID}, nil
		},
	}
	ts := newPlacerTestServer(t, "sekret", mgr, nil)

	resp := postJSON(t, ts.URL+"/v1/checkpoints/ck_00000000000000aa/claim", "sekret", `{}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got types.ClaimResponse
	if err := json.UnmarshalRead(resp.Body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ID != "sb_healed" {
		t.Errorf("id = %q, want the healed claim", got.ID)
	}
	if mgr.healCalls != 1 {
		t.Errorf("heal called %d times, want 1", mgr.healCalls)
	}
}

func TestCheckpointClaimNoRedirectGoesStraightToHeal(t *testing.T) {
	mgr := &fakeManager{
		healCheckpoint: func(ckptID string) (*types.Sandbox, error) {
			return &types.Sandbox{ID: "sb_healed", Token: "htok", FromCheckpoint: ckptID}, nil
		},
	}
	prober := &fakeProber{owners: []string{"owner-a:7777"}}
	ts := newPlacerTestServer(t, "sekret", mgr, prober)

	resp := postJSON(t, ts.URL+"/v1/checkpoints/ck_00000000000000aa/claim", "sekret", `{"no_redirect":true}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got types.ClaimResponse
	if err := json.UnmarshalRead(resp.Body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Redirect) != 0 {
		t.Errorf("redirect = %v, want none: no_redirect must not bounce", got.Redirect)
	}
	if got.ID != "sb_healed" {
		t.Errorf("id = %q, want the healed claim", got.ID)
	}
	if mgr.healCalls != 1 {
		t.Errorf("heal called %d times, want 1", mgr.healCalls)
	}
}

func TestCheckpointClaimHealBusyIs503WithRetryAfter(t *testing.T) {
	mgr := &fakeManager{
		healCheckpoint: func(string) (*types.Sandbox, error) {
			return nil, pool.ErrHealBusy
		},
	}
	ts := newPlacerTestServer(t, "sekret", mgr, nil)

	resp := postJSON(t, ts.URL+"/v1/checkpoints/ck_00000000000000aa/claim", "sekret", `{}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got == "" {
		t.Error("Retry-After header missing on a 503")
	}
}

func TestCheckpointBlobUnknownIs404(t *testing.T) {
	ts := newTestServer(t, "sekret", &fakeManager{}, nil)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		ts.URL+"/v1/checkpoints/ck_00000000000000aa/blob", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer sekret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 so the puller tries the next owner", resp.StatusCode)
	}
}

func TestCheckpointBlobStreamsRecord(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "mem"), []byte("guest-pages"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	mgr := &fakeManager{ckptDir: src}
	ts := newTestServer(t, "sekret", mgr, nil)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		ts.URL+"/v1/checkpoints/ck_00000000000000aa/blob", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer sekret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	dst := t.TempDir()
	if err := peer.Untar(resp.Body, dst); err != nil {
		t.Fatalf("Untar the streamed record: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(dst, "export", "mem")); err != nil || string(got) != "guest-pages" {
		t.Fatalf("streamed export/mem = %q, %v; want the record's bytes", got, err)
	}
	if _, err := os.Stat(filepath.Join(dst, "meta.json")); err != nil {
		t.Fatalf("meta.json missing from the streamed record: %v", err)
	}
}

func TestCheckpointBlobHeadUnauthenticated(t *testing.T) {
	const ckptID = "ck_00000000000000aa"
	mgr := &fakeManager{hasCheckpoint: map[string]bool{ckptID: true}}
	ts := newTestServer(t, "sekret", mgr, nil)

	head := func(id string) int {
		resp, err := http.Head(ts.URL + "/v1/checkpoints/" + id + "/blob")
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if got := head(ckptID); got != http.StatusOK {
		t.Errorf("HEAD present, no token: status = %d, want 200", got)
	}
	if got := head("ck_00000000000000ff"); got != http.StatusNotFound {
		t.Errorf("HEAD missing, no token: status = %d, want 404", got)
	}

	resp, err := http.Get(ts.URL + "/v1/checkpoints/" + ckptID + "/blob")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET no token: status = %d, want 401", resp.StatusCode)
	}
}

func TestCheckpointProbeRequiresValidMAC(t *testing.T) {
	const ckptID = "ck_00000000000000aa"
	key := peer.DeriveProbeKey([]byte("cluster-secret"))
	wrongKey := peer.DeriveProbeKey([]byte("other-secret"))
	mgr := &fakeManager{hasCheckpoint: map[string]bool{ckptID: true}}
	ts := newProbeAuthTestServer(t, mgr, key)

	head := func(sig string) int {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodHead, ts.URL+"/v1/checkpoints/"+ckptID+"/blob", nil)
		if sig != "" {
			req.Header.Set(peer.ProbeHeader, sig)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if got := head(""); got != http.StatusUnauthorized {
		t.Errorf("no signature: status = %d, want 401", got)
	}
	if got := head(peer.SignProbe(wrongKey, ckptID)); got != http.StatusUnauthorized {
		t.Errorf("wrong-key signature: status = %d, want 401", got)
	}
	if got := head(peer.SignProbe(key, ckptID)); got != http.StatusOK {
		t.Errorf("valid signature: status = %d, want 200", got)
	}

	if got := head(peer.SignProbe(key, "ck_00000000000000ff")); got != http.StatusUnauthorized {
		t.Errorf("signature for a different id: status = %d, want 401", got)
	}
}

func TestCheckpointClaimProbesRealPeerAndRedirects(t *testing.T) {
	const ckptID = "ck_00000000000000aa"
	mgrB := &fakeManager{hasCheckpoint: map[string]bool{ckptID: true}}
	srvB := New("sekret", "node-b:7777", mgrB, &fakeDialer{}, nil, nil, nil, nil)
	tsB := httptest.NewServer(srvB.Handler())
	t.Cleanup(tsB.Close)

	prober := &peer.HTTPProber{Peers: func() []string { return []string{tsB.URL} }}
	mgrA := &fakeManager{}
	srvA := New("sekret", "node-a:7777", mgrA, &fakeDialer{}, nil, prober, nil, nil)
	tsA := httptest.NewServer(srvA.Handler())
	t.Cleanup(tsA.Close)

	resp := postJSON(t, tsA.URL+"/v1/checkpoints/"+ckptID+"/claim", "sekret", `{}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 carrying a redirect", resp.StatusCode)
	}
	var got types.ClaimResponse
	if err := json.UnmarshalRead(resp.Body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Redirect) != 1 || got.Redirect[0] != tsB.URL {
		t.Errorf("redirect = %v, want [%s]", got.Redirect, tsB.URL)
	}
}

func TestClaimRejectsMetadataOverABound(t *testing.T) {
	pairs := make([]string, 17)
	for i := range pairs {
		pairs[i] = fmt.Sprintf(`"k%d":"v"`, i)
	}
	for _, tt := range []struct {
		path, body, want string
	}{
		{"/v1/claim", `{"template":"rt:24.04","metadata":{"a=b":"v"}}`, "metadata key"},
		{"/v1/claim", `{"template":"rt:24.04","metadata":{` + strings.Join(pairs, ",") + `}}`, "at most 16 pairs"},
		{"/v1/claim", `{"template":"rt:24.04","volumes":[{"name":"data"}],"metadata":{"k":"` + strings.Repeat("v", 513) + `"}}`, "metadata value"},
		{"/v1/checkpoints/ck_00000000000000aa/claim", `{"metadata":{"":"v"}}`, "metadata key"},
	} {
		mgr := &fakeManager{}
		ts := newTestServer(t, "sekret", mgr, nil)
		resp := postJSON(t, ts.URL+tt.path, "sekret", tt.body)
		var got struct {
			Error string `json:"error"`
		}
		_ = json.UnmarshalRead(resp.Body, &got)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(got.Error, tt.want) {
			t.Errorf("%s: status %d error %q, want 400 naming %q", tt.path, resp.StatusCode, got.Error, tt.want)
		}
		if mgr.warmCalls+mgr.provisionCalls+mgr.healCalls != 0 || mgr.gotMetadata != nil {
			t.Errorf("%s: the manager ran on a rejected claim", tt.path)
		}
	}
}

func TestClaimThreadsMetadataAndEnv(t *testing.T) {
	want := types.Metadata{"team": "a"}
	wantEnv := types.Env{"GW": {Value: "Bearer k", Guest: new(false)}}
	hit := func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
		return &types.Sandbox{ID: "sb_1", Token: "tok"}, nil
	}
	ckpt := func(string) (*types.Sandbox, error) { return &types.Sandbox{ID: "sb_1", Token: "tok"}, nil }
	warm := func(f *fakeManager) { f.warmClaim = hit }
	cold := func(*fakeManager) {}
	for _, tt := range []struct {
		name, path, body string
		setup            func(*fakeManager)
	}{
		{"warm", "/v1/claim", `{"template":"rt:24.04","metadata":{"team":"a"},"env":{"GW":{"value":"Bearer k","guest":false}}}`, warm},
		{"provision", "/v1/claim", `{"template":"rt:24.04","metadata":{"team":"a"},"env":{"GW":{"value":"Bearer k","guest":false}}}`, cold},
		{"volume warm", "/v1/claim", `{"template":"rt:24.04","volumes":[{"name":"data"}],"metadata":{"team":"a"},"env":{"GW":{"value":"Bearer k","guest":false}}}`, warm},
		{"volume provision", "/v1/claim", `{"template":"rt:24.04","volumes":[{"name":"data"}],"metadata":{"team":"a"},"env":{"GW":{"value":"Bearer k","guest":false}}}`, cold},
		{"volume promoted", "/v1/claim", `{"template":"rt:24.04","volumes":[{"name":"data"}],"require_promoted":true,"metadata":{"team":"a"},"env":{"GW":{"value":"Bearer k","guest":false}}}`, func(f *fakeManager) { f.hasPromoted = true }},
		{"checkpoint", "/v1/checkpoints/ck_00000000000000aa/claim", `{"metadata":{"team":"a"},"env":{"GW":{"value":"Bearer k","guest":false}}}`, func(f *fakeManager) { f.claimCheckpoint = ckpt }},
		{"checkpoint heal", "/v1/checkpoints/ck_00000000000000aa/claim", `{"metadata":{"team":"a"},"env":{"GW":{"value":"Bearer k","guest":false}}}`, func(f *fakeManager) { f.healCheckpoint = ckpt }},
	} {
		mgr := &fakeManager{}
		tt.setup(mgr)
		ts := newTestServer(t, "sekret", mgr, nil)
		resp := postJSON(t, ts.URL+tt.path, "sekret", tt.body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !maps.Equal(mgr.gotMetadata, want) || !mgr.gotEnv.Equal(wantEnv) {
			t.Errorf("%s: status %d, manager saw metadata %v env %v, want 200 and %v %v", tt.name, resp.StatusCode, mgr.gotMetadata, mgr.gotEnv, want, wantEnv)
		}
	}
}

func TestSandboxesFilterByMetadata(t *testing.T) {
	for _, tt := range []struct {
		query string
		code  int
		want  types.Metadata
	}{
		{"", http.StatusOK, nil},
		{"?metadata=team%3Da&metadata=env%3Dprod", http.StatusOK, types.Metadata{"team": "a", "env": "prod"}},
		{"?metadata=note%3Dx%3Dy", http.StatusOK, types.Metadata{"note": "x=y"}},
		{"?metadata=empty%3D", http.StatusOK, types.Metadata{"empty": ""}},
		{"?metadata=team", http.StatusBadRequest, nil},
		{"?metadata=", http.StatusBadRequest, nil},
		{"?metadata=%3Dv", http.StatusBadRequest, nil},
		{"?metadata=a%3D1&metadata=a%3D2", http.StatusBadRequest, nil},
	} {
		mgr := &fakeManager{}
		ts := newTestServer(t, "root", mgr, nil)
		resp := doReq(t, http.MethodGet, ts.URL+"/v1/sandboxes"+tt.query, "root", "")
		resp.Body.Close()
		if resp.StatusCode != tt.code || !maps.Equal(mgr.gotMetadata, tt.want) {
			t.Errorf("%q: status %d, manager saw %v, want %d and %v", tt.query, resp.StatusCode, mgr.gotMetadata, tt.code, tt.want)
		}
	}
}

func TestExpireActionThreadsThroughEveryDecoder(t *testing.T) {
	ckpt := func(string) (*types.Sandbox, error) { return &types.Sandbox{ID: "sb_1", Token: "tok"}, nil }
	for _, tt := range []struct {
		name, path, body string
		setup            func(*fakeManager)
	}{
		{"claim", "/v1/claim", `{"template":"rt:24.04","on_expire":"archive"}`, func(*fakeManager) {}},
		{"volume claim", "/v1/claim", `{"template":"rt:24.04","volumes":[{"name":"data"}],"on_expire":"archive"}`, func(*fakeManager) {}},
		{"checkpoint", "/v1/checkpoints/ck_00000000000000aa/claim", `{"on_expire":"archive"}`, func(f *fakeManager) { f.claimCheckpoint = ckpt }},
		{"checkpoint heal", "/v1/checkpoints/ck_00000000000000aa/claim", `{"on_expire":"archive"}`, func(f *fakeManager) { f.healCheckpoint = ckpt }},
		{"fork", "/v1/sandboxes/sb_1/fork", `{"token":"tok","count":1,"on_expire":"archive"}`, func(f *fakeManager) {
			f.fork = func(string, string, int, time.Duration) ([]*types.Sandbox, error) {
				return []*types.Sandbox{{ID: "sb_2", Token: "tok2"}}, nil
			}
		}},
		{"renew", "/v1/sandboxes/sb_1/renew", `{"on_expire":"archive"}`, func(f *fakeManager) {
			f.renew = func(string, string, time.Duration) (time.Time, error) { return time.Now(), nil }
		}},
	} {
		mgr := &fakeManager{}
		tt.setup(mgr)
		ts := newTestServer(t, "sekret", mgr, nil)
		resp := postJSON(t, ts.URL+tt.path, "sekret", tt.body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || mgr.gotOnExpire != types.ExpireArchive {
			t.Errorf("%s: status %d, manager saw on_expire %q, want 200 and archive", tt.name, resp.StatusCode, mgr.gotOnExpire)
		}

		mgr = &fakeManager{}
		tt.setup(mgr)
		ts = newTestServer(t, "sekret", mgr, nil)
		resp = postJSON(t, ts.URL+tt.path, "sekret", strings.Replace(tt.body, `"archive"`, `"pause"`, 1))
		var got struct {
			Error string `json:"error"`
		}
		_ = json.UnmarshalRead(resp.Body, &got)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(got.Error, "on_expire") {
			t.Errorf("%s with on_expire pause: status %d error %q, want 400 naming on_expire", tt.name, resp.StatusCode, got.Error)
		}
	}
}

func assertVolumeClaimResponse(t *testing.T, body io.Reader, wantRedirect string, wantRequirePromoted bool) {
	t.Helper()
	var got types.ClaimResponse
	if err := json.UnmarshalRead(body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if wantRedirect != "" {
		if !slices.Equal(got.Redirect, []string{wantRedirect}) {
			t.Errorf("redirect=%v, want [%s]", got.Redirect, wantRedirect)
		}
		if got.RequirePromoted != wantRequirePromoted {
			t.Errorf("require_promoted=%v, want %v", got.RequirePromoted, wantRequirePromoted)
		}
		return
	}
	if got.ID == "" || len(got.Redirect) != 0 {
		t.Errorf("response=%+v, want local claim", got)
	}
}

func newTestServer(t *testing.T, apiToken string, mgr Manager, dialer Dialer) *httptest.Server {
	t.Helper()
	return newTenantTestServer(t, apiToken, nil, mgr, dialer)
}

func newTenantTestServer(t *testing.T, apiToken string, tenants []config.TenantSpec, mgr Manager, dialer Dialer) *httptest.Server {
	t.Helper()
	if dialer == nil {
		dialer = &fakeDialer{}
	}
	if fm, ok := mgr.(*fakeManager); ok {
		fm.tenants = tenants
	}
	srv := New(apiToken, "node:7777", mgr, dialer, nil, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		srv.CloseRelays()
	})
	return ts
}

func newProbeAuthTestServer(t *testing.T, mgr Manager, probeKey []byte) *httptest.Server {
	t.Helper()
	srv := New("", "node:7777", mgr, &fakeDialer{}, nil, nil, probeKey, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		srv.CloseRelays()
	})
	return ts
}

func doReq(t *testing.T, method, url, token, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

func credToken(cred pool.Cred) string {
	if cred.Operator {
		return ""
	}
	return cred.Token
}

type claimFunc func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error)

type sandboxVerbFunc func(string, string) error

type checkpointClaimFunc func(string) (*types.Sandbox, error)

type idActionFunc func(string) error

type fakeManager struct {
	tenants              []config.TenantSpec
	setTenants           func(specs []config.TenantSpec) error
	putTenant            func(spec config.TenantSpec) error
	deleteTenant         func(name string) error
	tenantList           []pool.TenantInfo
	tenantErr            error
	gotEgressClass       string
	gotTenantPage        [2]string
	ckptDir              string
	hasCheckpoint        map[string]bool
	claim                claimFunc
	warmClaim            claimFunc
	release              sandboxVerbFunc
	releaseOp            idActionFunc
	socket               func(id, token string) (string, error)
	dialPort             func(id, token string, port uint16) (net.Conn, error)
	hibernate            sandboxVerbFunc
	wake                 sandboxVerbFunc
	renew                func(id, token string, ttl time.Duration) (time.Time, error)
	fork                 func(id, token string, count int, ttl time.Duration) ([]*types.Sandbox, error)
	promote              func(id, token, template string) error
	promoteContentDigest string

	deleteGolden  func(key types.PoolKey) error
	hasGolden     bool
	hasPoolGolden bool
	hasPromoted   bool

	audited             func(id string, line []byte)
	checkpoint          func(id, token, name string) (types.Checkpoint, error)
	claimCheckpoint     checkpointClaimFunc
	healCheckpoint      checkpointClaimFunc
	healCalls           int
	checkpoints         []types.Checkpoint
	deleteCheckpoint    idActionFunc
	setPools            func(pools []config.PoolSpec) error
	setInstanceMetadata func(id string, doc []byte) error
	infoPools           []pool.PoolInfo
	infoTemplates       []pool.TemplateInfo
	labeled             []string
	labelErr            error
	envSet              []string
	envPatched          []string
	gotPatch            types.EnvPatch
	envErr              error
	env                 types.Env
	claimDeadline       func(id, token string) (time.Time, error)

	gotTenant          string
	gotDigest          string
	gotClaimRef        string
	gotMetadata        types.Metadata
	gotEnv             types.Env
	gotOnExpire        types.ExpireAction
	gotVolumes         []types.Volume
	gotWarmVolumes     []types.Volume
	gotRequirePromoted bool
	gotNoEgress        bool
	netRoute           types.NetRoute
	warmCalls          int
	provisionCalls     int
	gotNoForward       bool
	tenantClaims       map[string]int
	volumePlacement    func(key types.PoolKey, tenant string, names []string) (bool, error)
	placementCalls     int
	placementNames     []string
	volumeCatalog      func(tenant string, holders map[string]int) []types.VolumeInfo
	sandboxIndex       func(tenant, claimRef string) []pool.SandboxSummary
	sandboxByID        func(id string) (pool.SandboxSummary, bool)
	draining           bool
}

func (f *fakeManager) ClaimWarm(ctx context.Context, key types.PoolKey, o pool.ClaimOptions) (*types.Sandbox, error) {
	f.warmCalls++
	f.gotTenant, f.gotEgressClass = o.Tenant, o.EgressClass
	f.gotClaimRef = o.ClaimRef
	f.gotMetadata = o.Metadata
	f.gotEnv = o.Env
	f.gotOnExpire = o.OnExpire
	f.gotWarmVolumes = slices.Clone(o.Volumes)
	f.gotNoEgress = o.NoEgress
	if f.warmClaim == nil {
		return nil, pool.ErrNoWarm
	}
	return f.warmClaim(ctx, key, o.TTL)
}

func (f *fakeManager) NetRoute(*types.Sandbox) types.NetRoute { return f.netRoute }

func (f *fakeManager) ClaimProvision(ctx context.Context, key types.PoolKey, o pool.ClaimOptions) (*types.Sandbox, error) {
	return fakeClaimProvision(ctx, f, key, o)
}

func (f *fakeManager) Release(_ context.Context, id string, cred pool.Cred) error {
	if cred.Operator {
		if f.releaseOp == nil {
			return nil
		}
		return f.releaseOp(id)
	}
	if f.release == nil {
		return nil
	}
	return f.release(id, cred.Token)
}

func (f *fakeManager) AgentSocket(id, token string) (string, error) {
	if f.socket == nil {
		return "/v/sock", nil
	}
	return f.socket(id, token)
}

func (f *fakeManager) WakeAgentSocket(_ context.Context, id, token string) (string, func(), error) {
	sock, err := f.AgentSocket(id, token)
	return sock, func() {}, err
}

func (f *fakeManager) Renew(_ context.Context, id string, cred pool.Cred, ttl time.Duration, onExpire types.ExpireAction) (time.Time, error) {
	f.gotOnExpire = onExpire
	if f.renew == nil {
		return time.Time{}, pool.ErrUnknownSandbox
	}
	return f.renew(id, credToken(cred), ttl)
}

func (f *fakeManager) DialPort(_ context.Context, id string, cred pool.Cred, port uint16) (net.Conn, error) {
	if f.dialPort == nil {
		return nil, pool.ErrUnknownSandbox
	}
	return f.dialPort(id, credToken(cred), port)
}

func (f *fakeManager) Hibernate(_ context.Context, id string, cred pool.Cred) error {
	if f.hibernate == nil {
		return nil
	}
	return f.hibernate(id, credToken(cred))
}

func (f *fakeManager) Fork(_ context.Context, id string, cred pool.Cred, count int, ttl time.Duration, onExpire types.ExpireAction, _ string) ([]*types.Sandbox, error) {
	f.gotOnExpire = onExpire
	if f.fork == nil {
		return nil, pool.ErrUnknownSandbox
	}
	return f.fork(id, credToken(cred), count, ttl)
}

func (f *fakeManager) Promote(_ context.Context, id string, cred pool.Cred, template, tenant string) (types.PoolKey, string, error) {
	f.gotTenant = tenant
	if f.promote == nil {
		return types.PoolKey{}, "", pool.ErrUnknownSandbox
	}
	if err := f.promote(id, credToken(cred), template); err != nil {
		return types.PoolKey{}, "", err
	}
	return types.PoolKey{Template: template, Net: types.NetNone, Size: types.SizeSmall}, f.promoteContentDigest, nil
}

func (f *fakeManager) DeleteTemplate(_ context.Context, key types.PoolKey, tenant, digest string) error {
	f.gotTenant, f.gotDigest = tenant, digest
	if f.deleteGolden == nil {
		return pool.ErrUnknownTemplate
	}
	return f.deleteGolden(key)
}

func (f *fakeManager) HasGolden(context.Context, types.PoolKey, string) bool {
	return f.hasGolden
}

func (f *fakeManager) HasPoolGolden(types.PoolKey) bool {
	return f.hasPoolGolden
}

func (f *fakeManager) HasPromotedTemplate(context.Context, types.PoolKey, string) bool {
	return f.hasPromoted
}

func (f *fakeManager) ClaimDeadline(id, token string) (time.Time, error) {
	if f.claimDeadline != nil {
		return f.claimDeadline(id, token)
	}
	if f.socket == nil {
		return time.Now().Add(time.Hour), nil
	}
	if _, err := f.socket(id, token); err != nil {
		return time.Time{}, pool.ErrUnknownSandbox
	}
	return time.Now().Add(time.Hour), nil
}

func (f *fakeManager) Counters() pool.Counters { return pool.Counters{} }

func (f *fakeManager) TenantClaims() map[string]int { return f.tenantClaims }

func (f *fakeManager) TenantByToken(_ context.Context, token string) (*config.TenantRecord, error) {
	if f.tenantErr != nil {
		return nil, f.tenantErr
	}
	for _, t := range f.tenants {
		if t.Token == token {
			return &config.TenantRecord{Name: t.Name, MaxClaims: t.MaxClaims, EgressClass: t.EgressClass}, nil
		}
	}
	return nil, pool.ErrUnknownTenant
}

func (f *fakeManager) Tenants(_ context.Context, after string, limit int) (pool.TenantPage, error) {
	f.gotTenantPage = [2]string{after, strconv.Itoa(limit)}
	return pool.TenantPage{Tenants: f.tenantList, Digest: "set-digest"}, nil
}

func (f *fakeManager) SetTenants(_ context.Context, specs []config.TenantSpec) error {
	if f.setTenants != nil {
		return f.setTenants(specs)
	}
	return nil
}

func (f *fakeManager) PutTenant(_ context.Context, spec config.TenantSpec) error {
	if f.putTenant != nil {
		return f.putTenant(spec)
	}
	return nil
}

func (f *fakeManager) DeleteTenant(_ context.Context, name string) error {
	if f.deleteTenant != nil {
		return f.deleteTenant(name)
	}
	return nil
}

func (f *fakeManager) VolumePlacement(key types.PoolKey, tenant string, names []string) (bool, error) {
	f.placementCalls++
	f.gotTenant = tenant
	f.placementNames = slices.Clone(names)
	if f.volumePlacement == nil {
		return true, nil
	}
	return f.volumePlacement(key, tenant, names)
}

func (f *fakeManager) Volumes(tenant string, holders map[string]int) []types.VolumeInfo {
	f.gotTenant = tenant
	if f.volumeCatalog == nil {
		return nil
	}
	return f.volumeCatalog(tenant, holders)
}

func (f *fakeManager) Sandboxes(tenant, claimRef string, metadata types.Metadata) []pool.SandboxSummary {
	f.gotTenant = tenant
	f.gotMetadata = metadata
	if f.sandboxIndex == nil {
		return nil
	}
	return f.sandboxIndex(tenant, claimRef)
}

func (f *fakeManager) Sandbox(id string) (pool.SandboxSummary, bool) {
	if f.sandboxByID == nil {
		return pool.SandboxSummary{}, false
	}
	return f.sandboxByID(id)
}

func (f *fakeManager) Stats(context.Context, string) (pool.SandboxStats, bool) {
	return pool.SandboxStats{}, false
}

func (f *fakeManager) SetInstanceMetadata(_ context.Context, id string, doc []byte) error {
	if f.setInstanceMetadata == nil {
		return nil
	}
	return f.setInstanceMetadata(id, doc)
}

func (f *fakeManager) Wake(_ context.Context, id string, cred pool.Cred) error {
	if f.wake == nil {
		return nil
	}
	return f.wake(id, credToken(cred))
}

func (f *fakeManager) Audit(_ context.Context, id string, line []byte) {
	if f.audited != nil {
		f.audited(id, line)
	}
}

func (f *fakeManager) AuditEnabled() bool { return f.audited != nil }

func (f *fakeManager) Checkpoint(_ context.Context, id string, cred pool.Cred, name, tenant string) (types.Checkpoint, error) {
	f.gotTenant = tenant
	if f.checkpoint == nil {
		return types.Checkpoint{}, pool.ErrUnknownSandbox
	}
	return f.checkpoint(id, credToken(cred), name)
}

func (f *fakeManager) ClaimCheckpoint(_ context.Context, ckptID string, o pool.ClaimOptions) (*types.Sandbox, error) {
	onExpire, tenant, metadata := o.OnExpire, o.Tenant, o.Metadata
	f.gotTenant = tenant
	f.gotMetadata = metadata
	f.gotEnv = o.Env
	f.gotOnExpire = onExpire
	if f.claimCheckpoint == nil {
		return nil, pool.ErrUnknownCheckpoint
	}
	return f.claimCheckpoint(ckptID)
}

func (f *fakeManager) ClaimCheckpointHeal(_ context.Context, ckptID string, o pool.ClaimOptions) (*types.Sandbox, error) {
	onExpire, tenant, metadata := o.OnExpire, o.Tenant, o.Metadata
	f.gotTenant = tenant
	f.gotMetadata = metadata
	f.gotEnv = o.Env
	f.gotOnExpire = onExpire
	f.healCalls++
	if f.healCheckpoint == nil {
		return nil, pool.ErrUnknownCheckpoint
	}
	return f.healCheckpoint(ckptID)
}

func (f *fakeManager) Checkpoints(_ context.Context, tenant string) ([]types.Checkpoint, error) {
	f.gotTenant = tenant
	return f.checkpoints, nil
}

func (f *fakeManager) DeleteCheckpoint(_ context.Context, ckptID, tenant string, scope pool.DeleteScope) error {
	f.gotTenant = tenant
	f.gotNoForward = scope == pool.DeleteLocal
	if f.deleteCheckpoint == nil {
		return pool.ErrUnknownCheckpoint
	}
	return f.deleteCheckpoint(ckptID)
}

func (f *fakeManager) SetPools(_ context.Context, pools []config.PoolSpec) error {
	if f.setPools == nil {
		return nil
	}
	return f.setPools(pools)
}

func (f *fakeManager) Info() ([]pool.PoolInfo, pool.Gauges) {
	return f.infoPools, pool.Gauges{Draining: f.draining}
}

func (f *fakeManager) Templates() []pool.TemplateInfo { return f.infoTemplates }

func (f *fakeManager) SetTemplateLabels(_ context.Context, key types.PoolKey, labels types.Metadata, tenant, digest string) error {
	f.labeled = append(f.labeled, fmt.Sprintf("%s %s %v %q", key.Template, key.Size, labels, tenant))
	f.gotDigest = digest
	return f.labelErr
}

func (f *fakeManager) SetEnv(_ context.Context, id string, env types.Env, tenant string) error {
	f.envSet = append(f.envSet, fmt.Sprintf("%s %d %q", id, len(env), tenant))
	return f.envErr
}

func (f *fakeManager) PatchEnv(_ context.Context, id string, patch types.EnvPatch, tenant string) error {
	names := slices.Sorted(maps.Keys(patch))
	f.envPatched = append(f.envPatched, fmt.Sprintf("%s %v %q", id, names, tenant))
	f.gotPatch = patch
	return f.envErr
}

func (f *fakeManager) Env(_, tenant string) (types.Env, error) {
	f.gotTenant = tenant
	return f.env, f.envErr
}

func (f *fakeManager) Drain(context.Context) { f.draining = true }

func (f *fakeManager) Uncordon(context.Context) { f.draining = false }

func (f *fakeManager) FetchCheckpoint(_ context.Context, _ string) (string, []byte, func(), error) {
	if f.ckptDir == "" {
		return "", nil, nil, pool.ErrUnknownCheckpoint
	}
	return f.ckptDir, []byte(`{"id":"ck_00000000000000aa"}`), func() {}, nil
}

func (f *fakeManager) HasCheckpoint(_ context.Context, ckptID string) bool {
	return f.hasCheckpoint[ckptID]
}

type fakeDialer struct {
	dial func(ctx context.Context, sock string) (net.Conn, error)
}

func (f *fakeDialer) DialSilkd(ctx context.Context, sock string) (net.Conn, error) {
	if f.dial == nil {
		c, _ := net.Pipe()
		return c, nil
	}
	return f.dial(ctx, sock)
}

type fakePlacer struct {
	addrs                []string
	owners               []string
	ownersByProbe        map[string][]string
	volumeCandidates     []string
	volumeOwners         []string
	templateVolumeOwners []string
	volumeHolders        map[string]int
	candidateCalls       int
	volumeCandidateCalls int
	volumeCandidateNames []string
	volumeOwnerCalls     int
	templateOwnerCalls   int
	templateVolumeCalls  int
}

func (f *fakePlacer) Candidates(string) []string {
	f.candidateCalls++
	return f.addrs
}

func (f *fakePlacer) VolumeCandidates(_ string, names []string) []string {
	f.volumeCandidateCalls++
	f.volumeCandidateNames = slices.Clone(names)
	return f.volumeCandidates
}

func (f *fakePlacer) TemplateOwners(probe string) []string {
	f.templateOwnerCalls++
	if f.ownersByProbe != nil {
		return f.ownersByProbe[probe]
	}
	return f.owners
}

func (f *fakePlacer) VolumeOwners([]string) []string {
	f.volumeOwnerCalls++
	return f.volumeOwners
}

func (f *fakePlacer) TemplateVolumeOwners(probe string, _ []string) []string {
	f.templateVolumeCalls++
	if f.ownersByProbe != nil {
		return f.ownersByProbe[probe]
	}
	return f.templateVolumeOwners
}
func (f *fakePlacer) VolumeHolders() map[string]int { return f.volumeHolders }
func (f *fakePlacer) PeerAddrs() []string           { return f.addrs }
func (f *fakePlacer) ClientAddr(addr string) string { return addr }
func (f *fakePlacer) ConfigMismatches() int         { return 0 }

type fakeProber struct {
	owners    []string
	forgotten []string
}

func (f *fakeProber) Owners(context.Context, string) []string { return f.owners }
func (f *fakeProber) Forget(id string)                        { f.forgotten = append(f.forgotten, id) }

func newPlacerTestServer(t *testing.T, apiToken string, mgr Manager, prober CheckpointProber) *httptest.Server {
	t.Helper()
	srv := New(apiToken, "node:7777", mgr, &fakeDialer{}, nil, prober, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func postJSON(t *testing.T, url, token, body string) *http.Response {
	t.Helper()
	return doReq(t, http.MethodPost, url, token, body)
}

func fakeClaimProvision(ctx context.Context, f *fakeManager, key types.PoolKey, o pool.ClaimOptions) (*types.Sandbox, error) {
	f.provisionCalls++
	f.gotTenant = o.Tenant
	f.gotClaimRef = o.ClaimRef
	f.gotMetadata = o.Metadata
	f.gotEnv = o.Env
	f.gotOnExpire = o.OnExpire
	f.gotVolumes = slices.Clone(o.Volumes)
	f.gotRequirePromoted = o.RequirePromoted
	f.gotNoEgress = o.NoEgress
	if f.claim == nil {
		return &types.Sandbox{ID: "sb_1", Token: "tok", Volumes: slices.Clone(o.Volumes)}, nil
	}
	return f.claim(ctx, key, o.TTL)
}
