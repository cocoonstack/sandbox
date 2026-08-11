package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/store/peer"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

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
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cr.ID != "sb_1" || cr.Token != "tok" || !cr.Deadline.Equal(time.Unix(42, 0)) {
		t.Errorf("got %+v", cr)
	}
	if cr.TemplateDigest != "sha256:claim-digest" {
		t.Errorf("template digest %q, want sha256:claim-digest", cr.TemplateDigest)
	}
	want := types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall, Engine: types.EngineCH}
	if gotKey != want {
		t.Errorf("key %+v, want defaults %+v", gotKey, want)
	}
	if gotTTL != time.Minute {
		t.Errorf("ttl %v, want 1m", gotTTL)
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
	srv := New("root", []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}, "node:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
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
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
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
	mgr := &fakeManager{sandboxIndex: func(tenant string) []pool.SandboxSummary {
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
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := []types.Volume{{Name: "imagenet", Mount: "/datasets/imagenet"}}
	if len(got.Sandboxes) != 1 || got.Sandboxes[0].ID != "sb_1" || !slices.Equal(got.Sandboxes[0].Volumes, want) {
		t.Errorf("sandboxes = %+v, want sb_1 with %+v", got.Sandboxes, want)
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

// TestMetricsTenantGauge checks the per-tenant live-claim gauge renders with
// the configured tenants only.
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

// TestPutPoolsRejectsUnknownFields: the operator body decodes strictly — a
// mistyped key (e.g. "egres") must 400, not succeed with the policy dropped.
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
		if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
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
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Pools) != 1 || out.Pools[0].Target != 2 || !out.Pools[0].Golden {
		t.Fatalf("info response %+v, want updated pool", out)
	}
}

// TestSandboxVerbFlows drives the per-sandbox-token verb paths. Hibernate runs
// through handleSandboxVerb; release runs through handleRelease with an unset
// api token (so isRootToken is always false and it takes the same per-sandbox
// path) — both share auth, error mapping, and id/token plumbing by construction.
// TestReleaseOperatorToken covers release's root-token elevation separately.
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

// TestReleaseOperatorToken proves the release route's authorization split: the
// node root api_token releases any sandbox by id via an Operator credential (no
// per-sandbox token), while a per-sandbox token still takes the token-checked
// path and a tenant/wrong token gets no operator elevation (it resolves as a
// non-matching sandbox token and 404s). Exactly one hook runs per request.
func TestReleaseOperatorToken(t *testing.T) {
	const rootTok, sbTok = "sekret", "sb-secret"
	tenants := []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}
	tests := []struct {
		name        string
		auth        string
		wantOp      bool   // operator (Cred.Operator) release expected
		wantRelease bool   // per-sandbox release expected
		wantToken   string // token the per-sandbox release should receive
		releaseErr  error  // error the per-sandbox release returns
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
			mgr := &fakeManager{socket: func(id, token string) (string, error) { return "/v/sock", tt.sockErr }}
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
	mgr := &fakeManager{fork: func(id, token string, count int, ttl time.Duration) ([]*types.Sandbox, error) {
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
	// Fork creates node resources: the header carries the API token like a
	// claim, and the sandbox's own token rides in the body.
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
	if err := json.NewDecoder(resp.Body).Decode(&fr); err != nil {
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
		promote: func(id, token, template string) error {
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
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
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
	want := types.PoolKey{Template: "tpl:x", Net: types.NetNone, Size: types.SizeSmall, Engine: types.EngineCH}
	if gotKey != want {
		t.Errorf("delete key %+v, want %+v (claim defaults applied)", gotKey, want)
	}
	if got := del("Bearer sekret", "template=nope"); got != http.StatusNotFound {
		t.Errorf("unknown delete status %d, want 404", got)
	}
	// Node-level endpoint: the API token guards it.
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
		req, _ := http.NewRequest(http.MethodPost, ts.URL+path, strings.NewReader(body))
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
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil || cr.Checkpoint.ID != "ck_0011223344556677" {
		t.Fatalf("checkpoint: status %d, %+v, %v", resp.StatusCode, cr, err)
	}
	if cr.Checkpoint.Name != "step-1" {
		t.Errorf("name %q, want step-1", cr.Checkpoint.Name)
	}

	resp2 := post("/v1/checkpoints/"+cr.Checkpoint.ID+"/claim", `{}`)
	defer resp2.Body.Close()
	var claim types.ClaimResponse
	if err := json.NewDecoder(resp2.Body).Decode(&claim); err != nil || claim.ID != "sb_branch" {
		t.Fatalf("claim from checkpoint: status %d, %+v, %v", resp2.StatusCode, claim, err)
	}

	resp3 := post("/v1/checkpoints/ck_ffffffffffffffff/claim", `{}`)
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusNotFound {
		t.Errorf("unknown checkpoint claim: status %d, want 404", resp3.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/checkpoints/"+cr.Checkpoint.ID, nil)
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

// TestDeleteCheckpointNoForwardQueryParam proves the no_forward query param
// reaches the manager, mirroring how handleDeleteTemplate already threads
// no_redirect: a delete arriving from another node's broadcast must not
// re-broadcast, or the fleet loops the delete forever.
func TestDeleteCheckpointNoForwardQueryParam(t *testing.T) {
	mgr := &fakeManager{deleteCheckpoint: func(string) error { return nil }}
	ts := newTestServer(t, "api", mgr, &fakeDialer{})

	del := func(path string) int {
		req, _ := http.NewRequest(http.MethodDelete, ts.URL+path, nil)
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

// TestDeleteCheckpointForgetsProbeCache proves a delete evicts any cached
// probe answer for the id, whether the delete is the original (fleet scope)
// or a forwarded broadcast (no_forward) that finds nothing to delete locally
// — both cases mean this node just learned id's ownership changed.
func TestDeleteCheckpointForgetsProbeCache(t *testing.T) {
	prober := &fakeProber{}

	t.Run("original delete", func(t *testing.T) {
		prober.forgotten = nil
		mgr := &fakeManager{deleteCheckpoint: func(string) error { return nil }}
		ts := newPlacerTestServer(t, "api", mgr, prober)
		req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/checkpoints/ck_0011223344556677", nil)
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
		req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/checkpoints/ck_0011223344556677?no_forward=1", nil)
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
	// Warm miss (fakeManager.ClaimWarm always misses) + a placer with
	// candidates → a redirect response, and the local manager never provisions.
	provisioned := false
	mgr := &fakeManager{claim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
		provisioned = true
		return &types.Sandbox{ID: "sb_local"}, nil
	}}
	srv := New("", nil, "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{addrs: []string{"node-b:7777", "node-c:7777"}}, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

	resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(`{"template":"rt:24.04"}`))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer resp.Body.Close()
	var cr types.ClaimResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
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
	// Warm miss, no warm candidates, no local golden, but gossip names a
	// template owner → redirect there instead of cold-booting a nonexistent
	// image ref. A local golden suppresses the redirect: provision here.
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
			srv := New("", nil, "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{owners: []string{"node-b:7777"}}, nil, nil, nil)
			ts := httptest.NewServer(srv.Handler())
			t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

			resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(`{"template":"tpl"}`))
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			defer resp.Body.Close()
			var cr types.ClaimResponse
			if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
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

func TestDeleteTemplateRedirectsToOwner(t *testing.T) {
	// Unknown locally + gossip names an owner → the claim redirect shape;
	// unknown everywhere stays 404.
	mgr := &fakeManager{} // DeleteTemplate → ErrUnknownTemplate
	srv := New("", nil, "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{owners: []string{"node-b:7777"}}, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/templates?template=tpl", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200 redirect", resp.StatusCode)
	}
	var cr types.ClaimResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&cr); decodeErr != nil {
		t.Fatalf("decode: %v", decodeErr)
	}
	if len(cr.Redirect) != 1 || cr.Redirect[0] != "node-b:7777" {
		t.Errorf("redirect %v, want [node-b:7777]", cr.Redirect)
	}

	// no_redirect answers for this node alone, even with owners in gossip.
	req2, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/templates?template=tpl&no_redirect=1", nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("status %d, want 404 with no_redirect despite known owners", resp2.StatusCode)
	}

	srvNoOwner := New("", nil, "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{}, nil, nil, nil)
	ts2 := httptest.NewServer(srvNoOwner.Handler())
	t.Cleanup(func() { ts2.Close(); srvNoOwner.CloseRelays() })
	req3, _ := http.NewRequest(http.MethodDelete, ts2.URL+"/v1/templates?template=tpl", nil)
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
	// Warm miss + a placer with no candidates → local provision.
	mgr := &fakeManager{claim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
		return &types.Sandbox{ID: "sb_local", Token: "tok"}, nil
	}}
	srv := New("", nil, "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{addrs: nil}, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

	resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(`{"template":"rt:24.04","claim_ref":"ns1/w1"}`))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer resp.Body.Close()
	var cr types.ClaimResponse
	_ = json.NewDecoder(resp.Body).Decode(&cr)
	if cr.ID != "sb_local" || len(cr.Redirect) != 0 {
		t.Errorf("got %+v, want local sandbox", cr)
	}
	// The claim_ref on the wire must thread through the handler to the claim.
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
			srv := New("", nil, "node-a:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
			ts := httptest.NewServer(srv.Handler())
			t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

			resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(`{"template":"rt:24.04","volumes":[{"name":"imagenet"}]}`))
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			defer resp.Body.Close()
			var got types.ClaimResponse
			if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
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
			srv := New("", nil, "node-a:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
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

func TestVolumeClaimValidatesKeyBeforePlacement(t *testing.T) {
	mgr := &fakeManager{
		volumePlacement: func(types.PoolKey, string, []string) (bool, error) {
			return false, pool.ErrNoEgress
		},
	}
	placer := &fakePlacer{volumeOwners: []string{"volume:7777"}}
	srv := New("", nil, "node-a:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
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
	srv := New("", nil, "node-a:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
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
		`{"template":"rt:24.04","engine":"fc","volumes":[{"name":"data"}]}`,
		`{"template":"rt:24.04","volumes":[{"name":"data","mount":"relative"}]}`,
		`{"template":"rt:24.04","volumes":[{"name":"data","mount":"/datasets"},{"name":"other","mount":"/datasets/nested"}]}`,
		`{"template":"rt:24.04","volumes":[{"name":"a"},{"name":"b"},{"name":"c"},{"name":"d"},{"name":"e"},{"name":"f"},{"name":"g"},{"name":"h"},{"name":"i"}]}`,
	} {
		mgr := &fakeManager{}
		placer := &fakePlacer{addrs: []string{"warm-peer:7777"}, owners: []string{"owner:7777"}}
		srv := New("", nil, "node-a:7777", mgr, &fakeDialer{}, placer, nil, nil, nil)
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
	// AgentSocket succeeds → this node owns it → 200 with owner addr.
	mgr := &fakeManager{socket: func(id, token string) (string, error) {
		if token == "good" {
			return "/v/sock", nil
		}
		return "", pool.ErrUnknownSandbox
	}}
	srv := New("", nil, "node-b:7777", mgr, &fakeDialer{}, nil, nil, nil, nil)
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
	_ = json.NewDecoder(resp.Body).Decode(&body)
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

// TestPreviewHandlerZeroDeadlineMintsLiveToken covers a claim with no
// deadline (e.g. archived with ArchiveDeleteAfterSeconds=0): the minted
// preview token must still verify, not be stamped already-expired.
func TestPreviewHandlerZeroDeadlineMintsLiveToken(t *testing.T) {
	mgr := &fakeManager{claimDeadline: func(string, string) (time.Time, error) {
		return time.Time{}, nil
	}}
	ps := NewPreviewServer("secret", "node:7777", &fakePreviewMgr{})
	srv := New("", nil, "node:7777", mgr, &fakeDialer{}, nil, nil, nil, ps)
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
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	token := strings.TrimSuffix(out.URL[strings.Index(out.URL, "/p/")+3:], "/")
	claims, ok := ps.verify(token)
	if !ok {
		t.Fatalf("minted token %q does not verify", token)
	}
	if claims.Exp <= time.Now().Unix() {
		t.Errorf("exp %d, want in the future", claims.Exp)
	}
}

// TestCheckpointClaimRedirectsToOwner is L2: checkpoints are node-local, so a
// branch that lands on a node without the record must be pointed at one that
// has it — the clone then runs on the node whose disk already holds the data,
// on its local reflink fast path. Failing here instead would make "snapshot on
// A, branch from B" simply not work.
func TestCheckpointClaimRedirectsToOwner(t *testing.T) {
	mgr := &fakeManager{} // ClaimCheckpoint defaults to ErrUnknownCheckpoint
	prober := &fakeProber{owners: []string{"owner-a:7777"}}
	ts := newPlacerTestServer(t, "sekret", mgr, prober)

	resp := postJSON(t, ts.URL+"/v1/checkpoints/ck_00000000000000aa/claim", "sekret", `{}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 carrying a redirect (the mesh redirect is a 200, not a 3xx)", resp.StatusCode)
	}
	var got types.ClaimResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Redirect) != 1 || got.Redirect[0] != "owner-a:7777" {
		t.Errorf("redirect = %v, want [owner-a:7777]", got.Redirect)
	}
	if got.ID != "" {
		t.Errorf("id = %q, want empty: a redirect and a delivered sandbox are mutually exclusive", got.ID)
	}
}

// TestCheckpointClaimNoRedirectResolvesLocally: the retry at a redirect target
// must resolve there or two nodes would bounce the request between them.
func TestCheckpointClaimNoRedirectResolvesLocally(t *testing.T) {
	prober := &fakeProber{owners: []string{"owner-a:7777"}}
	ts := newPlacerTestServer(t, "sekret", &fakeManager{}, prober)

	resp := postJSON(t, ts.URL+"/v1/checkpoints/ck_00000000000000aa/claim", "sekret", `{"no_redirect":true}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: a no_redirect retry must not bounce again", resp.StatusCode)
	}
}

// TestCheckpointClaimNoOwnersIs404: nothing gossiped the record, so a miss
// falls through to heal, and a heal miss (item 4) is an honest 404 rather
// than a redirect to nowhere.
func TestCheckpointClaimNoOwnersIs404(t *testing.T) {
	mgr := &fakeManager{} // ClaimCheckpointHeal defaults to ErrUnknownCheckpoint
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

// TestCheckpointClaimRedirectBeatsHeal is the tier-order regression test: a
// gossiped owner must redirect (zero bytes moved) and never fall through to
// the heal pull, even though the local ClaimCheckpoint missed.
func TestCheckpointClaimRedirectBeatsHeal(t *testing.T) {
	mgr := &fakeManager{} // ClaimCheckpoint defaults to ErrUnknownCheckpoint
	prober := &fakeProber{owners: []string{"owner-a:7777"}}
	ts := newPlacerTestServer(t, "sekret", mgr, prober)

	resp := postJSON(t, ts.URL+"/v1/checkpoints/ck_00000000000000aa/claim", "sekret", `{}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 carrying a redirect", resp.StatusCode)
	}
	var got types.ClaimResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Redirect) != 1 || got.Redirect[0] != "owner-a:7777" {
		t.Errorf("redirect = %v, want [owner-a:7777]", got.Redirect)
	}
	if mgr.healCalls != 0 {
		t.Errorf("heal called %d times, want 0: a known owner must redirect, not heal", mgr.healCalls)
	}
}

// TestCheckpointClaimFallsBackToHealWhenNoOwner: no gossiped owner to redirect
// to, so the server pulls the record itself via heal.
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
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ID != "sb_healed" {
		t.Errorf("id = %q, want the healed claim", got.ID)
	}
	if mgr.healCalls != 1 {
		t.Errorf("heal called %d times, want 1", mgr.healCalls)
	}
}

// TestCheckpointClaimNoRedirectGoesStraightToHeal: a no_redirect retry must
// not bounce again, but it must still try heal rather than answer 404 outright.
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
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
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

// TestCheckpointClaimHealBusyIs503WithRetryAfter: a full heal budget is the
// node's own transient exhaustion, not a caller error — the client should
// retry, and Retry-After says so instead of leaving it to guess.
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

// TestCheckpointBlobUnknownIs404 lets a puller move on to the next owner
// instead of treating a stale gossip entry as a transfer failure.
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

// TestCheckpointBlobStreamsRecord is the serving half of L3: the record leaves
// as a tar the peer can unpack, with its export contents intact.
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
	// The blob is a whole record: meta.json at the root plus the export under
	// export/. Streaming only the export produced a directory that published
	// cleanly and then failed every read as "unknown checkpoint".
	if got, err := os.ReadFile(filepath.Join(dst, "export", "mem")); err != nil || string(got) != "guest-pages" {
		t.Fatalf("streamed export/mem = %q, %v; want the record's bytes", got, err)
	}
	if _, err := os.Stat(filepath.Join(dst, "meta.json")); err != nil {
		t.Fatalf("meta.json missing from the streamed record: %v", err)
	}
}

// TestCheckpointBlobHeadUnauthenticated proves the probe route bypasses
// requireToken while GET still enforces it. With no probe key configured
// (no cluster_key -- an unencrypted mesh has no shared secret to build one
// from), the id remains the only capability, same as before this batch.
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

// TestCheckpointProbeRequiresValidMAC proves that with a probe key
// configured (an encrypted mesh), handleCheckpointProbe rejects a missing or
// wrongly-signed HEAD before it ever reads HasCheckpoint, and accepts one
// signed with the matching key.
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
	// A valid signature for a DIFFERENT checkpoint must not authorize this one.
	if got := head(peer.SignProbe(key, "ck_00000000000000ff")); got != http.StatusUnauthorized {
		t.Errorf("signature for a different id: status = %d, want 401", got)
	}
}

// TestCheckpointClaimProbesRealPeerAndRedirects is the end-to-end probe: a
// live HTTPProber HEADs an actual peer server rather than a fake, proving the
// probe wire protocol (unauthenticated HEAD, addr scheme defaulting) against
// the real handler, not just an interface stub.
func TestCheckpointClaimProbesRealPeerAndRedirects(t *testing.T) {
	const ckptID = "ck_00000000000000aa"
	mgrB := &fakeManager{hasCheckpoint: map[string]bool{ckptID: true}}
	srvB := New("sekret", nil, "node-b:7777", mgrB, &fakeDialer{}, nil, nil, nil, nil)
	tsB := httptest.NewServer(srvB.Handler())
	t.Cleanup(tsB.Close)

	prober := &peer.HTTPProber{Peers: func() []string { return []string{tsB.URL} }}
	mgrA := &fakeManager{} // ClaimCheckpoint defaults to ErrUnknownCheckpoint
	srvA := New("sekret", nil, "node-a:7777", mgrA, &fakeDialer{}, nil, prober, nil, nil)
	tsA := httptest.NewServer(srvA.Handler())
	t.Cleanup(tsA.Close)

	resp := postJSON(t, tsA.URL+"/v1/checkpoints/"+ckptID+"/claim", "sekret", `{}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 carrying a redirect", resp.StatusCode)
	}
	var got types.ClaimResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Redirect) != 1 || got.Redirect[0] != tsB.URL {
		t.Errorf("redirect = %v, want [%s]", got.Redirect, tsB.URL)
	}
}

func assertVolumeClaimResponse(t *testing.T, body io.Reader, wantRedirect string, wantRequirePromoted bool) {
	t.Helper()
	var got types.ClaimResponse
	if err := json.NewDecoder(body).Decode(&got); err != nil {
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
	srv := New(apiToken, tenants, "node:7777", mgr, dialer, nil, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		srv.CloseRelays()
	})
	return ts
}

// newProbeAuthTestServer builds a server with a probe key configured, for
// tests of handleCheckpointProbe's MAC verification specifically.
func newProbeAuthTestServer(t *testing.T, mgr Manager, probeKey []byte) *httptest.Server {
	t.Helper()
	srv := New("", nil, "node:7777", mgr, &fakeDialer{}, nil, nil, probeKey, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		srv.CloseRelays()
	})
	return ts
}

// credToken recovers the token a fake hook expects from cred: empty for an
// Operator credential, mirroring the collapsed manager's own translation.
func credToken(cred pool.Cred) string {
	if cred.Operator {
		return ""
	}
	return cred.Token
}

// fakeManager implements Manager with overridable behavior. ClaimWarm misses
// unless warmClaim is set; claim stands in for the provision result.
// Tenant-scoped methods record the tenant they were handed in gotTenant.
type fakeManager struct {
	ckptDir              string
	hasCheckpoint        map[string]bool
	claim                func(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error)
	warmClaim            func(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error)
	release              func(id, token string) error
	releaseOp            func(id string) error
	socket               func(id, token string) (string, error)
	hibernate            func(id, token string) error
	wake                 func(id, token string) error
	fork                 func(id, token string, count int, ttl time.Duration) ([]*types.Sandbox, error)
	promote              func(id, token, template string) error
	promoteContentDigest string

	deleteGolden func(key types.PoolKey) error
	hasGolden    bool
	hasPromoted  bool

	audited          func(id string, line []byte)
	checkpoint       func(id, token, name string) (types.Checkpoint, error)
	claimCheckpoint  func(ckptID string) (*types.Sandbox, error)
	healCheckpoint   func(ckptID string) (*types.Sandbox, error)
	healCalls        int
	checkpoints      []types.Checkpoint
	deleteCheckpoint func(ckptID string) error
	setPools         func(pools []config.PoolSpec) error
	infoPools        []pool.PoolInfo
	claimDeadline    func(id, token string) (time.Time, error)

	gotTenant          string
	gotClaimRef        string
	gotVolumes         []types.Volume
	gotWarmVolumes     []types.Volume
	gotRequirePromoted bool
	warmCalls          int
	provisionCalls     int
	gotNoForward       bool
	tenantClaims       map[string]int
	volumePlacement    func(key types.PoolKey, tenant string, names []string) (bool, error)
	placementCalls     int
	placementNames     []string
	volumeCatalog      func(tenant string, holders map[string]int) []types.VolumeInfo
	sandboxIndex       func(tenant string) []pool.SandboxSummary
	draining           bool
}

func (f *fakeManager) ClaimWarm(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef string, volumes []types.Volume) (*types.Sandbox, error) {
	f.warmCalls++
	f.gotTenant = tenant
	f.gotClaimRef = claimRef
	f.gotWarmVolumes = slices.Clone(volumes)
	if f.warmClaim == nil {
		return nil, pool.ErrNoWarm
	}
	return f.warmClaim(ctx, key, ttl)
}

func (f *fakeManager) ClaimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef string, volumes []types.Volume) (*types.Sandbox, error) {
	return fakeClaimProvision(f, ctx, key, ttl, tenant, claimRef, volumes, false)
}

func (f *fakeManager) ClaimProvisionPromoted(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef string, volumes []types.Volume) (*types.Sandbox, error) {
	return fakeClaimProvision(f, ctx, key, ttl, tenant, claimRef, volumes, true)
}

// Release dispatches to the operator or per-sandbox hook, mirroring the
// collapsed manager's own Cred split.
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

func (f *fakeManager) WakeAgentSocket(_ context.Context, id, token string) (string, error) {
	return f.AgentSocket(id, token)
}

func (f *fakeManager) Hibernate(_ context.Context, id string, cred pool.Cred) error {
	if f.hibernate == nil {
		return nil
	}
	return f.hibernate(id, credToken(cred))
}

func (f *fakeManager) Fork(_ context.Context, id string, cred pool.Cred, count int, ttl time.Duration) ([]*types.Sandbox, error) {
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

func (f *fakeManager) DeleteTemplate(_ context.Context, key types.PoolKey, tenant string) error {
	f.gotTenant = tenant
	if f.deleteGolden == nil {
		return pool.ErrUnknownTemplate
	}
	return f.deleteGolden(key)
}

func (f *fakeManager) HasGolden(context.Context, types.PoolKey) bool {
	return f.hasGolden
}

func (f *fakeManager) HasPromotedTemplate(context.Context, types.PoolKey) bool {
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

func (f *fakeManager) Sandboxes(tenant string) []pool.SandboxSummary {
	f.gotTenant = tenant
	if f.sandboxIndex == nil {
		return nil
	}
	return f.sandboxIndex(tenant)
}

func (f *fakeManager) Sandbox(string) (pool.SandboxSummary, bool) {
	return pool.SandboxSummary{}, false
}

func (f *fakeManager) Stats(context.Context, string) (pool.SandboxStats, bool) {
	return pool.SandboxStats{}, false
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

func (f *fakeManager) ClaimCheckpoint(_ context.Context, ckptID string, _ time.Duration, tenant string) (*types.Sandbox, error) {
	f.gotTenant = tenant
	if f.claimCheckpoint == nil {
		return nil, pool.ErrUnknownCheckpoint
	}
	return f.claimCheckpoint(ckptID)
}

func (f *fakeManager) ClaimCheckpointHeal(_ context.Context, ckptID string, _ time.Duration, tenant string) (*types.Sandbox, error) {
	f.gotTenant = tenant
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

func (f *fakeManager) Drain(context.Context) { f.draining = true }

func (f *fakeManager) Uncordon(context.Context) { f.draining = false }

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
	volumeCandidates     []string
	volumeOwners         []string
	templateVolumeOwners []string
	volumeHolders        map[string]int
	candidateCalls       int
	volumeCandidateCalls int
	volumeCandidateNames []string
	volumeOwnerCalls     int
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
func (f *fakePlacer) TemplateOwners(string) []string { return f.owners }
func (f *fakePlacer) VolumeOwners([]string) []string {
	f.volumeOwnerCalls++
	return f.volumeOwners
}

func (f *fakePlacer) TemplateVolumeOwners(string, []string) []string {
	f.templateVolumeCalls++
	return f.templateVolumeOwners
}
func (f *fakePlacer) VolumeHolders() map[string]int { return f.volumeHolders }
func (f *fakePlacer) PeerAddrs() []string           { return f.addrs }
func (f *fakePlacer) ConfigMismatches() int         { return 0 }

// fakeProber stubs CheckpointProber with a fixed answer, standing in for a
// live HEAD fan-out. forgotten records every id Forget was called with, so
// tests can assert a delete evicted the right cache entry.
type fakeProber struct {
	owners    []string
	forgotten []string
}

func (f *fakeProber) Owners(context.Context, string) []string { return f.owners }
func (f *fakeProber) Forget(id string)                        { f.forgotten = append(f.forgotten, id) }

// FetchCheckpoint serves the peer-transfer read; the fake reports every record
// missing unless a test supplies a directory.
func (f *fakeManager) FetchCheckpoint(_ context.Context, _ string) (string, []byte, func(), error) {
	if f.ckptDir == "" {
		return "", nil, nil, pool.ErrUnknownCheckpoint
	}
	return f.ckptDir, []byte(`{"id":"ck_00000000000000aa"}`), func() {}, nil
}

// HasCheckpoint answers the probe endpoint straight from the fake's map;
// unset ids report false.
func (f *fakeManager) HasCheckpoint(_ context.Context, ckptID string) bool {
	return f.hasCheckpoint[ckptID]
}

// newPlacerTestServer builds a server with a checkpoint prober, which the
// shared helper deliberately leaves nil (most tests are single-node); the
// mesh placer stays nil throughout — these tests exercise checkpoint claims
// only.
func newPlacerTestServer(t *testing.T, apiToken string, mgr Manager, prober CheckpointProber) *httptest.Server {
	t.Helper()
	srv := New(apiToken, nil, "node:7777", mgr, &fakeDialer{}, nil, prober, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func postJSON(t *testing.T, url, token, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader(body))
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

func fakeClaimProvision(f *fakeManager, ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef string, volumes []types.Volume, requirePromoted bool) (*types.Sandbox, error) {
	f.provisionCalls++
	f.gotTenant = tenant
	f.gotClaimRef = claimRef
	f.gotVolumes = slices.Clone(volumes)
	f.gotRequirePromoted = requirePromoted
	if f.claim == nil {
		return &types.Sandbox{ID: "sb_1", Token: "tok", Volumes: slices.Clone(volumes)}, nil
	}
	return f.claim(ctx, key, ttl)
}
