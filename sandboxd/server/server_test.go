package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestClaimHappyPath(t *testing.T) {
	var gotKey types.PoolKey
	var gotTTL time.Duration
	mgr := &fakeManager{
		claim: func(_ context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
			gotKey, gotTTL = key, ttl
			return &types.Sandbox{ID: "sb_1", Token: "tok", Deadline: time.Unix(42, 0).UTC()}, nil
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
	want := types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall}
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

// TestTenantAuthMatrix drives the three token kinds across the two endpoint
// classes: resource-creating verbs take root or tenant tokens (the tenant
// scope reaches the manager), operator surfaces answer 403 to a tenant —
// authenticated but not authorized — and 401 to anything unknown.
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
		{"tenant forbidden on info", http.MethodGet, "/v1/info", "acme-tok", http.StatusForbidden, ""},
		{"tenant forbidden on index", http.MethodGet, "/v1/sandboxes", "acme-tok", http.StatusForbidden, ""},
		{"tenant forbidden on metrics", http.MethodGet, "/metrics", "acme-tok", http.StatusForbidden, ""},
		{"tenant forbidden on pools", http.MethodPut, "/v1/pools", "acme-tok", http.StatusForbidden, ""},
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
			reachedManager := resp.StatusCode == http.StatusOK &&
				(tt.path == "/v1/claim" || tt.path == "/v1/checkpoints")
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
// node root api_token releases any sandbox by id via ReleaseOperator (no
// per-sandbox token), while a per-sandbox token still takes the token-checked
// Release path and a tenant/wrong token gets no operator elevation (it falls to
// Release and 404s). Exactly one manager method runs per request.
func TestReleaseOperatorToken(t *testing.T) {
	const rootTok, sbTok = "sekret", "sb-secret"
	tenants := []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}
	tests := []struct {
		name        string
		auth        string
		wantOp      bool   // ReleaseOperator expected
		wantRelease bool   // per-sandbox Release expected
		wantToken   string // token Release should receive (per-sandbox path)
		releaseErr  error  // error the per-sandbox Release returns
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
				t.Errorf("ReleaseOperator called=%v, want %v", opCalled, tt.wantOp)
			}
			if relCalled != tt.wantRelease {
				t.Errorf("Release called=%v, want %v", relCalled, tt.wantRelease)
			}
			if tt.wantOp && opID != "sb_1" {
				t.Errorf("ReleaseOperator id=%q, want sb_1", opID)
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

	promote := func(auth, body string) int {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/v1/sandboxes/sb_1/promote", strings.NewReader(body))
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
			if got := promote(tt.auth, tt.body); got != tt.want {
				t.Errorf("status %d, want %d", got, tt.want)
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

func TestClaimRedirectsOnWarmMiss(t *testing.T) {
	// Warm miss (fakeManager.ClaimWarm always misses) + a placer with
	// candidates → a redirect response, and the local manager never provisions.
	provisioned := false
	mgr := &fakeManager{claim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
		provisioned = true
		return &types.Sandbox{ID: "sb_local"}, nil
	}}
	srv := New("", nil, "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{addrs: []string{"node-b:7777", "node-c:7777"}}, nil)
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
			srv := New("", nil, "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{owners: []string{"node-b:7777"}}, nil)
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
	srv := New("", nil, "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{owners: []string{"node-b:7777"}}, nil)
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

	srvNoOwner := New("", nil, "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{}, nil)
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
	srv := New("", nil, "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{addrs: nil}, nil)
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

func TestOwnerEndpoint(t *testing.T) {
	// AgentSocket succeeds → this node owns it → 200 with owner addr.
	mgr := &fakeManager{socket: func(id, token string) (string, error) {
		if token == "good" {
			return "/v/sock", nil
		}
		return "", pool.ErrUnknownSandbox
	}}
	srv := New("", nil, "node-b:7777", mgr, &fakeDialer{}, nil, nil)
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

func newTestServer(t *testing.T, apiToken string, mgr Manager, dialer Dialer) *httptest.Server {
	t.Helper()
	return newTenantTestServer(t, apiToken, nil, mgr, dialer)
}

func newTenantTestServer(t *testing.T, apiToken string, tenants []config.TenantSpec, mgr Manager, dialer Dialer) *httptest.Server {
	t.Helper()
	if dialer == nil {
		dialer = &fakeDialer{}
	}
	srv := New(apiToken, tenants, "node:7777", mgr, dialer, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		srv.CloseRelays()
	})
	return ts
}

// fakeManager implements Manager with overridable behavior. ClaimWarm always
// misses, so the server's warm-miss → redirect → provision path is exercised;
// the claim hook stands in for the provision result. Tenant-scoped methods
// record the tenant they were handed in gotTenant.
type fakeManager struct {
	claim     func(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error)
	release   func(id, token string) error
	releaseOp func(id string) error
	socket    func(id, token string) (string, error)
	hibernate func(id, token string) error
	fork      func(id, token string, count int, ttl time.Duration) ([]*types.Sandbox, error)
	promote   func(id, token, template string) error

	deleteGolden func(key types.PoolKey) error
	hasGolden    bool

	audited          func(id string, line []byte)
	checkpoint       func(id, token, name string) (types.Checkpoint, error)
	claimCheckpoint  func(ckptID string) (*types.Sandbox, error)
	checkpoints      []types.Checkpoint
	deleteCheckpoint func(ckptID string) error
	setPools         func(pools []config.PoolSpec) error
	infoPools        []pool.PoolInfo

	gotTenant    string
	gotClaimRef  string
	tenantClaims map[string]int
	draining     bool
}

func (f *fakeManager) ClaimWarm(_ context.Context, _ types.PoolKey, _ time.Duration, tenant, claimRef string) (*types.Sandbox, error) {
	f.gotTenant = tenant
	f.gotClaimRef = claimRef
	return nil, pool.ErrNoWarm
}

func (f *fakeManager) ClaimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef string) (*types.Sandbox, error) {
	f.gotTenant = tenant
	f.gotClaimRef = claimRef
	if f.claim == nil {
		return &types.Sandbox{ID: "sb_1", Token: "tok"}, nil
	}
	return f.claim(ctx, key, ttl)
}

func (f *fakeManager) Release(_ context.Context, id, token string) error {
	if f.release == nil {
		return nil
	}
	return f.release(id, token)
}

func (f *fakeManager) ReleaseOperator(_ context.Context, id string) error {
	if f.releaseOp == nil {
		return nil
	}
	return f.releaseOp(id)
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

func (f *fakeManager) Hibernate(_ context.Context, id, token string) error {
	if f.hibernate == nil {
		return nil
	}
	return f.hibernate(id, token)
}

func (f *fakeManager) Fork(_ context.Context, id, token string, count int, ttl time.Duration) ([]*types.Sandbox, error) {
	if f.fork == nil {
		return nil, pool.ErrUnknownSandbox
	}
	return f.fork(id, token, count, ttl)
}

func (f *fakeManager) Promote(_ context.Context, id, token, template, tenant string) (types.PoolKey, error) {
	f.gotTenant = tenant
	if f.promote == nil {
		return types.PoolKey{}, pool.ErrUnknownSandbox
	}
	if err := f.promote(id, token, template); err != nil {
		return types.PoolKey{}, err
	}
	return types.PoolKey{Template: template, Net: types.NetNone, Size: types.SizeSmall}, nil
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

func (f *fakeManager) ClaimDeadline(id, token string) (time.Time, error) {
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

func (f *fakeManager) Sandboxes() []pool.SandboxSummary { return nil }

func (f *fakeManager) Audit(_ context.Context, id string, line []byte) {
	if f.audited != nil {
		f.audited(id, line)
	}
}

func (f *fakeManager) AuditEnabled() bool { return f.audited != nil }

func (f *fakeManager) Checkpoint(_ context.Context, id, token, name, tenant string) (types.Checkpoint, error) {
	f.gotTenant = tenant
	if f.checkpoint == nil {
		return types.Checkpoint{}, pool.ErrUnknownSandbox
	}
	return f.checkpoint(id, token, name)
}

func (f *fakeManager) ClaimCheckpoint(_ context.Context, ckptID string, _ time.Duration, tenant string) (*types.Sandbox, error) {
	f.gotTenant = tenant
	if f.claimCheckpoint == nil {
		return nil, pool.ErrUnknownCheckpoint
	}
	return f.claimCheckpoint(ckptID)
}

func (f *fakeManager) Checkpoints(_ context.Context, tenant string) ([]types.Checkpoint, error) {
	f.gotTenant = tenant
	return f.checkpoints, nil
}

func (f *fakeManager) DeleteCheckpoint(_ context.Context, ckptID, tenant string) error {
	f.gotTenant = tenant
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
	addrs  []string
	owners []string
}

func (f *fakePlacer) Candidates(string) []string     { return f.addrs }
func (f *fakePlacer) TemplateOwners(string) []string { return f.owners }
func (f *fakePlacer) PeerAddrs() []string            { return f.addrs }
func (f *fakePlacer) ConfigMismatches() int          { return 0 }
