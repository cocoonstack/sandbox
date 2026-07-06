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
		{"healthz open", "/healthz", http.MethodGet, "", http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body io.Reader
			if tt.method == http.MethodPost {
				body = strings.NewReader(`{"template":"rt:24.04"}`)
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

// TestSandboxVerbFlows drives both handleSandboxVerb routes; release and
// hibernate share auth, error mapping, and id/token plumbing by construction.
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

func newTestServer(t *testing.T, apiToken string, mgr Manager, dialer Dialer) *httptest.Server {
	t.Helper()
	if dialer == nil {
		dialer = &fakeDialer{}
	}
	srv := New(apiToken, "node:7777", mgr, dialer, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		srv.CloseRelays()
	})
	return ts
}

// fakeManager implements Manager with overridable behavior. ClaimWarm always
// misses, so the server's warm-miss → redirect → provision path is exercised;
// the claim hook stands in for the provision result.
type fakeManager struct {
	claim     func(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error)
	release   func(id, token string) error
	socket    func(id, token string) (string, error)
	hibernate func(id, token string) error
	fork      func(id, token string, count int, ttl time.Duration) ([]*types.Sandbox, error)
	promote   func(id, token, template string) error

	deleteGolden func(key types.PoolKey) error
}

func (f *fakeManager) ClaimWarm(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
	return nil, pool.ErrNoWarm
}

func (f *fakeManager) ClaimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
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

func (f *fakeManager) Promote(_ context.Context, id, token, template string) (types.PoolKey, error) {
	if f.promote == nil {
		return types.PoolKey{}, pool.ErrUnknownSandbox
	}
	if err := f.promote(id, token, template); err != nil {
		return types.PoolKey{}, err
	}
	return types.PoolKey{Template: template, Net: types.NetNone, Size: types.SizeSmall}, nil
}

func (f *fakeManager) DeleteTemplate(key types.PoolKey) error {
	if f.deleteGolden == nil {
		return pool.ErrUnknownTemplate
	}
	return f.deleteGolden(key)
}

func (f *fakeManager) Info() ([]pool.PoolInfo, int, int) {
	return []pool.PoolInfo{}, 0, 0
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

type fakePlacer struct{ addrs []string }

func (f *fakePlacer) Candidates(string) []string { return f.addrs }
func (f *fakePlacer) PeerAddrs() []string        { return f.addrs }

func TestClaimRedirectsOnWarmMiss(t *testing.T) {
	// Warm miss (fakeManager.ClaimWarm always misses) + a placer with
	// candidates → a redirect response, and the local manager never provisions.
	provisioned := false
	mgr := &fakeManager{claim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
		provisioned = true
		return &types.Sandbox{ID: "sb_local"}, nil
	}}
	srv := New("", "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{addrs: []string{"node-b:7777", "node-c:7777"}})
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

func TestClaimProvisionsWhenNoCandidate(t *testing.T) {
	// Warm miss + a placer with no candidates → local provision.
	mgr := &fakeManager{claim: func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) {
		return &types.Sandbox{ID: "sb_local", Token: "tok"}, nil
	}}
	srv := New("", "node-a:7777", mgr, &fakeDialer{}, &fakePlacer{addrs: nil})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.CloseRelays() })

	resp, err := http.Post(ts.URL+"/v1/claim", "application/json", strings.NewReader(`{"template":"rt:24.04"}`))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer resp.Body.Close()
	var cr types.ClaimResponse
	_ = json.NewDecoder(resp.Body).Decode(&cr)
	if cr.ID != "sb_local" || len(cr.Redirect) != 0 {
		t.Errorf("got %+v, want local sandbox", cr)
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
	srv := New("", "node-b:7777", mgr, &fakeDialer{}, nil)
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
