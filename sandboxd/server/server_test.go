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

func TestReleaseFlow(t *testing.T) {
	tests := []struct {
		name string
		auth string
		err  error
		want int
	}{
		{"ok", "Bearer tok", nil, http.StatusNoContent},
		{"unknown or bad token", "Bearer bad", pool.ErrUnknownSandbox, http.StatusNotFound},
		{"missing bearer", "", nil, http.StatusUnauthorized},
		{"engine failure", "Bearer tok", errors.New("rm failed"), http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{release: func(id, token string) error { return tt.err }}
			ts := newTestServer(t, "", mgr, nil)
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

// fakeManager implements Manager with overridable behavior. ClaimWarm reports
// a miss unless warmHit is set, so the server's warm→redirect→provision path
// is exercised; the claim hook stands in for the provision result.
type fakeManager struct {
	claim   func(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error)
	warmHit bool
	release func(id, token string) error
	socket  func(id, token string) (string, error)
}

func (f *fakeManager) doClaim(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
	if f.claim == nil {
		return &types.Sandbox{ID: "sb_1", Token: "tok"}, nil
	}
	return f.claim(ctx, key, ttl)
}

func (f *fakeManager) Claim(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
	return f.doClaim(ctx, key, ttl)
}

func (f *fakeManager) ClaimWarm(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
	if !f.warmHit {
		return nil, pool.ErrNoWarm
	}
	return f.doClaim(ctx, key, ttl)
}

func (f *fakeManager) ClaimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
	return f.doClaim(ctx, key, ttl)
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

func (f *fakeManager) Info() ([]pool.PoolInfo, int) {
	return []pool.PoolInfo{}, 0
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

// fakePlacer returns canned candidates.
type fakePlacer struct{ addrs []string }

func (f *fakePlacer) Candidates(string) []string { return f.addrs }

func TestClaimRedirectsOnWarmMiss(t *testing.T) {
	// Warm miss (fakeManager.warmHit=false) + a placer with candidates → a
	// redirect response, and the local manager never provisions.
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
