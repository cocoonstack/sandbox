package server

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestVolumeRegistrationRoutesRequireRoot(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		for _, tt := range []struct {
			name, apiToken, token string
			code                  int
		}{
			{"root", "root", "root", http.StatusNoContent},
			{"tenant", "root", "acme-token", http.StatusForbidden},
			{"sandbox", "root", "sandbox-token", http.StatusUnauthorized},
			{"unknown", "root", "unknown", http.StatusUnauthorized},
			{"missing", "root", "", http.StatusUnauthorized},
			{"open without token", "", "", http.StatusForbidden},
			{"open with tenant token", "", "acme-token", http.StatusForbidden},
		} {
			t.Run(method+"/"+tt.name, func(t *testing.T) {
				called := false
				mgr := &fakeManager{
					registerVolume:   func(context.Context, string, types.VolumeRegistration) error { called = true; return nil },
					unregisterVolume: func(context.Context, string) error { called = true; return nil },
				}
				ts := newTenantTestServer(t, tt.apiToken, []tenants.TenantSpec{{Name: "acme", Token: "acme-token"}}, mgr, nil)
				body := `{"path":"/private/data.img"}`
				if tt.code != http.StatusNoContent {
					body = `{invalid`
				}
				resp := doReq(t, method, ts.URL+"/v1/volumes/data", tt.token, body)
				defer resp.Body.Close()
				got, _ := io.ReadAll(resp.Body)
				if resp.StatusCode != tt.code || called != (tt.code == http.StatusNoContent) {
					t.Fatalf("status=%d called=%v body=%s, want %d", resp.StatusCode, called, got, tt.code)
				}
				if tt.code == http.StatusNoContent && len(got) != 0 {
					t.Fatalf("204 response body = %q", got)
				}
			})
		}
	}
}

func TestVolumeRegistrationRequiresConfiguredTokenBeforeReadingBody(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.img")
	if err := os.WriteFile(path, []byte("existing backing"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		for _, token := range []string{"", "acme-token", "unknown"} {
			t.Run(method+"/"+token, func(t *testing.T) {
				called := false
				mgr := &fakeManager{
					registerVolume:   func(context.Context, string, types.VolumeRegistration) error { called = true; return nil },
					unregisterVolume: func(context.Context, string) error { called = true; return nil },
				}
				handler := New("", "", mgr, nil, nil, nil, nil, nil).Handler()
				body := bytes.NewBufferString(fmt.Sprintf(`{"path":%q,"writable":true}`, path))
				bodyLen := body.Len()
				req := httptest.NewRequest(method, "/v1/volumes/data", body)
				if token != "" {
					req.Header.Set("Authorization", "Bearer "+token)
				}
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, req)
				if w.Code != http.StatusForbidden || called || body.Len() != bodyLen {
					t.Fatalf("status=%d called=%v unread=%d/%d, want 403 without reading body or calling manager", w.Code, called, body.Len(), bodyLen)
				}
				if w.Header().Get("Retry-After") != "" {
					t.Fatalf("permanent refusal advertises retry: %v", w.Header())
				}
			})
		}
	}
}

func TestVolumeRegistrationRoutesForwardBindingAndContext(t *testing.T) {
	type requestKey struct{}
	const body = `{"path":"/private/images/../data.img","directio":"auto","writable":true,"tenants":["beta","acme"]}`
	want := types.VolumeRegistration{Path: "/private/images/../data.img", DirectIO: "auto", Writable: true, Tenants: []string{"beta", "acme"}}
	var registered types.VolumeRegistration
	var registeredName, removedName string
	checkContext := func(ctx context.Context) {
		if ctx.Value(requestKey{}) != "request-value" {
			t.Error("manager did not receive the request context")
		}
	}
	mgr := &fakeManager{
		registerVolume: func(ctx context.Context, name string, spec types.VolumeRegistration) error {
			checkContext(ctx)
			registeredName, registered = name, spec
			return nil
		},
		unregisterVolume: func(ctx context.Context, name string) error {
			checkContext(ctx)
			removedName = name
			return nil
		},
	}
	handler := New("root", "", mgr, nil, nil, nil, nil, nil).Handler()
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		for range 2 {
			req := httptest.NewRequest(method, "/v1/volumes/data-v1", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer root")
			req = req.WithContext(context.WithValue(req.Context(), requestKey{}, "request-value"))
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != http.StatusNoContent || w.Body.Len() != 0 {
				t.Fatalf("%s status=%d body=%s, want empty 204", method, w.Code, w.Body)
			}
		}
	}
	if registeredName != "data-v1" || removedName != "data-v1" || !reflect.DeepEqual(registered, want) {
		t.Fatalf("registered %q %+v, removed %q, want data-v1 %+v", registeredName, registered, removedName, want)
	}
}

func TestVolumeRegistrationRejectsInvalidBodies(t *testing.T) {
	for _, tt := range []struct{ name, body string }{
		{"missing", ""},
		{"malformed", `{"path":`},
		{"array", `[]`},
		{"unknown field", `{"path":"/private/data.img","extra":true}`},
		{"body name", `{"name":"other","path":"/private/data.img"}`},
		{"duplicate field", `{"path":"/private/a.img","path":"/private/b.img"}`},
		{"trailing object", `{"path":"/private/data.img"}{}`},
		{"trailing text", `{"path":"/private/data.img"}oops`},
		{"wrong writable type", `{"path":"/private/data.img","writable":"true"}`},
		{"wrong tenants type", `{"path":"/private/data.img","tenants":"acme"}`},
		{"oversized", `{"path":"/` + strings.Repeat("a", maxBodyBytes) + `"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			mgr := &fakeManager{registerVolume: func(context.Context, string, types.VolumeRegistration) error {
				called = true
				return nil
			}}
			ts := newTestServer(t, "root", mgr, nil)
			resp := doReq(t, http.MethodPut, ts.URL+"/v1/volumes/data", "root", tt.body)
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusBadRequest || called {
				t.Fatalf("status=%d called=%v body=%s, want 400 without registration", resp.StatusCode, called, body)
			}
		})
	}
}

func TestVolumeRegistrationRoutesValidateNames(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		for _, tt := range []struct {
			name, encoded string
			valid         bool
		}{
			{"data", "data", true},
			{"a_b-0", "a_b-0", true},
			{"abcdefghijklmnopqrst", "abcdefghijklmnopqrst", true},
			{"Data", "Data", false},
			{"1data", "1data", false},
			{"cocoon-data", "cocoon-data", false},
			{"abcdefghijklmnopqrstu", "abcdefghijklmnopqrstu", false},
			{"data/set", "data%2Fset", false},
			{"data set", "data%20set", false},
		} {
			t.Run(method+"/"+tt.name, func(t *testing.T) {
				var gotName string
				mgr := &fakeManager{
					registerVolume:   func(_ context.Context, name string, _ types.VolumeRegistration) error { gotName = name; return nil },
					unregisterVolume: func(_ context.Context, name string) error { gotName = name; return nil },
				}
				ts := newTestServer(t, "root", mgr, nil)
				resp := doReq(t, method, ts.URL+"/v1/volumes/"+tt.encoded, "root", `{"path":"/private/data.img"}`)
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)
				wantCode, wantName := http.StatusBadRequest, ""
				if tt.valid {
					wantCode, wantName = http.StatusNoContent, tt.name
				}
				if resp.StatusCode != wantCode || gotName != wantName {
					t.Fatalf("status=%d name=%q body=%s, want %d name=%q", resp.StatusCode, gotName, body, wantCode, wantName)
				}
			})
		}
	}
}

func TestVolumeRegistrationRoutesMapSafeErrors(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		for _, tt := range []struct {
			name string
			err  error
			code int
		}{
			{"invalid registration", fmt.Errorf("%w: invalid directio", pool.ErrBadVolume), http.StatusBadRequest},
			{"binding conflict", fmt.Errorf("%w: /private/data.img", pool.ErrVolumeRegistrationConflict), http.StatusConflict},
			{"OS root required", pool.ErrVolumeCatalogReadOnly, http.StatusConflict},
			{"invalid catalog destination", fmt.Errorf("%w: /private/runtime-volumes.json", pool.ErrVolumeCatalogInvalid), http.StatusConflict},
			{"catalog unavailable", fmt.Errorf("%w: sync /private/runtime-volumes.json", pool.ErrVolumeCatalogUnavailable), http.StatusServiceUnavailable},
			{"uncertain cancellation", fmt.Errorf("%w: %w", pool.ErrVolumeCatalogUnavailable, context.Canceled), http.StatusServiceUnavailable},
			{"uncertain timeout", fmt.Errorf("%w: %w", pool.ErrVolumeCatalogUnavailable, context.DeadlineExceeded), http.StatusServiceUnavailable},
			{"timeout", fmt.Errorf("register: %w", context.DeadlineExceeded), http.StatusGatewayTimeout},
			{"canceled", fmt.Errorf("register: %w", context.Canceled), http.StatusRequestTimeout},
			{"unexpected failure", errors.New("open /private/data.img: permission denied"), http.StatusInternalServerError},
		} {
			t.Run(method+"/"+tt.name, func(t *testing.T) {
				mgr := &fakeManager{
					registerVolume:   func(context.Context, string, types.VolumeRegistration) error { return tt.err },
					unregisterVolume: func(context.Context, string) error { return tt.err },
				}
				ts := newTestServer(t, "root", mgr, nil)
				resp := doReq(t, method, ts.URL+"/v1/volumes/data", "root", `{"path":"/private/data.img"}`)
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)
				var got map[string]string
				if err := json.Unmarshal(body, &got); err != nil || len(got) != 1 || got["error"] == "" {
					t.Fatalf("error body = %s, decode error = %v", body, err)
				}
				if resp.StatusCode != tt.code || strings.Contains(string(body), "/private") {
					t.Fatalf("status=%d body=%s, want safe %d", resp.StatusCode, body, tt.code)
				}
				if tt.code == http.StatusServiceUnavailable {
					seconds, err := strconv.Atoi(resp.Header.Get("Retry-After"))
					if err != nil || seconds < 1 || !strings.Contains(got["error"], "retry") {
						t.Fatalf("503 missing retry guidance: headers=%v body=%s", resp.Header, body)
					}
				} else if resp.Header.Get("Retry-After") != "" {
					t.Fatalf("permanent refusal advertises retry: %v", resp.Header)
				}
			})
		}
	}
}

func TestVolumeRegistrationKeepsCatalogListingPrivate(t *testing.T) {
	for _, token := range []string{"root", "acme-token"} {
		t.Run(token, func(t *testing.T) {
			mgr := &fakeManager{volumeCatalog: func(tenant string, _ map[string]int) []types.VolumeInfo {
				wantTenant := ""
				if token == "acme-token" {
					wantTenant = "acme"
				}
				if tenant != wantTenant {
					t.Errorf("listing tenant = %q, want %q", tenant, wantTenant)
				}
				return []types.VolumeInfo{{Name: "data", DefaultMount: "/volumes/data", SizeBytes: 42, Available: true, Nodes: 1, Writable: true}}
			}}
			ts := newTenantTestServer(t, "root", []tenants.TenantSpec{{Name: "acme", Token: "acme-token"}}, mgr, nil)
			resp := doReq(t, http.MethodGet, ts.URL+"/v1/volumes", token, "")
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			const want = `{"volumes":[{"name":"data","default_mount":"/volumes/data","size_bytes":42,"available":true,"nodes":1,"writable":true}]}`
			if resp.StatusCode != http.StatusOK || string(body) != want {
				t.Fatalf("catalog = %d %s, want 200 %s", resp.StatusCode, body, want)
			}
		})
	}
}
