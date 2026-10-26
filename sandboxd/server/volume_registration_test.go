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
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestVolumeRegistrationRoutesRequireRootBeforeReadingBody(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		for _, tt := range []struct {
			name, apiToken, token string
			code                  int
			wantCalled            bool
		}{
			{"root", "root", "root", http.StatusNoContent, true},
			{"tenant", "root", "acme-token", http.StatusForbidden, false},
			{"sandbox", "root", "sandbox-token", http.StatusUnauthorized, false},
			{"unknown", "root", "unknown", http.StatusUnauthorized, false},
			{"missing", "root", "", http.StatusUnauthorized, false},
			{"no api token", "", "", http.StatusForbidden, false},
		} {
			t.Run(method+"/"+tt.name, func(t *testing.T) {
				called := false
				mgr := &fakeManager{
					registerVolume:   func(context.Context, string, types.VolumeRegistration) error { called = true; return nil },
					unregisterVolume: func(context.Context, string) error { called = true; return nil },
					tenants:          []tenants.TenantSpec{{Name: "acme", Token: "acme-token"}},
				}
				body := bytes.NewBufferString(`{"path":"/private/data.img","writable":true}`)
				unread := body.Len()
				req := httptest.NewRequest(method, "/v1/volumes/data", body)
				req.Header.Set("Authorization", "Bearer "+tt.token)
				w := httptest.NewRecorder()
				New(tt.apiToken, "node:7777", mgr, &fakeDialer{}, nil, nil, nil, nil).Handler().ServeHTTP(w, req)
				if w.Code != tt.code || called != tt.wantCalled || !tt.wantCalled && body.Len() != unread || w.Header().Get("Retry-After") != "" {
					t.Fatalf("status=%d called=%v unread=%d/%d body=%s headers=%v, want %d", w.Code, called, body.Len(), unread, w.Body, w.Header(), tt.code)
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
		req := httptest.NewRequest(method, "/v1/volumes/data-v1", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer root")
		req = req.WithContext(context.WithValue(req.Context(), requestKey{}, "request-value"))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusNoContent || w.Body.Len() != 0 {
			t.Fatalf("%s status=%d body=%s, want empty 204", method, w.Code, w.Body)
		}
	}
	if registeredName != "data-v1" || removedName != "data-v1" || !reflect.DeepEqual(registered, want) {
		t.Fatalf("registered %q %+v, removed %q, want data-v1 %+v", registeredName, registered, removedName, want)
	}
}

func TestVolumeRegistrationRejectsBodyName(t *testing.T) {
	called := false
	mgr := &fakeManager{registerVolume: func(context.Context, string, types.VolumeRegistration) error {
		called = true
		return nil
	}}
	ts := newTestServer(t, "root", mgr, nil)
	resp := doReq(t, http.MethodPut, ts.URL+"/v1/volumes/data", "root", `{"name":"other","path":"/private/data.img"}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest || called {
		t.Fatalf("status=%d called=%v body=%s, want 400 without registration", resp.StatusCode, called, body)
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
				retry := tt.code == http.StatusServiceUnavailable
				seconds, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
				if resp.StatusCode != tt.code || strings.Contains(string(body), "/private") || (seconds >= 1) != retry || strings.Contains(got["error"], "retry") != retry {
					t.Fatalf("status=%d headers=%v body=%s, want safe %d with retry=%t", resp.StatusCode, resp.Header, body, tt.code, retry)
				}
			})
		}
	}
}
