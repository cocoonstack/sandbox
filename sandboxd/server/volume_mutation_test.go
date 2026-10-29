package server

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestLiveVolumeRoutesAuthorizeAndDecode(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPatch} {
		for _, cred := range []struct{ token, tenant string }{{"acme-token", "acme"}, {"root-token", ""}} {
			t.Run(method+"/"+cred.token, func(t *testing.T) {
				called := false
				check := func(id, tenant string) {
					called = true
					if id != "sb_1" || tenant != cred.tenant {
						t.Errorf("scope %s %q", id, tenant)
					}
				}
				view := types.SandboxVolumesResponse{VolumesWant: []types.Volume{{Name: "data"}}, States: []types.VolumeState{{Name: "data", State: "pending"}}}
				mgr := &fakeManager{
					getVolumes: func(id, tenant string) (types.SandboxVolumesResponse, error) {
						check(id, tenant)
						return view, nil
					},
					setVolumes: func(id, tenant string, v []types.Volume) (types.SandboxVolumesResponse, error) {
						check(id, tenant)
						if !slices.Equal(v, []types.Volume{{Name: "data"}}) {
							t.Errorf("volumes %v", v)
						}
						return view, nil
					},
					patchVolumes: func(id, tenant string, p types.VolumePatch) (types.SandboxVolumesResponse, error) {
						check(id, tenant)
						if !slices.Equal(p.Remove, []string{"data"}) {
							t.Errorf("patch %v", p)
						}
						return view, nil
					},
				}
				ts := newTenantTestServer(t, "root-token", []tenants.TenantSpec{{Name: "acme", Token: "acme-token"}}, mgr, nil)
				body := `{"volumes":[{"name":"data"}]}`
				if method == http.MethodPatch {
					body = `{"remove":["data"]}`
				}
				resp := doReq(t, method, ts.URL+"/v1/sandboxes/sb_1/volumes", cred.token, body)
				defer resp.Body.Close()
				var got types.SandboxVolumesResponse
				if err := json.UnmarshalRead(resp.Body, &got); err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != http.StatusOK || !called || len(got.VolumesWant) != 1 || got.States[0].State != "pending" {
					t.Fatalf("status=%d called=%v view=%+v", resp.StatusCode, called, got)
				}
			})
		}
	}
}

func TestLiveVolumeRoutesErrors(t *testing.T) {
	for _, tt := range []struct {
		name, token, body string
		err               error
		code              int
	}{
		{"missing token", "", `{"volumes":[]}`, nil, http.StatusUnauthorized},
		{"unknown field", "root", `{"volumes":[],"path":"/host"}`, nil, http.StatusBadRequest},
		{"missing volumes", "root", `{}`, nil, http.StatusBadRequest},
		{"null volumes", "root", `{"volumes":null}`, nil, http.StatusBadRequest},
		{"wrong sandbox", "root", `{"volumes":[]}`, pool.ErrUnknownSandbox, http.StatusNotFound},
		{"bad volume", "root", `{"volumes":[]}`, pool.ErrBadVolume, http.StatusBadRequest},
		{"paused", "root", `{"volumes":[]}`, pool.ErrPaused, http.StatusConflict},
		{"failed", "root", `{"volumes":[]}`, pool.ErrFailed, http.StatusConflict},
		{"sandbox token", "sandbox-token", `{"volumes":[]}`, nil, http.StatusUnauthorized},
		{"invalid token", "invalid", `{"volumes":[]}`, nil, http.StatusUnauthorized},
		{"foreign mount", "root", `{"volumes":[]}`, &pool.VolumeMountConflict{Message: `volume mount "/data" has a different device, root, or mode; restore the expected mount before retrying`}, http.StatusConflict},
		{"bind mount", "root", `{"volumes":[]}`, &pool.VolumeMountConflict{Message: `volume device has another mount at "/other"; remove guest mount references before retrying`}, http.StatusConflict},
		{"unmount refused", "root", `{"volumes":[]}`, &pool.VolumeMountConflict{Message: `cannot unmount "/data"; close guest references and retry`}, http.StatusConflict},
		{"unsupported runtime", "root", `{"volumes":[]}`, &pool.LiveVolumeUnsupported{Version: "v0.7.1"}, http.StatusServiceUnavailable},
		{"tenant store down", "root", `{"volumes":[]}`, fmt.Errorf("%w: connect host /private/pg", pool.ErrTenantStoreDown), http.StatusServiceUnavailable},
		{"unknown runtime", "root", `{"volumes":[]}`, &pool.LiveVolumeUnsupported{}, http.StatusServiceUnavailable},
		{"timeout", "root", `{"volumes":[]}`, context.DeadlineExceeded, http.StatusGatewayTimeout},
		{"cancel", "root", `{"volumes":[]}`, context.Canceled, http.StatusRequestTimeout},
		{"engine", "root", `{"volumes":[]}`, errors.New("host /private/image failed"), http.StatusInternalServerError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{setVolumes: func(string, string, []types.Volume) (types.SandboxVolumesResponse, error) {
				return types.SandboxVolumesResponse{VolumesWant: []types.Volume{{Name: "data"}}, States: []types.VolumeState{{Name: "data", State: "pending", Error: "volume operation failed; retry after resolving the runtime error"}}}, tt.err
			}}
			ts := newTestServer(t, "root", mgr, nil)
			resp := doReq(t, http.MethodPut, ts.URL+"/v1/sandboxes/sb_1/volumes", tt.token, tt.body)
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tt.code || strings.Contains(string(body), "/private") {
				t.Fatalf("status=%d body=%s", resp.StatusCode, body)
			}
			if errors.Is(tt.err, pool.ErrTenantStoreDown) && resp.Header.Get("Retry-After") == "" {
				t.Fatal("store outage without Retry-After")
			}
			if tt.err != nil && tt.code != http.StatusNotFound {
				var got types.SandboxVolumesResponse
				if err := json.Unmarshal(body, &got); err != nil || len(got.VolumesWant) != 1 || len(got.States) != 1 || got.Error == "" || got.States[0].Error != "volume operation failed; retry after resolving the runtime error" {
					t.Fatalf("lost partial state: %s %v", body, err)
				}
			}
		})
	}
}

func TestLiveVolumeRoutesHideOtherTenants(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			called := false
			check := func(id, tenant string) (types.SandboxVolumesResponse, error) {
				called = true
				if id != "sb_1" || tenant != "other" {
					t.Errorf("scope %s %q", id, tenant)
				}
				return types.SandboxVolumesResponse{}, pool.ErrUnknownSandbox
			}
			mgr := &fakeManager{
				getVolumes: check,
				setVolumes: func(id, tenant string, _ []types.Volume) (types.SandboxVolumesResponse, error) {
					return check(id, tenant)
				},
				patchVolumes: func(id, tenant string, _ types.VolumePatch) (types.SandboxVolumesResponse, error) {
					return check(id, tenant)
				},
			}
			ts := newTenantTestServer(t, "root", []tenants.TenantSpec{{Name: "other", Token: "other-token"}}, mgr, nil)
			body := `{"volumes":[]}`
			if method == http.MethodPatch {
				body = `{}`
			}
			resp := doReq(t, method, ts.URL+"/v1/sandboxes/sb_1/volumes", "other-token", body)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound || !called {
				t.Fatalf("status=%d called=%v", resp.StatusCode, called)
			}
		})
	}
}

func TestLiveVolumeRoutesReportRefusalsOverRecordedDiagnostics(t *testing.T) {
	const cause = "cannot unmount /data; close guest references and retry"
	for _, refusal := range []error{pool.ErrVolumeBusy, pool.ErrVolumeCapture, pool.ErrReplicaPresent, pool.ErrReplicatingPool, pool.ErrReplicaStranded} {
		t.Run(refusal.Error(), func(t *testing.T) {
			mgr := &fakeManager{setVolumes: func(string, string, []types.Volume) (types.SandboxVolumesResponse, error) {
				return types.SandboxVolumesResponse{VolumesWant: []types.Volume{{Name: "old"}}, States: []types.VolumeState{{Name: "old", State: "pending", Error: cause}}, Error: "volume change failed; retry the desired target"}, fmt.Errorf("%w: volume %q", refusal, "data")
			}}
			ts := newTestServer(t, "root", mgr, nil)
			resp := doReq(t, http.MethodPut, ts.URL+"/v1/sandboxes/sb_1/volumes", "root", `{"volumes":[]}`)
			defer resp.Body.Close()
			var got types.SandboxVolumesResponse
			if err := json.UnmarshalRead(resp.Body, &got); err != nil {
				t.Fatal(err)
			}
			wantStatus := http.StatusConflict
			if errors.Is(refusal, pool.ErrReplicaPresent) {
				wantStatus = http.StatusServiceUnavailable
				if resp.Header.Get("Retry-After") == "" {
					t.Fatal("temporary replica retirement refusal has no Retry-After")
				}
			} else if resp.Header.Get("Retry-After") != "" {
				t.Fatal("permanent conflict advertises a retry delay")
			}
			if resp.StatusCode != wantStatus || !strings.HasPrefix(got.Error, refusal.Error()) || len(got.States) != 1 || got.States[0].Error != cause {
				t.Fatalf("status=%d view=%+v", resp.StatusCode, got)
			}
		})
	}
}

func TestClaimVolumeCompatibilityExpansion(t *testing.T) {
	for _, tt := range []struct {
		name string
		req  ClaimRequest
		want []types.Volume
		err  bool
	}{
		{"default mount", ClaimRequest{Volumes: []types.Volume{{Name: "data"}}}, []types.Volume{{Name: "data", Mount: "/volumes/data"}}, false},
		{"legacy all devices", ClaimRequest{VolumesAttachOnly: true, Volumes: []types.Volume{{Name: "data"}, {Name: "work", Mode: types.VolumeModeRW}}}, []types.Volume{{Name: "data", AttachOnly: true}, {Name: "work", Mode: types.VolumeModeRW, AttachOnly: true}}, false},
		{"mixed", ClaimRequest{Volumes: []types.Volume{{Name: "data"}, {Name: "work", AttachOnly: true}}}, []types.Volume{{Name: "data", Mount: "/volumes/data"}, {Name: "work", AttachOnly: true}}, false},
		{"mixed reversed", ClaimRequest{Volumes: []types.Volume{{Name: "work", AttachOnly: true}, {Name: "data"}}}, []types.Volume{{Name: "work", AttachOnly: true}, {Name: "data", Mount: "/volumes/data"}}, false},
		{"empty legacy", ClaimRequest{VolumesAttachOnly: true}, nil, true},
		{"legacy mount conflict", ClaimRequest{VolumesAttachOnly: true, Volumes: []types.Volume{{Name: "data", Mount: "/data"}}}, nil, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := slices.Clone(tt.req.Volumes)
			got, err := tt.req.normalizedVolumes()
			if (err != nil) != tt.err || !slices.Equal(got, tt.want) {
				t.Fatalf("normalized volumes=%+v err=%v, want %+v error=%t", got, err, tt.want, tt.err)
			}
			if !slices.Equal(tt.req.Volumes, before) {
				t.Fatal("legacy expansion mutated caller input")
			}
		})
	}
}
