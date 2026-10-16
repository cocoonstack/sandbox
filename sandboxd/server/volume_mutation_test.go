package server

import (
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestLiveVolumeRoutesAuthorizeAndDecode(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPatch} {
		for _, token := range []string{"sandbox-token", "root-token"} {
			t.Run(method+"/"+token, func(t *testing.T) {
				called := false
				check := func(id string, cred pool.Cred) {
					called = true
					if id != "sb_1" || cred.Operator != (token == "root-token") || !cred.Operator && cred.Token != token {
						t.Errorf("credentials %s %+v", id, cred)
					}
				}
				view := types.SandboxVolumesResponse{VolumesWant: []types.Volume{{Name: "data"}}, States: []types.VolumeState{{Name: "data", State: "pending"}}}
				mgr := &fakeManager{
					getVolumes: func(id string, cred pool.Cred) (types.SandboxVolumesResponse, error) {
						check(id, cred)
						return view, nil
					},
					setVolumes: func(id string, cred pool.Cred, v []types.Volume) (types.SandboxVolumesResponse, error) {
						check(id, cred)
						if !slices.Equal(v, []types.Volume{{Name: "data"}}) {
							t.Errorf("volumes %v", v)
						}
						return view, nil
					},
					patchVolumes: func(id string, cred pool.Cred, p types.VolumePatch) (types.SandboxVolumesResponse, error) {
						check(id, cred)
						if !slices.Equal(p.Remove, []string{"data"}) {
							t.Errorf("patch %v", p)
						}
						return view, nil
					},
				}
				ts := newTestServer(t, "root-token", mgr, nil)
				body := `{"volumes":[{"name":"data"}]}`
				if method == http.MethodPatch {
					body = `{"remove":["data"]}`
				}
				req, _ := http.NewRequest(method, ts.URL+"/v1/sandboxes/sb_1/volumes", strings.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := ts.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
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
		{"unknown field", "tok", `{"volumes":[],"path":"/host"}`, nil, http.StatusBadRequest},
		{"missing volumes", "tok", `{}`, nil, http.StatusBadRequest},
		{"null volumes", "tok", `{"volumes":null}`, nil, http.StatusBadRequest},
		{"wrong sandbox", "tok", `{"volumes":[]}`, pool.ErrUnknownSandbox, http.StatusNotFound},
		{"bad volume", "tok", `{"volumes":[]}`, pool.ErrBadVolume, http.StatusBadRequest},
		{"paused", "tok", `{"volumes":[]}`, pool.ErrPaused, http.StatusConflict},
		{"failed", "tok", `{"volumes":[]}`, pool.ErrFailed, http.StatusConflict},
		{"engine", "tok", `{"volumes":[]}`, errors.New("host /private/image failed"), http.StatusInternalServerError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{setVolumes: func(string, pool.Cred, []types.Volume) (types.SandboxVolumesResponse, error) {
				return types.SandboxVolumesResponse{VolumesWant: []types.Volume{{Name: "data"}}, States: []types.VolumeState{{Name: "data", State: "pending", Error: "host /private/image failed"}}}, tt.err
			}}
			ts := newTestServer(t, "root", mgr, nil)
			req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/sandboxes/sb_1/volumes", strings.NewReader(tt.body))
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tt.code || strings.Contains(string(body), "/private") {
				t.Fatalf("status=%d body=%s", resp.StatusCode, body)
			}
			if tt.err != nil {
				var got types.SandboxVolumesResponse
				if err := json.Unmarshal(body, &got); err != nil || len(got.VolumesWant) != 1 || len(got.States) != 1 || got.Error == "" {
					t.Fatalf("lost partial state: %s %v", body, err)
				}
			}
		})
	}
}
