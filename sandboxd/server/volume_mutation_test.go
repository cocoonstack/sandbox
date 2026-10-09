package server

import (
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
	for _, verb := range []string{"attach", "detach"} {
		for _, token := range []string{"sandbox-token", "root-token"} {
			t.Run(verb+"/"+token, func(t *testing.T) {
				called := false
				check := func(id string, cred pool.Cred) {
					called = true
					if id != "sb_1" || cred.Operator != (token == "root-token") || !cred.Operator && cred.Token != token {
						t.Errorf("wrong credentials: %s %+v", id, cred)
					}
				}
				mgr := &fakeManager{
					attachVolumes: func(id string, cred pool.Cred, v []types.Volume) ([]types.Volume, error) {
						check(id, cred)
						if !slices.Equal(v, []types.Volume{{Name: "data"}}) {
							t.Errorf("volumes: %v", v)
						}
						return []types.Volume{{Name: "data", Mount: "/volumes/data"}}, nil
					},
					detachVolumes: func(id string, cred pool.Cred, names []string) ([]types.Volume, error) {
						check(id, cred)
						if !slices.Equal(names, []string{"data"}) {
							t.Errorf("names: %v", names)
						}
						return nil, nil
					},
				}
				ts := newTestServer(t, "root-token", mgr, nil)
				body := `{"volumes":[{"name":"data"}]}`
				if verb == "detach" {
					body = `{"names":["data"]}`
				}
				req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/sandboxes/sb_1/volumes/"+verb, strings.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := ts.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK || !called {
					t.Fatalf("status=%d called=%v", resp.StatusCode, called)
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
		{"missing token", "", `{}`, nil, http.StatusUnauthorized},
		{"unknown field", "tok", `{"volumes":[],"path":"/host"}`, nil, http.StatusBadRequest},
		{"wrong sandbox", "tok", `{}`, pool.ErrUnknownSandbox, http.StatusNotFound},
		{"bad volume", "tok", `{}`, pool.ErrBadVolume, http.StatusBadRequest},
		{"pending", "tok", `{}`, pool.ErrVolumePending, http.StatusConflict},
		{"paused", "tok", `{}`, pool.ErrPaused, http.StatusConflict},
		{"failed", "tok", `{}`, pool.ErrFailed, http.StatusConflict},
		{"engine", "tok", `{}`, errors.New("host /private/image failed"), http.StatusInternalServerError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{attachVolumes: func(string, pool.Cred, []types.Volume) ([]types.Volume, error) { return nil, tt.err }}
			ts := newTestServer(t, "root", mgr, nil)
			req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/sandboxes/sb_1/volumes/attach", strings.NewReader(tt.body))
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
		})
	}
}
