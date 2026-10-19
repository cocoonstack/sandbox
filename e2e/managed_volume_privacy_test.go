package e2e

import (
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/server"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
)

func TestLiveAttachManagedVolumePrivacy(t *testing.T) {
	st := startManagedHTTPStack(t, "root-token")
	for name, token := range map[string]string{"private": "t1-token", "owned": "t2-token"} {
		created := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/"+name, token, `{"size_gib":1}`, "")
		if created.status != http.StatusCreated {
			t.Fatalf("create %s: %d %s", name, created.status, created.body)
		}
	}
	claim := requestManagedVolume(t, st, http.MethodPost, "/v1/claim", "t2-token", `{"template":"rt:24.04","net":"none","size":"small"}`, "")
	if claim.status != http.StatusOK {
		t.Fatalf("claim: %d %s", claim.status, claim.body)
	}
	var sb server.ClaimResponse
	if err := json.Unmarshal(claim.body, &sb); err != nil {
		t.Fatal(err)
	}
	route := "/v1/sandboxes/" + sb.ID + "/volumes"
	foreign := requestManagedVolume(t, st, http.MethodPatch, route, "t2-token", `{"add":[{"name":"private","mount":"/volumes/probe"}]}`, "")
	unknown := requestManagedVolume(t, st, http.MethodPatch, route, "t2-token", `{"add":[{"name":"unknown","mount":"/volumes/probe"}]}`, "")
	if foreign.status != http.StatusNotFound || unknown.status != http.StatusNotFound || string(foreign.body) != string(unknown.body) {
		t.Errorf("foreign=%d %s; unknown=%d %s; want identical 404 replies", foreign.status, foreign.body, unknown.status, unknown.body)
	}
	owned := requestManagedVolume(t, st, http.MethodPatch, route, "t2-token", `{"add":[{"name":"owned","mount":"/volumes/probe"}]}`, "")
	if owned.status != http.StatusBadRequest || !strings.Contains(string(owned.body), "claim-time attachment only") {
		t.Errorf("owned=%d %s; want unsupported live attach", owned.status, owned.body)
	}
}

type catalogPeerPlacer struct{ edgePlacer }

func (*catalogPeerPlacer) VolumeCandidates(string, []string) []string { return []string{"peer:7777"} }
func (*catalogPeerPlacer) VolumeOwners([]string) []string             { return []string{"peer:7777"} }
func (*catalogPeerPlacer) VolumeHolders() map[string]int              { return map[string]int{"peer-only": 1} }

func TestRootPeerCatalogRedirectWithManagedStore(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			var st *stack
			if enabled {
				st = startManagedHTTPStack(t, "root-token")
			} else {
				st = startConfigStack(t, "root-token", &config.Config{})
			}
			srv := server.New(st.token, "", st.mgr, st.eng.real, &catalogPeerPlacer{}, nil, nil, nil)
			ts := httptest.NewServer(srv.Handler())
			defer ts.Close()
			defer srv.CloseRelays()
			probe := *st
			probe.addr = strings.TrimPrefix(ts.URL, "http://")
			response := requestManagedVolume(t, &probe, http.MethodPost, "/v1/claim", "root-token", `{"template":"rt:24.04","net":"none","size":"small","volumes":[{"name":"peer-only"}]}`, "")
			if response.status != http.StatusOK {
				t.Fatalf("claim: %d %s", response.status, response.body)
			}
			var reply server.ClaimResponse
			if err := json.Unmarshal(response.body, &reply); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(reply.Redirect, []string{"peer:7777"}) || reply.ID != "" {
				t.Fatalf("redirect reply = %+v", reply)
			}
		})
	}
}

func TestStaticVolumePrivacyWithManagedStore(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "catalog.raw")
			if err := os.WriteFile(path, []byte("catalog"), 0600); err != nil {
				t.Fatal(err)
			}
			cfg := &config.Config{Volumes: []config.VolumeSpec{{Name: "private-static", Path: path, Tenants: []string{"t1"}}}}
			expected := http.StatusBadRequest
			if enabled {
				cfg.ManagedVolumeDir = filepath.Join(dir, "managed")
				expected = http.StatusNotFound
			}
			st := startConfigStack(t, "root-token", cfg)
			if err := st.mgr.SetTenants(t.Context(), []tenants.TenantSpec{{Name: "t1", Token: "t1-token"}, {Name: "t2", Token: "t2-token"}}); err != nil {
				t.Fatal(err)
			}
			t.Run("claim", func(t *testing.T) {
				foreign := requestManagedVolume(t, st, http.MethodPost, "/v1/claim", "t2-token", `{"template":"rt:24.04","net":"none","size":"small","volumes":[{"name":"private-static"}]}`, "")
				unknown := requestManagedVolume(t, st, http.MethodPost, "/v1/claim", "t2-token", `{"template":"rt:24.04","net":"none","size":"small","volumes":[{"name":"unknown"}]}`, "")
				if foreign.status != expected || unknown.status != expected || string(foreign.body) != string(unknown.body) {
					t.Fatalf("foreign=%d %s; unknown=%d %s; want identical %d replies", foreign.status, foreign.body, unknown.status, unknown.body, expected)
				}
			})
			t.Run("live-attach", func(t *testing.T) {
				claim := requestManagedVolume(t, st, http.MethodPost, "/v1/claim", "t2-token", `{"template":"rt:24.04","net":"none","size":"small"}`, "")
				if claim.status != http.StatusOK {
					t.Fatalf("claim: %d %s", claim.status, claim.body)
				}
				var sb server.ClaimResponse
				if err := json.Unmarshal(claim.body, &sb); err != nil {
					t.Fatal(err)
				}
				route := "/v1/sandboxes/" + sb.ID + "/volumes"
				foreign := requestManagedVolume(t, st, http.MethodPatch, route, "t2-token", `{"add":[{"name":"private-static","mount":"/volumes/probe"}]}`, "")
				unknown := requestManagedVolume(t, st, http.MethodPatch, route, "t2-token", `{"add":[{"name":"unknown","mount":"/volumes/probe"}]}`, "")
				if foreign.status != expected || unknown.status != expected || string(foreign.body) != string(unknown.body) {
					t.Fatalf("foreign=%d %s; unknown=%d %s; want identical %d replies", foreign.status, foreign.body, unknown.status, unknown.body, expected)
				}
			})
		})
	}
}
