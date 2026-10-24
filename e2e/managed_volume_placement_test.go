package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/server"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
)

func managedGossipKey(tenant, name string) string {
	sum := sha256.Sum256([]byte("volume|" + tenant + "|" + name))
	return "managed/" + hex.EncodeToString(sum[:16])
}

type managedOwnerPlacer struct {
	edgePlacer
	key      string
	template bool
}

func (p *managedOwnerPlacer) owners(names []string) []string {
	if slices.Contains(names, p.key) {
		return []string{"peer:7777"}
	}
	return nil
}

func (p *managedOwnerPlacer) VolumeCandidates(_ string, names []string) []string {
	return p.owners(names)
}

func (p *managedOwnerPlacer) VolumeOwners(names []string) []string { return p.owners(names) }

func (p *managedOwnerPlacer) TemplateVolumeOwners(_ string, names []string) []string {
	return p.owners(names)
}

func (p *managedOwnerPlacer) TemplateOwners(string) []string {
	if p.template {
		return []string{"peer:7777"}
	}
	return nil
}

func placedManagedStack(t *testing.T, placer server.Placer) *stack {
	t.Helper()
	return placeStack(t, startManagedHTTPStack(t, "root-token"), placer)
}

func placeStack(t *testing.T, st *stack, placer server.Placer) *stack {
	t.Helper()
	srv := server.New(st.token, "", st.mgr, st.eng.real, placer, nil, nil, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	t.Cleanup(srv.CloseRelays)
	probe := *st
	probe.addr = strings.TrimPrefix(ts.URL, "http://")
	return &probe
}

func TestManagedVolumeClaimRedirectsToOwnerNode(t *testing.T) {
	entries := map[string]func(*testing.T) *stack{
		"entry with managed storage": func(t *testing.T) *stack { return startManagedHTTPStack(t, "root-token") },
		"entry without managed storage": func(t *testing.T) *stack {
			st := startConfigStack(t, "root-token", &config.Config{})
			if err := st.mgr.SetTenants(t.Context(), []tenants.TenantSpec{{Name: "t1", Token: "t1-token"}, {Name: "t2", Token: "t2-token"}}); err != nil {
				t.Fatal(err)
			}
			return st
		},
	}
	for name, entry := range entries {
		t.Run(name, func(t *testing.T) {
			st := placeStack(t, entry(t), &managedOwnerPlacer{key: managedGossipKey("t1", "work")})
			claim := `{"template":"rt:24.04","net":"none","size":"small","volumes":[{"name":"%s","mode":"rw"}]}`
			reply := requestManagedVolume(t, st, http.MethodPost, "/v1/claim", "t1-token", strings.Replace(claim, "%s", "work", 1), "")
			var resp server.ClaimResponse
			_ = json.Unmarshal(reply.body, &resp)
			if reply.status != http.StatusOK || len(resp.Redirect) == 0 {
				t.Fatalf("owner tenant claim of a peer's managed volume: %d %s, want a redirect to the owner node", reply.status, reply.body)
			}
			foreign := requestManagedVolume(t, st, http.MethodPost, "/v1/claim", "t2-token", strings.Replace(claim, "%s", "work", 1), "")
			unknown := requestManagedVolume(t, st, http.MethodPost, "/v1/claim", "t2-token", strings.Replace(claim, "%s", "nope", 1), "")
			if foreign.status != http.StatusBadRequest || string(foreign.body) != string(unknown.body) {
				t.Fatalf("other tenant: %d %s; unknown: %d %s; want identical 400 replies", foreign.status, foreign.body, unknown.status, unknown.body)
			}
		})
	}
}

func TestManagedVolumeCreateRefusesANamePeerAlreadyHolds(t *testing.T) {
	st := placedManagedStack(t, &managedOwnerPlacer{internal: "peer:7777", public: "https://peer.example", key: managedGossipKey("t1", "work")})
	for _, c := range []struct{ token, body string }{
		{"t1-token", `{"size_gib":1}`},
		{"root-token", `{"size_gib":1,"tenant":"t1"}`},
	} {
		reply := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/work", c.token, c.body, "")
		if reply.status != http.StatusConflict || reply.retryAfter != "" || !strings.Contains(string(reply.body), "https://peer.example") {
			t.Fatalf("%s create of t1's work held on peer:7777: %d Retry-After=%q %s, want 409 naming https://peer.example", c.token, reply.status, reply.retryAfter, reply.body)
		}
	}
	if got := requestManagedVolume(t, st, http.MethodGet, "/v1/volumes/work", "root-token", "", ""); got.status != http.StatusNotFound {
		t.Fatalf("refused create left a local volume: %d %s", got.status, got.body)
	}
	if other := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/work", "t2-token", `{"size_gib":1}`, ""); other.status != http.StatusCreated {
		t.Fatalf("t2 create of a name only t1 holds on a peer: %d %s, want 201", other.status, other.body)
	}
}

func TestManagedVolumeClaimFailsClearlyWhenNoNodeHasTemplateAndVolume(t *testing.T) {
	st := placedManagedStack(t, &managedOwnerPlacer{template: true})
	if c := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/work", "t1-token", `{"size_gib":1}`, ""); c.status != http.StatusCreated {
		t.Fatalf("create: %d %s", c.status, c.body)
	}
	reply := requestManagedVolume(t, st, http.MethodPost, "/v1/claim", "t1-token", `{"template":"rt:24.04","net":"none","size":"small","volumes":[{"name":"work","mode":"rw"}]}`, "")
	if reply.status != http.StatusConflict || !strings.Contains(string(reply.body), "no node holds both") {
		t.Fatalf("volume local, template only on a peer: %d %s, want 409 saying no node holds both", reply.status, reply.body)
	}
}
