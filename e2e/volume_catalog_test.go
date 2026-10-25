package e2e

import (
	"encoding/json/v2"
	"net/http"
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func TestRuntimeVolumePublicAdmissionThroughHTTP(t *testing.T) {
	st := startTenantStack(t, "node-token", []tenants.TenantSpec{{Name: "acme", Token: "acme-token"}}, nil)
	acme, err := sandbox.Connect(st.addr, sandbox.WithAPIToken("acme-token"))
	if err != nil {
		t.Fatal(err)
	}
	request, err := json.Marshal(map[string]any{"path": writeVolumeImage(t, "home.img", "shared bytes"), "writable": true})
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []struct {
		token string
		want  int
	}{{"acme-token", http.StatusForbidden}, {"", http.StatusUnauthorized}, {"node-token", http.StatusNoContent}} {
		caller := *st
		caller.token = scope.token
		if status, body := rawJSON(t, &caller, http.MethodPut, "/v1/volumes/home", string(request)); status != scope.want {
			t.Fatalf("registration auth: status=%d want=%d body=%s", status, scope.want, body)
		}
	}
	volumes, err := acme.Volumes(t.Context())
	if err != nil || len(volumes) != 1 || volumes[0].Name != "home" {
		t.Fatalf("empty runtime ACL is public: volumes=%v err=%v", volumes, err)
	}
	volume := sandbox.Volume{Name: "home", Mount: "/mnt/home"}
	first, err := acme.New(t.Context(), "rt:24.04", sandbox.WithVolumes(volume))
	if err != nil {
		t.Fatalf("tenant claim with public runtime volume: %v", err)
	}
	defer first.Close()
	if !slices.Equal(first.Volumes, []sandbox.Volume{volume}) {
		t.Fatalf("claim did not retain mounted volume: %+v", first.Volumes)
	}
	if _, err = first.SetVolumes(t.Context(), []sandbox.Volume{}); err != nil {
		t.Fatal(err)
	}
	if view, attachErr := first.PatchVolumes(t.Context(), sandbox.VolumePatch{Add: []sandbox.Volume{volume}}); attachErr != nil || !slices.Equal(view.Volumes, []sandbox.Volume{volume}) {
		t.Fatalf("tenant PATCH of public runtime volume: view=%+v err=%v", view, attachErr)
	}
	if _, err = first.SetVolumes(t.Context(), []sandbox.Volume{}); err != nil {
		t.Fatal(err)
	}
	if view, attachErr := first.SetVolumes(t.Context(), []sandbox.Volume{volume}); attachErr != nil || !slices.Equal(view.Volumes, []sandbox.Volume{volume}) {
		t.Fatalf("tenant PUT of public runtime volume: view=%+v err=%v", view, attachErr)
	}
}

func TestRuntimeVolumeExplicitACLAndStaticPublicThroughHTTP(t *testing.T) {
	st := startTenantStack(t, "node-token", []tenants.TenantSpec{
		{Name: "acme", Token: "acme-token"}, {Name: "beta", Token: "beta-token"},
	}, []config.VolumeSpec{{Name: "public", Path: writeVolumeImage(t, "public.img", "shared")}})
	request, err := json.Marshal(map[string]any{"path": writeVolumeImage(t, "private.img", "private"), "tenants": []string{"acme"}})
	if err != nil {
		t.Fatal(err)
	}
	if status, body := rawJSON(t, st, http.MethodPut, "/v1/volumes/private", string(request)); status != http.StatusNoContent {
		t.Fatalf("registration: %d %s", status, body)
	}
	for _, tc := range []struct {
		token   string
		want    []string
		allowed bool
	}{{"acme-token", []string{"private", "public"}, true}, {"beta-token", []string{"public"}, false}} {
		client, connectErr := sandbox.Connect(st.addr, sandbox.WithAPIToken(tc.token))
		if connectErr != nil {
			t.Fatal(connectErr)
		}
		volumes, listErr := client.Volumes(t.Context())
		var names []string
		for _, volume := range volumes {
			names = append(names, volume.Name)
		}
		if listErr != nil || !slices.Equal(names, tc.want) {
			t.Fatalf("catalog ACL: names=%v want=%v err=%v", names, tc.want, listErr)
		}
		claim, claimErr := client.New(t.Context(), "rt:24.04", sandbox.WithVolumes(sandbox.Volume{Name: "public"}))
		if claimErr != nil {
			t.Fatal(claimErr)
		}
		defer claim.Close()
		_, attachErr := claim.PatchVolumes(t.Context(), sandbox.VolumePatch{Add: []sandbox.Volume{{Name: "private"}}})
		if (attachErr == nil) != tc.allowed {
			t.Fatalf("explicit ACL admission for %s: %v", tc.token, attachErr)
		}
	}
	caller := *st
	caller.token = "beta-token"
	var responses [][]byte
	for _, name := range []string{"private", "unknown"} {
		status, body := rawJSON(t, &caller, http.MethodPost, "/v1/claim", `{"template":"rt:24.04","volumes":[{"name":"`+name+`"}]}`)
		if status != http.StatusBadRequest {
			t.Fatalf("claim privacy for %s: status=%d body=%s", name, status, body)
		}
		responses = append(responses, body)
	}
	if string(responses[0]) != string(responses[1]) {
		t.Fatalf("forbidden and unknown responses differ: %s versus %s", responses[0], responses[1])
	}
}
