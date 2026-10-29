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

func TestRuntimeVolumeRegistrationThroughHTTP(t *testing.T) {
	st := startTenantStack(t, "node-token", []tenants.TenantSpec{
		{Name: "acme", Token: "acme-token"}, {Name: "beta", Token: "beta-token"},
	}, []config.VolumeSpec{{Name: "public", Path: writeVolumeImage(t, "public.img", "shared")}})
	for name, body := range map[string]map[string]any{
		"home":    {"path": writeVolumeImage(t, "home.img", "shared"), "writable": true},
		"private": {"path": writeVolumeImage(t, "private.img", "private"), "tenants": []string{"acme"}},
	} {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		if status, resp := rawJSON(t, st, http.MethodPut, "/v1/volumes/"+name, string(raw)); status != http.StatusNoContent {
			t.Fatalf("register %s: status=%d body=%s", name, status, resp)
		}
	}
	home := sandbox.Volume{Name: "home", Mount: "/mnt/home"}
	for _, tc := range []struct {
		token   string
		want    []string
		private bool
	}{{"acme-token", []string{"home", "private", "public"}, true}, {"beta-token", []string{"home", "public"}, false}} {
		client, err := sandbox.Connect(st.addr, sandbox.WithAPIToken(tc.token))
		if err != nil {
			t.Fatal(err)
		}
		volumes, err := client.Volumes(t.Context())
		names := make([]string, 0, len(volumes))
		for _, volume := range volumes {
			names = append(names, volume.Name)
		}
		if err != nil || !slices.Equal(names, tc.want) {
			t.Fatalf("catalog for %s: names=%v want=%v err=%v", tc.token, names, tc.want, err)
		}
		claim, err := client.New(t.Context(), "rt:24.04", sandbox.WithVolumes(home))
		if err != nil || !slices.Equal(claim.Volumes, []sandbox.Volume{home}) {
			t.Fatalf("claim with public runtime volume: %+v, %v", claim, err)
		}
		defer claim.Close()
		if _, err = claim.SetVolumes(t.Context(), []sandbox.Volume{}); err != nil {
			t.Fatal(err)
		}
		view, err := claim.PatchVolumes(t.Context(), sandbox.VolumePatch{Add: []sandbox.Volume{home, {Name: "private"}}})
		if (err == nil) != tc.private || tc.private && !slices.Contains(view.Volumes, home) {
			t.Fatalf("live attach for %s: view=%+v err=%v", tc.token, view, err)
		}
	}
	caller := *st
	caller.token = "beta-token"
	var responses []string
	for _, name := range []string{"private", "unknown"} {
		status, body := rawJSON(t, &caller, http.MethodPost, "/v1/claim", `{"template":"rt:24.04","volumes":[{"name":"`+name+`"}]}`)
		if status != http.StatusBadRequest {
			t.Fatalf("claim privacy for %s: status=%d body=%s", name, status, body)
		}
		responses = append(responses, string(body))
	}
	if responses[0] != responses[1] {
		t.Fatalf("forbidden and unknown responses differ: %s versus %s", responses[0], responses[1])
	}
}
