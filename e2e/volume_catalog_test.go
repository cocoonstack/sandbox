package e2e

import (
	"encoding/json/v2"
	"errors"
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

func TestRuntimeVolumeDetachHibernateWakeThroughHTTP(t *testing.T) {
	st := startTenantStack(t, "node-token", []tenants.TenantSpec{{Name: "acme", Token: "acme-token"}}, nil)
	request, err := json.Marshal(map[string]any{"path": writeVolumeImage(t, "home.img", "external bytes"), "writable": true, "tenants": []string{"acme"}})
	if err != nil {
		t.Fatal(err)
	}
	if status, body := rawJSON(t, st, http.MethodPut, "/v1/volumes/home", string(request)); status != http.StatusNoContent {
		t.Fatalf("registration: %d %s", status, body)
	}
	acme, err := sandbox.Connect(st.addr, sandbox.WithAPIToken("acme-token"))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := acme.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	other, err := acme.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	operator := st.client.Attach(st.addr, claim.ID, "")
	otherOperator := st.client.Attach(st.addr, other.ID, "")
	volume := sandbox.Volume{Name: "home", Mode: "rw", AttachOnly: true}
	if _, err = operator.PatchVolumes(t.Context(), sandbox.VolumePatch{Add: []sandbox.Volume{volume}}); err != nil {
		t.Fatal(err)
	}
	if err = claim.Hibernate(t.Context()); err == nil {
		t.Fatal("captured an attached disk")
	} else if refusal, ok := errors.AsType[*sandbox.APIError](err); !ok || refusal.Status != http.StatusConflict {
		t.Fatalf("attached capture refusal: %v", err)
	}
	if _, err = otherOperator.PatchVolumes(t.Context(), sandbox.VolumePatch{Add: []sandbox.Volume{volume}}); err == nil {
		t.Fatal("root admitted a second writer before detach")
	} else if refusal, ok := errors.AsType[*sandbox.APIError](err); !ok || refusal.Status != http.StatusConflict {
		t.Fatalf("second writer refusal: %v", err)
	}
	view, err := operator.PatchVolumes(t.Context(), sandbox.VolumePatch{Remove: []string{"home"}})
	if err != nil || len(view.Volumes) != 0 || len(view.VolumesWant) != 0 || len(view.States) != 0 || view.Converging {
		t.Fatalf("detach acknowledgment: %+v %v", view, err)
	}
	if err = claim.Hibernate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !hibernated(t, st, claim.ID) {
		t.Fatal("detach followed by hibernate did not sleep the claim")
	}
	registered, listErr := st.client.Volumes(t.Context())
	if listErr != nil || len(registered) != 1 || registered[0].Name != "home" || !registered[0].Writable {
		t.Fatalf("registration lost across sleep: %+v %v", registered, listErr)
	}
	if _, err = other.PatchVolumes(t.Context(), sandbox.VolumePatch{Add: []sandbox.Volume{volume}}); err != nil {
		t.Fatalf("detached volume stayed reserved during sleep: %v", err)
	}
	if _, err = otherOperator.PatchVolumes(t.Context(), sandbox.VolumePatch{Remove: []string{"home"}}); err != nil {
		t.Fatal(err)
	}
	beforeWake := st.eng.volumeOpsLog()
	if out, execErr := claim.Exec(t.Context(), "echo", "awake"); execErr != nil || out != "awake\n" {
		t.Fatalf("implicit wake: %q %v", out, execErr)
	}
	if hibernated(t, st, claim.ID) {
		t.Fatal("guest request did not wake the original claim")
	}
	if ops := st.eng.volumeOpsLog(); !slices.Equal(ops, beforeWake) {
		t.Fatalf("wake unexpectedly operated on external disks: %v -> %v", beforeWake, ops)
	}
	view, err = claim.GetVolumes(t.Context())
	if err != nil || len(view.Volumes) != 0 || len(view.VolumesWant) != 0 || len(view.States) != 0 || view.Converging {
		t.Fatalf("wake unexpectedly restored volume references: %+v %v", view, err)
	}
	view, err = operator.PatchVolumes(t.Context(), sandbox.VolumePatch{Add: []sandbox.Volume{volume}})
	if err != nil || !slices.Equal(view.Volumes, []sandbox.Volume{volume}) || view.Converging {
		t.Fatalf("controller reattachment: %+v %v", view, err)
	}
	if status, body := rawJSON(t, st, http.MethodDelete, "/v1/volumes/home", ""); status != http.StatusConflict {
		t.Fatalf("unregister attached restored disk: %d %s", status, body)
	}
}
