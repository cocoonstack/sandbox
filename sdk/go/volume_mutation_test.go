package sandbox

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"
)

func TestLiveVolumesUseOwnerAndAPIToken(t *testing.T) {
	for _, request := range []struct{ method, body string }{
		{http.MethodGet, ""},
		{http.MethodPut, `{"volumes":[{"name":"data"}]}`},
		{http.MethodPatch, `{"add":[{"name":"data"}],"remove":["old"]}`},
	} {
		t.Run(request.method, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != request.method || r.URL.Path != "/v1/sandboxes/sb_1/volumes" || r.Header.Get("Authorization") != "Bearer operator-token" {
					t.Errorf("request: %s %s %s", r.Method, r.URL.Path, r.Header.Get("Authorization"))
				}
				body, _ := io.ReadAll(r.Body)
				if string(body) != request.body {
					t.Errorf("body=%s want=%s", body, request.body)
				}
				_, _ = io.WriteString(w, `{"volumes_want":[{"name":"data","attach_only":true}],"volumes":[{"name":"data","attach_only":true}],"converging":true,"states":[{"name":"data","state":"attached"}]}`)
			}))
			t.Cleanup(ts.Close)
			c := testClient(t, ts, WithAPIToken("operator-token"))
			sb := &Sandbox{ID: "sb_1", token: "sandbox-token", owner: c.addr, c: c}
			c.addr = "unused.invalid:7777"
			got, err := callVolumes(t, sb, request.method)
			if err != nil || !slices.Equal(got.Volumes, []Volume{{Name: "data", AttachOnly: true}}) || !slices.Equal(got.VolumesWant, got.Volumes) || got.States[0].State != VolumeAttached || !got.Converging {
				t.Fatalf("got=%+v err=%v", got, err)
			}
			got.Volumes[0].Name, got.VolumesWant[0].Name = "caller copy", "caller copy"
			if sb.Volumes[0].Name != "data" || sb.VolumesWant[0].Name != "data" {
				t.Fatal("returned volumes alias public state")
			}
		})
	}
}

func TestLiveVolumeFailureRetainsPartialView(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"volumes_want":[{"name":"new","mode":"rw","attach_only":true}],"volumes":[{"name":"old","attach_only":true}],"converging":false,"states":[{"name":"new","state":"pending","error":"retry"},{"name":"old","state":"detaching"}],"error":"convergence failed"}`)
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPatch} {
		sb := &Sandbox{ID: "sb_1", token: "tok", owner: c.addr, c: c}
		got, err := callVolumes(t, sb, method)
		failure, isVolume := errors.AsType[*VolumeError](err)
		api, isAPI := errors.AsType[*APIError](err)
		if !isVolume || !isAPI || api.Status != http.StatusInternalServerError || failure.Response == nil {
			t.Fatalf("%s error lost response or APIError: %v", method, err)
		}
		if !reflect.DeepEqual(got, *failure.Response) || got.States[0].State != "pending" || got.States[1].State != "detaching" {
			t.Fatalf("partial view=%+v", got)
		}
		if !slices.Equal(sb.VolumesWant, []Volume{{Name: "new", Mode: "rw", AttachOnly: true}}) || !slices.Equal(sb.Volumes, []Volume{{Name: "old", AttachOnly: true}}) {
			t.Fatalf("partial view=%+v handle=%+v", got, sb)
		}
		failure.Response.Volumes[0].Name = "error caller"
		if got.Volumes[0].Name != "old" || sb.Volumes[0].Name != "old" {
			t.Fatal("error response aliases returned or handle state")
		}
	}
}

func TestLiveVolumeValidationFailurePreservesLastKnownSet(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":"invalid volume"}`)
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	sb := &Sandbox{ID: "sb_1", token: "tok", owner: c.addr, c: c, Volumes: []Volume{{Name: "old"}}, VolumesWant: []Volume{{Name: "wanted"}}}
	_, err := sb.SetVolumes(t.Context(), []Volume{{Name: "new"}})
	if failure, ok := errors.AsType[*VolumeError](err); !ok || failure.Response != nil || failure.Status != http.StatusBadRequest {
		t.Fatalf("error=%v", err)
	}
	if sb.Volumes[0].Name != "old" || sb.VolumesWant[0].Name != "wanted" {
		t.Fatal("validation error replaced last known view")
	}
}

func TestLiveVolumesExplicitEmptyAndCancellation(t *testing.T) {
	var bodies []map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.UnmarshalRead(r.Body, &body); err != nil {
			t.Error(err)
		}
		bodies = append(bodies, body)
		_, _ = io.WriteString(w, `{"volumes_want":[],"volumes":[],"converging":false,"states":[]}`)
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	sb := &Sandbox{ID: "sb_1", owner: c.addr, c: c}
	if _, err := sb.SetVolumes(t.Context(), nil); err == nil {
		t.Fatal("nil desired set was accepted")
	}
	if _, err := sb.SetVolumes(t.Context(), []Volume{}); err != nil {
		t.Fatal(err)
	}
	if _, err := sb.PatchVolumes(t.Context(), VolumePatch{}); err != nil {
		t.Fatal(err)
	}
	if _, err := sb.PatchVolumes(t.Context(), VolumePatch{Add: []Volume{}, Remove: []string{}}); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 3 || !reflect.DeepEqual(bodies[0], map[string]any{"volumes": []any{}}) || len(bodies[1]) != 0 || !reflect.DeepEqual(bodies[2], map[string]any{"add": []any{}, "remove": []any{}}) {
		t.Fatalf("requests=%+v", bodies)
	}
	if sb.Volumes == nil || sb.VolumesWant == nil {
		t.Fatal("explicitly empty response became absent")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := sb.GetVolumes(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context returned %v", err)
	}
}

func TestLiveVolumesRetrySameDesiredSet(t *testing.T) {
	var requests []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests = append(requests, string(body))
		if len(requests) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"volumes_want":[{"name":"data","mount":"/data"}],"volumes":[],"converging":false,"states":[{"name":"data","state":"pending"}],"error":"retry"}`)
			return
		}
		_, _ = io.WriteString(w, `{"volumes_want":[{"name":"data","mount":"/data"}],"volumes":[{"name":"data","mount":"/data"}],"converging":false,"states":[{"name":"data","state":"attached"}]}`)
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	sb := &Sandbox{ID: "sb_1", token: "tok", owner: c.addr, c: c}
	target := []Volume{{Name: "data", Mount: "/data"}}
	if _, err := sb.SetVolumes(t.Context(), target); err == nil || len(sb.Volumes) != 0 || !slices.Equal(sb.VolumesWant, target) {
		t.Fatalf("failed convergence lost desired set: want=%v applied=%v error=%v", sb.VolumesWant, sb.Volumes, err)
	}
	view, err := sb.SetVolumes(t.Context(), target)
	if err != nil || !slices.Equal(view.Volumes, target) || !slices.Equal(sb.Volumes, target) {
		t.Fatalf("retry did not converge: %+v error=%v", view, err)
	}
	if len(requests) != 2 || requests[0] != requests[1] {
		t.Fatalf("retry changed target: %v", requests)
	}
}

func TestLiveVolumeIncompleteViewPreservesHandle(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusConflict} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"volumes_want":[],"volumes":[],"states":[]}`)
			}))
			t.Cleanup(ts.Close)
			c := testClient(t, ts)
			sb := &Sandbox{ID: "sb_1", owner: c.addr, c: c, Volumes: []Volume{{Name: "old"}}, VolumesWant: []Volume{{Name: "wanted"}}}
			_, err := sb.GetVolumes(t.Context())
			if err == nil || sb.Volumes[0].Name != "old" || sb.VolumesWant[0].Name != "wanted" {
				t.Fatalf("handle overwritten: %+v, %v", sb, err)
			}
			if failure, ok := errors.AsType[*VolumeError](err); status == http.StatusConflict && (!ok || failure.Response != nil) {
				t.Fatalf("partial view accepted: %v", err)
			}
		})
	}
}

func TestLiveVolumeMutationEncodesDeviceFields(t *testing.T) {
	const want = `{"volumes":[{"name":"device","mode":"rw","attach_only":true},{"name":"mounted","mount":"/work"}]}`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if r.Method != http.MethodPut || string(body) != want {
			t.Errorf("request=%s %s want=PUT %s", r.Method, body, want)
		}
		_, _ = io.WriteString(w, `{"volumes_want":[],"volumes":[],"states":[],"converging":false}`)
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	sb := &Sandbox{ID: "sb_1", owner: c.addr, c: c}
	_, err := sb.SetVolumes(t.Context(), []Volume{
		{Name: "device", Mode: "rw", AttachOnly: true},
		{Name: "mounted", Mount: "/work", AttachOnly: false},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func callVolumes(t *testing.T, sb *Sandbox, method string) (SandboxVolumesResponse, error) {
	t.Helper()
	switch method {
	case http.MethodPut:
		return sb.SetVolumes(t.Context(), []Volume{{Name: "data"}})
	case http.MethodPatch:
		return sb.PatchVolumes(t.Context(), VolumePatch{Add: []Volume{{Name: "data"}}, Remove: []string{"old"}})
	default:
		return sb.GetVolumes(t.Context())
	}
}
