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
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != method || r.URL.Path != "/v1/sandboxes/sb_1/volumes" || r.Header.Get("Authorization") != "Bearer operator-token" {
					t.Errorf("request: %s %s %s", r.Method, r.URL.Path, r.Header.Get("Authorization"))
				}
				body, _ := io.ReadAll(r.Body)
				want := ""
				switch method {
				case http.MethodPut:
					want = `{"volumes":[{"name":"data"}]}`
				case http.MethodPatch:
					want = `{"add":[{"name":"data"}],"remove":["old"]}`
				}
				if string(body) != want {
					t.Errorf("body=%s want=%s", body, want)
				}
				_, _ = io.WriteString(w, `{"volumes_want":[{"name":"data","mount":"/data"}],"volumes":[{"name":"data","mount":"/data"}],"converging":true,"states":[{"name":"data","state":"attached"}]}`)
			}))
			t.Cleanup(ts.Close)
			c := testClient(t, ts, WithAPIToken("operator-token"))
			sb := &Sandbox{ID: "sb_1", token: "sandbox-token", owner: c.addr, c: c}
			c.addr = "unused.invalid:7777"
			got, err := callVolumes(t, sb, method)
			if err != nil || !slices.Equal(got.Volumes, []Volume{{Name: "data", Mount: "/data"}}) || !slices.Equal(got.VolumesWant, got.Volumes) || got.States[0].State != VolumeAttached || !got.Converging {
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
		_, _ = io.WriteString(w, `{"volumes_want":[{"name":"new","mode":"rw"}],"volumes":[{"name":"old"}],"converging":false,"states":[{"name":"new","state":"pending","error":"retry"},{"name":"old","state":"detaching"}],"error":"convergence failed"}`)
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
		for _, entries := range [][]Volume{got.Volumes, got.VolumesWant, sb.Volumes, sb.VolumesWant} {
			if len(entries) != 1 || !entries[0].AttachOnly {
				t.Fatalf("%s partial view lost attach_only: %+v", method, entries)
			}
		}
		if !reflect.DeepEqual(got, *failure.Response) || got.States[0].State != "pending" || got.States[1].State != "detaching" || sb.VolumesWant[0].Name != "new" || sb.Volumes[0].Name != "old" {
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

func TestLiveVolumeAttachOnlyRoundTrip(t *testing.T) {
	const entries = `[{"name":"mounted-ro","mount":"/volumes/mounted-ro"},{"name":"mounted-rw","mount":"/work","mode":"rw"},{"name":"device-ro","attach_only":true},{"name":"device-rw","mode":"rw","attach_only":true}]`
	var forwarded []map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			var body struct {
				Volumes []map[string]any `json:"volumes"`
			}
			if err := json.UnmarshalRead(r.Body, &body); err != nil {
				t.Errorf("decode replacement: %v", err)
			}
			forwarded = body.Volumes
		}
		_, _ = io.WriteString(w, `{"volumes_want":`+entries+`,"volumes":`+entries+`,"states":[],"converging":false}`)
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	sb := &Sandbox{ID: "sb_1", owner: c.addr, c: c}
	view, err := sb.GetVolumes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = sb.SetVolumes(t.Context(), view.VolumesWant); err != nil {
		t.Fatal(err)
	}
	var want []map[string]any
	if err = json.Unmarshal([]byte(entries), &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(forwarded, want) {
		t.Fatalf("GET-to-PUT changed volume intent: got=%v want=%v", forwarded, want)
	}
	if !slices.Equal(sb.Volumes, view.Volumes) || !slices.Equal(sb.VolumesWant, view.VolumesWant) {
		t.Fatal("roundtrip changed handle state")
	}
}

func TestLiveVolumesRejectConflictingAttachOnlyBeforeRequest(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("invalid volume reached server") }))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	sb := c.Attach(c.addr, "sb_1", "tok")
	volumes := []Volume{{Name: "data", Mount: "/data", AttachOnly: true}}
	if _, err := sb.SetVolumes(t.Context(), volumes); err == nil {
		t.Fatal("SetVolumes accepted mount with attach_only")
	}
	if _, err := sb.PatchVolumes(t.Context(), VolumePatch{Add: volumes}); err == nil {
		t.Fatal("PatchVolumes accepted mount with attach_only")
	}
}

func TestVolumeResponseInferenceDoesNotChangeRequestDecoding(t *testing.T) {
	var request Volume
	if err := json.Unmarshal([]byte(`{"name":"default-mounted"}`), &request); err != nil || request.AttachOnly {
		t.Fatalf("ordinary request decode acquired raw intent: %+v err=%v", request, err)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sandboxes" {
			_, _ = io.WriteString(w, `{"sandboxes":[{"id":"sb_1","volumes":[{"name":"raw"}],"volumes_want":[{"name":"mounted","mount":"/data"}]},{"id":"sb_2","volumes":[],"volumes_want":[]},{"id":"sb_3"}]}`)
			return
		}
		_, _ = io.WriteString(w, `{"volumes":[{"name":"raw"}],"volumes_want":[{"name":"raw","mount":""}],"states":[],"converging":false}`)
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	rows, err := c.Sandboxes(t.Context())
	if err != nil || !rows[0].Volumes[0].AttachOnly || rows[0].VolumesWant[0].AttachOnly || rows[1].Volumes == nil || rows[2].Volumes != nil {
		t.Fatalf("summary inference or nil/empty changed: %+v err=%v", rows, err)
	}
	sb := c.Attach(c.addr, "sb_1", "tok")
	view, err := sb.GetVolumes(t.Context())
	if err != nil || !view.Volumes[0].AttachOnly || !view.VolumesWant[0].AttachOnly {
		t.Fatalf("live inference: %+v err=%v", view, err)
	}
}

func TestClaimResponseDecoderAndOwnedCopies(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"id":"sb_1","token":"tok","volumes":[{"name":"raw"},{"name":"mounted","mount":"/data"}]}`)
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	wire, err := c.doJSON[claimResponse](t.Context(), http.MethodPost, c.addr, "/v1/claim", nil, "", "claim")
	if err != nil {
		t.Fatal(err)
	}
	sb := c.handleFrom("node", wire)
	if !sb.Volumes[0].AttachOnly || sb.Volumes[1].AttachOnly || !wire.Volumes[0].AttachOnly {
		t.Fatalf("response decode lost attach-only state: handle=%+v wire=%+v", sb.Volumes, wire)
	}
	sb.Volumes[0].Name = "caller"
	if wire.Volumes[0].Name != "raw" || sb.VolumesWant[0].Name != "raw" {
		t.Fatal("claim volume views alias the wire or each other")
	}
	if c.handleFrom("node", claimResponse{}).Volumes != nil || c.handleFrom("node", claimResponse{Volumes: []Volume{}}).Volumes == nil {
		t.Fatal("claim response changed nil versus empty volume semantics")
	}
}

func TestForkResponseDecodesNestedVolumes(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"children":[{"id":"sb_child","token":"tok","volumes":[{"name":"raw"},{"name":"mounted","mount":"/data"}]}]}`)
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	reply, err := c.doJSON[forkResponse](t.Context(), http.MethodPost, c.addr, "/v1/sandboxes/sb_1/fork", nil, "", "fork")
	if err != nil || len(reply.Children) != 1 || !reply.Children[0].Volumes[0].AttachOnly || reply.Children[0].Volumes[1].AttachOnly {
		t.Fatalf("nested fork volumes did not decode: %+v err=%v", reply, err)
	}
}
