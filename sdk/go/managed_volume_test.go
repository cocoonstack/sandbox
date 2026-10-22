package sandbox

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestManagedVolumeRequests(t *testing.T) {
	want := ManagedVolumeInfo{Name: "data", Kind: "managed", ID: "vol_123", Tenant: "t1", Node: "node-1", State: "ready", CreatedAt: time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC), SizeBytes: 20 << 30, Available: true, Writable: true, Holders: []VolumeHolder{{SandboxID: "sb_1", Mode: "rw", State: "attached"}}}
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/v1/volumes/data" || r.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("request = %s %s, authorization %q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
		}
		switch r.Method {
		case http.MethodPut:
			var body map[string]any
			if err := json.UnmarshalRead(r.Body, &body); err != nil {
				t.Errorf("decode create: %v", err)
			}
			if body["size_gib"] != float64(20) || body["tenant"] != "t1" {
				t.Errorf("create body = %v", body)
			}
			if requests == 1 {
				w.WriteHeader(http.StatusCreated)
			}
			_ = json.MarshalWrite(w, volumeResponse{Volume: &want})
		case http.MethodGet:
			w.Header().Set("ETag", `"vol_123"`)
			_ = json.MarshalWrite(w, volumeResponse{Volume: &want})
		case http.MethodDelete:
			if got := r.Header.Get("If-Match"); got != `"vol_123"` {
				t.Errorf("If-Match = %q", got)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts, WithAPIToken("token"))
	for range 2 {
		got, err := c.CreateVolume(t.Context(), "data", VolumeCreate{SizeGiB: 20, Tenant: "t1"})
		if err != nil || !reflect.DeepEqual(got, &want) {
			t.Fatalf("create = %+v, %v", got, err)
		}
	}
	got, err := c.GetVolume(t.Context(), "data")
	if err != nil || !reflect.DeepEqual(got, &want) {
		t.Fatalf("get = %+v, %v", got, err)
	}
	if err := c.DeleteVolume(t.Context(), "data", want.ID); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteVolume(t.Context(), "data", want.ID); err != nil {
		t.Fatal(err)
	}
	if requests != 5 {
		t.Fatalf("requests = %d, want 5", requests)
	}
}

func TestManagedVolumeFailures(t *testing.T) {
	statuses := []int{400, 401, 403, 404, 409, 412, 428, 503}
	for _, status := range statuses {
		t.Run(http.StatusText(status), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_ = json.MarshalWrite(w, map[string]any{"error": "volume unavailable", "code": "volume_busy", "volume": "data", "holders": []map[string]string{{"sandbox_id": "sb_1", "mode": "rw", "state": "retiring", "host_path": "/secret"}}})
			}))
			t.Cleanup(ts.Close)
			_, err := testClient(t, ts).GetVolume(t.Context(), "data")
			var apiErr *ManagedVolumeError
			if !errors.As(err, &apiErr) || apiErr.Status != status || apiErr.Code != "volume_busy" || apiErr.Volume != "data" || !reflect.DeepEqual(apiErr.Holders, []VolumeHolder{{SandboxID: "sb_1", Mode: "rw", State: "retiring"}}) {
				t.Fatalf("error = %#v", err)
			}
			if strings.Contains(apiErr.Error(), "/secret") {
				t.Fatal("error exposed a host path")
			}
		})
	}
}

func TestManagedVolumeRejectsReplyWithoutVolume(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	if _, err := c.GetVolume(t.Context(), "data"); err == nil {
		t.Fatal("accepted missing response volume")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.GetVolume(ctx, "data"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
	if requests != 1 {
		t.Fatalf("canceled request reached server, count %d", requests)
	}
}

func TestCreateVolumeForwardsInvalidName(t *testing.T) {
	const refusal = "invalid volume name or size_gib (1 through 1024)"
	type request struct {
		path string
		size float64
	}
	var got []request
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.UnmarshalRead(r.Body, &body); err != nil {
			t.Errorf("decode create: %v", err)
		}
		size, _ := body["size_gib"].(float64)
		got = append(got, request{r.URL.EscapedPath(), size})
		w.WriteHeader(http.StatusBadRequest)
		_ = json.MarshalWrite(w, map[string]string{"error": refusal})
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	cases := []struct {
		name string
		size int64
		want request
	}{
		{"bad/name", 20, request{"/v1/volumes/bad%2Fname", 20}},
		{"cocoon-data", 20, request{"/v1/volumes/cocoon-data", 20}},
		{"data", 0, request{"/v1/volumes/data", 0}},
	}
	for _, tc := range cases {
		_, err := c.CreateVolume(t.Context(), tc.name, VolumeCreate{SizeGiB: tc.size})
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Status != http.StatusBadRequest || apiErr.Message != refusal {
			t.Errorf("create %q size %d = %v, want the server's 400", tc.name, tc.size, err)
		}
	}
	want := make([]request, len(cases))
	for i, tc := range cases {
		want[i] = tc.want
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("server saw %+v, want %+v", got, want)
	}
}

func TestManagedVolumeConflictPreservesAllHolders(t *testing.T) {
	holders := make([]VolumeHolder, 100)
	for i := range holders {
		holders[i] = VolumeHolder{SandboxID: fmt.Sprintf("sb_%032d", i), Mode: "ro", State: "claimed"}
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.MarshalWrite(w, map[string]any{"error": "volume held", "code": "volume_busy", "volume": "data", "holders": holders})
	}))
	t.Cleanup(ts.Close)
	_, err := testClient(t, ts).GetVolume(t.Context(), "data")
	var conflict *ManagedVolumeError
	if !errors.As(err, &conflict) || conflict.Status != http.StatusConflict || conflict.Code != "volume_busy" {
		t.Fatalf("expected typed volume conflict, got %#v", err)
	}
	if !reflect.DeepEqual(conflict.Holders, holders) {
		t.Fatalf("conflict returned %d holders, want all %d with intact fields", len(conflict.Holders), len(holders))
	}
}

func TestCatalogVolumeInfoWireShape(t *testing.T) {
	catalog := VolumeInfo{Name: "data", DefaultMount: "/volumes/data", SizeBytes: 42, Available: true, Nodes: 1}
	const baseline = `{"name":"data","default_mount":"/volumes/data","size_bytes":42,"available":true,"nodes":1}`
	for _, v := range []any{catalog, ManagedVolumeInfo{VolumeInfo: catalog}} {
		if raw, err := json.Marshal(v); err != nil || string(raw) != baseline {
			t.Fatalf("catalog changed from baseline: %s %v", raw, err)
		}
	}
}
