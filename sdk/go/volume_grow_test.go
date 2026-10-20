package sandbox

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const growOperation = "efbf4748-1c72-43ee-8304-b4e1a91322b5"

func growVolumeFixture(state string) ManagedVolumeInfo {
	v := ManagedVolumeInfo{Name: "data", Kind: "managed", ID: "vol_123", Tenant: "t1", Node: "node-1", State: "growing", CreatedAt: time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC), SizeBytes: 20 << 30}
	v.Growth = &VolumeGrowth{OperationID: growOperation, TargetBytes: 40 << 30, State: state, RuntimeOutcome: "acknowledged", FilesystemStatus: "not_managed"}
	if state == "completed" {
		v.State, v.SizeBytes, v.CapacityGeneration, v.Growth.GuestSizeBytes = "ready", 40<<30, 1, 40<<30
	}
	return v
}

func TestGrowVolumeRequestAndReplay(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusAccepted} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			state := "completed"
			if status == http.StatusAccepted {
				state = "pending"
			}
			want := growVolumeFixture(state)
			requests := 0
			grow := VolumeGrow{SizeGiB: 40, ExpectedGeneration: 0, OperationID: growOperation}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.URL.Path != "/v1/volumes/data/grow" || r.Method != http.MethodPost || r.Header.Get("If-Match") != `"vol_123"` || r.Header.Get("Authorization") != "Bearer token" || r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("request = %s %s %+v", r.Method, r.URL.Path, r.Header)
				}
				var got VolumeGrow
				if err := json.UnmarshalRead(r.Body, &got); err != nil || got != grow {
					t.Errorf("body = %+v, %v", got, err)
				}
				w.WriteHeader(status)
				_ = json.MarshalWrite(w, volumeResponse{Volume: &want})
			}))
			t.Cleanup(ts.Close)
			c := testClient(t, ts, WithAPIToken("token"))
			for range 2 {
				got, err := c.GrowVolume(t.Context(), "data", "vol_123", grow)
				if err != nil || !reflect.DeepEqual(got, &want) {
					t.Fatalf("grow = %+v, %v", got, err)
				}
			}
			if requests != 2 {
				t.Fatalf("requests = %d", requests)
			}
		})
	}
}

func TestGrowVolumeValidatesBeforeSending(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { requests++; w.WriteHeader(503) }))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	valid := VolumeGrow{SizeGiB: 40, OperationID: growOperation}
	for _, tc := range []struct {
		name, id string
		grow     VolumeGrow
	}{
		{"bad/name", "vol_123", valid},
		{"data", "", valid},
		{"data", `"vol_123"`, valid},
		{"data", "bad\r\nid", valid},
		{"data", "vol_123", VolumeGrow{SizeGiB: 0, OperationID: growOperation}},
		{"data", "vol_123", VolumeGrow{SizeGiB: 1025, OperationID: growOperation}},
		{"data", "vol_123", VolumeGrow{SizeGiB: 40}},
		{"data", "vol_123", VolumeGrow{SizeGiB: 40, OperationID: strings.ToUpper(growOperation)}},
		{"data", "vol_123", VolumeGrow{SizeGiB: 40, OperationID: "00000000-0000-0000-0000-000000000000"}},
	} {
		if _, err := c.GrowVolume(t.Context(), tc.name, tc.id, tc.grow); err == nil {
			t.Errorf("accepted %+v", tc)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.GrowVolume(ctx, "data", "vol_123", valid); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
	if requests != 0 {
		t.Fatalf("invalid requests sent = %d", requests)
	}
}

func TestGrowVolumeErrorsAndInvalidSuccess(t *testing.T) {
	for _, status := range []int{400, 401, 404, 409, 412, 428, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			requests := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests++
				w.WriteHeader(status)
				_ = json.MarshalWrite(w, map[string]string{"error": "growth unavailable", "code": "volume_grow_recovery_required"})
			}))
			t.Cleanup(ts.Close)
			_, err := testClient(t, ts).GrowVolume(t.Context(), "data", "vol_123", VolumeGrow{SizeGiB: 40, OperationID: growOperation})
			var failure *ManagedVolumeError
			if !errors.As(err, &failure) || failure.Status != status || failure.Code != "volume_grow_recovery_required" || requests != 1 {
				t.Fatalf("error = %#v, requests %d", err, requests)
			}
		})
	}
	for _, state := range []string{"", "pending", "recovery_required"} {
		want := growVolumeFixture(state)
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.MarshalWrite(w, volumeResponse{Volume: &want}) }))
		t.Cleanup(ts.Close)
		if _, err := testClient(t, ts).GrowVolume(t.Context(), "data", "vol_123", VolumeGrow{SizeGiB: 40, OperationID: growOperation}); err == nil {
			t.Errorf("accepted 200 with state %q", state)
		}
	}
}

func TestGetVolumeRetainsGrowthProgress(t *testing.T) {
	want := growVolumeFixture("recovery_required")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.MarshalWrite(w, volumeResponse{Volume: &want}) }))
	t.Cleanup(ts.Close)
	got, err := testClient(t, ts).GetVolume(t.Context(), "data")
	if err != nil || !reflect.DeepEqual(got, &want) {
		t.Fatalf("get = %+v, %v", got, err)
	}
}

func TestGrowVolumeLostResponseReplay(t *testing.T) {
	var requests atomic.Int32
	want := growVolumeFixture("completed")
	grow := VolumeGrow{SizeGiB: 40, OperationID: growOperation}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := requests.Add(1)
		var got VolumeGrow
		if err := json.UnmarshalRead(r.Body, &got); err != nil || got != grow || r.Header.Get("If-Match") != `"vol_123"` {
			t.Errorf("replayed request = %+v, %v", got, err)
		}
		if request == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		_ = json.MarshalWrite(w, volumeResponse{Volume: &want})
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	if _, err := c.GrowVolume(t.Context(), "data", "vol_123", grow); err == nil || requests.Load() != 1 {
		t.Fatalf("lost response = %v, requests %d", err, requests.Load())
	}
	got, err := c.GrowVolume(t.Context(), "data", "vol_123", grow)
	if err != nil || !reflect.DeepEqual(got, &want) || requests.Load() != 2 {
		t.Fatalf("replay = %+v, %v, requests %d", got, err, requests.Load())
	}
}

func TestVolumeGenerationBelongsToManagedDetail(t *testing.T) {
	catalog := VolumeInfo{Name: "data", DefaultMount: "/volumes/data", SizeBytes: 42, Available: true, Nodes: 1}
	raw, err := json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	const baseline = `{"name":"data","default_mount":"/volumes/data","size_bytes":42,"available":true,"nodes":1}`
	if string(raw) != baseline {
		t.Fatalf("catalog changed: %s", raw)
	}
	var detail ManagedVolumeInfo
	if err = json.Unmarshal([]byte(`{"name":"data","capacity_generation":0}`), &detail); err != nil {
		t.Fatal(err)
	}
	raw, err = json.Marshal(detail)
	if err != nil || !strings.Contains(string(raw), `"capacity_generation":0`) {
		t.Fatalf("managed initial generation missing: %s %v", raw, err)
	}
}
