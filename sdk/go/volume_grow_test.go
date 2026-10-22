package sandbox

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestGrowVolumeRequest(t *testing.T) {
	want := ManagedVolumeInfo{Name: "data", Kind: "managed", ID: "vol_123", Tenant: "t1", Node: "node-1", State: "ready", CreatedAt: time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC), SizeBytes: 40 << 30}
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/v1/volumes/data/grow" || r.Method != http.MethodPost || r.Header.Get("If-Match") != `"vol_123"` || r.Header.Get("Authorization") != "Bearer token" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("request = %s %s %+v", r.Method, r.URL.Path, r.Header)
		}
		var body map[string]any
		if err := json.UnmarshalRead(r.Body, &body); err != nil || !reflect.DeepEqual(body, map[string]any{"size_gib": float64(40)}) {
			t.Errorf("body = %v, %v", body, err)
		}
		_ = json.MarshalWrite(w, volumeResponse{Volume: &want})
	}))
	t.Cleanup(ts.Close)
	got, err := testClient(t, ts, WithAPIToken("token")).GrowVolume(t.Context(), "data", "vol_123", 40)
	if err != nil || !reflect.DeepEqual(got, &want) || requests != 1 {
		t.Fatalf("grow = %+v, %v, requests %d", got, err, requests)
	}
}

func TestGrowVolumeCanceledBeforeSending(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { requests++; w.WriteHeader(503) }))
	t.Cleanup(ts.Close)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := testClient(t, ts).GrowVolume(ctx, "data", "vol_123", 40); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
	if requests != 0 {
		t.Fatalf("canceled requests sent = %d", requests)
	}
}

func TestGrowVolumeErrors(t *testing.T) {
	for _, status := range []int{http.StatusAccepted, 400, 401, 404, 409, 412, 428, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			requests := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests++
				w.WriteHeader(status)
				_ = json.MarshalWrite(w, map[string]string{"error": "growth unavailable"})
			}))
			t.Cleanup(ts.Close)
			_, err := testClient(t, ts).GrowVolume(t.Context(), "data", "vol_123", 40)
			var failure *APIError
			if !errors.As(err, &failure) || failure.Status != status || failure.Message != "growth unavailable" || requests != 1 {
				t.Fatalf("error = %#v, requests %d", err, requests)
			}
		})
	}
}
