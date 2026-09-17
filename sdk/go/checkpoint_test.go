package sandbox

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCheckpointNewFollowsRedirect(t *testing.T) {
	var nodeACalls int
	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req checkpointClaimRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if !req.NoRedirect {
			t.Error("retry did not carry no_redirect")
		}
		_ = json.NewEncoder(w).Encode(claimResponse{ID: "sb_ck1", Token: "tok"})
	}))
	t.Cleanup(nodeB.Close)

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		nodeACalls++
		_ = json.NewEncoder(w).Encode(claimResponse{
			Redirect: []string{strings.TrimPrefix(nodeB.URL, "http://")},
		})
	}))
	t.Cleanup(nodeA.Close)

	c := testClient(t, nodeA)
	ck := checkpointHandle(c, c.addr, checkpointRecord{ID: "ck_1"})
	sb, err := ck.New(t.Context())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if sb.ID != "sb_ck1" {
		t.Errorf("id %q, want sb_ck1 (claimed at the redirect target)", sb.ID)
	}
	if sb.owner != strings.TrimPrefix(nodeB.URL, "http://") {
		t.Errorf("owner %q, want node B", sb.owner)
	}
	if nodeACalls != 1 {
		t.Errorf("node A called %d times, want 1", nodeACalls)
	}
}

func TestCheckpointNewRejectsVolumeOptionsLocally(t *testing.T) {
	tests := []struct {
		name string
		opt  Option
	}{
		{"WithClaimRef", WithClaimRef("ns/claim")},
		{"WithVolumesAttachOnly", WithVolumesAttachOnly()},
		{"WithVolumes", WithVolumes(Volume{Name: "data"})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ck := &Checkpoint{}
			sb, err := ck.New(t.Context(), tt.opt)
			if err == nil || !strings.Contains(err.Error(), tt.name) {
				t.Errorf("err %v, want local %s rejection", err, tt.name)
			}
			if sb != nil {
				t.Errorf("sandbox %+v, want nil", sb)
			}
		})
	}
}

func TestCheckpointNewRedirectAllCandidatesFail(t *testing.T) {
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(broken.Close)

	var entryCalls int
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entryCalls++
		if entryCalls == 1 {
			_ = json.NewEncoder(w).Encode(claimResponse{
				Redirect: []string{strings.TrimPrefix(broken.URL, "http://")},
			})
			return
		}
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(errorResponse{Error: "origin also failed"})
	}))
	t.Cleanup(entry.Close)

	c := testClient(t, entry)
	ck := checkpointHandle(c, c.addr, checkpointRecord{ID: "ck_2"})
	sb, err := ck.New(t.Context())
	if err == nil {
		t.Fatal("New: want error, got nil")
	}
	if !strings.Contains(err.Error(), "origin also failed") {
		t.Errorf("err %v, want the origin's failure surfaced", err)
	}
	if sb != nil {
		t.Errorf("sb %+v, want nil on failure", sb)
	}
	if entryCalls != 2 {
		t.Errorf("entry called %d times, want 2 (redirect + fallback)", entryCalls)
	}
}

func TestCheckpointNewRedirectFallbackHeals(t *testing.T) {
	busy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(busy.Close)

	var entryCalls int
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entryCalls++
		if entryCalls == 1 {
			_ = json.NewEncoder(w).Encode(claimResponse{Redirect: []string{strings.TrimPrefix(busy.URL, "http://")}})
			return
		}
		var req checkpointClaimRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if !req.NoRedirect {
			t.Error("origin fallback did not carry no_redirect")
		}
		_ = json.NewEncoder(w).Encode(claimResponse{ID: "sb_healed", Token: "t"})
	}))
	t.Cleanup(entry.Close)

	c := testClient(t, entry)
	ck := checkpointHandle(c, c.addr, checkpointRecord{ID: "ck_5"})
	sb, err := ck.New(t.Context())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if sb.ID != "sb_healed" {
		t.Errorf("id %q, want sb_healed", sb.ID)
	}
	if entryCalls != 2 {
		t.Errorf("entry called %d times, want 2 (redirect + fallback)", entryCalls)
	}
}

func TestCheckpointNewRedirectNeverYieldsEmptyID(t *testing.T) {
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(claimResponse{Redirect: []string{"127.0.0.1:1"}})
	}))
	t.Cleanup(entry.Close)

	c := testClient(t, entry)
	ck := checkpointHandle(c, c.addr, checkpointRecord{ID: "ck_3"})
	sb, err := ck.New(t.Context())
	if err == nil {
		t.Fatal("New: want error (dead redirect target), got nil")
	}
	if sb != nil {
		t.Errorf("sb %+v, want nil", sb)
	}
}

func TestCheckpointNewSecondLevelRedirectFails(t *testing.T) {
	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(claimResponse{Redirect: []string{"127.0.0.1:1"}})
	}))
	t.Cleanup(nodeB.Close)

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(claimResponse{
			Redirect: []string{strings.TrimPrefix(nodeB.URL, "http://")},
		})
	}))
	t.Cleanup(nodeA.Close)

	c := testClient(t, nodeA)
	ck := checkpointHandle(c, c.addr, checkpointRecord{ID: "ck_4"})
	sb, err := ck.New(t.Context())
	if err == nil {
		t.Fatal("New: want error, got nil")
	}
	if sb != nil {
		t.Errorf("sb %+v, want nil", sb)
	}
}
