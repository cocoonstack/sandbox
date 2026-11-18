package sandbox

import (
	"encoding/json/v2"
	"net/http"
	"strings"
	"testing"
)

func TestCheckpointNewFollowsRedirect(t *testing.T) {
	var nodeACalls int
	nodeB := serveFunc(t, func(w http.ResponseWriter, r *http.Request) {
		var req checkpointClaimRequest
		_ = json.UnmarshalRead(r.Body, &req)
		if !req.NoRedirect {
			t.Error("retry did not carry no_redirect")
		}
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_ck1", Token: "tok"})
	})

	nodeA := serveFunc(t, func(w http.ResponseWriter, _ *http.Request) {
		nodeACalls++
		_ = json.MarshalWrite(w, claimResponse{
			Redirect: []string{addrOf(nodeB)},
		})
	})

	c := testClient(t, nodeA)
	ck := checkpointHandle(c, c.addr, checkpointRecord{ID: "ck_1"})
	sb, err := ck.New(t.Context())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if sb.ID != "sb_ck1" {
		t.Errorf("id %q, want sb_ck1 (claimed at the redirect target)", sb.ID)
	}
	if sb.owner != addrOf(nodeB) {
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
		{"WithVolumes", WithVolumes(Volume{Name: "data"})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ck := &Checkpoint{}
			sb, err := ck.New(t.Context(), tt.opt)
			if err == nil || !strings.Contains(err.Error(), "checkpoint claims do not accept") {
				t.Errorf("err %v, want local %s rejection", err, tt.name)
			}
			if sb != nil {
				t.Errorf("sandbox %+v, want nil", sb)
			}
		})
	}
}

func TestCheckpointNewRedirectNeverYieldsEmptyID(t *testing.T) {
	dead := retiredAddr(t)
	entry := serveFunc(t, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, claimResponse{Redirect: []string{dead}})
	})

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
	dead := retiredAddr(t)
	nodeB := serveFunc(t, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, claimResponse{Redirect: []string{dead}})
	})

	nodeA := serveFunc(t, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, claimResponse{
			Redirect: []string{addrOf(nodeB)},
		})
	})

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

func TestCheckpointNewCarriesMetadata(t *testing.T) {
	var got checkpointClaimRequest
	ts := serveFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.UnmarshalRead(r.Body, &got)
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_ck1", Token: "tok"})
	})

	c := testClient(t, ts)
	ck := checkpointHandle(c, c.addr, checkpointRecord{ID: "ck_1"})
	if _, err := ck.New(t.Context(), WithMetadata(map[string]string{"role": "branch"}), WithArchiveOnExpire()); err != nil {
		t.Fatalf("New: %v", err)
	}
	if got.Metadata["role"] != "branch" || got.OnExpire != "archive" {
		t.Errorf("checkpoint claim body %+v, want metadata role=branch and on_expire archive", got)
	}
}
