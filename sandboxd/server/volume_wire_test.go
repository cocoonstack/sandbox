package server

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestClaimVolumeCompatibilityExpansion(t *testing.T) {
	for _, tt := range []struct {
		name string
		req  ClaimRequest
		want []types.Volume
		err  bool
	}{
		{"default mount", ClaimRequest{Volumes: []types.Volume{{Name: "data"}}}, []types.Volume{{Name: "data", Mount: "/volumes/data"}}, false},
		{"legacy all devices", ClaimRequest{VolumesAttachOnly: true, Volumes: []types.Volume{{Name: "data"}, {Name: "work", Mode: types.VolumeModeRW}}}, []types.Volume{{Name: "data", AttachOnly: true}, {Name: "work", Mode: types.VolumeModeRW, AttachOnly: true}}, false},
		{"mixed", ClaimRequest{Volumes: []types.Volume{{Name: "data"}, {Name: "work", AttachOnly: true}}}, []types.Volume{{Name: "data", Mount: "/volumes/data"}, {Name: "work", AttachOnly: true}}, false},
		{"mixed reversed", ClaimRequest{Volumes: []types.Volume{{Name: "work", AttachOnly: true}, {Name: "data"}}}, []types.Volume{{Name: "work", AttachOnly: true}, {Name: "data", Mount: "/volumes/data"}}, false},
		{"empty legacy", ClaimRequest{VolumesAttachOnly: true}, nil, true},
		{"legacy mount conflict", ClaimRequest{VolumesAttachOnly: true, Volumes: []types.Volume{{Name: "data", Mount: "/data"}}}, nil, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := slices.Clone(tt.req.Volumes)
			got, err := tt.req.normalizedVolumes()
			if (err != nil) != tt.err || !slices.Equal(got, tt.want) {
				t.Fatalf("normalized volumes=%+v err=%v, want %+v error=%t", got, err, tt.want, tt.err)
			}
			if !slices.Equal(tt.req.Volumes, before) {
				t.Fatal("legacy expansion mutated caller input")
			}
		})
	}
}

func TestVolumeHTTPResponsesPreservePublishedWireShape(t *testing.T) {
	volumes := []types.Volume{{Name: "raw", Mode: types.VolumeModeRW, AttachOnly: true}, {Name: "mounted", Mount: "/data"}}
	view := types.SandboxVolumesResponse{Volumes: volumes, VolumesWant: volumes}
	summary := pool.SandboxSummary{Volumes: volumes, VolumesWant: volumes}
	for name, payload := range map[string]any{
		"claim":   ClaimResponse{Volumes: volumes},
		"fork":    ForkResponse{Children: []ClaimResponse{{Volumes: volumes}}},
		"volumes": view,
		"error":   types.SandboxVolumesResponse{Volumes: volumes, VolumesWant: volumes, Error: "retry"},
		"summary": summary,
		"list":    map[string]any{"sandboxes": []*pool.SandboxSummary{&summary}},
		"pointer": &volumes[0],
	} {
		t.Run(name, func(t *testing.T) {
			reply := httptest.NewRecorder()
			writeJSON(reply, http.StatusOK, payload)
			body := reply.Body.String()
			if !jsontext.Value(body).IsValid() || strings.Contains(body, "attach_only") || !strings.Contains(body, `"name":"raw","mode":"rw"`) {
				t.Fatalf("published response shape changed: %s", body)
			}
		})
	}
	for _, payload := range []any{ClaimRequest{Volumes: volumes}, &types.Sandbox{Volumes: volumes, VolumesWant: volumes}} {
		raw, err := json.Marshal(payload)
		if err != nil || !strings.Contains(string(raw), `"attach_only":true`) {
			t.Fatalf("internal serialization lost attachment intent: %s, %v", raw, err)
		}
	}
}

func TestVolumeHTTPProjectionPreservesInvalidUTF8Handling(t *testing.T) {
	reply := httptest.NewRecorder()
	writeJSON(reply, http.StatusOK, []types.Volume{{Name: "raw\xff", AttachOnly: true}})
	if got := reply.Body.String(); !jsontext.Value(got).IsValid() || !strings.Contains(got, "raw�") {
		t.Fatalf("response no longer replaces invalid UTF8: %q", got)
	}
}

func TestVolumeRoutesKeepLegacyResponseFields(t *testing.T) {
	volumes := []types.Volume{{Name: "raw", Mode: types.VolumeModeRW, AttachOnly: true}}
	sb := &types.Sandbox{ID: "sb_1", Token: "tok", Volumes: volumes}
	view := types.SandboxVolumesResponse{Volumes: volumes, VolumesWant: volumes}
	summary := pool.SandboxSummary{ID: sb.ID, Volumes: volumes, VolumesWant: volumes}
	mgr := &fakeManager{
		claim:           func(context.Context, types.PoolKey, time.Duration) (*types.Sandbox, error) { return sb, nil },
		fork:            func(string, string, int, time.Duration) ([]*types.Sandbox, error) { return []*types.Sandbox{sb}, nil },
		claimCheckpoint: func(string) (*types.Sandbox, error) { return sb, nil },
		sandboxByID:     func(string) (pool.SandboxSummary, bool) { return summary, true },
		sandboxIndex:    func(string, string) []pool.SandboxSummary { return []pool.SandboxSummary{summary} },
		getVolumes:      func(string, string) (types.SandboxVolumesResponse, error) { return view, nil },
		setVolumes:      func(string, string, []types.Volume) (types.SandboxVolumesResponse, error) { return view, nil },
		patchVolumes: func(string, string, types.VolumePatch) (types.SandboxVolumesResponse, error) {
			return view, pool.ErrBadVolume
		},
	}
	ts := newTestServer(t, "root", mgr, nil)
	for _, tt := range []struct {
		method, path, body string
		status             int
	}{
		{http.MethodPost, "/v1/claim", `{"template":"rt:24.04"}`, http.StatusOK},
		{http.MethodPost, "/v1/sandboxes/sb_1/fork", `{"token":"tok","count":1}`, http.StatusOK},
		{http.MethodPost, "/v1/checkpoints/ckpt_1/claim", `{}`, http.StatusOK},
		{http.MethodGet, "/v1/sandboxes/sb_1", "", http.StatusOK},
		{http.MethodGet, "/v1/sandboxes", "", http.StatusOK},
		{http.MethodGet, "/v1/sandboxes/sb_1/volumes", "", http.StatusOK},
		{http.MethodPut, "/v1/sandboxes/sb_1/volumes", `{"volumes":[]}`, http.StatusOK},
		{http.MethodPatch, "/v1/sandboxes/sb_1/volumes", `{"remove":["raw"]}`, http.StatusBadRequest},
	} {
		t.Run(tt.method+tt.path, func(t *testing.T) {
			reply := doReq(t, tt.method, ts.URL+tt.path, "root", tt.body)
			defer reply.Body.Close()
			var body map[string]jsontext.Value
			if err := json.UnmarshalRead(reply.Body, &body); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(body)
			if err != nil || reply.StatusCode != tt.status || strings.Contains(string(raw), "attach_only") || !strings.Contains(string(raw), `"name":"raw","mode":"rw"`) {
				t.Fatalf("legacy response changed: status=%d body=%s err=%v", reply.StatusCode, raw, err)
			}
		})
	}
}
