package server

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	growOperation = "efbf4748-1c72-43ee-8304-b4e1a91322b5"
	growBody      = `{"size_gib":40,"expected_generation":0,"operation_id":"` + growOperation + `"}`
)

func TestManagedVolumeRecoveryRetryHints(t *testing.T) {
	for _, tc := range []struct {
		code  string
		retry bool
	}{
		{"volume_create_incomplete", false},
		{"volume_backing_mismatch", false},
		{"volume_storage_unavailable", true},
		{"volume_grow_unsupported", false},
	} {
		t.Run(tc.code, func(t *testing.T) {
			mgr := &volumeManagerFake{fakeManager: &fakeManager{}, get: func(string, string) (types.ManagedVolumeInfo, error) {
				return types.ManagedVolumeInfo{}, &pool.VolumeError{Status: http.StatusServiceUnavailable, Code: tc.code, Message: "volume unavailable"}
			}}
			ts := newTestServer(t, "root", mgr, nil)
			resp := doReq(t, http.MethodGet, ts.URL+"/v1/volumes/data", "root", "")
			defer resp.Body.Close()
			var body pool.VolumeError
			if err := json.UnmarshalRead(resp.Body, &body); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusServiceUnavailable || body.Code != tc.code || body.Message != "volume unavailable" {
				t.Fatalf("status=%d error=%+v", resp.StatusCode, body)
			}
			if retry := resp.Header.Get("Retry-After") != ""; retry != tc.retry {
				t.Fatalf("Retry-After=%q, want retry hint=%v", resp.Header.Get("Retry-After"), tc.retry)
			}
		})
	}
}

func TestManagedVolumeStorageFallbackKeepsRetryAfter(t *testing.T) {
	outage := errors.New("write /private/claims.json: no space left on device")
	for _, tc := range []struct {
		name, method, path, match, body string
		mgr                             Manager
	}{
		{"create", http.MethodPut, "/v1/volumes/data", "", `{"size_gib":1}`, &volumeManagerFake{fakeManager: &fakeManager{}, err: outage}},
		{"delete", http.MethodDelete, "/v1/volumes/data", `"vol_123"`, "", &volumeManagerFake{fakeManager: &fakeManager{}, err: outage}},
		{"grow", http.MethodPost, "/v1/volumes/data/grow", `"vol_123"`, growBody, &volumeGrowFake{volumeManagerFake: &volumeManagerFake{fakeManager: &fakeManager{}}, grow: func(context.Context, string, string, types.VolumeGrow, string) (types.ManagedVolumeInfo, error) {
			return types.ManagedVolumeInfo{}, outage
		}}},
		{"grow unsupported lookup", http.MethodPost, "/v1/volumes/data/grow", `"vol_123"`, growBody, &volumeManagerFake{fakeManager: &fakeManager{}, get: func(string, string) (types.ManagedVolumeInfo, error) {
			return types.ManagedVolumeInfo{}, outage
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestServer(t, "root-token", tc.mgr, nil)
			req, _ := http.NewRequest(tc.method, ts.URL+tc.path, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer root-token")
			if tc.match != "" {
				req.Header.Set("If-Match", tc.match)
			}
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			var body pool.VolumeError
			if err := json.UnmarshalRead(resp.Body, &body); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusServiceUnavailable || body.Code != "volume_storage_unavailable" || strings.Contains(body.Message, "/private") {
				t.Fatalf("status=%d body=%+v", resp.StatusCode, body)
			}
			if resp.Header.Get("Retry-After") == "" {
				t.Fatalf("storage outage %q without Retry-After", body.Message)
			}
		})
	}
}

type volumeManagerFake struct {
	*fakeManager
	get func(string, string) (types.ManagedVolumeInfo, error)
	err error
}

func (f *volumeManagerFake) CreateVolume(context.Context, string, types.VolumeCreate, string) (types.ManagedVolumeInfo, bool, error) {
	return types.ManagedVolumeInfo{}, false, f.err
}

func (f *volumeManagerFake) GetVolume(name, tenant string) (types.ManagedVolumeInfo, error) {
	return f.get(name, tenant)
}

func (f *volumeManagerFake) DeleteVolume(context.Context, string, string, string) error { return f.err }

func (f *volumeManagerFake) ManagedVolumeStatus() error { return nil }

func (f *volumeManagerFake) ManagedVolumesEnabled() bool { return true }

type volumeGrowFake struct {
	*volumeManagerFake
	grow func(context.Context, string, string, types.VolumeGrow, string) (types.ManagedVolumeInfo, error)
}

func (f *volumeGrowFake) GrowVolume(ctx context.Context, name, id string, req types.VolumeGrow, tenant string) (types.ManagedVolumeInfo, error) {
	return f.grow(ctx, name, id, req, tenant)
}

func TestGrowVolumeHTTP(t *testing.T) {
	for _, state := range []string{"completed", "pending", "recovery_required"} {
		t.Run(state, func(t *testing.T) {
			calls := 0
			mgr := &volumeGrowFake{volumeManagerFake: &volumeManagerFake{fakeManager: &fakeManager{
				tenants: []tenants.TenantSpec{{Name: "tenant-1", Token: "tenant-token"}},
			}}, grow: func(ctx context.Context, name, id string, req types.VolumeGrow, tenant string) (types.ManagedVolumeInfo, error) {
				calls++
				if ctx.Err() != nil || name != "data" || id != "vol_123" || tenant != "tenant-1" || req != (types.VolumeGrow{SizeGiB: 40, ExpectedGeneration: 0, OperationID: growOperation}) {
					t.Errorf("grow args = %q %q %+v %q, context %v", name, id, req, tenant, ctx.Err())
				}
				return types.ManagedVolumeInfo{ID: "vol_123", Growth: &types.VolumeGrowth{OperationID: growOperation, State: state}}, nil
			}}
			ts := newTestServer(t, "root-token", mgr, nil)
			req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/volumes/data/grow", strings.NewReader(growBody))
			req.Header.Set("Authorization", "Bearer tenant-token")
			req.Header.Set("If-Match", `"vol_123"`)
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			want := map[string]int{"completed": 200, "pending": 202, "recovery_required": 409}[state]
			if resp.StatusCode != want || calls != 1 {
				t.Fatalf("status %d calls %d", resp.StatusCode, calls)
			}
			if state == "recovery_required" {
				var failure pool.VolumeError
				if err := json.UnmarshalRead(resp.Body, &failure); err != nil || failure.Code != "volume_grow_recovery_required" {
					t.Fatalf("failure = %+v, %v", failure, err)
				}
			} else if resp.Header.Get("ETag") != `"vol_123"` {
				t.Fatalf("ETag %q", resp.Header.Get("ETag"))
			}
		})
	}
}

func TestGrowVolumeHTTPRefusesBeforeAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, token, match, body string
		status                   int
	}{
		{"no token", "", `"vol_123"`, growBody, 401},
		{"sandbox token", "sandbox-token", `"vol_123"`, growBody, 401},
		{"missing identity", "root-token", "", growBody, 428},
		{"wildcard", "root-token", "*", growBody, 412},
		{"weak identity", "root-token", `W/"vol_123"`, growBody, 412},
		{"identity list", "root-token", `"vol_123", "vol_456"`, growBody, 412},
		{"missing generation", "root-token", `"vol_123"`, `{"size_gib":40,"operation_id":"` + growOperation + `"}`, 400},
		{"null generation", "root-token", `"vol_123"`, `{"size_gib":40,"expected_generation":null,"operation_id":"` + growOperation + `"}`, 400},
		{"negative generation", "root-token", `"vol_123"`, strings.Replace(growBody, `"expected_generation":0`, `"expected_generation":-1`, 1), 400},
		{"unknown member", "root-token", `"vol_123"`, strings.Replace(growBody, `"size_gib":40`, `"extra":true,"size_gib":40`, 1), 400},
		{"duplicate member", "root-token", `"vol_123"`, strings.Replace(growBody, `"size_gib":40`, `"size_gib":20,"size_gib":40`, 1), 400},
		{"trailing body", "root-token", `"vol_123"`, growBody + `{}`, 400},
		{"zero target", "root-token", `"vol_123"`, strings.Replace(growBody, `40`, `0`, 1), 400},
		{"fractional target", "root-token", `"vol_123"`, strings.Replace(growBody, `40`, `40.5`, 1), 400},
		{"oversize target", "root-token", `"vol_123"`, strings.Replace(growBody, `40`, `1025`, 1), 400},
		{"uppercase UUID", "root-token", `"vol_123"`, strings.Replace(growBody, growOperation, strings.ToUpper(growOperation), 1), 400},
		{"zero UUID", "root-token", `"vol_123"`, strings.Replace(growBody, growOperation, "00000000-0000-0000-0000-000000000000", 1), 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := &volumeGrowFake{volumeManagerFake: &volumeManagerFake{fakeManager: &fakeManager{}}, grow: func(context.Context, string, string, types.VolumeGrow, string) (types.ManagedVolumeInfo, error) {
				t.Error("invalid request reached growth admission")
				return types.ManagedVolumeInfo{}, nil
			}}
			ts := newTestServer(t, "root-token", mgr, nil)
			req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/volumes/data/grow", strings.NewReader(tc.body))
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			req.Header.Set("If-Match", tc.match)
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.status {
				raw, _ := io.ReadAll(resp.Body)
				t.Fatalf("status %d want %d, %s", resp.StatusCode, tc.status, raw)
			}
		})
	}
}

func TestGrowVolumeHTTPErrorPrivacyAndPreconditions(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{
		{404, "volume_not_found"}, {412, "volume_precondition_failed"}, {409, "volume_grow_conflict"}, {409, "volume_busy"}, {503, "volume_grow_unsupported"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			mgr := &volumeGrowFake{volumeManagerFake: &volumeManagerFake{fakeManager: &fakeManager{}}, grow: func(context.Context, string, string, types.VolumeGrow, string) (types.ManagedVolumeInfo, error) {
				return types.ManagedVolumeInfo{}, &pool.VolumeError{Status: tc.status, Code: tc.code, Message: "volume unavailable"}
			}}
			ts := newTestServer(t, "root-token", mgr, nil)
			req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/volumes/data/grow", strings.NewReader(growBody))
			req.Header.Set("Authorization", "Bearer root-token")
			req.Header.Set("If-Match", `"vol_123"`)
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			var failure pool.VolumeError
			if err := json.UnmarshalRead(resp.Body, &failure); err != nil || resp.StatusCode != tc.status || failure.Code != tc.code {
				t.Fatalf("status %d failure %+v err %v", resp.StatusCode, failure, err)
			}
		})
	}
	for _, foreign := range []bool{false, true} {
		mgr := &volumeManagerFake{fakeManager: &fakeManager{}, get: func(string, string) (types.ManagedVolumeInfo, error) {
			if foreign {
				return types.ManagedVolumeInfo{}, &pool.VolumeError{Status: 404, Code: "volume_not_found", Message: "unknown volume"}
			}
			return types.ManagedVolumeInfo{}, nil
		}}
		ts := newTestServer(t, "root-token", mgr, nil)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/volumes/data/grow", strings.NewReader(growBody))
		req.Header.Set("Authorization", "Bearer root-token")
		req.Header.Set("If-Match", `"vol_123"`)
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		want := 503
		if foreign {
			want = 404
		}
		if resp.StatusCode != want {
			t.Fatalf("optional capability status %d want %d", resp.StatusCode, want)
		}
	}
}

func TestGrowVolumeHTTPDoesNotExposeInternalErrors(t *testing.T) {
	mgr := &volumeGrowFake{volumeManagerFake: &volumeManagerFake{fakeManager: &fakeManager{}}, grow: func(context.Context, string, string, types.VolumeGrow, string) (types.ManagedVolumeInfo, error) {
		return types.ManagedVolumeInfo{}, errors.New("runtime /secret/backing pid 123 failed")
	}}
	ts := newTestServer(t, "root-token", mgr, nil)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/volumes/data/grow", strings.NewReader(growBody))
	req.Header.Set("Authorization", "Bearer root-token")
	req.Header.Set("If-Match", `"vol_123"`)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 503 || strings.Contains(string(raw), "/secret") || strings.Contains(string(raw), "pid 123") {
		t.Fatalf("unsafe response %d %s", resp.StatusCode, raw)
	}
}
