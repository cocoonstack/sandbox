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

const growBody = `{"size_gib":40}`

func TestManagedVolumeRecoveryRetryHints(t *testing.T) {
	for _, tc := range []struct {
		code  string
		retry bool
	}{
		{"volume_backing_mismatch", false},
		{"volume_storage_unavailable", true},
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
		{"grow", http.MethodPost, "/v1/volumes/data/grow", `"vol_123"`, growBody, &volumeGrowFake{volumeManagerFake: &volumeManagerFake{fakeManager: &fakeManager{}}, grow: func(context.Context, string, string, int64, string) (types.ManagedVolumeInfo, error) {
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

type volumeGrowFake struct {
	*volumeManagerFake
	grow func(context.Context, string, string, int64, string) (types.ManagedVolumeInfo, error)
}

func (f *volumeGrowFake) GrowVolume(ctx context.Context, name, id string, sizeGiB int64, tenant string) (types.ManagedVolumeInfo, error) {
	return f.grow(ctx, name, id, sizeGiB, tenant)
}

func TestGrowVolumeHTTP(t *testing.T) {
	calls := 0
	mgr := &volumeGrowFake{volumeManagerFake: &volumeManagerFake{fakeManager: &fakeManager{
		tenants: []tenants.TenantSpec{{Name: "tenant-1", Token: "tenant-token"}},
	}}, grow: func(ctx context.Context, name, id string, sizeGiB int64, tenant string) (types.ManagedVolumeInfo, error) {
		calls++
		if ctx.Err() != nil || name != "data" || id != "vol_123" || tenant != "tenant-1" || sizeGiB != 40 {
			t.Errorf("grow args = %q %q %d %q, context %v", name, id, sizeGiB, tenant, ctx.Err())
		}
		return types.ManagedVolumeInfo{ID: "vol_123", SizeBytes: 40 << 30}, nil
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
	var body VolumeResponse
	if err := json.UnmarshalRead(resp.Body, &body); err != nil || resp.StatusCode != http.StatusOK || calls != 1 || body.Volume.SizeBytes != 40<<30 {
		t.Fatalf("status %d calls %d body %+v err %v", resp.StatusCode, calls, body, err)
	}
	if resp.Header.Get("ETag") != `"vol_123"` {
		t.Fatalf("ETag %q", resp.Header.Get("ETag"))
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
		{"unknown member", "root-token", `"vol_123"`, `{"extra":true,"size_gib":40}`, 400},
		{"duplicate member", "root-token", `"vol_123"`, `{"size_gib":20,"size_gib":40}`, 400},
		{"trailing body", "root-token", `"vol_123"`, growBody + `{}`, 400},
		{"fractional target", "root-token", `"vol_123"`, `{"size_gib":40.5}`, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := &volumeGrowFake{volumeManagerFake: &volumeManagerFake{fakeManager: &fakeManager{}}, grow: func(context.Context, string, string, int64, string) (types.ManagedVolumeInfo, error) {
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
		{404, "volume_not_found"}, {412, "volume_precondition_failed"}, {409, "volume_grow_conflict"}, {409, "volume_busy"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			mgr := &volumeGrowFake{volumeManagerFake: &volumeManagerFake{fakeManager: &fakeManager{}}, grow: func(context.Context, string, string, int64, string) (types.ManagedVolumeInfo, error) {
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
}

func TestGrowVolumeHTTPDoesNotExposeInternalErrors(t *testing.T) {
	mgr := &volumeGrowFake{volumeManagerFake: &volumeManagerFake{fakeManager: &fakeManager{}}, grow: func(context.Context, string, string, int64, string) (types.ManagedVolumeInfo, error) {
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
