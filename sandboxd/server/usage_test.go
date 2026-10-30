package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

func TestUsageIsRootOnlyAndNamesAMissingStore(t *testing.T) {
	ts := newTenantTestServer(t, "root", []tenants.TenantSpec{{Name: "acme", Token: "acme-tok"}}, &fakeManager{}, nil)
	for token, want := range map[string]int{"acme-tok": http.StatusForbidden, "": http.StatusUnauthorized, "root": http.StatusNotImplemented} {
		resp := doReq(t, http.MethodGet, ts.URL+"/v1/usage", token, "")
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET /v1/usage with %q: %d %s, want %d", token, resp.StatusCode, body, want)
		}
		if want == http.StatusNotImplemented && !strings.Contains(string(body), "no shared usage store configured") {
			t.Errorf("GET /v1/usage without a store: %s", body)
		}
	}
}

func TestUsagePassesTheQueryAndPages(t *testing.T) {
	t0 := time.Date(2026, 10, 30, 1, 2, 3, 400000000, time.UTC)
	reader := &fakeUsage{recs: []usage.Record{
		{Cursor: 7, Node: "n1", Digest: [32]byte{1}, Event: usage.Event{Time: t0, Event: "claim", ID: "sb_1", Tenant: "acme"}},
		{Cursor: 9, Node: "n2", Event: usage.Event{Time: t0, Event: "release", ID: "sb_1"}},
	}}
	ts := usageTestServer(t, reader)
	resp := doReq(t, http.MethodGet, ts.URL+"/v1/usage?tenant=acme&since=2026-10-30T00:00:00Z&until=2026-10-31T00:00:00.5%2B08:00&after=5&limit=2", "root", "")
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	want := `{"events":[{"cursor":7,"node":"n1","t":"2026-10-30T01:02:03.4Z","ev":"claim","id":"sb_1","tenant":"acme"},` +
		`{"cursor":9,"node":"n2","t":"2026-10-30T01:02:03.4Z","ev":"release","id":"sb_1"}],"next":9}`
	if resp.StatusCode != http.StatusOK || string(body) != want {
		t.Errorf("full page: %d %s, want %s", resp.StatusCode, body, want)
	}
	until := time.Date(2026, 10, 31, 0, 0, 0, 500000000, time.FixedZone("", 8*3600))
	if q := reader.got; q.Tenant != "acme" || !q.Since.Equal(time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)) || !q.Until.Equal(until) || q.After != 5 || q.Limit != 2 {
		t.Errorf("reader got %+v", q)
	}

	resp = doReq(t, http.MethodGet, ts.URL+"/v1/usage?after=9", "root", "")
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || strings.Contains(string(body), "next") || reader.got != (usage.Query{After: 9, Limit: 1000}) {
		t.Errorf("default partial page: %d %s, reader got %+v", resp.StatusCode, body, reader.got)
	}

	reader.recs = nil
	resp = doReq(t, http.MethodGet, ts.URL+"/v1/usage", "root", "")
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != `{"events":[]}` {
		t.Errorf("empty page: %s", body)
	}
}

func TestUsageRefusesABadQueryAndADownStore(t *testing.T) {
	reader := &fakeUsage{}
	ts := usageTestServer(t, reader)
	for _, q := range []string{"after=-1", "after=x", "limit=0", "limit=10001", "limit=x", "since=yesterday", "until=2026-10-30"} {
		resp := doReq(t, http.MethodGet, ts.URL+"/v1/usage?"+q, "root", "")
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", q, resp.StatusCode)
		}
	}
	reader.err = errors.New("connection refused")
	resp := doReq(t, http.MethodGet, ts.URL+"/v1/usage", "root", "")
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || strings.Contains(string(body), "refused") {
		t.Errorf("down store: %d %s, want 503 without the cause", resp.StatusCode, body)
	}
}

func usageTestServer(t *testing.T, reader UsageReader) *httptest.Server {
	t.Helper()
	srv := New("root", "node:7777", &fakeManager{}, &fakeDialer{}, nil, nil, nil, nil)
	srv.SetUsage(reader)
	return serveTestServer(t, srv)
}

type fakeUsage struct {
	recs []usage.Record
	err  error
	got  usage.Query
}

func (f *fakeUsage) List(_ context.Context, q usage.Query) ([]usage.Record, error) {
	f.got = q
	return f.recs, f.err
}
