package server

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
)

func TestMoveRouteResolvesThePeerAndMapsTheOutcome(t *testing.T) {
	var gotAddr string
	var gotBudget time.Duration
	var outcome error
	mgr := &fakeManager{move: func(_, addr string, budget time.Duration) (pool.MoveResult, error) {
		gotAddr, gotBudget = addr, budget
		return pool.MoveResult{Rounds: 3, Downtime: 180 * time.Millisecond}, outcome
	}}
	srv := newMeshTestServer(t, "root", mgr, &fakePlacer{addrs: []string{"10.0.0.2:7777"}}, nil)

	resp := doReq(t, http.MethodPost, srv.URL+"/v1/sandboxes/sb_1/move", "root", `{"to":"10.0.0.2:7777","max_downtime_ms":1500}`)
	defer func() { _ = resp.Body.Close() }()
	var body MoveResponse
	if err := json.UnmarshalRead(resp.Body, &body); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("move: %d %v", resp.StatusCode, err)
	}
	if gotAddr != "10.0.0.2:7777" || gotBudget != 1500*time.Millisecond || body != (MoveResponse{OwnerAddr: "10.0.0.2:7777", Rounds: 3, DowntimeMS: 180}) {
		t.Errorf("move to %s within %s answered %+v", gotAddr, gotBudget, body)
	}
	for _, tt := range []struct {
		body string
		err  error
		want int
	}{
		{`{"to":"10.0.0.9:7777"}`, nil, http.StatusBadRequest},
		{`{"to":"10.0.0.2:7777","max_downtime_ms":-1}`, nil, http.StatusBadRequest},
		{`{"to":"10.0.0.2:7777","max_downtime_ms":401}`, nil, http.StatusOK},
		{`{"to":"10.0.0.2:7777"}`, pool.ErrUnknownSandbox, http.StatusNotFound},
		{`{"to":"10.0.0.2:7777"}`, fmt.Errorf("%w: it has volumes", pool.ErrNotMovable), http.StatusConflict},
		{`{"to":"10.0.0.2:7777"}`, fmt.Errorf("%w: target: 3 moves in flight", pool.ErrQuota), http.StatusTooManyRequests},
		{`{"to":"10.0.0.2:7777"}`, pool.ErrMoveTarget, http.StatusBadGateway},
		{`{"to":"10.0.0.2:7777"}`, fmt.Errorf("%w: %w", pool.ErrMoveStoreDown, pool.ErrMoveTarget), http.StatusServiceUnavailable},
		{`{"to":"10.0.0.2:7777"}`, pool.ErrMoveDowntime, http.StatusServiceUnavailable},
		{`{"to":"10.0.0.2:7777"}`, pool.ErrLeaseLost, http.StatusServiceUnavailable},
	} {
		outcome = tt.err
		resp := doReq(t, http.MethodPost, srv.URL+"/v1/sandboxes/sb_1/move", "root", tt.body)
		_ = resp.Body.Close()
		if resp.StatusCode != tt.want {
			t.Errorf("move %s with %v: %d, want %d", tt.body, tt.err, resp.StatusCode, tt.want)
		}
	}
}

func TestMoveRouteRefusesABudgetTheStartAllowanceAloneExceedsBeforeAnyMoveStarts(t *testing.T) {
	calls := 0
	mgr := &fakeManager{move: func(string, string, time.Duration) (pool.MoveResult, error) {
		calls++
		return pool.MoveResult{}, nil
	}}
	srv := newMeshTestServer(t, "root", mgr, &fakePlacer{addrs: []string{"10.0.0.2:7777"}}, nil)
	resp := doReq(t, http.MethodPost, srv.URL+"/v1/sandboxes/sb_1/move", "root", `{"to":"10.0.0.2:7777","max_downtime_ms":400}`)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "above 400") {
		t.Errorf("answer %d %s %v, want a 400 naming the floor", resp.StatusCode, body, err)
	}
	if calls != 0 {
		t.Errorf("the manager saw %d moves, want none", calls)
	}
}

func TestMoveClientReachesATargetAdvertisedByHostname(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/commit") || r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusOK, moveAdmitResponse{Node: "b"})
	}))
	defer target.Close()
	hostname := strings.Replace(strings.TrimPrefix(target.URL, "http://"), "127.0.0.1", "localhost", 1)
	c := &MoveClient{Token: "root"}
	if node, err := c.Admit(t.Context(), hostname, "sb_1", pool.MoveAdmission{From: "a"}); err != nil || node != "b" {
		t.Errorf("admit at %s: %q %v, want node b", hostname, node, err)
	}
	if err := c.Commit(t.Context(), hostname, "sb_1", []byte("{}"), 1); err != nil {
		t.Errorf("commit at %s: %v", hostname, err)
	}
	if err := c.Drop(t.Context(), hostname, "sb_1"); err != nil {
		t.Errorf("drop at %s: %v", hostname, err)
	}
}

func TestMoveClientPassesTheTargetsRefusalThrough(t *testing.T) {
	status, reason := http.StatusOK, ""
	var gotAuth, gotPath string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotPath = r.Header.Get("Authorization"), r.URL.Path
		if status != http.StatusOK {
			writeErr(w, status, reason)
			return
		}
		writeJSON(w, http.StatusOK, moveAdmitResponse{Node: "b"})
	}))
	defer target.Close()
	c := &MoveClient{Token: "root"}
	node, err := c.Admit(t.Context(), strings.TrimPrefix(target.URL, "http://"), "sb_1", pool.MoveAdmission{From: "a"})
	if err != nil || node != "b" || gotAuth != "Bearer root" || gotPath != "/v1/moves/sb_1" {
		t.Fatalf("admit: %q %v auth %q path %q", node, err, gotAuth, gotPath)
	}
	for _, tt := range []struct {
		status int
		want   error
	}{
		{http.StatusTooManyRequests, pool.ErrQuota},
		{http.StatusConflict, pool.ErrNotMovable},
		{http.StatusInternalServerError, pool.ErrMoveTarget},
	} {
		status, reason = tt.status, "admission refused: 4 live claims, cap 4"
		_, err := c.Admit(t.Context(), target.URL, "sb_1", pool.MoveAdmission{From: "a"})
		if !errors.Is(err, tt.want) || !strings.Contains(err.Error(), reason) {
			t.Errorf("admit answered %d: %v, want %v with the target's reason", tt.status, err, tt.want)
		}
	}
}

func TestMoveClientReachesATargetWithItsCurrentOrNextRootToken(t *testing.T) {
	srv := New("root-a", "node-b:7777", &fakeManager{}, &fakeDialer{}, nil, nil, nil, nil)
	srv.SetTokens("", "root-b", "")
	addr := strings.TrimPrefix(serveTestServer(t, srv).URL, "http://")
	for _, tt := range []struct {
		token string
		ok    bool
	}{
		{"root-a", true},
		{"root-b", true},
		{"root-c", false},
	} {
		c := &MoveClient{Token: tt.token}
		_, err := c.Admit(t.Context(), addr, "sb_1", pool.MoveAdmission{From: "a"})
		if tt.ok != (err == nil) || !tt.ok && !strings.Contains(err.Error(), "answered 401") {
			t.Errorf("admit with %s: %v, want accepted %v", tt.token, err, tt.ok)
		}
		if err = c.Drop(t.Context(), addr, "sb_1"); tt.ok != (err == nil) {
			t.Errorf("drop with %s: %v, want accepted %v", tt.token, err, tt.ok)
		}
	}
}
