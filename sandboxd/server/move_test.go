package server

import (
	"encoding/json/v2"
	"errors"
	"fmt"
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
	outcome := error(nil)
	mgr := &fakeManager{move: func(_, addr string, budget time.Duration) (pool.MoveResult, error) {
		gotAddr, gotBudget = addr, budget
		return pool.MoveResult{Rounds: 3, Downtime: 180 * time.Millisecond}, outcome
	}}
	srv := newMeshTestServer(t, "root", mgr, &fakePlacer{addrs: []string{"10.0.0.2:7777"}}, nil)

	resp := doReq(t, http.MethodPost, srv.URL+"/v1/sandboxes/sb_1/move", "root", `{"to":"10.0.0.2:7777","max_downtime_ms":1500}`)
	var body MoveResponse
	if err := json.UnmarshalRead(resp.Body, &body); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("move: %d %v", resp.StatusCode, err)
	}
	if gotAddr != "10.0.0.2:7777" || gotBudget != 1500*time.Millisecond || body != (MoveResponse{OwnerAddr: "10.0.0.2:7777", Rounds: 3, DowntimeMS: 180}) {
		t.Errorf("move to %s within %s answered %+v", gotAddr, gotBudget, body)
	}
	for _, tt := range []struct {
		token, body string
		err         error
		want        int
	}{
		{"root", `{"to":"10.0.0.9:7777"}`, nil, http.StatusBadRequest},
		{"root", `{"to":"10.0.0.2:7777","max_downtime_ms":-1}`, nil, http.StatusBadRequest},
		{"root", `{"to":"10.0.0.2:7777"}`, pool.ErrUnknownSandbox, http.StatusNotFound},
		{"root", `{"to":"10.0.0.2:7777"}`, fmt.Errorf("%w: it has volumes", pool.ErrNotMovable), http.StatusConflict},
		{"root", `{"to":"10.0.0.2:7777"}`, fmt.Errorf("%w: target: 3 moves in flight", pool.ErrQuota), http.StatusTooManyRequests},
		{"root", `{"to":"10.0.0.2:7777"}`, pool.ErrMoveTarget, http.StatusBadGateway},
		{"root", `{"to":"10.0.0.2:7777"}`, fmt.Errorf("%w: %w", pool.ErrMoveStoreDown, pool.ErrMoveTarget), http.StatusServiceUnavailable},
		{"root", `{"to":"10.0.0.2:7777"}`, pool.ErrMoveDowntime, http.StatusServiceUnavailable},
		{"root", `{"to":"10.0.0.2:7777"}`, pool.ErrLeaseLost, http.StatusServiceUnavailable},
	} {
		outcome = tt.err
		resp := doReq(t, http.MethodPost, srv.URL+"/v1/sandboxes/sb_1/move", tt.token, tt.body)
		if resp.StatusCode != tt.want {
			t.Errorf("move %s with %v: %d, want %d", tt.body, tt.err, resp.StatusCode, tt.want)
		}
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
		status, reason = tt.status, "node claim quota reached: 4 live claims, cap 4"
		_, err := c.Admit(t.Context(), target.URL, "sb_1", pool.MoveAdmission{From: "a"})
		if !errors.Is(err, tt.want) || !strings.Contains(err.Error(), reason) {
			t.Errorf("admit answered %d: %v, want %v with the target's reason", tt.status, err, tt.want)
		}
	}
}
