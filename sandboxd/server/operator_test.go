package server

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	rootToken     = "root"
	operatorToken = "op-tok"
	tenantToken   = "acme-tok"
)

func TestOperatorTokenActsAsRootOnExactlyItsRoutes(t *testing.T) {
	seen := map[string]bool{}
	operatorCred := func(token string) bool {
		seen[token] = true
		return token == ""
	}
	verb := func(_, token string) error {
		if operatorCred(token) {
			return nil
		}
		return pool.ErrUnknownSandbox
	}
	mgr := &fakeManager{
		release:   verb,
		hibernate: verb,
		wake:      verb,
		renew: func(_, token string, _ time.Duration) (time.Time, error) {
			if operatorCred(token) {
				return time.Now().Add(time.Hour), nil
			}
			return time.Time{}, pool.ErrUnknownSandbox
		},
		socket: func(_, token string) (string, error) {
			if operatorCred(token) {
				return "/v/sock", nil
			}
			return "", pool.ErrUnknownSandbox
		},
		dialPort: func(_, token string, _ uint16) (net.Conn, error) {
			if operatorCred(token) {
				return nil, pool.ErrPaused
			}
			return nil, pool.ErrUnknownSandbox
		},
		sandboxByID: func(string) (pool.SandboxSummary, bool) {
			return pool.SandboxSummary{ID: "sb_1", Token: "victim"}, true
		},
	}
	ts := newOperatorTestServer(t, operatorToken, mgr, pipeDialer(okGuest))

	routes := []struct {
		pattern, path, body string
		operator            bool
		want, tenant        int
	}{
		{"POST /v1/claim", "/v1/claim", `{"template":"rt:24.04"}`, true, http.StatusOK, http.StatusOK},
		{"GET /v1/sandboxes", "/v1/sandboxes", "", true, http.StatusOK, http.StatusOK},
		{"GET /v1/sandboxes/{id}", "/v1/sandboxes/sb_1", "", true, http.StatusOK, http.StatusForbidden},
		{"POST /v1/sandboxes/{id}/release", "/v1/sandboxes/sb_1/release", "", true, http.StatusNoContent, http.StatusNotFound},
		{"POST /v1/sandboxes/{id}/hibernate", "/v1/sandboxes/sb_1/hibernate", "", true, http.StatusNoContent, http.StatusNotFound},
		{"POST /v1/sandboxes/{id}/wake", "/v1/sandboxes/sb_1/wake", "", true, http.StatusNoContent, http.StatusNotFound},
		{"POST /v1/sandboxes/{id}/renew", "/v1/sandboxes/sb_1/renew", "", true, http.StatusOK, http.StatusNotFound},
		{"GET /v1/sandboxes/{id}/env", "/v1/sandboxes/sb_1/env", "", true, http.StatusOK, http.StatusOK},
		{"PUT /v1/sandboxes/{id}/env", "/v1/sandboxes/sb_1/env", `{"env":{}}`, true, http.StatusNoContent, http.StatusNoContent},
		{"PATCH /v1/sandboxes/{id}/env", "/v1/sandboxes/sb_1/env", `{"env":{}}`, true, http.StatusNoContent, http.StatusNoContent},
		{"GET /v1/sandboxes/{id}/ports/{port}", "/v1/sandboxes/sb_1/ports/8080", "", true, http.StatusConflict, http.StatusNotFound},
		{"POST /v1/sandboxes/{id}/exec", "/v1/sandboxes/sb_1/exec", `{"argv":["true"]}`, true, http.StatusOK, http.StatusNotFound},
		{"GET /v1/sandboxes/{id}/owner", "/v1/sandboxes/sb_1/owner", "", true, http.StatusOK, http.StatusNotFound},
		{"GET /v1/tenants", "/v1/tenants", "", true, http.StatusOK, http.StatusForbidden},
		{"PUT /v1/tenants", "/v1/tenants", `{"tenants":[]}`, true, http.StatusOK, http.StatusForbidden},
		{"PUT /v1/tenants/{name}", "/v1/tenants/beta", `{"token":"b"}`, true, http.StatusNoContent, http.StatusForbidden},
		{"DELETE /v1/tenants/{name}", "/v1/tenants/acme", "", true, http.StatusNoContent, http.StatusForbidden},
		{"GET /v1/info", "/v1/info", "", true, http.StatusOK, http.StatusForbidden},
		{"GET /v1/peers", "/v1/peers", "", true, http.StatusOK, http.StatusOK},

		{"GET /v1/volumes", "/v1/volumes", "", false, 0, 0},
		{"PUT /v1/volumes/{name}", "/v1/volumes/data", `{}`, false, 0, http.StatusForbidden},
		{"DELETE /v1/volumes/{name}", "/v1/volumes/data", "", false, 0, http.StatusForbidden},
		{"GET /v1/sandboxes/{id}/volumes", "/v1/sandboxes/sb_1/volumes", "", false, 0, 0},
		{"PUT /v1/sandboxes/{id}/volumes", "/v1/sandboxes/sb_1/volumes", `{"volumes":[]}`, false, 0, 0},
		{"PATCH /v1/sandboxes/{id}/volumes", "/v1/sandboxes/sb_1/volumes", `{}`, false, 0, 0},
		{"GET /v1/sandboxes/{id}/stats", "/v1/sandboxes/sb_1/stats", "", false, 0, http.StatusForbidden},
		{"PUT /v1/sandboxes/{id}/instance-metadata", "/v1/sandboxes/sb_1/instance-metadata", `{}`, false, 0, http.StatusForbidden},
		{"PUT /v1/sandboxes/{id}/tun", "/v1/sandboxes/sb_1/tun", `{}`, false, 0, http.StatusForbidden},
		{"POST /v1/sandboxes/{id}/fork", "/v1/sandboxes/sb_1/fork", `{"count":1}`, false, 0, 0},
		{"POST /v1/sandboxes/{id}/promote", "/v1/sandboxes/sb_1/promote", `{"template":"t"}`, false, 0, 0},
		{"POST /v1/sandboxes/{id}/preview", "/v1/sandboxes/sb_1/preview", `{"port":80}`, false, 0, 0},
		{"POST /v1/sandboxes/{id}/checkpoint", "/v1/sandboxes/sb_1/checkpoint", `{}`, false, 0, 0},
		{"POST /v1/checkpoints/{id}/claim", "/v1/checkpoints/ck_00000000000000aa/claim", `{}`, false, 0, 0},
		{"GET /v1/checkpoints", "/v1/checkpoints", "", false, 0, 0},
		{"GET /v1/checkpoints/{id}/blob", "/v1/checkpoints/ck_00000000000000aa/blob", "", false, 0, http.StatusForbidden},
		{"DELETE /v1/checkpoints/{id}", "/v1/checkpoints/ck_00000000000000aa", "", false, 0, 0},
		{"DELETE /v1/templates", "/v1/templates?template=rt:24.04", "", false, 0, 0},
		{"PUT /v1/templates/labels", "/v1/templates/labels?template=rt:24.04", `{"labels":{}}`, false, 0, 0},
		{"PUT /v1/pools", "/v1/pools", `{"pools":[]}`, false, 0, http.StatusForbidden},
		{"POST /v1/config/reload", "/v1/config/reload", "", false, 0, http.StatusForbidden},
		{"POST /v1/sandboxes/{id}/move", "/v1/sandboxes/sb_1/move", `{"addr":"node-b:7777"}`, false, 0, http.StatusForbidden},
		{"POST /v1/moves/{id}", "/v1/moves/sb_1", `{}`, false, 0, http.StatusForbidden},
		{"PUT /v1/moves/{id}/rounds/{n}", "/v1/moves/sb_1/rounds/1", "", false, 0, http.StatusForbidden},
		{"POST /v1/moves/{id}/commit", "/v1/moves/sb_1/commit", `{}`, false, 0, http.StatusForbidden},
		{"DELETE /v1/moves/{id}", "/v1/moves/sb_1", "", false, 0, http.StatusForbidden},
		{"POST /v1/drain", "/v1/drain", "", false, 0, http.StatusForbidden},
		{"DELETE /v1/drain", "/v1/drain", "", false, 0, http.StatusForbidden},
		{"POST /v1/cordon", "/v1/cordon", "", false, 0, http.StatusForbidden},
		{"DELETE /v1/cordon", "/v1/cordon", "", false, 0, http.StatusForbidden},
		{"GET /v1/usage", "/v1/usage", "", false, 0, http.StatusForbidden},
		{"GET /v1/sandboxes/{id}/agent", "/v1/sandboxes/sb_1/agent", "", false, 0, 0},
		{"GET /metrics", "/metrics", "", false, 0, http.StatusForbidden},
	}
	classified := map[string]bool{"HEAD /v1/checkpoints/{id}/blob": true, "GET /healthz": true, "/p/{token}/": true, "/healthz": true}
	for _, tt := range routes {
		classified[tt.pattern] = true
		t.Run(tt.pattern, func(t *testing.T) {
			if tt.operator {
				for _, token := range []string{rootToken, operatorToken} {
					if got := routeStatus(t, ts, tt.pattern, tt.path, token, tt.body); got != tt.want {
						t.Errorf("%s: status %d, want %d", token, got, tt.want)
					}
				}
			} else if got := routeStatus(t, ts, tt.pattern, tt.path, operatorToken, tt.body); got != http.StatusForbidden {
				t.Errorf("operator: status %d, want 403", got)
			}
			if tt.tenant == 0 {
				return
			}
			if got := routeStatus(t, ts, tt.pattern, tt.path, tenantToken, tt.body); got != tt.tenant {
				t.Errorf("tenant: status %d, want %d", got, tt.tenant)
			}
		})
	}
	if seen[operatorToken] {
		t.Error("the operator token reached the manager as a claim token")
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list sources: %v", err)
	}
	registration := regexp.MustCompile(`mux\.Handle(?:Func)?\("([^"]+)"`)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, m := range registration.FindAllStringSubmatch(string(src), -1) {
			if !classified[m[1]] {
				t.Errorf("route %q in %s has no operator token row", m[1], file)
			}
		}
	}
}

func TestOperatorTokenReadOfAClaimTokenIsAudited(t *testing.T) {
	reads := make(chan string, 2)
	mgr := &fakeManager{
		sandboxByID: func(string) (pool.SandboxSummary, bool) {
			return pool.SandboxSummary{ID: "sb_1", Token: "victim"}, true
		},
		tokenRead: func(id, caller string) { reads <- id + " " + caller },
	}
	ts := newOperatorTestServer(t, operatorToken, mgr, nil)
	for _, token := range []string{rootToken, operatorToken} {
		resp := doReq(t, http.MethodGet, ts.URL+"/v1/sandboxes/sb_1", token, "")
		got := decodeJSON[pool.SandboxSummary](t, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || got.Token != "victim" {
			t.Errorf("%s: status %d token %q, want 200 and the claim token", token, resp.StatusCode, got.Token)
		}
	}
	if got := await(t, reads, "the operator's token read was not audited"); got != "sb_1 "+callerOperator {
		t.Errorf("token read audited as %q, want sb_1 by the operator", got)
	}
	if len(reads) != 0 {
		t.Errorf("root's token read was audited as %q", <-reads)
	}
}

func TestOperatorTokenCannotClaimVolumes(t *testing.T) {
	const withVolumes = `{"template":"rt:24.04","volumes":[{"name":"imagenet"}]}`
	tests := []struct {
		name, token, body string
		want              int
		reached           bool
	}{
		{"operator with volumes", operatorToken, withVolumes, http.StatusForbidden, false},
		{"operator without volumes", operatorToken, `{"template":"rt:24.04"}`, http.StatusOK, true},
		{"root with volumes", rootToken, withVolumes, http.StatusOK, true},
		{"tenant with volumes", tenantToken, withVolumes, http.StatusOK, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			placed := false
			mgr := &fakeManager{volumePlacement: func(types.PoolKey, string, []string) (bool, error) {
				placed = true
				return true, nil
			}}
			ts := newOperatorTestServer(t, operatorToken, mgr, nil)
			if got := routeStatus(t, ts, "POST /v1/claim", "/v1/claim", tt.token, tt.body); got != tt.want {
				t.Errorf("status %d, want %d", got, tt.want)
			}
			if reached := placed || mgr.warmCalls+mgr.provisionCalls > 0; reached != tt.reached {
				t.Errorf("reached the manager %v, want %v", reached, tt.reached)
			}
		})
	}
}

func TestOperatorTokenUnsetOrEmptyMatchesNothing(t *testing.T) {
	const info, release = "GET /v1/info", "POST /v1/sandboxes/{id}/release"
	tests := []struct {
		name, operator, bearer, pattern, path string
		want                                  int
	}{
		{"unset operator token on a node route", "", operatorToken, info, "/v1/info", http.StatusUnauthorized},
		{"unset operator token is a claim token", "", operatorToken, release, "/v1/sandboxes/sb_1/release", http.StatusNotFound},
		{"empty bearer with no operator token", "", "", info, "/v1/info", http.StatusUnauthorized},
		{"empty bearer with an operator token", operatorToken, "", info, "/v1/info", http.StatusUnauthorized},
		{"empty bearer on a sandbox route", operatorToken, "", release, "/v1/sandboxes/sb_1/release", http.StatusUnauthorized},
		{"set operator token", operatorToken, operatorToken, info, "/v1/info", http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &fakeManager{release: func(string, string) error { return pool.ErrUnknownSandbox }}
			ts := newOperatorTestServer(t, tt.operator, mgr, nil)
			if got := routeStatus(t, ts, tt.pattern, tt.path, tt.bearer, ""); got != tt.want {
				t.Errorf("status %d, want %d", got, tt.want)
			}
		})
	}
}

func TestAuditRecordsTheCallerKind(t *testing.T) {
	tests := []struct {
		name, pattern, path, token, body, want string
	}{
		{"root exec", "POST /v1/sandboxes/{id}/exec", "/v1/sandboxes/sb_1/exec", rootToken, `{"argv":["true"]}`, callerRoot},
		{"operator exec", "POST /v1/sandboxes/{id}/exec", "/v1/sandboxes/sb_1/exec", operatorToken, `{"argv":["true"]}`, callerOperator},
		{"claim exec", "POST /v1/sandboxes/{id}/exec", "/v1/sandboxes/sb_1/exec", "tok", `{"argv":["true"]}`, callerClaim},
		{"root tenant write", "PUT /v1/tenants/{name}", "/v1/tenants/beta", rootToken, `{"token":"b"}`, callerRoot},
		{"operator tenant write", "PUT /v1/tenants/{name}", "/v1/tenants/beta", operatorToken, `{"token":"b"}`, callerOperator},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callers := make(chan string, 1)
			mgr := &fakeManager{auditedBy: func(caller string) { callers <- caller }}
			ts := newOperatorTestServer(t, operatorToken, mgr, pipeDialer(okGuest))
			if code := routeStatus(t, ts, tt.pattern, tt.path, tt.token, tt.body); code >= http.StatusBadRequest {
				t.Fatalf("status %d", code)
			}
			if got := await(t, callers, "no audit record"); got != tt.want {
				t.Errorf("audit caller %q, want %q", got, tt.want)
			}
		})
	}
}

func newOperatorTestServer(t *testing.T, operator string, mgr *fakeManager, dialer Dialer) *httptest.Server {
	t.Helper()
	if dialer == nil {
		dialer = &fakeDialer{}
	}
	mgr.tenants = []tenants.TenantSpec{{Name: "acme", Token: tenantToken}}
	srv := New(rootToken, "node:7777", mgr, dialer, nil, nil, nil, nil)
	srv.SetOperatorToken(operator)
	return serveTestServer(t, srv)
}

func routeStatus(t *testing.T, ts *httptest.Server, pattern, path, token, body string) int {
	t.Helper()
	method, _, _ := strings.Cut(pattern, " ")
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	switch {
	case strings.Contains(pattern, "/ports/"):
		req.Header.Set("Upgrade", upgradeProtoTCP)
		req.Header.Set("Connection", "Upgrade")
	case strings.HasSuffix(pattern, "/agent"):
		req.Header.Set("Upgrade", upgradeProto)
		req.Header.Set("Connection", "Upgrade")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}
