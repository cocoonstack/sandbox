package sandbox

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestConnectAddress(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want string
		ok   bool
	}{
		{"single", "n1:7777", "n1:7777", true},
		{"seed list uses first", "n1:7777, n2:7777", "n1:7777", true},
		{"empty", "", "", false},
		{"only commas", " , ", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := Connect(tt.addr)
			if tt.ok != (err == nil) {
				t.Fatalf("err=%v, want ok=%v", err, tt.ok)
			}
			if tt.ok && c.addr != tt.want {
				t.Errorf("addr %q, want %q", c.addr, tt.want)
			}
		})
	}
}

func TestNewSendsClaim(t *testing.T) {
	var gotBody claimRequest
	var gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/claim" {
			t.Errorf("got %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.NewEncoder(w).Encode(claimResponse{
			ID: "sb_1", Token: "tok", Deadline: time.Unix(42, 0), TemplateDigest: "sha256:template",
		})
	}))
	t.Cleanup(ts.Close)

	c := testClient(t, ts, WithAPIToken("sekret"))
	sb, err := c.New(t.Context(), "python:3.12",
		WithNetwork(NetEgress), WithSize(Medium), WithTimeout(90*time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := claimRequest{Template: "python:3.12", Net: "egress", Size: "medium", TTLSeconds: 90}
	if !reflect.DeepEqual(gotBody, want) {
		t.Errorf("body %+v, want %+v", gotBody, want)
	}
	if gotAuth != "Bearer sekret" {
		t.Errorf("auth %q", gotAuth)
	}
	if sb.ID != "sb_1" || sb.token != "tok" {
		t.Errorf("handle %+v", sb)
	}
	if sb.TemplateDigest != "sha256:template" {
		t.Errorf("template digest %q, want sha256:template", sb.TemplateDigest)
	}
}

func TestNewSurfacesServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(errorResponse{Error: "node has no egress attachment"})
	}))
	t.Cleanup(ts.Close)

	_, err := testClient(t, ts).New(t.Context(), "rt:24.04")
	if err == nil || !strings.Contains(err.Error(), "no egress attachment") ||
		!strings.Contains(err.Error(), "409") {
		t.Errorf("got %v, want surfaced 409 message", err)
	}
}

func TestCloseReleases(t *testing.T) {
	tests := []struct {
		name   string
		status int
		ok     bool
	}{
		{"released", http.StatusNoContent, true},
		{"already gone", http.StatusNotFound, true},
		{"server failure", http.StatusInternalServerError, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath, gotAuth string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
				w.WriteHeader(tt.status)
			}))
			t.Cleanup(ts.Close)

			c := testClient(t, ts)
			sb := &Sandbox{ID: "sb_9", c: c, token: "tok9", owner: c.addr}
			err := sb.Close()
			if tt.ok != (err == nil) {
				t.Fatalf("err=%v, want ok=%v", err, tt.ok)
			}
			if gotPath != "/v1/sandboxes/sb_9/release" || gotAuth != "Bearer tok9" {
				t.Errorf("got %s auth %q", gotPath, gotAuth)
			}
		})
	}
}

func TestNewFollowsRedirect(t *testing.T) {
	// The first node redirects; the second answers with the claim. The SDK
	// must follow transparently and end up owning the sandbox at node B.
	var nodeB *httptest.Server
	nodeB = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(claimResponse{
			ID: "sb_2", Token: "tok", OwnerAddr: strings.TrimPrefix(nodeB.URL, "http://"),
		})
	}))
	t.Cleanup(nodeB.Close)

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(claimResponse{
			Redirect: []string{strings.TrimPrefix(nodeB.URL, "http://")},
		})
	}))
	t.Cleanup(nodeA.Close)

	sb, err := testClient(t, nodeA).New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if sb.ID != "sb_2" {
		t.Errorf("id %q, want sb_2 (claimed at the redirect target)", sb.ID)
	}
	if sb.owner != strings.TrimPrefix(nodeB.URL, "http://") {
		t.Errorf("owner %q, want node B", sb.owner)
	}
}

func TestDeleteTemplateFollowsRedirect(t *testing.T) {
	// The entry node answers with the owner's address; the SDK retries the
	// delete there, once.
	var deletedAtB, gotNoRedirect bool
	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletedAtB = true
			gotNoRedirect = r.URL.Query().Get("no_redirect") != ""
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(nodeB.Close)

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(claimResponse{
			Redirect: []string{strings.TrimPrefix(nodeB.URL, "http://")},
		})
	}))
	t.Cleanup(nodeA.Close)

	if err := testClient(t, nodeA).DeleteTemplate(t.Context(), "tpl"); err != nil {
		t.Fatalf("DeleteTemplate: %v", err)
	}
	if !deletedAtB {
		t.Error("delete never reached the owner node")
	}
	if !gotNoRedirect {
		t.Error("redirected delete did not carry no_redirect")
	}
}

// TestDeleteTemplateRetryPolicy pins the walk semantics shared with the
// Python SDK: a 404 moves to the next owner, a served error surfaces at
// once (walking on would let a later 404 mask it).
func TestDeleteTemplateRetryPolicy(t *testing.T) {
	status := func(code int, hit *bool) *httptest.Server {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			*hit = true
			w.WriteHeader(code)
		}))
		t.Cleanup(s.Close)
		return s
	}
	entry := func(candidates ...*httptest.Server) *httptest.Server {
		addrs := make([]string, len(candidates))
		for i, c := range candidates {
			addrs[i] = strings.TrimPrefix(c.URL, "http://")
		}
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(claimResponse{Redirect: addrs})
		}))
		t.Cleanup(s.Close)
		return s
	}

	t.Run("miss walks to the next owner", func(t *testing.T) {
		var missHit, ownerHit bool
		miss := status(http.StatusNotFound, &missHit)
		owner := status(http.StatusNoContent, &ownerHit)
		if err := testClient(t, entry(miss, owner)).DeleteTemplate(t.Context(), "tpl"); err != nil {
			t.Fatalf("DeleteTemplate: %v", err)
		}
		if !missHit || !ownerHit {
			t.Errorf("miss=%v owner=%v, want both tried", missHit, ownerHit)
		}
	})
	t.Run("served error stops the walk", func(t *testing.T) {
		var brokenHit, ownerHit bool
		broken := status(http.StatusInternalServerError, &brokenHit)
		owner := status(http.StatusNoContent, &ownerHit)
		err := testClient(t, entry(broken, owner)).DeleteTemplate(t.Context(), "tpl")
		if err == nil || !strings.Contains(err.Error(), "http 500") {
			t.Fatalf("err %v, want the 500 surfaced", err)
		}
		if ownerHit {
			t.Error("walk continued past a served error")
		}
	})
}

func TestRedirectSetsNoRedirect(t *testing.T) {
	// The retry at a redirect target must carry no_redirect so the target
	// warm-or-provisions instead of bouncing the claim back.
	var gotNoRedirect bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req claimRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		gotNoRedirect = req.NoRedirect
		_ = json.NewEncoder(w).Encode(claimResponse{ID: "sb_3", Token: "t"})
	}))
	t.Cleanup(target.Close)

	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(claimResponse{
			Redirect: []string{strings.TrimPrefix(target.URL, "http://")},
		})
	}))
	t.Cleanup(entry.Close)

	sb, err := testClient(t, entry).New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if sb.ID != "sb_3" {
		t.Errorf("id %q, want sb_3", sb.ID)
	}
	if !gotNoRedirect {
		t.Error("retry did not set no_redirect")
	}
}

func TestRedirectTriesAllCandidates(t *testing.T) {
	// First candidate is dead; the SDK must fall through to the second.
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(claimResponse{ID: "sb_4", Token: "t"})
	}))
	t.Cleanup(good.Close)

	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(claimResponse{
			Redirect: []string{"127.0.0.1:1", strings.TrimPrefix(good.URL, "http://")},
		})
	}))
	t.Cleanup(entry.Close)

	sb, err := testClient(t, entry).New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if sb.ID != "sb_4" {
		t.Errorf("id %q, want sb_4 (second candidate)", sb.ID)
	}
}

func TestRedirectCandidateDefinitiveErrorStillTriesNextCandidate(t *testing.T) {
	// Candidate one is wrong for this claim (403, definitive) but candidate
	// two would still succeed — the per-candidate walk retries broadly, so
	// one ill-suited candidate must not cost a candidate that would answer.
	forbidden := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(forbidden.Close)

	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(claimResponse{ID: "sb_5", Token: "t"})
	}))
	t.Cleanup(good.Close)

	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(claimResponse{
			Redirect: []string{strings.TrimPrefix(forbidden.URL, "http://"), strings.TrimPrefix(good.URL, "http://")},
		})
	}))
	t.Cleanup(entry.Close)

	sb, err := testClient(t, entry).New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if sb.ID != "sb_5" {
		t.Errorf("id %q, want sb_5 (second candidate, reached despite the first's 403)", sb.ID)
	}
}

func TestRedirectFallbackHeals(t *testing.T) {
	// The only candidate holds the record but is full (429); once it's
	// exhausted, New must fall back to the origin itself, which heals.
	var fullCalls int
	full := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fullCalls++
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(full.Close)

	var entryCalls int
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entryCalls++
		if entryCalls == 1 {
			_ = json.NewEncoder(w).Encode(claimResponse{Redirect: []string{strings.TrimPrefix(full.URL, "http://")}})
			return
		}
		var req claimRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if !req.NoRedirect {
			t.Error("origin fallback did not carry no_redirect")
		}
		_ = json.NewEncoder(w).Encode(claimResponse{ID: "sb_healed", Token: "t"})
	}))
	t.Cleanup(entry.Close)

	sb, err := testClient(t, entry).New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if sb.ID != "sb_healed" {
		t.Errorf("id %q, want sb_healed", sb.ID)
	}
	if sb.owner != strings.TrimPrefix(entry.URL, "http://") {
		t.Errorf("owner %q, want the origin", sb.owner)
	}
	if entryCalls != 2 {
		t.Errorf("origin called %d times, want 2 (initial + fallback)", entryCalls)
	}
	if fullCalls != 1 {
		t.Errorf("candidate called %d times, want 1", fullCalls)
	}
}

func TestRedirectFallbackAlsoFails(t *testing.T) {
	// Every candidate is full (429); the origin fallback answers too, but
	// with a definitive error, which must surface as the final error.
	var fullCalls int
	full := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fullCalls++
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(errorResponse{Error: "candidate full"})
	}))
	t.Cleanup(full.Close)

	var entryCalls int
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entryCalls++
		if entryCalls == 1 {
			_ = json.NewEncoder(w).Encode(claimResponse{Redirect: []string{strings.TrimPrefix(full.URL, "http://")}})
			return
		}
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(errorResponse{Error: "origin also unavailable"})
	}))
	t.Cleanup(entry.Close)

	sb, err := testClient(t, entry).New(t.Context(), "rt:24.04")
	if err == nil {
		t.Fatal("New: want error, got nil")
	}
	if !strings.Contains(err.Error(), "origin also unavailable") {
		t.Errorf("err %v, want the origin's failure surfaced", err)
	}
	// The candidate's failure must survive into the message too: it is what
	// says why the claim left the origin in the first place.
	if !strings.Contains(err.Error(), "candidate full") {
		t.Errorf("err %v, want the candidate's failure surfaced too", err)
	}
	if sb != nil {
		t.Errorf("sb %+v, want nil", sb)
	}
	if entryCalls != 2 {
		t.Errorf("origin called %d times, want 2 (initial + fallback)", entryCalls)
	}
	if fullCalls != 1 {
		t.Errorf("candidate called %d times, want 1", fullCalls)
	}
}

func TestRedirectDefinitiveErrorSkipsFallback(t *testing.T) {
	tests := []struct {
		name string
		code int
		want string
	}{
		{"conflict", http.StatusConflict, "no egress attachment"},
		{"forbidden", http.StatusForbidden, "tenant not allowed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A definitive 4xx is not worth trying another candidate, and
			// not worth falling back to the origin either — the origin
			// would fail the exact same way.
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.code)
				_ = json.NewEncoder(w).Encode(errorResponse{Error: tt.want})
			}))
			t.Cleanup(bad.Close)

			var entryCalls int
			entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				entryCalls++
				_ = json.NewEncoder(w).Encode(claimResponse{Redirect: []string{strings.TrimPrefix(bad.URL, "http://")}})
			}))
			t.Cleanup(entry.Close)

			_, err := testClient(t, entry).New(t.Context(), "rt:24.04")
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err %v, want %q surfaced", err, tt.want)
			}
			if entryCalls != 1 {
				t.Errorf("entry called %d times, want 1 (no origin fallback for a definitive error)", entryCalls)
			}
		})
	}
}

func TestRedirectRotating401CandidateFallsBackToOrigin(t *testing.T) {
	var staleCalls int
	stale := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		staleCalls++
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(errorResponse{Error: "invalid api token"})
	}))
	t.Cleanup(stale.Close)

	var entryCalls int
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entryCalls++
		if entryCalls == 1 {
			_ = json.NewEncoder(w).Encode(claimResponse{Redirect: []string{strings.TrimPrefix(stale.URL, "http://")}})
			return
		}
		var req claimRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if !req.NoRedirect {
			t.Error("origin fallback did not carry no_redirect")
		}
		_ = json.NewEncoder(w).Encode(claimResponse{ID: "sb_local", Token: "t"})
	}))
	t.Cleanup(entry.Close)

	sb, err := testClient(t, entry).New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if sb.ID != "sb_local" {
		t.Errorf("id %q, want sb_local (provisioned at the already-authorized origin)", sb.ID)
	}
	if staleCalls != 1 || entryCalls != 2 {
		t.Errorf("stale=%d entry=%d calls, want 1 and 2 (redirect + fallback)", staleCalls, entryCalls)
	}
}

func TestLookupScatter(t *testing.T) {
	// The owner is node B; the entry node A doesn't own the sandbox but lists
	// B as a peer. Lookup must scatter to B and bind the handle there.
	var owner *httptest.Server
	owner = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sandboxes/sb_1/owner" && r.Header.Get("Authorization") == "Bearer tok" {
			_ = json.NewEncoder(w).Encode(map[string]string{"owner_addr": strings.TrimPrefix(owner.URL, "http://")})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(owner.Close)

	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sandboxes/sb_1/owner":
			w.WriteHeader(http.StatusNotFound) // not owned here
		case "/v1/peers":
			_ = json.NewEncoder(w).Encode(map[string]any{"peers": []string{strings.TrimPrefix(owner.URL, "http://")}})
		}
	}))
	t.Cleanup(entry.Close)

	sb, err := testClient(t, entry).Lookup(t.Context(), "sb_1", "tok")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if sb.ID != "sb_1" || sb.owner != strings.TrimPrefix(owner.URL, "http://") {
		t.Errorf("handle %+v, want owner=B", sb)
	}
}

func testClient(t *testing.T, ts *httptest.Server, opts ...ClientOption) *Client {
	t.Helper()
	c, err := Connect(strings.TrimPrefix(ts.URL, "http://"), opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return c
}
