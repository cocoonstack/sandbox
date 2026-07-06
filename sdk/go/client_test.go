package sandbox

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		_ = json.NewEncoder(w).Encode(claimResponse{ID: "sb_1", Token: "tok", Deadline: time.Unix(42, 0)})
	}))
	t.Cleanup(ts.Close)

	c := testClient(t, ts, WithAPIToken("sekret"))
	sb, err := c.New(t.Context(), "python:3.12",
		WithNetwork(NetEgress), WithSize(Medium), WithTimeout(90*time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := claimRequest{Template: "python:3.12", Net: "egress", Size: "medium", TTLSeconds: 90}
	if gotBody != want {
		t.Errorf("body %+v, want %+v", gotBody, want)
	}
	if gotAuth != "Bearer sekret" {
		t.Errorf("auth %q", gotAuth)
	}
	if sb.ID != "sb_1" || sb.token != "tok" {
		t.Errorf("handle %+v", sb)
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

func testClient(t *testing.T, ts *httptest.Server, opts ...ClientOption) *Client {
	t.Helper()
	c, err := Connect(strings.TrimPrefix(ts.URL, "http://"), opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return c
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
		case "/v1/info":
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
