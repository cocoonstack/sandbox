package sandbox

import (
	"encoding/json/v2"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sdk/go/silkd/silkdtest"
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
		if err := json.UnmarshalRead(r.Body, &gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.MarshalWrite(w, claimResponse{
			ID: "sb_1", Token: "tok", Deadline: time.Unix(42, 0), TemplateDigest: "sha256:template", NetRoute: "relay",
		})
	}))
	t.Cleanup(ts.Close)

	c := testClient(t, ts, WithAPIToken("sekret"))
	md := map[string]string{"team": "a"}
	withMetadata := WithMetadata(md)
	md["team"] = "mutated"
	sb, err := c.New(t.Context(), "python:3.12",
		WithNetwork(NetEgress), WithSize(Medium), WithTimeout(90*time.Second), withMetadata, WithArchiveOnExpire())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := claimRequest{Template: "python:3.12", Net: "egress", Size: "medium", TTLSeconds: 90, Metadata: map[string]string{"team": "a"}, OnExpire: "archive"}
	if !reflect.DeepEqual(gotBody, want) {
		t.Errorf("body %+v, want %+v", gotBody, want)
	}
	if gotAuth != "Bearer sekret" {
		t.Errorf("auth %q", gotAuth)
	}
	if sb.ID != "sb_1" || sb.token != "tok" {
		t.Errorf("handle %+v", sb)
	}
	if sb.TemplateDigest != "sha256:template" || sb.NetRoute != "relay" {
		t.Errorf("template digest %q route %q, want sha256:template and relay", sb.TemplateDigest, sb.NetRoute)
	}
}

func TestNewSurfacesServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.MarshalWrite(w, errorResponse{Error: "node has no egress attachment"})
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
				if strings.HasSuffix(r.URL.Path, "/release") {
					gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
				}
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

func TestCloseReleasesASandboxThatMoved(t *testing.T) {
	var released atomic.Bool
	var adopter *httptest.Server
	adopter = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sandboxes/sb_9/owner":
			_ = json.MarshalWrite(w, map[string]string{"owner_addr": strings.TrimPrefix(adopter.URL, "http://")})
		case "/v1/sandboxes/sb_9/release":
			released.Store(true)
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(adopter.Close)
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peers" {
			_ = json.MarshalWrite(w, map[string]any{"peers": []string{strings.TrimPrefix(adopter.URL, "http://")}})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(old.Close)

	c := testClient(t, old)
	sb := &Sandbox{ID: "sb_9", c: c, token: "tok9", owner: c.addr}
	if err := sb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !released.Load() {
		t.Error("Close reported success without releasing at the adopter")
	}
}

func TestAMovedSandboxFollowsItsNewOwnerOnTheNextCall(t *testing.T) {
	sb, adopter := movedSandbox(t)
	if _, err := sb.Stat(t.Context(), "/"); err != nil {
		t.Fatalf("stat after the move: %v", err)
	}
	if got := sb.Owner(); got != adopter {
		t.Errorf("owner %s, want the adopter %s", got, adopter)
	}
}

func TestAMovedSandboxRetriesAControlCallAtItsNewOwner(t *testing.T) {
	sb, adopter := movedSandbox(t)
	if _, err := sb.Renew(t.Context(), time.Minute); err != nil {
		t.Fatalf("renew after the move: %v", err)
	}
	if got := sb.Owner(); got != adopter {
		t.Errorf("owner %s, want the adopter %s", got, adopter)
	}
}

func TestAMovedSandboxDoesNotReplayARunThatStreamedStdin(t *testing.T) {
	sb, adopter := movedSandbox(t)
	if _, err := sb.Run(t.Context(), Cmd{Argv: []string{"cat"}, Stdin: strings.NewReader("once")}); err == nil {
		t.Fatal("a run whose stdin went to the old owner succeeded")
	}
	if got := sb.Owner(); got != adopter {
		t.Errorf("owner %s, want the adopter %s for the next call", got, adopter)
	}
	if out, err := sb.Exec(t.Context(), "echo", "42"); err != nil || out != "42\n" {
		t.Errorf("exec at the adopter: %q, %v", out, err)
	}
}

func TestAHandleFollowsItsSandboxOffARetiredNode(t *testing.T) {
	sb, adopter := movedSandbox(t)
	sb.mu.Lock()
	sb.owner = retiredAddr(t)
	sb.mu.Unlock()
	if _, err := sb.Stat(t.Context(), "/"); err != nil {
		t.Fatalf("stat after the owner retired: %v", err)
	}
	if got := sb.Owner(); got != adopter {
		t.Errorf("owner %s, want the adopter %s", got, adopter)
	}
}

func TestAWriterErrorTaggedForThisSandboxIsNotReplayed(t *testing.T) {
	fake := silkdtest.NewFake(t.TempDir())
	var elsewhere atomic.Int32
	other := newAgentServer(t, func(c net.Conn) {
		elsewhere.Add(1)
		fake.ServeConn(c)
	})
	owner := newAgentServer(t, fake.ServeConn)
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, map[string]string{"owner_addr": strings.TrimPrefix(other.URL, "http://")})
	}))
	t.Cleanup(entry.Close)
	sb := &Sandbox{ID: "sb_1", c: testClient(t, entry), token: "tok", owner: strings.TrimPrefix(owner.URL, "http://")}
	t.Cleanup(sb.pool.drain)
	tagged := writerFunc(func([]byte) (int, error) {
		return 0, &ownerGoneError{err: syscall.ECONNREFUSED, id: sb.ID, addr: sb.Owner()}
	})
	if _, err := sb.Run(t.Context(), Cmd{Argv: []string{"echo", "42"}, Stdout: tagged}); !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("run: %v, want the writer's error", err)
	}
	if n := elsewhere.Load(); n != 0 {
		t.Errorf("the command was sent again to another node %d times", n)
	}
}

func TestAnotherSandboxsErrorInAWriterIsNotReplayed(t *testing.T) {
	fake := silkdtest.NewFake(t.TempDir())
	var elsewhere atomic.Int32
	other := newAgentServer(t, func(c net.Conn) {
		elsewhere.Add(1)
		fake.ServeConn(c)
	})
	owner := newAgentServer(t, fake.ServeConn)
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/sandboxes/sb_1/owner" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.MarshalWrite(w, map[string]string{"owner_addr": strings.TrimPrefix(other.URL, "http://")})
	}))
	t.Cleanup(entry.Close)
	c := testClient(t, entry)
	sb := &Sandbox{ID: "sb_1", c: c, token: "tok", owner: strings.TrimPrefix(owner.URL, "http://")}
	gone := &Sandbox{ID: "sb_2", c: c, token: "tok", owner: retiredAddr(t)}
	t.Cleanup(sb.pool.drain)
	writeElsewhere := writerFunc(func([]byte) (int, error) {
		_, err := gone.Stat(t.Context(), "/")
		return 0, err
	})
	if _, err := sb.Run(t.Context(), Cmd{Argv: []string{"echo", "42"}, Stdout: writeElsewhere}); err == nil {
		t.Fatal("run succeeded although its writer failed")
	}
	if n := elsewhere.Load(); n != 0 {
		t.Errorf("the command was sent again to another node %d times", n)
	}
}

func TestACallThatLostARaceToAMoveRetriesAtTheOwnerFound(t *testing.T) {
	entry := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(entry.Close)
	sb := &Sandbox{ID: "sb_1", c: testClient(t, entry), token: "tok", owner: "new:7777"}
	if !sb.rebind(t.Context(), &ownerGoneError{err: errors.New("gone"), id: "sb_1", addr: "old:7777"}) {
		t.Error("a call tagged by the old owner did not retry at the owner another call found")
	}
	if sb.rebind(t.Context(), &ownerGoneError{err: errors.New("gone"), id: "sb_2", addr: "old:7777"}) {
		t.Error("another sandbox's tag rebound this handle")
	}
}

func TestCloseFollowsAMoveOffARetiredNode(t *testing.T) {
	var released atomic.Bool
	var adopter *httptest.Server
	adopter = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sandboxes/sb_9/owner":
			_ = json.MarshalWrite(w, map[string]string{"owner_addr": strings.TrimPrefix(adopter.URL, "http://")})
		case "/v1/sandboxes/sb_9/release":
			released.Store(true)
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(adopter.Close)

	sb := &Sandbox{ID: "sb_9", c: testClient(t, adopter), token: "tok9", owner: retiredAddr(t)}
	if err := sb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !released.Load() {
		t.Error("Close did not follow the move off a retired node")
	}
}

func TestCloseReportsARetiredNodeWhenNoNodeHoldsTheSandbox(t *testing.T) {
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(entry.Close)

	sb := &Sandbox{ID: "sb_9", c: testClient(t, entry), token: "tok9", owner: retiredAddr(t)}
	if err := sb.Close(); err == nil {
		t.Error("Close hid the transport error with no owner found")
	}
}

func TestNewFollowsRedirect(t *testing.T) {
	var nodeB *httptest.Server
	nodeB = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, claimResponse{
			ID: "sb_2", Token: "tok", OwnerAddr: strings.TrimPrefix(nodeB.URL, "http://"),
		})
	}))
	t.Cleanup(nodeB.Close)

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, claimResponse{
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
		_ = json.MarshalWrite(w, claimResponse{
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
			_ = json.MarshalWrite(w, claimResponse{Redirect: addrs})
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
	var gotNoRedirect bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req claimRequest
		_ = json.UnmarshalRead(r.Body, &req)
		gotNoRedirect = req.NoRedirect
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_3", Token: "t"})
	}))
	t.Cleanup(target.Close)

	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, claimResponse{
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
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_4", Token: "t"})
	}))
	t.Cleanup(good.Close)

	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, claimResponse{
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

func TestRedirectStopsWhenACandidateMayHaveClaimed(t *testing.T) {
	var goodCalls, entryCalls atomic.Int32
	lost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	t.Cleanup(lost.Close)

	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		goodCalls.Add(1)
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_6", Token: "t"})
	}))
	t.Cleanup(good.Close)

	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entryCalls.Add(1)
		_ = json.MarshalWrite(w, claimResponse{
			Redirect: []string{strings.TrimPrefix(lost.URL, "http://"), strings.TrimPrefix(good.URL, "http://")},
		})
	}))
	t.Cleanup(entry.Close)

	if _, err := testClient(t, entry).New(t.Context(), "rt:24.04"); err == nil {
		t.Fatal("New succeeded after a candidate dropped the reply to a delivered claim")
	}
	if goodCalls.Load() != 0 || entryCalls.Load() != 1 {
		t.Errorf("good=%d entry=%d calls, want 0 and 1: a delivered claim is never sent again", goodCalls.Load(), entryCalls.Load())
	}
}

func TestRedirectCandidateDefinitiveErrorStillTriesNextCandidate(t *testing.T) {
	forbidden := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(forbidden.Close)

	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_5", Token: "t"})
	}))
	t.Cleanup(good.Close)

	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, claimResponse{
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
			_ = json.MarshalWrite(w, claimResponse{Redirect: []string{strings.TrimPrefix(full.URL, "http://")}})
			return
		}
		var req claimRequest
		_ = json.UnmarshalRead(r.Body, &req)
		if !req.NoRedirect {
			t.Error("origin fallback did not carry no_redirect")
		}
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_healed", Token: "t"})
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
	var fullCalls int
	full := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fullCalls++
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.MarshalWrite(w, errorResponse{Error: "candidate full"})
	}))
	t.Cleanup(full.Close)

	var entryCalls int
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entryCalls++
		if entryCalls == 1 {
			_ = json.MarshalWrite(w, claimResponse{Redirect: []string{strings.TrimPrefix(full.URL, "http://")}})
			return
		}
		w.WriteHeader(http.StatusConflict)
		_ = json.MarshalWrite(w, errorResponse{Error: "origin also unavailable"})
	}))
	t.Cleanup(entry.Close)

	sb, err := testClient(t, entry).New(t.Context(), "rt:24.04")
	if err == nil {
		t.Fatal("New: want error, got nil")
	}
	if !strings.Contains(err.Error(), "origin also unavailable") {
		t.Errorf("err %v, want the origin's failure surfaced", err)
	}

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
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.code)
				_ = json.MarshalWrite(w, errorResponse{Error: tt.want})
			}))
			t.Cleanup(bad.Close)

			var entryCalls int
			entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				entryCalls++
				_ = json.MarshalWrite(w, claimResponse{Redirect: []string{strings.TrimPrefix(bad.URL, "http://")}})
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
		_ = json.MarshalWrite(w, errorResponse{Error: "invalid api token"})
	}))
	t.Cleanup(stale.Close)

	var entryCalls int
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entryCalls++
		if entryCalls == 1 {
			_ = json.MarshalWrite(w, claimResponse{Redirect: []string{strings.TrimPrefix(stale.URL, "http://")}})
			return
		}
		var req claimRequest
		_ = json.UnmarshalRead(r.Body, &req)
		if !req.NoRedirect {
			t.Error("origin fallback did not carry no_redirect")
		}
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_local", Token: "t"})
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
	var owner *httptest.Server
	owner = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sandboxes/sb_1/owner" && r.Header.Get("Authorization") == "Bearer tok" {
			_ = json.MarshalWrite(w, map[string]string{"owner_addr": strings.TrimPrefix(owner.URL, "http://")})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(owner.Close)

	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sandboxes/sb_1/owner":
			w.WriteHeader(http.StatusNotFound)
		case "/v1/peers":
			_ = json.MarshalWrite(w, map[string]any{"peers": []string{strings.TrimPrefix(owner.URL, "http://")}})
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

func TestLookupReportsAFailedPeerDiscovery(t *testing.T) {
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sandboxes/sb_1/owner":
			w.WriteHeader(http.StatusNotFound)
		case "/v1/peers":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":"gossip not ready"}`)
		}
	}))
	t.Cleanup(entry.Close)

	_, err := testClient(t, entry).Lookup(t.Context(), "sb_1", "tok")
	he, ok := errors.AsType[*APIError](err)
	if !ok || he.Verb != "peers" || he.Status != http.StatusServiceUnavailable {
		t.Fatalf("Lookup: %v, want the peers 503 joined into the error", err)
	}
	if !strings.Contains(err.Error(), "no owner found") {
		t.Errorf("Lookup: %v, want the no-owner verdict kept alongside", err)
	}
}

func TestAPIErrorDrainsOversizedBody(t *testing.T) {
	body := strings.NewReader(`{"error":"` + strings.Repeat("x", 3*4096) + `"}`)
	err := apiError("claim", &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(body)})

	he, ok := errors.AsType[*APIError](err)
	if !ok || he.Status != http.StatusTooManyRequests {
		t.Fatalf("apiError = %v, want a 429 APIError", err)
	}
	if body.Len() != 0 {
		t.Errorf("apiError left %d bytes unread; the transport cannot reuse the connection", body.Len())
	}
}

func movedSandbox(t *testing.T) (*Sandbox, string) {
	t.Helper()
	fake := silkdtest.NewFake(t.TempDir())
	mux := http.NewServeMux()
	var adopter *httptest.Server
	mux.HandleFunc("GET /v1/sandboxes/sb_1/agent", func(w http.ResponseWriter, _ *http.Request) {
		conn, err := silkdtest.Upgrade(w)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		fake.ServeConn(conn)
	})
	mux.HandleFunc("GET /v1/sandboxes/sb_1/owner", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, map[string]string{"owner_addr": strings.TrimPrefix(adopter.URL, "http://")})
	})
	mux.HandleFunc("POST /v1/sandboxes/sb_1/renew", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, map[string]time.Time{"deadline": time.Now().Add(time.Minute)})
	})
	adopter = httptest.NewServer(mux)
	t.Cleanup(adopter.Close)
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peers" {
			_ = json.MarshalWrite(w, map[string]any{"peers": []string{strings.TrimPrefix(adopter.URL, "http://")}})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(old.Close)
	sb := testSandbox(t, old)
	return sb, strings.TrimPrefix(adopter.URL, "http://")
}

func retiredAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func testClient(t testing.TB, ts *httptest.Server, opts ...ClientOption) *Client {
	t.Helper()
	c, err := Connect(strings.TrimPrefix(ts.URL, "http://"), opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return c
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
