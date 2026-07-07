package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakePreviewMgr struct {
	dial func(id string, port uint16) (net.Conn, error)
}

func (f *fakePreviewMgr) PreviewDial(_ context.Context, id string, port uint16) (net.Conn, error) {
	return f.dial(id, port)
}

func mintToken(ps *PreviewServer, id string, port uint16, ttl time.Duration) string {
	url := ps.Mint(id, port, ttl)
	return strings.TrimSuffix(url[strings.Index(url, "/p/")+3:], "/")
}

func TestPreviewTokenRoundTrip(t *testing.T) {
	ps := NewPreviewServer("secret", "node:9000", &fakePreviewMgr{})
	token := mintToken(ps, "sb_1", 8080, time.Hour)

	claims, ok := ps.verify(token)
	if !ok || claims.ID != "sb_1" || claims.Port != 8080 || claims.Owner != "node:9000" {
		t.Fatalf("verify %+v ok=%v", claims, ok)
	}
	if _, ok := ps.verify(token + "x"); ok {
		t.Error("tampered token verified")
	}
	if _, ok := NewPreviewServer("other-secret", "node:9000", &fakePreviewMgr{}).verify(token); ok {
		t.Error("token verified under the wrong secret")
	}
}

func TestPreviewRejectsExpired(t *testing.T) {
	ps := NewPreviewServer("secret", "node:9000", &fakePreviewMgr{})
	token := mintToken(ps, "sb_1", 8080, -time.Second) // already expired
	if _, ok := ps.verify(token); ok {
		t.Error("expired token verified")
	}
}

func TestPreviewProxiesToGuest(t *testing.T) {
	// A real HTTP server stands in for the guest app; PreviewDial hands the
	// proxy a raw conn to it.
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "guest saw "+r.URL.Path)
	}))
	t.Cleanup(guest.Close)
	guestAddr := strings.TrimPrefix(guest.URL, "http://")

	dialed := false
	ps := NewPreviewServer("secret", "node:9000", &fakePreviewMgr{
		dial: func(id string, port uint16) (net.Conn, error) {
			dialed = true
			if id != "sb_1" || port != 8080 {
				t.Errorf("dial %s:%d", id, port)
			}
			return net.Dial("tcp", guestAddr)
		},
	})
	ts := httptest.NewServer(ps.Handler())
	t.Cleanup(ts.Close)

	token := mintToken(ps, "sb_1", 8080, time.Hour)

	resp, err := http.Get(ts.URL + "/p/" + token + "/hello/world")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !dialed || string(body) != "guest saw /hello/world" {
		t.Errorf("body %q dialed=%v", body, dialed)
	}
}

func TestPreviewRevokedWhenDialFails(t *testing.T) {
	// A released sandbox: PreviewDial errors, so the URL 502s — statelessly.
	ps := NewPreviewServer("secret", "node:9000", &fakePreviewMgr{
		dial: func(string, uint16) (net.Conn, error) { return nil, net.ErrClosed },
	})
	ts := httptest.NewServer(ps.Handler())
	t.Cleanup(ts.Close)
	token := mintToken(ps, "sb_gone", 3000, time.Hour)

	resp, err := http.Get(ts.URL + "/p/" + token + "/")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status %d, want 502 for a gone sandbox", resp.StatusCode)
	}
}

func TestPreviewForwardsToOwner(t *testing.T) {
	// A token owned by a different node is proxied to that node verbatim.
	var forwarded bool
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		forwarded = true
		_, _ = io.WriteString(w, "owner served")
	}))
	t.Cleanup(owner.Close)
	ownerAddr := strings.TrimPrefix(owner.URL, "http://")

	// Mint on the owner, serve on a different node.
	ownerPS := NewPreviewServer("secret", ownerAddr, &fakePreviewMgr{})
	token := mintToken(ownerPS, "sb_1", 8080, time.Hour)

	entry := NewPreviewServer("secret", "entry:9000", &fakePreviewMgr{})
	ts := httptest.NewServer(entry.Handler())
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/p/" + token + "/")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if !forwarded {
		t.Error("request not forwarded to the owner node")
	}
}
