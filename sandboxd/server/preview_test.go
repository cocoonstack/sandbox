package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPreviewTokenRoundTrip(t *testing.T) {
	ps := NewPreviewServer("secret", "https://preview.example.com", "node:7777", &fakePreviewMgr{})
	token := mintToken(ps, "sb_1", 8080, time.Hour)

	claims, ok := ps.verify(token)
	if !ok || claims.ID != "sb_1" || claims.Port != 8080 || claims.Owner != "node:7777" {
		t.Fatalf("verify %+v ok=%v", claims, ok)
	}
	if url := ps.Mint("sb_1", 8080, time.Hour); !strings.HasPrefix(url, "https://preview.example.com/p/") {
		t.Errorf("url %q, want public preview base", url)
	}
	if _, ok := ps.verify(token + "x"); ok {
		t.Error("tampered token verified")
	}
	if _, ok := NewPreviewServer("other-secret", "https://preview.example.com", "node:7777", &fakePreviewMgr{}).verify(token); ok {
		t.Error("token verified under the wrong secret")
	}
}

func TestPreviewRejectsExpired(t *testing.T) {
	ps := NewPreviewServer("secret", "node:9000", "node:7777", &fakePreviewMgr{})
	token := mintToken(ps, "sb_1", 8080, -time.Second)
	if _, ok := ps.verify(token); ok {
		t.Error("expired token verified")
	}
}

func TestPreviewProxiesToGuest(t *testing.T) {
	guestAddr := newGuestServer(t, func(r *http.Request) string { return "guest saw " + r.URL.Path })

	dialed := false
	ps := NewPreviewServer("secret", "node:9000", "node:7777", &fakePreviewMgr{
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
	ps := NewPreviewServer("secret", "node:9000", "node:7777", &fakePreviewMgr{
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

func TestPreviewRechecksClaimForEveryRequest(t *testing.T) {
	guestAddr := newGuestServer(t, func(*http.Request) string { return "guest" })

	var live atomic.Bool
	var dials atomic.Int32
	live.Store(true)
	ps := NewPreviewServer("secret", "node:9000", "node:7777", &fakePreviewMgr{
		dial: func(string, uint16) (net.Conn, error) {
			dials.Add(1)
			if !live.Load() {
				return nil, net.ErrClosed
			}
			return net.Dial("tcp", guestAddr)
		},
	})
	ts := httptest.NewServer(ps.Handler())
	t.Cleanup(ts.Close)
	url := ts.URL + "/p/" + mintToken(ps, "sb_1", 8080, time.Hour) + "/"

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, want 200", resp.StatusCode)
	}

	live.Store(false)
	resp, err = http.Get(url)
	if err != nil {
		t.Fatalf("get after release: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status after release = %d, want 502", resp.StatusCode)
	}
	if got := dials.Load(); got != 2 {
		t.Errorf("PreviewDial calls = %d, want one per request", got)
	}
}

func TestPreviewForwardsToOwner(t *testing.T) {
	guestAddr := newGuestServer(t, func(r *http.Request) string { return "guest saw " + r.URL.Path })

	owner := httptest.NewUnstartedServer(nil)
	ownerAddr := owner.Listener.Addr().String()
	ownerPS := NewPreviewServer("secret", "https://preview.example.com", ownerAddr, &fakePreviewMgr{
		dial: func(string, uint16) (net.Conn, error) { return net.Dial("tcp", guestAddr) },
	})
	ownerSrv := New("", nil, ownerAddr, &fakeManager{}, &fakeDialer{}, nil, nil, nil, ownerPS)
	owner.Config.Handler = ownerSrv.Handler()
	owner.Start()
	t.Cleanup(func() { owner.Close(); ownerSrv.CloseRelays() })

	token := mintToken(ownerPS, "sb_1", 8080, time.Hour)
	entry := NewPreviewServer("secret", "https://preview.example.com", "entry:7777", &fakePreviewMgr{})
	ts := httptest.NewServer(entry.Handler())
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/p/" + token + "/via-owner")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "guest saw /via-owner" {
		t.Errorf("body %q, want request forwarded through owner to guest", body)
	}
}

type fakePreviewMgr struct {
	dial func(id string, port uint16) (net.Conn, error)
}

func (f *fakePreviewMgr) PreviewDial(_ context.Context, id string, port uint16) (net.Conn, error) {
	return f.dial(id, port)
}

func newGuestServer(t *testing.T, body func(r *http.Request) string) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body(r))
	}))
	t.Cleanup(ts.Close)
	return strings.TrimPrefix(ts.URL, "http://")
}

func mintToken(ps *PreviewServer, id string, port uint16, ttl time.Duration) string {
	url := ps.Mint(id, port, ttl)
	return strings.TrimSuffix(url[strings.Index(url, "/p/")+3:], "/")
}
