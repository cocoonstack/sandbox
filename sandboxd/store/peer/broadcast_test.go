package peer

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestBroadcastDeleteSendsNoForward(t *testing.T) {
	var gotNoForward, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotNoForward = r.URL.Query().Get("no_forward")
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	b := &Broadcaster{Peers: func() []string { return []string{srv.URL} }, Token: "fleet-token"}
	b.Delete(t.Context(), testID)

	if gotNoForward != "1" {
		t.Errorf("no_forward = %q, want 1", gotNoForward)
	}
	if gotAuth != "Bearer fleet-token" {
		t.Errorf("Authorization = %q, want the fleet token", gotAuth)
	}
}

func TestBroadcastDeleteFansOutToEveryPeer(t *testing.T) {
	var hits atomic.Int32
	newSrv := func() *httptest.Server {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusNoContent)
		}))
		t.Cleanup(srv.Close)
		return srv
	}
	addrs := []string{newSrv().URL, newSrv().URL, newSrv().URL}

	b := &Broadcaster{Peers: func() []string { return addrs }}
	b.Delete(t.Context(), testID)

	if got := hits.Load(); got != 3 {
		t.Errorf("peers hit %d times, want 3 (one per distinct peer)", got)
	}
}

func TestBroadcastDeleteDedupsRepeatedAddrs(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	b := &Broadcaster{Peers: func() []string { return []string{srv.URL, srv.URL, srv.URL} }}
	b.Delete(t.Context(), testID)

	if got := hits.Load(); got != 1 {
		t.Errorf("server hit %d times, want 1 (dupes deduped)", got)
	}
}

func TestBroadcastDeleteSwallowsFailures(t *testing.T) {
	fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer fail.Close()
	var reached atomic.Int32
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ok.Close()

	b := &Broadcaster{Peers: func() []string { return []string{fail.URL, ok.URL, "127.0.0.1:1"} }}
	b.Delete(t.Context(), testID)
	if got := reached.Load(); got != 1 {
		t.Errorf("healthy peer hit %d times, want 1 (fan-out continues past a failure)", got)
	}
}

func TestBroadcastDeleteNoPeersNoOp(t *testing.T) {
	b := &Broadcaster{Peers: func() []string { return nil }}
	b.Delete(t.Context(), testID)
}
