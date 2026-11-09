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
	var addrs []string
	var hits []*atomic.Int32
	for range 3 {
		srv, n := countingServer(t, http.StatusNoContent)
		addrs, hits = append(addrs, srv.URL), append(hits, n)
	}

	b := &Broadcaster{Peers: func() []string { return addrs }}
	b.Delete(t.Context(), testID)

	got := 0
	for _, n := range hits {
		got += int(n.Load())
	}
	if got != 3 {
		t.Errorf("peers hit %d times, want 3 (one per distinct peer)", got)
	}
}

func TestBroadcastDeleteDedupsRepeatedAddrs(t *testing.T) {
	srv, hits := countingServer(t, http.StatusNoContent)

	b := &Broadcaster{Peers: func() []string { return []string{srv.URL, srv.URL, srv.URL} }}
	b.Delete(t.Context(), testID)

	if got := hits.Load(); got != 1 {
		t.Errorf("server hit %d times, want 1 (dupes deduped)", got)
	}
}

func TestBroadcastDeleteSwallowsFailures(t *testing.T) {
	fail := statusServer(t, http.StatusInternalServerError)
	ok, reached := countingServer(t, http.StatusNoContent)

	b := &Broadcaster{Peers: func() []string { return []string{fail.URL, ok.URL, "127.0.0.1:1"} }}
	b.Delete(t.Context(), testID)
	if got := reached.Load(); got != 1 {
		t.Errorf("healthy peer hit %d times, want 1 (fan-out continues past a failure)", got)
	}
}
