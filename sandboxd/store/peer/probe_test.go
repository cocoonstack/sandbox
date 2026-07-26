package peer

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestOwnersReturnsOnly200: a mixed 200/404 answer set redirects only to the
// peer that actually holds the record.
func TestOwnersReturnsOnly200(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()
	miss := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer miss.Close()

	p := &HTTPProber{Peers: func() []string { return []string{ok.URL, miss.URL} }}
	owners := p.Owners(t.Context(), testID)
	if len(owners) != 1 || owners[0] != ok.URL {
		t.Errorf("owners = %v, want [%s]", owners, ok.URL)
	}
}

// TestOwnersAllMissIsEmpty: nobody answering 200 means nobody to redirect to.
func TestOwnersAllMissIsEmpty(t *testing.T) {
	miss := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer miss.Close()

	p := &HTTPProber{Peers: func() []string { return []string{miss.URL} }}
	if owners := p.Owners(t.Context(), testID); len(owners) != 0 {
		t.Errorf("owners = %v, want none", owners)
	}
}

// TestOwnersHungPeerExcludedWithoutBlockingOthers: a wedged peer must not
// stall the whole probe past its own timeout, nor keep a healthy peer's
// answer from coming back.
func TestOwnersHungPeerExcludedWithoutBlockingOthers(t *testing.T) {
	block := make(chan struct{})
	hung := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-block
	}))
	defer hung.Close()
	defer close(block)
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	start := time.Now()
	p := &HTTPProber{Peers: func() []string { return []string{hung.URL, ok.URL} }}
	owners := p.Owners(t.Context(), testID)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Owners took %v, want bounded by the per-probe timeout", elapsed)
	}
	if len(owners) != 1 || owners[0] != ok.URL {
		t.Errorf("owners = %v, want [%s]", owners, ok.URL)
	}
}

// TestOwnersDedupsAddrs: a duplicated peer list must probe each address once.
func TestOwnersDedupsAddrs(t *testing.T) {
	var hits atomic.Int32
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	p := &HTTPProber{Peers: func() []string { return []string{ok.URL, ok.URL, ok.URL} }}
	owners := p.Owners(t.Context(), testID)
	if len(owners) != 1 || owners[0] != ok.URL {
		t.Errorf("owners = %v, want a single deduped [%s]", owners, ok.URL)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("server probed %d times, want 1 (dupes deduped before the fan-out)", got)
	}
}

// TestOwnersCapsAtThree: a redirect answer must stay small even with a large
// fleet behind the record.
func TestOwnersCapsAtThree(t *testing.T) {
	var addrs []string
	for range 4 {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		addrs = append(addrs, srv.URL)
	}

	p := &HTTPProber{Peers: func() []string { return addrs }}
	if owners := p.Owners(t.Context(), testID); len(owners) != 3 {
		t.Errorf("owners = %v, want capped at 3", owners)
	}
}
