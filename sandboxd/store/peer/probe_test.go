package peer

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
)

func TestOwnersReturnsOnly200(t *testing.T) {
	ok := statusServer(t, http.StatusOK)
	miss := statusServer(t, http.StatusNotFound)

	p := &HTTPProber{Peers: func() []string { return []string{ok.URL, miss.URL} }}
	owners := p.Owners(t.Context(), testID)
	if len(owners) != 1 || owners[0] != ok.URL {
		t.Errorf("owners = %v, want [%s]", owners, ok.URL)
	}
}

func TestOwnersAllMissIsEmpty(t *testing.T) {
	miss := statusServer(t, http.StatusNotFound)

	p := &HTTPProber{Peers: func() []string { return []string{miss.URL} }}
	if owners := p.Owners(t.Context(), testID); len(owners) != 0 {
		t.Errorf("owners = %v, want none", owners)
	}
}

func TestOwnersReturnsPromptlyWithOneOwner(t *testing.T) {
	hung := hungServer(t)
	ok := statusServer(t, http.StatusOK)

	p := &HTTPProber{Peers: func() []string { return []string{hung.URL, ok.URL} }}
	start := time.Now()
	owners := p.Owners(t.Context(), testID)
	if elapsed := time.Since(start); elapsed >= probeTimeout-100*time.Millisecond {
		t.Errorf("Owners took %v with one owner, want ~grace, not the full probe timeout", elapsed)
	}
	if len(owners) != 1 || owners[0] != ok.URL {
		t.Errorf("owners = %v, want [%s]", owners, ok.URL)
	}
}

func TestOwnersDedupsAddrs(t *testing.T) {
	ok, hits := countingServer(t, http.StatusOK)

	p := &HTTPProber{Peers: func() []string { return []string{ok.URL, ok.URL, ok.URL} }}
	owners := p.Owners(t.Context(), testID)
	if len(owners) != 1 || owners[0] != ok.URL {
		t.Errorf("owners = %v, want a single deduped [%s]", owners, ok.URL)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("server probed %d times, want 1 (dupes deduped before the fan-out)", got)
	}
}

func TestOwnersCapsAtTheRedirectLimit(t *testing.T) {
	var addrs []string
	for range maxRedirectOwners + 1 {
		addrs = append(addrs, statusServer(t, http.StatusOK).URL)
	}

	p := &HTTPProber{Peers: func() []string { return addrs }}
	if owners := p.Owners(t.Context(), testID); len(owners) != maxRedirectOwners {
		t.Errorf("owners = %v, want capped at %d", owners, maxRedirectOwners)
	}
}

func TestOwnersCancelsStragglersOnceCapReached(t *testing.T) {
	var addrs []string
	for range maxRedirectOwners {
		addrs = append(addrs, statusServer(t, http.StatusOK).URL)
	}
	addrs = append(addrs, hungServer(t).URL)

	start := time.Now()
	p := &HTTPProber{Peers: func() []string { return addrs }}
	owners := p.Owners(t.Context(), testID)
	if elapsed := time.Since(start); elapsed >= probeTimeout-100*time.Millisecond {
		t.Errorf("Owners took %v, want fast: the cap was met without the straggler", elapsed)
	}
	if len(owners) != maxRedirectOwners {
		t.Errorf("owners = %v, want %d", owners, maxRedirectOwners)
	}
}

func TestHealOwnersReturnsMoreThanRedirectCap(t *testing.T) {
	const want = maxRedirectOwners + 2
	var addrs []string
	for range want {
		addrs = append(addrs, statusServer(t, http.StatusOK).URL)
	}

	p := &HTTPProber{Peers: func() []string { return addrs }}
	if owners := p.HealOwners(t.Context(), testID); len(owners) != want {
		t.Errorf("HealOwners = %v (%d), want all %d peers, not capped at the redirect limit", owners, len(owners), want)
	}
}

func TestHealOwnersIsExhaustiveDespiteAFastOwner(t *testing.T) {
	fast := statusServer(t, http.StatusOK)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * probeGrace)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()

	p := &HTTPProber{Peers: func() []string { return []string{fast.URL, slow.URL} }}
	if owners := p.HealOwners(t.Context(), testID); len(owners) != 2 {
		t.Errorf("HealOwners = %v, want both owners; the grace window must not truncate a heal", owners)
	}
}

func TestOwnersCoalescesConcurrentProbes(t *testing.T) {
	var hits atomic.Int32
	hold := make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		<-hold
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()
	defer release()

	p := &HTTPProber{Peers: func() []string { return []string{ok.URL} }}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { p.Owners(t.Context(), testID) })
	}
	storetest.WaitStacks(t, 10, func(stack string) bool { return strings.Contains(stack, "singleflight.(*Group).Do(") })
	release()
	wg.Wait()
	if got := hits.Load(); got != 1 {
		t.Errorf("peer hit %d times by 10 concurrent callers, want 1 (coalesced)", got)
	}
}

func TestOwnersCacheCollapsesRepeatedRedirects(t *testing.T) {
	const peers = 5
	var hits atomic.Int32
	var addrs []string
	for i := range peers {
		owns := i == 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			if owns {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}))
		t.Cleanup(srv.Close)
		addrs = append(addrs, srv.URL)
	}

	p := &HTTPProber{Peers: func() []string { return addrs }}
	for range 10 {
		p.Owners(t.Context(), testID)
	}
	if got := hits.Load(); got != peers {
		t.Errorf("peers hit %d times across 10 redirects for one id, want %d (once, not 10x%d)", got, peers, peers)
	}
}

func TestOwnersCacheExpires(t *testing.T) {
	ok, hits := countingServer(t, http.StatusOK)

	p := &HTTPProber{Peers: func() []string { return []string{ok.URL} }, cacheTTL: -time.Second}
	p.Owners(t.Context(), testID)
	p.Owners(t.Context(), testID)
	if got := hits.Load(); got != 2 {
		t.Errorf("peer hit %d times across two Owners calls spanning the TTL, want 2 (expired)", got)
	}
}

func TestForgetEvictsCachedEntry(t *testing.T) {
	ok, hits := countingServer(t, http.StatusOK)

	p := &HTTPProber{Peers: func() []string { return []string{ok.URL} }}
	p.Owners(t.Context(), testID)
	p.Forget(testID)
	p.Owners(t.Context(), testID)
	if got := hits.Load(); got != 2 {
		t.Errorf("peer hit %d times across two Owners calls with a Forget between, want 2 (re-probed)", got)
	}
}

func TestForgetOtherIDLeavesCacheAlone(t *testing.T) {
	ok, hits := countingServer(t, http.StatusOK)

	p := &HTTPProber{Peers: func() []string { return []string{ok.URL} }}
	p.Owners(t.Context(), testID)
	p.Forget("ck_ffffffffffffffff")
	p.Owners(t.Context(), testID)
	if got := hits.Load(); got != 1 {
		t.Errorf("peer hit %d times, want 1 (Forget of a different id must not evict testID)", got)
	}
}

func TestVerifyProbeMACRejectsWrongKey(t *testing.T) {
	key := DeriveProbeKey([]byte("cluster-secret"))
	other := DeriveProbeKey([]byte("different-secret"))
	sig := probeMAC(other, testID, currentBucket())
	if VerifyProbeMAC(key, testID, sig) {
		t.Error("VerifyProbeMAC accepted a MAC signed with a different key")
	}
}

func TestVerifyProbeMACRejectsWrongID(t *testing.T) {
	key := DeriveProbeKey([]byte("cluster-secret"))
	sig := probeMAC(key, testID, currentBucket())
	if VerifyProbeMAC(key, "ck_ffffffffffffffff", sig) {
		t.Error("VerifyProbeMAC accepted a MAC signed for a different id")
	}
}

func TestVerifyProbeMACRejectsEmptySignature(t *testing.T) {
	key := DeriveProbeKey([]byte("cluster-secret"))
	if VerifyProbeMAC(key, testID, "") {
		t.Error("VerifyProbeMAC accepted an empty signature")
	}
}

func TestVerifyProbeMACToleratesAdjacentBucket(t *testing.T) {
	key := DeriveProbeKey([]byte("cluster-secret"))
	for _, delta := range []int64{-1, 1} {
		sig := probeMAC(key, testID, currentBucket()+delta)
		if !VerifyProbeMAC(key, testID, sig) {
			t.Errorf("VerifyProbeMAC rejected bucket delta %d, want tolerated clock skew", delta)
		}
	}
}

func TestVerifyProbeMACRejectsOutsideWindow(t *testing.T) {
	key := DeriveProbeKey([]byte("cluster-secret"))
	sig := probeMAC(key, testID, currentBucket()-2)
	if VerifyProbeMAC(key, testID, sig) {
		t.Error("VerifyProbeMAC accepted a bucket two steps away, want rejected")
	}
}

func statusServer(t *testing.T, code int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }))
	t.Cleanup(srv.Close)
	return srv
}

func countingServer(t *testing.T, code int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(code)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func hungServer(t *testing.T) *httptest.Server {
	t.Helper()
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-block }))
	t.Cleanup(func() { close(block); srv.Close() })
	return srv
}
