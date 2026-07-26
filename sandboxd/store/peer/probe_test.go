package peer

import (
	"net/http"
	"net/http/httptest"
	"sync"
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

// TestOwnersReturnsPromptlyWithOneOwner: the common case is a single owner, so
// the fan-out must return shortly after that owner answers on the grace window,
// not block on a slow peer's full timeout the way it would waiting for a cap it
// will never reach.
func TestOwnersReturnsPromptlyWithOneOwner(t *testing.T) {
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

	p := &HTTPProber{Peers: func() []string { return []string{hung.URL, ok.URL} }}
	start := time.Now()
	owners := p.Owners(t.Context(), testID)
	if elapsed := time.Since(start); elapsed > probeGrace+500*time.Millisecond {
		t.Errorf("Owners took %v with one owner, want ~grace, not the full probe timeout", elapsed)
	}
	if len(owners) != 1 || owners[0] != ok.URL {
		t.Errorf("owners = %v, want [%s]", owners, ok.URL)
	}
}

// TestForgetDuringFlightPreventsStaleCache: a delete that lands while a probe
// is in flight must keep that probe's result out of the cache — the record it
// names may be the one just deleted, and a cached positive would redirect
// claims to a node that no longer holds it.
func TestForgetDuringFlightPreventsStaleCache(t *testing.T) {
	release := make(chan struct{})
	var hits atomic.Int32
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		<-release // hold the probe open so Forget can land mid-flight
		w.WriteHeader(http.StatusOK)
	}))
	defer owner.Close()

	p := &HTTPProber{Peers: func() []string { return []string{owner.URL} }}
	var owners []string
	done := make(chan struct{})
	go func() { owners = p.Owners(t.Context(), testID); close(done) }()

	for hits.Load() == 0 { // wait until the probe is in flight
		time.Sleep(time.Millisecond)
	}
	p.Forget(testID) // the delete lands while the flight is open
	close(release)
	<-done

	if len(owners) != 1 {
		t.Fatalf("owners = %v, want the one that answered", owners)
	}
	// The result must not have been cached: a second call re-probes.
	p.Owners(t.Context(), testID)
	if got := hits.Load(); got != 2 {
		t.Errorf("owner probed %d times, want 2: a Forget mid-flight must not leave a stale positive cached", got)
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

// TestOwnersCancelsStragglersOnceCapReached: once the redirect cap is met by
// fast peers, a straggler beyond it must not add its own timeout to the result.
func TestOwnersCancelsStragglersOnceCapReached(t *testing.T) {
	var addrs []string
	for range maxRedirectOwners {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		addrs = append(addrs, srv.URL)
	}
	block := make(chan struct{})
	hung := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-block
	}))
	defer hung.Close()
	defer close(block)
	addrs = append(addrs, hung.URL)

	start := time.Now()
	p := &HTTPProber{Peers: func() []string { return addrs }}
	owners := p.Owners(t.Context(), testID)
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("Owners took %v, want fast: the cap was met without the straggler", elapsed)
	}
	if len(owners) != maxRedirectOwners {
		t.Errorf("owners = %v, want %d", owners, maxRedirectOwners)
	}
}

// TestHealOwnersReturnsMoreThanRedirectCap: a heal wants the widest source
// list, not the redirect's couple of candidates.
func TestHealOwnersReturnsMoreThanRedirectCap(t *testing.T) {
	const want = maxRedirectOwners + 2
	var addrs []string
	for range want {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		addrs = append(addrs, srv.URL)
	}

	p := &HTTPProber{Peers: func() []string { return addrs }}
	if owners := p.HealOwners(t.Context(), testID); len(owners) != want {
		t.Errorf("HealOwners = %v (%d), want all %d peers, not capped at the redirect limit", owners, len(owners), want)
	}
}

// TestOwnersCoalescesConcurrentProbes: concurrent redirect probes for one id
// must fan out to each peer once, not once per caller.
func TestOwnersCoalescesConcurrentProbes(t *testing.T) {
	var hits atomic.Int32
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		time.Sleep(50 * time.Millisecond) // widen the window for concurrent callers to overlap
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	p := &HTTPProber{Peers: func() []string { return []string{ok.URL} }}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { p.Owners(t.Context(), testID) })
	}
	wg.Wait()
	if got := hits.Load(); got != 1 {
		t.Errorf("peer hit %d times by 10 concurrent callers, want 1 (coalesced)", got)
	}
}

// TestOwnersCacheCollapsesRepeatedRedirects is the hot-checkpoint-behind-a-
// load-balancer case: a fleet of peers, one of them the owner. Ten
// sequential redirect probes for the same id, well within the TTL, must
// fan out to the fleet once total, not once per call.
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
		defer srv.Close()
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

// TestOwnersCacheExpires: past the TTL, a redirect must re-probe rather than
// serve a result that could be stale forever.
func TestOwnersCacheExpires(t *testing.T) {
	var hits atomic.Int32
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	p := &HTTPProber{Peers: func() []string { return []string{ok.URL} }, cacheTTL: 10 * time.Millisecond}
	p.Owners(t.Context(), testID)
	time.Sleep(20 * time.Millisecond)
	p.Owners(t.Context(), testID)
	if got := hits.Load(); got != 2 {
		t.Errorf("peer hit %d times across two Owners calls spanning the TTL, want 2 (expired)", got)
	}
}

// TestForgetEvictsCachedEntry: after Forget, the next Owners call must
// re-probe rather than serve the (possibly now-stale) cached answer.
func TestForgetEvictsCachedEntry(t *testing.T) {
	var hits atomic.Int32
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	p := &HTTPProber{Peers: func() []string { return []string{ok.URL} }}
	p.Owners(t.Context(), testID)
	p.Forget(testID)
	p.Owners(t.Context(), testID)
	if got := hits.Load(); got != 2 {
		t.Errorf("peer hit %d times across two Owners calls with a Forget between, want 2 (re-probed)", got)
	}
}

// TestForgetOtherIDLeavesCacheAlone: Forget must evict only the named id,
// not the whole cache.
func TestForgetOtherIDLeavesCacheAlone(t *testing.T) {
	var hits atomic.Int32
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	p := &HTTPProber{Peers: func() []string { return []string{ok.URL} }}
	p.Owners(t.Context(), testID)
	p.Forget("ck_ffffffffffffffff")
	p.Owners(t.Context(), testID)
	if got := hits.Load(); got != 1 {
		t.Errorf("peer hit %d times, want 1 (Forget of a different id must not evict testID)", got)
	}
}

// TestVerifyProbeMACRejectsWrongKey: a MAC signed with a different key must
// not verify.
func TestVerifyProbeMACRejectsWrongKey(t *testing.T) {
	key := DeriveProbeKey([]byte("cluster-secret"))
	other := DeriveProbeKey([]byte("different-secret"))
	sig := probeMAC(other, testID, currentBucket())
	if VerifyProbeMAC(key, testID, sig) {
		t.Error("VerifyProbeMAC accepted a MAC signed with a different key")
	}
}

// TestVerifyProbeMACRejectsWrongID: a MAC signed for a different id must not
// verify -- the id itself must be bound into the signature.
func TestVerifyProbeMACRejectsWrongID(t *testing.T) {
	key := DeriveProbeKey([]byte("cluster-secret"))
	sig := probeMAC(key, testID, currentBucket())
	if VerifyProbeMAC(key, "ck_ffffffffffffffff", sig) {
		t.Error("VerifyProbeMAC accepted a MAC signed for a different id")
	}
}

// TestVerifyProbeMACRejectsEmptySignature: a missing header must not verify.
func TestVerifyProbeMACRejectsEmptySignature(t *testing.T) {
	key := DeriveProbeKey([]byte("cluster-secret"))
	if VerifyProbeMAC(key, testID, "") {
		t.Error("VerifyProbeMAC accepted an empty signature")
	}
}

// TestVerifyProbeMACToleratesAdjacentBucket: a signature from the bucket
// just before or after now must still verify, tolerating clock skew.
func TestVerifyProbeMACToleratesAdjacentBucket(t *testing.T) {
	key := DeriveProbeKey([]byte("cluster-secret"))
	for _, delta := range []int64{-1, 1} {
		sig := probeMAC(key, testID, currentBucket()+delta)
		if !VerifyProbeMAC(key, testID, sig) {
			t.Errorf("VerifyProbeMAC rejected bucket delta %d, want tolerated clock skew", delta)
		}
	}
}

// TestVerifyProbeMACRejectsOutsideWindow: a signature two buckets away is
// outside the tolerated skew and must be rejected -- the window is bounded,
// not an unlimited replay allowance.
func TestVerifyProbeMACRejectsOutsideWindow(t *testing.T) {
	key := DeriveProbeKey([]byte("cluster-secret"))
	sig := probeMAC(key, testID, currentBucket()-2)
	if VerifyProbeMAC(key, testID, sig) {
		t.Error("VerifyProbeMAC accepted a bucket two steps away, want rejected")
	}
}
