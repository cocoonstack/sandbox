package peer

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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

func TestForgetDuringFlightPreventsStaleCache(t *testing.T) {
	release := make(chan struct{})
	inFlight := make(chan struct{})
	probed := sync.OnceFunc(func() { close(inFlight) })
	var hits atomic.Int32
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		probed()
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer owner.Close()

	p := &HTTPProber{Peers: func() []string { return []string{owner.URL} }}
	var owners []string
	done := make(chan struct{})
	go func() { owners = p.Owners(t.Context(), testID); close(done) }()

	<-inFlight
	p.Forget(testID)
	close(release)
	<-done

	if len(owners) != 1 {
		t.Fatalf("owners = %v, want the one that answered", owners)
	}

	p.Owners(t.Context(), testID)
	if got := hits.Load(); got != 2 {
		t.Errorf("owner probed %d times, want 2: a Forget mid-flight must not leave a stale positive cached", got)
	}
}

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

func TestHealOwnersIsExhaustiveDespiteAFastOwner(t *testing.T) {
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer fast.Close()
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
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		time.Sleep(50 * time.Millisecond)
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
