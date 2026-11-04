package peer

import (
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	probeTimeout      = time.Second
	fanOutTimeout     = 2 * time.Second
	maxRedirectOwners = 3 // a redirect decision needs a couple of candidates, not the whole fleet
	maxHealOwners     = 8 // a heal wants the widest source list a few full/corrupt owners can't hide behind
	redirectCacheTTL  = 5 * time.Second

	// probeGrace bounds how long a fan-out waits for more owners once it has its first.
	probeGrace = 150 * time.Millisecond

	// ProbeHeader carries the base64 probe MAC when a ProbeKey is configured.
	ProbeHeader = "X-Cocoon-Probe"

	// probeBucketSeconds is the coarse time step the probe MAC is bound to.
	probeBucketSeconds = 30
)

type redirectCacheEntry struct {
	owners  []string
	expires time.Time
}

// HTTPProber finds which peers hold a checkpoint by probing them on a cross-node miss.
type HTTPProber struct {
	Peers func() []string

	// ProbeKey signs and verifies HEAD probes; empty means an unencrypted mesh.
	ProbeKey []byte

	redirectFlight singleflight.Group

	cacheMu sync.Mutex
	cache   map[string]redirectCacheEntry

	// cacheTTL overrides redirectCacheTTL; a test seam, 5 seconds is slow to wait out.
	cacheTTL time.Duration
}

func (p *HTTPProber) Owners(ctx context.Context, id string) []string {
	if owners, hit := p.cacheLookup(id); hit {
		return owners
	}
	v, _, _ := p.redirectFlight.Do(id, func() (any, error) {
		owners := p.fanOut(ctx, id, maxRedirectOwners, probeGrace)
		p.cachePut(id, owners)
		return owners, nil
	})
	return v.([]string)
}

// HealOwners is Owners' wide twin: a heal wants the largest source list, uncached.
func (p *HTTPProber) HealOwners(ctx context.Context, id string) []string {
	// No grace: a heal must not stop early on a fast owner and hide a slower valid source.
	return p.fanOut(ctx, id, maxHealOwners, 0)
}

func (p *HTTPProber) Forget(id string) {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	delete(p.cache, id)
}

// fanOut probes every peer concurrently, canceling stragglers once maxOwners have answered 200.
func (p *HTTPProber) fanOut(ctx context.Context, id string, maxOwners int, grace time.Duration) []string {
	addrs := slices.Compact(slices.Sorted(slices.Values(p.Peers())))
	if len(addrs) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fanOutTimeout)
	defer cancel()

	wins := make(chan string, len(addrs))
	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Go(func() {
			if probeOwner(ctx, p.ProbeKey, addr, id) {
				wins <- addr
			}
		})
	}
	go func() { wg.Wait(); close(wins) }()

	// Grace opens only after the first win: a genuine miss must hear from every peer.
	var owners []string
	var graceCh <-chan time.Time
	for {
		select {
		case addr, ok := <-wins:
			if !ok {
				return owners
			}
			owners = append(owners, addr)
			if len(owners) >= maxOwners {
				cancel()
				return owners
			}
			if grace > 0 && graceCh == nil {
				graceCh = time.After(grace)
			}
		case <-graceCh:
			cancel()
			return owners
		}
	}
}

func (p *HTTPProber) cacheLookup(id string) ([]string, bool) {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	entry, ok := p.cache[id]
	if !ok || time.Now().After(entry.expires) {
		return nil, false
	}
	return entry.owners, true
}

func (p *HTTPProber) cachePut(id string, owners []string) {
	if len(owners) == 0 {
		return
	}
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	if p.cache == nil {
		p.cache = make(map[string]redirectCacheEntry)
	}
	now := time.Now()
	maps.DeleteFunc(p.cache, func(_ string, e redirectCacheEntry) bool { return now.After(e.expires) })
	p.cache[id] = redirectCacheEntry{owners: owners, expires: now.Add(cmp.Or(p.cacheTTL, redirectCacheTTL))}
}

// DeriveProbeKey derives the probe-MAC key from the mesh cluster_key, separated from gossip.
func DeriveProbeKey(clusterKey []byte) []byte {
	mac := hmac.New(sha256.New, clusterKey)
	mac.Write([]byte("cocoon-checkpoint-probe-v1"))
	return mac.Sum(nil)
}

// SignProbe returns id's current probe MAC keyed by key — the ProbeHeader value.
func SignProbe(key []byte, id string) string {
	return probeMAC(key, id, currentBucket())
}

// VerifyProbeMAC checks sig against id's current time bucket and the one on either side.
func VerifyProbeMAC(key []byte, id, sig string) bool {
	if sig == "" {
		return false
	}
	now := currentBucket()
	for _, bucket := range [3]int64{now - 1, now, now + 1} {
		if hmac.Equal([]byte(sig), []byte(probeMAC(key, id, bucket))) {
			return true
		}
	}
	return false
}

func probeMAC(key []byte, id string, bucket int64) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(bucket)) //nolint:gosec // unix-seconds bucket, positive for any current clock
	mac.Write(buf[:])
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func currentBucket() int64 {
	return time.Now().Unix() / probeBucketSeconds
}

// probeOwner sends a probe MAC when probeKey is set; an empty key falls back to capability-only.
func probeOwner(ctx context.Context, probeKey []byte, addr, id string) bool {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, checkpointURL(addr, id, "/blob"), nil)
	if err != nil {
		return false
	}
	if len(probeKey) > 0 {
		req.Header.Set(ProbeHeader, SignProbe(probeKey, id))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusOK
}
