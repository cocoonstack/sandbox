package peer

import (
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	probeTimeout              = time.Second
	defaultProbeClientTimeout = 2 * time.Second
	maxRedirectOwners         = 3 // a redirect decision needs a couple of candidates, not the whole fleet
	maxHealOwners             = 8 // a heal wants the widest source list a few full/corrupt owners can't hide behind
	redirectCacheTTL          = 5 * time.Second

	// probeGrace bounds how long a fan-out waits for more owners once it has
	// its first: a checkpoint usually lives on one node, and without this the
	// collector would wait out every missing peer's timeout to return it.
	probeGrace = 150 * time.Millisecond

	// ProbeHeader carries the base64 probe MAC when a ProbeKey is configured.
	ProbeHeader = "X-Cocoon-Probe"

	// probeBucketSeconds is the coarse time step the probe MAC is bound to;
	// VerifyProbeMAC accepts the current bucket plus one on either side, so
	// clock skew up to a bucket is tolerated without an unbounded replay window.
	probeBucketSeconds = 30
)

// redirectCacheEntry is Owners' positive-cache record.
type redirectCacheEntry struct {
	owners  []string
	expires time.Time
}

// HTTPProber finds which peers hold a checkpoint by probing them directly:
// ownership is stable but unbounded, so it is queried on the rare cross-node
// miss instead of gossiped every second by every node.
type HTTPProber struct {
	// A nil Client uses a default with a 2-second timeout: a probe must never
	// hang on a wedged peer.
	Client *http.Client
	Peers  func() []string

	// ProbeKey signs and verifies HEAD probes (see DeriveProbeKey); empty
	// means an unencrypted mesh, which keeps the older capability-only
	// posture rather than losing the probe outright.
	ProbeKey []byte

	redirectFlight singleflight.Group
	healFlight     singleflight.Group

	cacheMu sync.Mutex
	cache   map[string]redirectCacheEntry
	// epoch is bumped by every Forget under cacheMu: a fan-out that started
	// before a delete must not write its now-suspect result back into the
	// cache, so cachePut only commits when the epoch it read still holds.
	epoch uint64

	// cacheTTL overrides redirectCacheTTL; a test seam, 5 seconds is slow to wait out.
	cacheTTL time.Duration
}

// Owners fans a HEAD out to every peer and returns up to maxRedirectOwners
// addresses that answered 200. Concurrent calls for one id share a fan-out;
// a short positive cache serves a hot id's repeat redirects. Cache staleness
// fails safely: a redirect to a peer that just deleted the record 404s, and
// the client's no_redirect fallback heals from the origin.
func (p *HTTPProber) Owners(ctx context.Context, id string) []string {
	owners, start, hit := p.cacheLookup(id)
	if hit {
		return owners
	}
	v, _, _ := p.redirectFlight.Do(id, func() (any, error) {
		owners := p.fanOut(ctx, id, maxRedirectOwners, probeGrace)
		p.cachePut(id, owners, start)
		return owners, nil
	})
	return v.([]string)
}

// HealOwners is Owners' wide twin: a heal wants the largest source list, so
// a few full or corrupt owners cannot hide a usable one. Uncached — a heal's
// source list should reflect the fleet now, not a redirect-tuned snapshot.
func (p *HTTPProber) HealOwners(ctx context.Context, id string) []string {
	v, _, _ := p.healFlight.Do(id, func() (any, error) {
		// No grace: a heal wants every owner it can find, so it must not stop
		// early on a fast one and hide a slower valid source behind it.
		return p.fanOut(ctx, id, maxHealOwners, 0), nil
	})
	return v.([]string)
}

// Forget evicts any cached Owners result for id -- called after any delete
// (original or forwarded broadcast) touches id here, since a stale positive
// would redirect claims to a node that no longer holds the record.
func (p *HTTPProber) Forget(id string) {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	p.epoch++
	delete(p.cache, id)
}

// fanOut probes every peer concurrently, canceling stragglers once maxOwners
// have answered 200. The context is detached and rebounded internally:
// singleflight shares one fan-out across callers whose own contexts cancel
// independently — one client disconnecting must not abort another's probe.
func (p *HTTPProber) fanOut(ctx context.Context, id string, maxOwners int, grace time.Duration) []string {
	addrs := dedupAddrs(p.Peers())
	if len(addrs) == 0 {
		return nil
	}
	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: defaultProbeClientTimeout}
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultProbeClientTimeout)
	defer cancel()

	wins := make(chan string, len(addrs))
	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Go(func() {
			if probeOwner(ctx, client, p.ProbeKey, addr, id) {
				wins <- addr
			}
		})
	}
	go func() { wg.Wait(); close(wins) }()

	// grace opens only after the first win: a genuine miss must hear from
	// every peer before concluding nobody holds the record. graceCh stays nil
	// when grace is 0 (a heal wants the exhaustive list), so the select then
	// runs to the cap or until every peer has answered.
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

// cacheLookup returns id's owners if a still-live positive entry exists, and
// always the current epoch: the caller carries it into the fan-out so cachePut
// can tell whether a Forget landed while the probe was in flight.
func (p *HTTPProber) cacheLookup(id string) (owners []string, epoch uint64, hit bool) {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	entry, ok := p.cache[id]
	if !ok || time.Now().After(entry.expires) {
		return nil, p.epoch, false
	}
	return entry.owners, p.epoch, true
}

// cachePut records a positive result, unless a Forget bumped the epoch since
// the fan-out began — that delete may have removed the very record this
// result names. A miss is never cached: a checkpoint appearing moments later
// must not hide behind a stale negative for the TTL.
func (p *HTTPProber) cachePut(id string, owners []string, start uint64) {
	if len(owners) == 0 {
		return
	}
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	if p.epoch != start {
		return
	}
	if p.cache == nil {
		p.cache = make(map[string]redirectCacheEntry)
	}
	now := time.Now()
	for k, e := range p.cache {
		if now.After(e.expires) {
			delete(p.cache, k)
		}
	}
	p.cache[id] = redirectCacheEntry{owners: owners, expires: now.Add(cmp.Or(p.cacheTTL, redirectCacheTTL))}
}

// probeOwner sends a probe MAC when probeKey is set (see DeriveProbeKey);
// an unencrypted mesh (empty probeKey) falls back to the older
// capability-only posture -- the id is unguessable, but nothing else guards it.
func probeOwner(ctx context.Context, client *http.Client, probeKey []byte, addr, id string) bool {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, checkpointURL(addr, id, "/blob"), nil)
	if err != nil {
		return false
	}
	if len(probeKey) > 0 {
		req.Header.Set(ProbeHeader, SignProbe(probeKey, id))
	}
	resp, err := client.Do(req) //nolint:gosec // addr comes from the mesh's own member view
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusOK
}

// dedupAddrs drops repeated addresses so a stale or duplicated peer list
// never probes the same node twice.
func dedupAddrs(addrs []string) []string {
	seen := make(map[string]struct{}, len(addrs))
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if _, ok := seen[a]; ok {
			continue
		}
		seen[a] = struct{}{}
		out = append(out, a)
	}
	return out
}

// DeriveProbeKey derives the probe-MAC key from the mesh's cluster_key via
// one HMAC step — domain separation from the gossip encryption. Call only
// with a non-empty clusterKey.
func DeriveProbeKey(clusterKey []byte) []byte {
	mac := hmac.New(sha256.New, clusterKey)
	mac.Write([]byte("cocoon-checkpoint-probe-v1"))
	return mac.Sum(nil)
}

// SignProbe returns id's current probe MAC keyed by key — the ProbeHeader
// value; exported for callers and tests that build a valid probe request.
func SignProbe(key []byte, id string) string {
	return probeMAC(key, id, currentBucket())
}

// VerifyProbeMAC checks sig against id's current time bucket and the one on
// either side, tolerating clock skew up to one bucket without an unbounded
// replay window.
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

// probeMAC returns the base64 HMAC-SHA256 tag for id in bucket, keyed by key.
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
