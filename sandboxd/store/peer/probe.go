package peer

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	probeTimeout              = time.Second
	defaultProbeClientTimeout = 2 * time.Second
	maxProbeOwners            = 3
)

// HTTPProber finds which peers hold a checkpoint by probing them directly:
// ownership is stable but unbounded, so it is queried on the rare cross-node
// miss instead of gossiped every second by every node.
type HTTPProber struct {
	// A nil Client uses a default with a 2-second timeout: a probe must never
	// hang on a wedged peer.
	Client *http.Client
	Peers  func() []string
}

// Owners fans a HEAD out to every peer in parallel and returns up to 3
// addresses that answered 200, in no particular order.
func (p *HTTPProber) Owners(ctx context.Context, id string) []string {
	addrs := dedupAddrs(p.Peers())
	if len(addrs) == 0 {
		return nil
	}
	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: defaultProbeClientTimeout}
	}

	var mu sync.Mutex
	var owners []string
	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Go(func() {
			if probeOwner(ctx, client, addr, id) {
				mu.Lock()
				owners = append(owners, addr)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if len(owners) > maxProbeOwners {
		owners = owners[:maxProbeOwners]
	}
	return owners
}

// probeOwner sends no Authorization header: the id itself is the
// unguessable capability, so an unauthenticated HEAD leaks only existence
// to a caller who already holds it.
func probeOwner(ctx context.Context, client *http.Client, addr, id string) bool {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	base := addr
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	u := base + "/v1/checkpoints/" + url.PathEscape(id) + "/blob"
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u, nil)
	if err != nil {
		return false
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
