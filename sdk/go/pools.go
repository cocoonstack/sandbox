package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
)

// PoolSpec is a desired warm pool for SetPools. Egress is config-owned on the
// node and rejected by the API, so it has no field here.
type PoolSpec struct {
	Template                  string   `json:"template"`
	Net                       NetShape `json:"net,omitempty"`
	Size                      Size     `json:"size,omitempty"`
	Warm                      int      `json:"warm"`
	WarmMax                   int      `json:"warm_max,omitempty"`
	IdleHibernateSeconds      int      `json:"idle_hibernate_seconds,omitempty"`
	ArchiveAfterSeconds       int      `json:"archive_after_seconds,omitempty"`
	ArchiveDeleteAfterSeconds int      `json:"archive_delete_after_seconds,omitempty"`
}

// PoolResult is one node's outcome from SetPoolsCluster.
type PoolResult struct {
	Addr string
	Info *NodeInfo
	Err  error
}

type poolUpdate struct {
	Pools []PoolSpec `json:"pools"`
}

// SetPools replaces the entry node's desired warm pools (PUT /v1/pools): a
// declarative full replace, so omitted pools drain. Requires the operator token.
func (c *Client) SetPools(ctx context.Context, pools []PoolSpec) (*NodeInfo, error) {
	return c.setPoolsAt(ctx, c.addr, pools)
}

// SetPoolsCluster applies pools to the entry node and every peer, returning a
// per-node result; retrying failed nodes is the whole protocol (idempotent
// replace). A non-nil error means peer discovery failed and only the entry node
// was reached — an incomplete apply to retry, not a single-node cluster (nil).
func (c *Client) SetPoolsCluster(ctx context.Context, pools []PoolSpec) ([]PoolResult, error) {
	peers, peersErr := c.peersOrErr(ctx)
	seen := map[string]struct{}{}
	var addrs []string
	for _, a := range append([]string{c.addr}, peers...) {
		if _, dup := seen[a]; dup {
			continue
		}
		seen[a] = struct{}{}
		addrs = append(addrs, a)
	}
	results := make([]PoolResult, len(addrs))
	var wg sync.WaitGroup
	for i, addr := range addrs {
		wg.Go(func() {
			info, err := c.setPoolsAt(ctx, addr, pools)
			results[i] = PoolResult{Addr: addr, Info: info, Err: err}
		})
	}
	wg.Wait()
	if peersErr != nil {
		return results, fmt.Errorf("discover peers: %w", peersErr)
	}
	return results, nil
}

func (c *Client) setPoolsAt(ctx context.Context, addr string, pools []PoolSpec) (*NodeInfo, error) {
	body, err := encodeBody("pools", poolUpdate{Pools: pools})
	if err != nil {
		return nil, err
	}
	info, err := doJSON[NodeInfo](ctx, c, http.MethodPut, addr, "/v1/pools", bytes.NewReader(body), c.apiToken, "pools")
	if err != nil {
		return nil, err
	}
	return &info, nil
}
