package sandbox

import (
	"bytes"
	"context"
	"net/http"
)

// PoolSpec is a desired warm pool for SetPools; egress, warmup, capture_trim, storage and egress_upstream_env are config-owned, so it has no field for them.
type PoolSpec struct {
	Template                  string   `json:"template"`
	Net                       NetShape `json:"net,omitempty"`
	Size                      Size     `json:"size,omitempty"`
	Warm                      int      `json:"warm"`
	WarmMax                   int      `json:"warm_max,omitzero"`
	IdleHibernateSeconds      int      `json:"idle_hibernate_seconds,omitzero"`
	ArchiveAfterSeconds       int      `json:"archive_after_seconds,omitzero"`
	ArchiveDeleteAfterSeconds int      `json:"archive_delete_after_seconds,omitzero"`
}

// PoolResult is one node's outcome from SetPoolsCluster.
type PoolResult = NodeResult

type poolUpdate struct {
	Pools []PoolSpec `json:"pools"`
}

// SetPools replaces the entry node's desired warm pools (PUT /v1/pools): omitted
// pools drain and lose their golden once idle. Requires the operator token.
func (c *Client) SetPools(ctx context.Context, pools []PoolSpec) (*NodeInfo, error) {
	return c.setPoolsAt(ctx, c.addr, pools)
}

// SetPoolsCluster applies pools to the entry node and every peer, returning a
// per-node result; retrying failed nodes is the whole protocol (idempotent
// replace). A non-nil error means peer discovery failed and only the entry node
// was reached — an incomplete apply to retry, not a single-node cluster (nil).
func (c *Client) SetPoolsCluster(ctx context.Context, pools []PoolSpec) ([]PoolResult, error) {
	return c.eachNode(ctx, func(addr string) (*NodeInfo, error) { return c.setPoolsAt(ctx, addr, pools) })
}

func (c *Client) setPoolsAt(ctx context.Context, addr string, pools []PoolSpec) (*NodeInfo, error) {
	body, err := encodeBody("pools", poolUpdate{Pools: pools})
	if err != nil {
		return nil, err
	}
	return c.doJSONPtr[NodeInfo](ctx, http.MethodPut, addr, "/v1/pools", bytes.NewReader(body), c.apiToken, "pools")
}
