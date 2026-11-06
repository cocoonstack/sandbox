package sandbox

import (
	"bytes"
	"context"
	"net/http"
)

// PoolSpec is a desired warm pool for SetPools; egress, warmup, capture_trim, storage, reader, replicate_seconds, egress_upstream_env and tun are config-owned, so it has no field for them.
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

type poolUpdate struct {
	Pools []PoolSpec `json:"pools"`
}

// SetPools replaces the entry node's desired warm pools with the operator token; omitted pools drain.
func (c *Client) SetPools(ctx context.Context, pools []PoolSpec) (*NodeInfo, error) {
	return c.setPoolsAt(ctx, c.addr, pools)
}

// SetPoolsCluster applies pools to every node and returns per-node results; an error means only the entry node was reached.
func (c *Client) SetPoolsCluster(ctx context.Context, pools []PoolSpec) ([]NodeResult, error) {
	return c.eachNode(ctx, func(addr string) (*NodeInfo, error) { return c.setPoolsAt(ctx, addr, pools) })
}

func (c *Client) setPoolsAt(ctx context.Context, addr string, pools []PoolSpec) (*NodeInfo, error) {
	body, err := encodeBody("pools", poolUpdate{Pools: pools})
	if err != nil {
		return nil, err
	}
	return c.doJSONPtr[NodeInfo](ctx, http.MethodPut, addr, "/v1/pools", bytes.NewReader(body), c.apiToken, "pools")
}
