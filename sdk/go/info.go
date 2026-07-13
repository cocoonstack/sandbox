package sandbox

import (
	"context"
	"net/http"
	"time"
)

// peersTimeout bounds the best-effort peer discovery inside Lookup so one
// slow entry node cannot stall the scatter.
const peersTimeout = 5 * time.Second

// NodeInfo is one node's operational state from GET /v1/info.
type NodeInfo struct {
	Pools      []PoolStatus `json:"pools"`
	Claimed    int          `json:"claimed"`
	Hibernated int          `json:"hibernated"`
	Archived   int          `json:"archived"`
	Peers      []string     `json:"peers,omitempty"`
}

// PoolKey identifies one warm pool on a node.
type PoolKey struct {
	Template string   `json:"template"`
	Net      NetShape `json:"net"`
	Size     Size     `json:"size"`
}

// PoolStatus reports one warm pool on a node.
type PoolStatus struct {
	Key       PoolKey `json:"key"`
	Warm      int     `json:"warm"`
	Refilling int     `json:"refilling"`
	Target    int     `json:"target"`
	Golden    bool    `json:"golden"`
}

// Info reports the entry node's pools, claim counts, and mesh peers.
func (c *Client) Info(ctx context.Context) (*NodeInfo, error) {
	info, err := doJSON[NodeInfo](ctx, c, http.MethodGet, c.addr, "/v1/info", nil, c.apiToken, "info")
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// peers fetches the cluster's node addresses, best-effort — a discovery
// failure yields nil, harmless for Lookup's read scatter.
func (c *Client) peers(ctx context.Context) []string {
	addrs, _ := c.peersOrErr(ctx)
	return addrs
}

// peersOrErr fetches the cluster's node addresses, surfacing a discovery
// failure. It reads the tenant-accessible /v1/peers, not /v1/info
// (operator-only), so it works under a tenant token.
func (c *Client) peersOrErr(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, peersTimeout)
	defer cancel()
	reply, err := doJSON[struct {
		Peers []string `json:"peers"`
	}](ctx, c, http.MethodGet, c.addr, "/v1/peers", nil, c.apiToken, "peers")
	if err != nil {
		return nil, err
	}
	return reply.Peers, nil
}
