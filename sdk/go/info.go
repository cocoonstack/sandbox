package sandbox

import (
	"context"
	"net/http"
	"time"
)

// peersTimeout bounds peer discovery so one slow entry node cannot stall the caller.
const peersTimeout = 5 * time.Second

// NodeInfo is one node's operational state from GET /v1/info.
type NodeInfo struct {
	Pools            []PoolStatus `json:"pools"`
	Claimed          int          `json:"claimed"`
	Hibernated       int          `json:"hibernated"`
	Archived         int          `json:"archived"`
	Draining         bool         `json:"draining,omitzero"`
	Peers            []string     `json:"peers,omitempty"`
	AtCapacity       bool         `json:"at_capacity,omitzero"`
	AtCapacityReason string       `json:"at_capacity_reason,omitempty"`
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

// SandboxSummary is one live claim as the scoped index reports it; never a
// token or a host path.
type SandboxSummary struct {
	ID             string    `json:"id"`
	Key            PoolKey   `json:"key"`
	Deadline       time.Time `json:"deadline"`
	ClaimedAt      time.Time `json:"claimed_at,omitzero"`
	Hibernated     bool      `json:"hibernated"`
	Archived       bool      `json:"archived,omitzero"`
	FromCheckpoint string    `json:"from_checkpoint,omitempty"`
	Volumes        []Volume  `json:"volumes,omitempty"`
	ClaimRef       string    `json:"claim_ref,omitempty"`
}

type sandboxListResponse struct {
	Sandboxes []SandboxSummary `json:"sandboxes"`
}

// Info reports the entry node's pools, claims, capacity state, and mesh peers.
func (c *Client) Info(ctx context.Context) (*NodeInfo, error) {
	return doJSONPtr[NodeInfo](ctx, c, http.MethodGet, c.addr, "/v1/info", nil, c.apiToken, "info")
}

// Sandboxes lists the live claims this token may see.
func (c *Client) Sandboxes(ctx context.Context) ([]SandboxSummary, error) {
	reply, err := doJSON[sandboxListResponse](ctx, c, http.MethodGet, c.addr, "/v1/sandboxes", nil, c.apiToken, "list sandboxes")
	if err != nil {
		return nil, err
	}
	return reply.Sandboxes, nil
}

// peersOrErr reads /v1/peers, which a tenant token may call too, surfacing a discovery failure.
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
