package sandbox

import (
	"context"
	"encoding/json"
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+c.addr+"/v1/info", nil)
	if err != nil {
		return nil, err
	}
	if c.apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiToken)
	}
	resp, err := c.hc.Do(req) //nolint:gosec // dialing the caller-configured node is the SDK's purpose
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError("info", resp)
	}
	var info NodeInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, err
	}
	return &info, nil
}

// peers fetches the cluster's other node addresses, best-effort.
func (c *Client) peers(ctx context.Context) []string {
	ctx, cancel := context.WithTimeout(ctx, peersTimeout)
	defer cancel()
	info, err := c.Info(ctx)
	if err != nil {
		return nil
	}
	return info.Peers
}
