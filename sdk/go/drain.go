package sandbox

import (
	"context"
	"net/http"
)

// Drain closes the entry node (root token): new claims/forks/branches are
// refused, live claims run to their leases; poll Info until Claimed is zero.
func (c *Client) Drain(ctx context.Context) (*NodeInfo, error) {
	return c.doJSONPtr[NodeInfo](ctx, http.MethodPost, c.addr, "/v1/drain", nil, c.apiToken, "drain")
}

// Undrain lifts a drain on the entry node (root token).
func (c *Client) Undrain(ctx context.Context) (*NodeInfo, error) {
	return c.doJSONPtr[NodeInfo](ctx, http.MethodDelete, c.addr, "/v1/drain", nil, c.apiToken, "undrain")
}

// Cordon refuses new claims on the entry node across restarts (root token); live claims and moves in stay admitted.
func (c *Client) Cordon(ctx context.Context) (*NodeInfo, error) {
	return c.doJSONPtr[NodeInfo](ctx, http.MethodPost, c.addr, "/v1/cordon", nil, c.apiToken, "cordon")
}

// Uncordon lifts a cordon on the entry node (root token); a drain stays.
func (c *Client) Uncordon(ctx context.Context) (*NodeInfo, error) {
	return c.doJSONPtr[NodeInfo](ctx, http.MethodDelete, c.addr, "/v1/cordon", nil, c.apiToken, "uncordon")
}
