package sandbox

import (
	"context"
	"net/http"
)

// Drain cordons the entry node (root token): new claims/forks/branches are
// refused, live claims run to their leases; poll Info until Claimed is zero.
func (c *Client) Drain(ctx context.Context) (*NodeInfo, error) {
	return doJSONPtr[NodeInfo](ctx, c, http.MethodPost, c.addr, "/v1/drain", nil, c.apiToken, "drain")
}

// Uncordon lifts a drain on the entry node (root token).
func (c *Client) Uncordon(ctx context.Context) (*NodeInfo, error) {
	return doJSONPtr[NodeInfo](ctx, c, http.MethodDelete, c.addr, "/v1/drain", nil, c.apiToken, "uncordon")
}
