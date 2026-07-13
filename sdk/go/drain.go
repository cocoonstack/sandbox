package sandbox

import (
	"context"
	"net/http"
)

// Drain cordons the entry node for maintenance (root token): new claims,
// forks, and branches are refused while live claims run to their leases.
// Answers the fresh node info — poll Info until Claimed reaches zero.
func (c *Client) Drain(ctx context.Context) (*NodeInfo, error) {
	info, err := doJSON[NodeInfo](ctx, c, http.MethodPost, c.addr, "/v1/drain", nil, c.apiToken, "drain")
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// Uncordon lifts a drain on the entry node (root token).
func (c *Client) Uncordon(ctx context.Context) (*NodeInfo, error) {
	info, err := doJSON[NodeInfo](ctx, c, http.MethodDelete, c.addr, "/v1/drain", nil, c.apiToken, "uncordon")
	if err != nil {
		return nil, err
	}
	return &info, nil
}
