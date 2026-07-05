// Package sandbox is the Go SDK for the cocoon sandbox control plane: claim
// a microVM from a sandboxd node, run commands in it over the relayed silkd
// protocol, release it. See cocoon-specs/design/sandbox-control-plane.md.
package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client talks to one sandboxd node.
type Client struct {
	addr     string
	apiToken string
	hc       *http.Client
}

// ClientOption configures Connect.
type ClientOption func(*Client)

// WithAPIToken authenticates node-level calls (claim, info).
func WithAPIToken(token string) ClientOption {
	return func(c *Client) { c.apiToken = token }
}

// Connect returns a client for a sandboxd node. addr accepts a
// comma-separated seed list for forward compatibility; v0 uses the first
// entry.
func Connect(addr string, opts ...ClientOption) (*Client, error) {
	first, _, _ := strings.Cut(addr, ",")
	first = strings.TrimSpace(first)
	if first == "" {
		return nil, fmt.Errorf("empty sandboxd address")
	}
	c := &Client{addr: first, hc: &http.Client{}}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// New claims a sandbox for template. Without options the node serves its
// defaults: the no-network lane and the smallest size tier. New returns when
// the sandbox's silkd is reachable; a warm pool hit is milliseconds, a cold
// key can take the full boot. Against a cluster, a warm miss redirects to a
// peer that holds one, which New follows transparently.
func (c *Client) New(ctx context.Context, template string, opts ...Option) (*Sandbox, error) {
	claim := claimRequest{Template: template}
	for _, opt := range opts {
		opt(&claim)
	}
	body, err := json.Marshal(claim)
	if err != nil {
		return nil, fmt.Errorf("encode claim: %w", err)
	}

	cr, err := c.claimAt(ctx, c.addr, body)
	if err != nil {
		return nil, err
	}
	if len(cr.Redirect) > 0 {
		// Retry at a peer with no_redirect set: it warm-or-provisions and
		// cannot bounce us again. Try each candidate so one dead/stale peer
		// (its addr lingering in a gossip view) doesn't fail the claim.
		claim.NoRedirect = true
		body, err = json.Marshal(claim)
		if err != nil {
			return nil, fmt.Errorf("encode claim: %w", err)
		}
		var lastErr error
		for _, addr := range cr.Redirect {
			target, err := c.claimAt(ctx, addr, body)
			if err != nil {
				lastErr = err
				continue
			}
			return c.handleFrom(addr, target), nil
		}
		return nil, fmt.Errorf("claim: all redirect targets failed: %w", lastErr)
	}
	return c.handleFrom(c.addr, cr), nil
}

// Lookup relocates a sandbox handle whose owner address was lost, given its
// id and token: it asks the entry node, then scatters across the cluster's
// peers, and returns a handle bound to whichever node owns it.
func (c *Client) Lookup(ctx context.Context, id, token string) (*Sandbox, error) {
	if owner, err := c.ownerAt(ctx, c.addr, id, token); err == nil {
		return &Sandbox{ID: id, token: token, c: c, owner: owner}, nil
	}
	for _, addr := range c.peers(ctx) {
		if owner, err := c.ownerAt(ctx, addr, id, token); err == nil {
			return &Sandbox{ID: id, token: token, c: c, owner: owner}, nil
		}
	}
	return nil, fmt.Errorf("lookup %s: no owner found", id)
}

// ownerAt asks one node whether it owns the sandbox, returning its data-plane
// address on success.
func (c *Client) ownerAt(ctx context.Context, addr, id, token string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/v1/sandboxes/"+id+"/owner", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.hc.Do(req) //nolint:gosec // dialing the caller-configured cluster is the SDK's purpose
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", apiError("owner", resp)
	}
	var body struct {
		OwnerAddr string `json:"owner_addr"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	owner := body.OwnerAddr
	if owner == "" {
		owner = addr
	}
	return owner, nil
}

// peers fetches the cluster's other node addresses from the entry node.
func (c *Client) peers(ctx context.Context) []string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+c.addr+"/v1/info", nil)
	if err != nil {
		return nil
	}
	if c.apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiToken)
	}
	resp, err := c.hc.Do(req) //nolint:gosec // dialing the caller-configured node is the SDK's purpose
	if err != nil {
		return nil
	}
	defer func() { _ = resp.Body.Close() }()
	var body struct {
		Peers []string `json:"peers"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return body.Peers
}

// handleFrom builds a sandbox handle, defaulting the data-plane owner to the
// node that answered when a single-node deployment omits owner_addr.
func (c *Client) handleFrom(dialed string, cr claimResponse) *Sandbox {
	owner := cr.OwnerAddr
	if owner == "" {
		owner = dialed
	}
	return &Sandbox{ID: cr.ID, Deadline: cr.Deadline, c: c, token: cr.Token, owner: owner}
}

func (c *Client) claimAt(ctx context.Context, addr string, body []byte) (claimResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/v1/claim", bytes.NewReader(body))
	if err != nil {
		return claimResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiToken)
	}
	resp, err := c.hc.Do(req) //nolint:gosec // dialing the caller-configured node is the SDK's purpose
	if err != nil {
		return claimResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return claimResponse{}, apiError("claim", resp)
	}
	var cr claimResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return claimResponse{}, fmt.Errorf("decode claim response: %w", err)
	}
	return cr, nil
}

// apiError surfaces the server's {"error": ...} body when present.
func apiError(verb string, resp *http.Response) error {
	var er errorResponse
	_ = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&er)
	if er.Error != "" {
		return fmt.Errorf("%s: %s (http %d)", verb, er.Error, resp.StatusCode)
	}
	return fmt.Errorf("%s: http %d", verb, resp.StatusCode)
}

// claimRequest mirrors sandboxd's wire type; duplicated so the SDK stays
// dependency-free — the e2e module guards against drift.
type claimRequest struct {
	Template   string `json:"template"`
	Net        string `json:"net,omitempty"`
	Size       string `json:"size,omitempty"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
	NoRedirect bool   `json:"no_redirect,omitempty"`
}

type claimResponse struct {
	ID        string    `json:"id"`
	Token     string    `json:"token"`
	Deadline  time.Time `json:"deadline"`
	OwnerAddr string    `json:"owner_addr,omitempty"`
	Redirect  []string  `json:"redirect,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}
