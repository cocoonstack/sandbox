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
// key can take the full boot.
func (c *Client) New(ctx context.Context, template string, opts ...Option) (*Sandbox, error) {
	claim := claimRequest{Template: template}
	for _, opt := range opts {
		opt(&claim)
	}
	body, err := json.Marshal(claim)
	if err != nil {
		return nil, fmt.Errorf("encode claim: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url("/v1/claim"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiToken)
	}
	resp, err := c.hc.Do(req) //nolint:gosec // dialing the caller-configured node is the SDK's purpose
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError("claim", resp)
	}
	var cr claimResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return nil, fmt.Errorf("decode claim response: %w", err)
	}
	return &Sandbox{ID: cr.ID, Deadline: cr.Deadline, c: c, token: cr.Token}, nil
}

func (c *Client) url(path string) string {
	return "http://" + c.addr + path
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
}

type claimResponse struct {
	ID       string    `json:"id"`
	Token    string    `json:"token"`
	Deadline time.Time `json:"deadline"`
}

type errorResponse struct {
	Error string `json:"error"`
}
