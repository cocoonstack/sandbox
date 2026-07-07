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
	"net/url"
	"strings"
	"sync"
	"time"
)

// ClientOption configures Connect.
type ClientOption func(*Client)

// WithAPIToken authenticates node-level calls (claim, info).
func WithAPIToken(token string) ClientOption {
	return func(c *Client) { c.apiToken = token }
}

// Client talks to one sandboxd node.
type Client struct {
	addr     string
	apiToken string
	hc       *http.Client
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
	body, err := encodeClaim(claim)
	if err != nil {
		return nil, err
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
		body, err = encodeClaim(claim)
		if err != nil {
			return nil, err
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
// peers concurrently, and returns a handle bound to whichever node confirms
// ownership first — one hung peer must not stall the whole lookup.
func (c *Client) Lookup(ctx context.Context, id, token string) (*Sandbox, error) {
	if owner, err := c.ownerAt(ctx, c.addr, id, token); err == nil {
		return &Sandbox{ID: id, token: token, c: c, owner: owner}, nil
	}
	peers := c.peers(ctx)
	scatterCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	owners := make(chan string, len(peers))
	var wg sync.WaitGroup
	for _, addr := range peers {
		wg.Go(func() {
			if owner, err := c.ownerAt(scatterCtx, addr, id, token); err == nil {
				owners <- owner
			}
		})
	}
	go func() { wg.Wait(); close(owners) }()
	if owner, ok := <-owners; ok {
		return &Sandbox{ID: id, token: token, c: c, owner: owner}, nil
	}
	return nil, fmt.Errorf("lookup %s: no owner found", id)
}

// DeleteTemplate removes a promoted template by name. When the entry node
// does not hold it but the mesh's gossip names an owner, the delete follows
// the redirect there (one hop); gossip lags a fresh promote by about a tick,
// so right after promoting prefer Template.Delete on the returned handle.
// The options pick the template's key axes exactly as a claim would
// (network lane, size); the same defaults apply.
func (c *Client) DeleteTemplate(ctx context.Context, template string, opts ...Option) error {
	claim := claimRequest{Template: template}
	for _, opt := range opts {
		opt(&claim)
	}
	u := url.Values{"template": {claim.Template}, "net": {claim.Net}, "size": {claim.Size}}
	redirect, err := c.deleteTemplates(ctx, c.addr, u)
	if err != nil || len(redirect) == 0 {
		return err
	}
	// The entry node doesn't hold the template but gossip named its owners.
	// The retry carries no_redirect, mirroring the claim protocol: the owner
	// answers for itself, never a second hop.
	u.Set("no_redirect", "1")
	for _, addr := range redirect {
		if _, err = c.deleteTemplates(ctx, addr, u); err == nil {
			return nil
		}
	}
	return fmt.Errorf("delete template at owner: %w", err)
}

// ownerAt asks one node whether it owns the sandbox, returning its data-plane
// address on success.
func (c *Client) ownerAt(ctx context.Context, addr, id, token string) (string, error) {
	body, err := doJSON[struct {
		OwnerAddr string `json:"owner_addr"`
	}](ctx, c, http.MethodGet, addr, "/v1/sandboxes/"+id+"/owner", nil, token, "owner")
	if err != nil {
		return "", err
	}
	owner := body.OwnerAddr
	if owner == "" {
		owner = addr
	}
	return owner, nil
}

// handleFrom builds a sandbox handle, defaulting the data-plane owner to the
// node that answered when a single-node deployment omits owner_addr.
func (c *Client) handleFrom(dialed string, cr claimResponse) *Sandbox {
	owner := cr.OwnerAddr
	if owner == "" {
		owner = dialed
	}
	return &Sandbox{ID: cr.ID, Deadline: cr.Deadline, FromCheckpoint: cr.FromCheckpoint, c: c, token: cr.Token, owner: owner}
}

func (c *Client) claimAt(ctx context.Context, addr string, body []byte) (claimResponse, error) {
	return doJSON[claimResponse](ctx, c, http.MethodPost, addr, "/v1/claim", bytes.NewReader(body), c.apiToken, "claim")
}

// deleteTemplates issues the template delete against one node; a 200 with a
// redirect list names the owners to retry at.
func (c *Client) deleteTemplates(ctx context.Context, addr string, u url.Values) ([]string, error) {
	resp, err := c.roundTrip(ctx, http.MethodDelete, addr, "/v1/templates?"+u.Encode(), nil, c.apiToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil, nil
	case http.StatusOK:
		var body claimResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return nil, fmt.Errorf("decode delete redirect: %w", err)
		}
		return body.Redirect, nil
	default:
		return nil, apiError("delete template", resp)
	}
}

// doJSON issues one control-plane request and decodes a 200 reply into T;
// any other status maps through apiError under verb. The shared plumbing
// behind every decode-a-reply verb in this file and its siblings.
func doJSON[T any](ctx context.Context, c *Client, method, addr, path string, body io.Reader, bearer, verb string) (T, error) {
	var out T
	resp, err := c.roundTrip(ctx, method, addr, path, body, bearer)
	if err != nil {
		return out, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return out, apiError(verb, resp)
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, fmt.Errorf("decode %s response: %w", verb, err)
	}
	return out, nil
}

// doNoContent is doJSON's reply-less twin: 204 or an apiError.
func doNoContent(ctx context.Context, c *Client, method, addr, path string, body io.Reader, bearer, verb string) error {
	resp, err := c.roundTrip(ctx, method, addr, path, body, bearer)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return apiError(verb, resp)
	}
	return nil
}

// roundTrip issues one control-plane request against addr, attaching bearer
// when non-empty; a non-nil body is JSON.
func (c *Client) roundTrip(ctx context.Context, method, addr, path string, body io.Reader, bearer string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://"+addr+path, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	return c.hc.Do(req) //nolint:gosec // dialing the caller-configured node is the SDK's purpose
}

// encodeClaim marshals a claim body.
func encodeClaim(claim claimRequest) ([]byte, error) {
	body, err := json.Marshal(claim)
	if err != nil {
		return nil, fmt.Errorf("encode claim: %w", err)
	}
	return body, nil
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
	ID             string    `json:"id"`
	Token          string    `json:"token"`
	Deadline       time.Time `json:"deadline"`
	OwnerAddr      string    `json:"owner_addr,omitempty"`
	FromCheckpoint string    `json:"from_checkpoint,omitempty"`
	Redirect       []string  `json:"redirect,omitempty"`
}

type forkRequest struct {
	Token      string `json:"token"`
	Count      int    `json:"count"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

type forkResponse struct {
	Children []claimResponse `json:"children"`
}

type promoteRequest struct {
	Token    string `json:"token"`
	Template string `json:"template"`
}

type promoteResponse struct {
	Key struct {
		Template string `json:"template"`
		Net      string `json:"net"`
		Size     string `json:"size"`
	} `json:"key"`
}

type errorResponse struct {
	Error string `json:"error"`
}
