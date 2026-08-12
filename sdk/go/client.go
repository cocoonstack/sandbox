// Package sandbox is the Go SDK for the cocoon sandbox control plane: claim
// a microVM from a sandboxd node, run commands in it over the relayed silkd
// protocol, release it.
package sandbox

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	templateQueryParam   = "template"
	netQueryParam        = "net"
	sizeQueryParam       = "size"
	noRedirectQueryParam = "no_redirect"
)

// ClientOption configures Connect.
type ClientOption func(*Client)

// WithAPIToken sets the operator bearer for every node-scoped call — claim and
// info, plus drain, pools, templates, checkpoints, and fork/promote/preview.
func WithAPIToken(token string) ClientOption {
	return func(c *Client) { c.apiToken = token }
}

// Client talks to one sandboxd node.
type Client struct {
	addr     string
	apiToken string
	hc       *http.Client
}

// New claims a sandbox for template. Without options the node serves its
// defaults: the no-network lane and the smallest size tier. New returns when
// the sandbox's silkd is reachable. Against a cluster, a warm miss redirects
// to a peer that holds one, which New follows transparently; if every
// candidate fails transiently (full, mid-heal, unreachable), New falls back
// to the origin once so it provisions or heals locally.
func (c *Client) New(ctx context.Context, template string, opts ...Option) (*Sandbox, error) {
	claim := claimRequest{Template: template}
	for _, opt := range opts {
		opt(&claim)
	}
	addr, cr, err := claimFollow(c.addr, "claim", func(noRedirect, requirePromoted bool) ([]byte, error) {
		claim.NoRedirect, claim.RequirePromoted = noRedirect, requirePromoted
		return encodeBody("claim", claim)
	}, func(a string, body []byte) (claimResponse, error) {
		return c.claimAt(ctx, a, body)
	})
	if err != nil {
		return nil, err
	}
	return c.handleFrom(addr, cr), nil
}

// Volumes lists the caller-visible fleet catalog; availability is local.
func (c *Client) Volumes(ctx context.Context) ([]VolumeInfo, error) {
	resp, err := doJSON[volumeListResponse](ctx, c, http.MethodGet, c.addr, "/v1/volumes", nil, c.apiToken, "list volumes")
	if err != nil {
		return nil, err
	}
	return resp.Volumes, nil
}

// Lookup relocates a sandbox handle whose owner address was lost, given its
// id and token: it asks the entry node, then scatters across the cluster's
// peers concurrently, and returns a handle bound to whichever node confirms
// ownership first — one hung peer must not stall the whole lookup.
func (c *Client) Lookup(ctx context.Context, id, token string) (*Sandbox, error) {
	if owner, err := c.ownerAt(ctx, c.addr, id, token); err == nil {
		return &Sandbox{ID: id, token: token, c: c, owner: owner}, nil
	}
	owner, ok := scatter(ctx, c.peers(ctx), func(ctx context.Context, addr string) (string, error) {
		return c.ownerAt(ctx, addr, id, token)
	})
	if !ok {
		return nil, fmt.Errorf("lookup %s: no owner found", id)
	}
	return &Sandbox{ID: id, token: token, c: c, owner: owner}, nil
}

// Attach binds a handle to an already-claimed sandbox whose owner data-plane
// address is already known (e.g. delivered by the L3 apiserver as annotations),
// with no lookup round-trip. ownerAddr is the node's sandboxd data-plane
// address; token is the per-sandbox credential.
func (c *Client) Attach(ownerAddr, id, token string) *Sandbox {
	return &Sandbox{ID: id, token: token, c: c, owner: ownerAddr}
}

// DeleteTemplate removes a promoted template by name. When the entry node
// does not hold it but the mesh's gossip names an owner, the delete follows
// the redirect there (one hop); gossip lags a fresh promote by about a tick,
// so right after promoting prefer Template.Delete on the returned handle.
func (c *Client) DeleteTemplate(ctx context.Context, template string, opts ...Option) error {
	claim := claimRequest{Template: template}
	for _, opt := range opts {
		opt(&claim)
	}
	u := url.Values{templateQueryParam: {claim.Template}, netQueryParam: {claim.Net}, sizeQueryParam: {claim.Size}}
	redirect, err := c.deleteTemplates(ctx, c.addr, u)
	if err != nil || len(redirect) == 0 {
		return err
	}
	// The entry node doesn't hold the template but gossip named its owners.
	// The retry carries no_redirect, mirroring the claim protocol: the owner
	// answers for itself, never a second hop.
	u.Set(noRedirectQueryParam, "1")
	if tryErr := tryEach(redirect, func(addr string) error {
		_, retryErr := c.deleteTemplates(ctx, addr, u)
		return retryErr
	}, retryMiss); tryErr != nil {
		return fmt.Errorf("delete template at owner: %w", tryErr)
	}
	return nil
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
	return cmp.Or(body.OwnerAddr, addr), nil
}

// handleFrom builds a sandbox handle, defaulting the data-plane owner to the
// node that answered when a single-node deployment omits owner_addr.
func (c *Client) handleFrom(dialed string, cr claimResponse) *Sandbox {
	return &Sandbox{
		ID: cr.ID, Deadline: cr.Deadline, Volumes: cr.Volumes,
		FromCheckpoint: cr.FromCheckpoint, TemplateDigest: cr.TemplateDigest,
		c: c, token: cr.Token, owner: cmp.Or(cr.OwnerAddr, dialed),
	}
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

func doJSONPtr[T any](ctx context.Context, c *Client, method, addr, path string, body io.Reader, bearer, verb string) (*T, error) {
	out, err := doJSON[T](ctx, c, method, addr, path, body, bearer, verb)
	if err != nil {
		return nil, err
	}
	return &out, nil
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

// tryEach calls call against each candidate in turn, stopping at the first
// success. An error for which retry is true moves on to the next candidate
// and the last such error propagates; any other error returns at once. The
// per-verb retry policy mirrors the Python SDK's _try_each.
func tryEach(candidates []string, call func(addr string) error, retry func(error) bool) error {
	var lastErr error
	for _, addr := range candidates {
		lastErr = call(addr)
		if lastErr == nil {
			return nil
		}
		if !retry(lastErr) {
			return lastErr
		}
	}
	return lastErr
}

// retryMiss retries a miss (the next candidate may own the record) or a
// transport failure (dead peer); a served error is real and stops the walk.
func retryMiss(err error) bool {
	var he *httpError
	return !errors.As(err, &he) || he.status == http.StatusNotFound
}

// retryAny retries a redirect candidate's failure unconditionally: one
// candidate being wrong for the claim (no egress, a stale name) must not
// stop the walk from reaching a candidate that would still succeed.
func retryAny(error) bool { return true }

// retryTransient reports whether an origin-fallback is worth the round-trip:
// true for a transport failure, a miss (404), full (429), mid-heal (503), an
// engine/proxy failure (500/502/504), or a mid-rotation 401 (the origin
// proved the token valid by issuing the redirect). A served 4xx like a bad
// request, a forbidden token, or an egress conflict is definitive: the
// origin would fail the same way.
func retryTransient(err error) bool {
	var he *httpError
	if !errors.As(err, &he) {
		return true
	}
	switch he.status {
	case http.StatusUnauthorized, http.StatusNotFound, http.StatusTooManyRequests,
		http.StatusServiceUnavailable, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// claimFollow runs the claim protocol from origin: claim there, and on a
// redirect re-encode with no_redirect and follow via redirectFallback. Only
// the fallback error carries the verb — first-contact errors return raw.
func claimFollow(origin, verb string, encode func(noRedirect, requirePromoted bool) ([]byte, error), claimAt func(addr string, body []byte) (claimResponse, error)) (string, claimResponse, error) {
	body, err := encode(false, false)
	if err != nil {
		return "", claimResponse{}, err
	}
	cr, err := claimAt(origin, body)
	if err != nil {
		return "", claimResponse{}, err
	}
	if len(cr.Redirect) == 0 {
		return origin, cr, nil
	}
	if body, err = encode(true, cr.RequirePromoted); err != nil {
		return "", claimResponse{}, err
	}
	addr, target, err := redirectFallback(origin, cr.Redirect, func(a string) (claimResponse, error) {
		return claimAt(a, body)
	})
	if err != nil {
		return "", claimResponse{}, fmt.Errorf("%s: %w", verb, err)
	}
	return addr, target, nil
}

// redirectFallback walks candidates via claimAt, retrying broadly (retryAny).
// If every candidate is exhausted and the last failure was transient
// (retryTransient), it gives origin one more no_redirect attempt — the node
// that issued the redirect provisions or heals locally instead of leaving
// the claim stuck on stale gossip. A definitive last failure (a bad request,
// an auth rejection, a conflict) skips the fallback: origin would fail the
// same way. A second-level redirect (a compliant server never sends one
// once no_redirect is set) fails the candidate rather than being followed.
func redirectFallback(origin string, candidates []string, claimAt func(addr string) (claimResponse, error)) (string, claimResponse, error) {
	claimNoRedirect := func(target string) (claimResponse, error) {
		cr, err := claimAt(target)
		if err != nil {
			return claimResponse{}, err
		}
		if len(cr.Redirect) > 0 {
			return claimResponse{}, fmt.Errorf("%s redirected again despite no_redirect", target)
		}
		return cr, nil
	}

	var won string
	var cr claimResponse
	tryErr := tryEach(candidates, func(addr string) error {
		target, err := claimNoRedirect(addr)
		if err != nil {
			return err
		}
		won, cr = addr, target
		return nil
	}, retryAny)
	if tryErr == nil {
		return won, cr, nil
	}
	if !retryTransient(tryErr) {
		return "", claimResponse{}, fmt.Errorf("all redirect targets failed: %w", tryErr)
	}
	cr, err := claimNoRedirect(origin)
	if err != nil {
		// Both halves matter to whoever reads this: the peers' failures say why
		// the claim left the origin, the origin's says why coming back did not help.
		return "", claimResponse{}, fmt.Errorf("all redirect targets failed, origin fallback failed: %w", errors.Join(tryErr, err))
	}
	return origin, cr, nil
}

// scatter probes addrs concurrently, returning the first success and
// canceling the losers — one hung peer must not stall the caller. When
// every probe fails (or addrs is empty) ok is false.
func scatter[T any](ctx context.Context, addrs []string, probe func(ctx context.Context, addr string) (T, error)) (result T, ok bool) {
	scatterCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wins := make(chan T, len(addrs))
	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Go(func() {
			if v, probeErr := probe(scatterCtx, addr); probeErr == nil {
				wins <- v
			}
		})
	}
	go func() { wg.Wait(); close(wins) }()
	result, ok = <-wins
	return result, ok
}

// encodeBody marshals a wire request body, wrapping failures under verb —
// the shared prelude of every POST-a-JSON-body call in this package.
func encodeBody(verb string, v any) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", verb, err)
	}
	return body, nil
}

// httpError is a non-2xx control-plane reply; redirect walks branch on the
// status (retryMiss).
type httpError struct {
	verb   string
	status int
	msg    string
}

func (e *httpError) Error() string {
	if e.msg != "" {
		return fmt.Sprintf("%s: %s (http %d)", e.verb, e.msg, e.status)
	}
	return fmt.Sprintf("%s: http %d", e.verb, e.status)
}

// apiError surfaces the server's {"error": ...} body when present.
func apiError(verb string, resp *http.Response) error {
	var er errorResponse
	_ = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&er)
	return &httpError{verb: verb, status: resp.StatusCode, msg: er.Error}
}

// claimRequest mirrors sandboxd's wire type; duplicated so the SDK stays
// dependency-free — the e2e module guards against drift.
type claimRequest struct {
	Template        string   `json:"template"`
	Net             string   `json:"net,omitempty"`
	Size            string   `json:"size,omitempty"`
	Volumes         []Volume `json:"volumes,omitempty"`
	TTLSeconds      int      `json:"ttl_seconds,omitempty"`
	NoRedirect      bool     `json:"no_redirect,omitempty"`
	RequirePromoted bool     `json:"require_promoted,omitempty"`
	Workspace       string   `json:"workspace,omitempty"`
}

// rejectPinnedAxes fails a snapshot claim (checkpoint, template) that passed
// WithNetwork/WithSize — those axes are pinned and would silently no-op.
func (r claimRequest) rejectPinnedAxes() error {
	if r.Net != "" || r.Size != "" {
		return fmt.Errorf("network and size are pinned by the snapshot; WithNetwork/WithSize are not accepted here")
	}
	return nil
}

type claimResponse struct {
	ID              string    `json:"id"`
	Token           string    `json:"token"`
	Deadline        time.Time `json:"deadline"`
	OwnerAddr       string    `json:"owner_addr,omitempty"`
	FromCheckpoint  string    `json:"from_checkpoint,omitempty"`
	TemplateDigest  string    `json:"template_digest,omitempty"`
	Volumes         []Volume  `json:"volumes,omitempty"`
	Redirect        []string  `json:"redirect,omitempty"`
	RequirePromoted bool      `json:"require_promoted,omitempty"`
}

type volumeListResponse struct {
	Volumes []VolumeInfo `json:"volumes"`
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
	ContentDigest string `json:"content_digest"`
}

type errorResponse struct {
	Error string `json:"error"`
}
