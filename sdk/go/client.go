// Package sandbox is the Go SDK for the cocoon sandbox control plane: claim a microVM from a sandboxd node, run commands in it over the relayed silkd protocol, release it.
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

type claimEncoder func(noRedirect, requirePromoted bool) ([]byte, error)

type claimPoster func(addr string, body []byte) (claimResponse, error)

type claimResponse struct {
	ID              string    `json:"id"`
	Token           string    `json:"token"`
	Deadline        time.Time `json:"deadline"`
	OwnerAddr       string    `json:"owner_addr,omitempty"`
	FromCheckpoint  string    `json:"from_checkpoint,omitempty"`
	TemplateDigest  string    `json:"template_digest,omitempty"`
	Volumes         []Volume  `json:"volumes,omitempty"`
	Redirect        []string  `json:"redirect,omitempty"`
	RequirePromoted bool      `json:"require_promoted,omitzero"`
}

type volumeListResponse struct {
	Volumes []VolumeInfo `json:"volumes"`
}

type forkRequest struct {
	Token      string `json:"token"`
	Count      int    `json:"count"`
	TTLSeconds int    `json:"ttl_seconds,omitzero"`
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

// APIError is a non-2xx control-plane reply.
type APIError struct {
	Verb    string
	Status  int
	Message string
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s: %s (http %d)", e.Verb, e.Message, e.Status)
	}
	return fmt.Sprintf("%s: http %d", e.Verb, e.Status)
}

type claimRequest struct {
	Template          string   `json:"template"`
	Net               string   `json:"net,omitempty"`
	Size              string   `json:"size,omitempty"`
	Volumes           []Volume `json:"volumes,omitempty"`
	VolumesAttachOnly bool     `json:"volumes_attach_only,omitzero"`
	TTLSeconds        int      `json:"ttl_seconds,omitzero"`
	NoRedirect        bool     `json:"no_redirect,omitzero"`
	RequirePromoted   bool     `json:"require_promoted,omitzero"`
	ClaimRef          string   `json:"claim_ref,omitempty"`
}

func (r claimRequest) rejectPinnedAxes() error {
	if r.Net != "" || r.Size != "" {
		return fmt.Errorf("network and size are pinned by the snapshot; WithNetwork/WithSize are not accepted here")
	}
	return nil
}

func (r claimRequest) validateVolumes() error {
	for _, v := range r.Volumes {
		if v.Mode != "" && v.Mode != volumeModeRW {
			return fmt.Errorf("volume %q: mode must be \"\", %q, or %q, got %q", v.Name, volumeModeRO, volumeModeRW, v.Mode)
		}
		if r.VolumesAttachOnly && v.Mount != "" {
			return fmt.Errorf("volume %q: mount %q is meaningless with WithVolumesAttachOnly, which leaves mounting to the caller", v.Name, v.Mount)
		}
	}
	return nil
}

// Client talks to one sandboxd node.
type Client struct {
	addr     string
	apiToken string
	hc       *http.Client
}

// Connect returns a client for a sandboxd node.
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

// New claims a sandbox for template.
func (c *Client) New(ctx context.Context, template string, opts ...Option) (*Sandbox, error) {
	claim := claimRequest{Template: template}
	for _, opt := range opts {
		opt(&claim)
	}
	if err := claim.validateVolumes(); err != nil {
		return nil, err
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

// Lookup relocates a sandbox handle whose owner address was lost, given its id and token: it asks the entry node, then scatters across the cluster's peers concurrently, and returns a handle bound to whichever node confirms ownership first — one hung peer must not stall the whole lookup.
func (c *Client) Lookup(ctx context.Context, id, token string) (*Sandbox, error) {
	if owner, err := c.ownerAt(ctx, c.addr, id, token); err == nil {
		return &Sandbox{ID: id, token: token, c: c, owner: owner}, nil
	}
	addrs, _ := c.peersOrErr(ctx)
	owner, ok := scatter(ctx, addrs, func(ctx context.Context, addr string) (string, error) {
		return c.ownerAt(ctx, addr, id, token)
	})
	if !ok {
		return nil, fmt.Errorf("lookup %s: no owner found", id)
	}
	return &Sandbox{ID: id, token: token, c: c, owner: owner}, nil
}

// Attach binds a handle to a known owner without a lookup.
func (c *Client) Attach(ownerAddr, id, token string) *Sandbox {
	return &Sandbox{ID: id, token: token, c: c, owner: ownerAddr}
}

// DeleteTemplate removes a promoted template by name.
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
	u.Set(noRedirectQueryParam, "1")
	if tryErr := tryEach(redirect, func(addr string) error {
		_, retryErr := c.deleteTemplates(ctx, addr, u)
		return retryErr
	}, retryMiss); tryErr != nil {
		return fmt.Errorf("delete template at owner: %w", tryErr)
	}
	return nil
}

func (c *Client) ownerAt(ctx context.Context, addr, id, token string) (string, error) {
	body, err := doJSON[struct {
		OwnerAddr string `json:"owner_addr"`
	}](ctx, c, http.MethodGet, addr, "/v1/sandboxes/"+id+"/owner", nil, token, "owner")
	if err != nil {
		return "", err
	}
	return cmp.Or(body.OwnerAddr, addr), nil
}

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

// WithAPIToken sets the operator bearer for every node-scoped call — claim and info, plus drain, pools, templates, checkpoints, and fork/promote/preview.
func WithAPIToken(token string) ClientOption {
	return func(c *Client) { c.apiToken = token }
}

// WithHTTPClient replaces the control-plane HTTP client, for callers that need their own transport, proxy, or timeout.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) { c.hc = hc }
}

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

func retryMiss(err error) bool {
	he, ok := errors.AsType[*APIError](err)
	return !ok || he.Status == http.StatusNotFound
}

func retryTransient(err error) bool {
	he, ok := errors.AsType[*APIError](err)
	if !ok {
		return true
	}
	switch he.Status {
	case http.StatusUnauthorized, http.StatusNotFound, http.StatusTooManyRequests,
		http.StatusServiceUnavailable, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func claimFollow(origin, verb string, encode claimEncoder, claimAt claimPoster) (string, claimResponse, error) {
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
	var lastErr error
	for _, addr := range candidates {
		cr, err := claimNoRedirect(addr)
		if err == nil {
			return addr, cr, nil
		}
		lastErr = err
	}
	if !retryTransient(lastErr) {
		return "", claimResponse{}, fmt.Errorf("all redirect targets failed: %w", lastErr)
	}
	cr, err := claimNoRedirect(origin)
	if err != nil {
		return "", claimResponse{}, fmt.Errorf("all redirect targets failed, origin fallback failed: %w", errors.Join(lastErr, err))
	}
	return origin, cr, nil
}

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

func encodeBody(verb string, v any) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", verb, err)
	}
	return body, nil
}

func apiError(verb string, resp *http.Response) error {
	var er errorResponse
	_ = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&er)
	_, _ = io.Copy(io.Discard, resp.Body)
	return &APIError{Verb: verb, Status: resp.StatusCode, Message: er.Error}
}
