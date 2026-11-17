package sandbox

import (
	"context"
	"net/http"
)

// Template is a promoted template bound to the node that holds it.
type Template struct {
	Name          string
	ContentDigest string

	c    *Client
	addr string
	net  string
	size string
}

// New claims a sandbox cloned from the template; its key axes cannot be overridden.
func (t *Template) New(ctx context.Context, opts ...Option) (*Sandbox, error) {
	claim := newClaim(t.Name, opts)
	if err := claim.rejectPinnedAxes(); err != nil {
		return nil, err
	}
	claim.Net, claim.Size = t.net, t.size
	noVolumes := len(claim.Volumes) == 0
	addr, cr, err := claimFollow(t.addr, "claim", func(noRedirect, _ bool) ([]byte, error) {
		claim.NoRedirect, claim.RequirePromoted = noRedirect || noVolumes, true
		return encodeBody("claim", claim)
	}, func(addr string, body []byte) (claimResponse, error) {
		return t.c.claimAt(ctx, addr, body)
	})
	if err != nil {
		return nil, err
	}
	return t.c.handleFrom(addr, cr), nil
}

// Delete removes the template from its owning node, never a same-name template elsewhere.
func (t *Template) Delete(ctx context.Context) error {
	u := templateQuery(t.Name, t.net, t.size)
	u.Set(noRedirectQueryParam, "1")
	_, err := t.c.deleteTemplates(ctx, t.addr, u)
	return err
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

// Promote publishes the sandbox's current state as a template; the returned handle is bound to the owning node.
func (s *Sandbox) Promote(ctx context.Context, template string) (*Template, error) {
	body, err := encodeBody("promote", promoteRequest{Token: s.token, Template: template})
	if err != nil {
		return nil, err
	}
	pr, addr, err := s.ownerJSON[promoteResponse](ctx, http.MethodPost, "/promote", body, s.c.apiToken, "promote")
	if err != nil {
		return nil, err
	}
	return &Template{
		Name: pr.Key.Template, ContentDigest: pr.ContentDigest,
		c: s.c, addr: addr, net: pr.Key.Net, size: pr.Key.Size,
	}, nil
}
