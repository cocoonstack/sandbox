package sandbox

import (
	"context"
	"net/url"
)

// Template is a promoted template bound to the node that holds it. Delete and
// New without volumes dial the owner directly, so they are usable the instant
// Promote returns; a volume claim may follow one placement redirect.
type Template struct {
	Name          string
	ContentDigest string

	c    *Client
	addr string
	net  string
	size string
}

// New claims a sandbox cloned from the template. Options may set the TTL or
// request volumes; the key axes (network lane, size) are the template's own
// and cannot be overridden.
func (t *Template) New(ctx context.Context, opts ...Option) (*Sandbox, error) {
	claim := claimRequest{Template: t.Name}
	for _, opt := range opts {
		opt(&claim)
	}
	if err := claim.rejectPinnedAxes(); err != nil {
		return nil, err
	}
	if err := claim.validateVolumes(); err != nil {
		return nil, err
	}
	claim.Net, claim.Size = t.net, t.size
	if len(claim.Volumes) == 0 {
		claim.NoRedirect = true
		body, err := encodeBody("claim", claim)
		if err != nil {
			return nil, err
		}
		cr, err := t.c.claimAt(ctx, t.addr, body)
		if err != nil {
			return nil, err
		}
		return t.c.handleFrom(t.addr, cr), nil
	}
	addr, cr, err := claimFollow(t.addr, "claim", func(noRedirect, requirePromoted bool) ([]byte, error) {
		claim.NoRedirect, claim.RequirePromoted = noRedirect, requirePromoted
		return encodeBody("claim", claim)
	}, func(addr string, body []byte) (claimResponse, error) {
		return t.c.claimAt(ctx, addr, body)
	})
	if err != nil {
		return nil, err
	}
	return t.c.handleFrom(addr, cr), nil
}

// Delete removes the template from its node. The handle is owner-bound, so
// the delete carries no_redirect: the owner answers for itself (404 once the
// template is gone there) and gossip about same-name templates elsewhere is
// never consulted.
func (t *Template) Delete(ctx context.Context) error {
	u := url.Values{
		templateQueryParam:   {t.Name},
		netQueryParam:        {t.net},
		sizeQueryParam:       {t.size},
		noRedirectQueryParam: {"1"},
	}
	_, err := t.c.deleteTemplates(ctx, t.addr, u)
	return err
}
