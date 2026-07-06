package sandbox

import (
	"context"
	"net/url"
)

// Template is a promoted template bound to the node that holds it.
// Templates are node-local: on a cluster, name-based Client calls only see
// the connected node's templates, so cross-node lifecycle rides this handle
// — its New and Delete always reach the owning node.
type Template struct {
	Name string

	c    *Client
	addr string
	net  string
	size string
}

// New claims a sandbox cloned from the template, on the template's node.
// Options may set the TTL; the key axes (network lane, size) are the
// template's own and cannot be overridden.
func (t *Template) New(ctx context.Context, opts ...Option) (*Sandbox, error) {
	claim := claimRequest{Template: t.Name}
	for _, opt := range opts {
		opt(&claim)
	}
	// The template only exists under its exact key, and this node holds it:
	// a redirect elsewhere could never find it.
	claim.Net, claim.Size, claim.NoRedirect = t.net, t.size, true
	body, err := encodeClaim(claim)
	if err != nil {
		return nil, err
	}
	cr, err := t.c.claimAt(ctx, t.addr, body)
	if err != nil {
		return nil, err
	}
	return t.c.handleFrom(t.addr, cr), nil
}

// Delete removes the template from its node.
func (t *Template) Delete(ctx context.Context) error {
	u := url.Values{"template": {t.Name}, "net": {t.net}, "size": {t.size}}
	return t.c.deleteTemplates(ctx, t.addr, u)
}
