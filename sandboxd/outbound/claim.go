package outbound

import (
	"cmp"
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// upstreamDirect is the claim value that dials direct even where a pool or egress class names a default upstream.
const upstreamDirect = "direct"

type resolvedPolicy struct {
	view *View
	eval egress.Evaluator
}

var (
	_ egress.Evaluator = (*claim)(nil)
	_ egress.Secrets   = (*claim)(nil)
	_ egress.Router    = (*claim)(nil)
)

// claim is one sandbox's side of its proxy: a policy that follows a reload, its secrets, and its upstream route.
type claim struct {
	h   *Host
	sb  *types.Sandbox
	cur atomic.Pointer[resolvedPolicy]
}

func newClaim(h *Host, sb *types.Sandbox, v *View, eval egress.Evaluator) *claim {
	c := &claim{h: h, sb: sb}
	c.cur.Store(&resolvedPolicy{view: v, eval: eval})
	return c
}

func (c *claim) Eval(host, method string, port uint16) (egress.Rule, egress.Decision) {
	return c.current().Eval(host, method, port)
}

func (c *claim) EvalHost(host string, port uint16) (egress.Rule, egress.Decision) {
	return c.current().EvalHost(host, port)
}

func (c *claim) EvalInner(host, method string, port uint16) (egress.Rule, egress.Decision) {
	return c.current().EvalInner(host, method, port)
}

func (c *claim) ServesSocks() bool {
	return c.current().ServesSocks()
}

func (c *claim) InterceptsHost(pattern string) bool {
	return c.current().InterceptsHost(pattern)
}

func (c *claim) Header(name string) (header, value string, ok bool) {
	secrets := c.h.o.View().secrets
	if header, value, ok = secrets.Header(name); !ok {
		return "", "", false
	}
	if own, set := c.sb.HiddenEnv(secrets.EnvName(name)); set {
		value = own
	}
	return header, value, true
}

func (c *claim) Credentials(host string) []egress.Credential {
	var out []egress.Credential
	for name, v := range c.sb.Injections(host) {
		out = append(out, egress.Credential{Name: name, Header: v.Inject.Header, Query: v.Inject.Query, Body: v.Inject.Body, Value: v.Value, Placeholder: v.Inject.Placeholder})
	}
	return out
}

func (c *claim) Route() string {
	view := c.h.o.View()
	if view.upstreamEnv == "" {
		return ""
	}
	if v, set := c.sb.HiddenEnv(view.upstreamEnv); set {
		if v == upstreamDirect {
			return ""
		}
		return v
	}
	return cmp.Or(view.classUpstream[c.sb.EgressClass], view.poolUpstream[c.sb.PolicyKey()])
}

func (c *claim) Dial(ctx context.Context, route, network, addr string) (net.Conn, error) {
	h := c.h
	if route == "" {
		return h.o.Dial(ctx, network, addr)
	}
	u, err := egress.ParseUpstream(route)
	if err != nil {
		return nil, err
	}
	if !h.o.View().upstreamAllow.Allows(u) {
		return nil, fmt.Errorf("egress: upstream %s is not allowed", u.Host)
	}
	target, internal, err := h.upstreamTarget(ctx, addr)
	if err != nil {
		return nil, err
	}
	if internal {
		return h.o.Dial(ctx, network, target.String())
	}
	return egress.DialUpstream(ctx, u, target.String(), new(net.Dialer).DialContext)
}

// current denies everything once a reload leaves the claim with no policy.
func (c *claim) current() egress.Evaluator {
	v := c.h.o.View()
	if r := c.cur.Load(); r.view == v {
		return r.eval
	}
	eval, ok := v.Resolve(c.sb, c.h.o.Pooled)
	if !ok {
		eval = egress.Policy{}
	}
	c.cur.Store(&resolvedPolicy{view: v, eval: eval})
	return eval
}
