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

var _ egress.Evaluator = (*livePolicy)(nil)

// livePolicy is a claim's egress policy that follows a reload: a new view re-resolves its pool and tenant layers once.
type livePolicy struct {
	h   *Host
	sb  *types.Sandbox
	cur atomic.Pointer[resolvedPolicy]
}

func newLivePolicy(h *Host, sb *types.Sandbox, v *View, eval egress.Evaluator) *livePolicy {
	l := &livePolicy{h: h, sb: sb}
	l.cur.Store(&resolvedPolicy{view: v, eval: eval})
	return l
}

func (l *livePolicy) Eval(host, method string, port uint16) (egress.Rule, egress.Decision) {
	return l.current().Eval(host, method, port)
}

func (l *livePolicy) EvalHost(host string, port uint16) (egress.Rule, egress.Decision) {
	return l.current().EvalHost(host, port)
}

func (l *livePolicy) EvalInner(host, method string, port uint16) (egress.Rule, egress.Decision) {
	return l.current().EvalInner(host, method, port)
}

func (l *livePolicy) ServesSocks() bool {
	return l.current().ServesSocks()
}

func (l *livePolicy) InterceptsHost(pattern string) bool {
	return l.current().InterceptsHost(pattern)
}

// current denies everything once a reload leaves the claim with no policy.
func (l *livePolicy) current() egress.Evaluator {
	v := l.h.o.View()
	if r := l.cur.Load(); r.view == v {
		return r.eval
	}
	eval, ok := v.Resolve(l.sb, func() bool { return l.h.o.Pooled(l.sb) })
	if !ok {
		eval = egress.Policy{}
	}
	l.cur.Store(&resolvedPolicy{view: v, eval: eval})
	return eval
}

var _ egress.Secrets = claimSecrets{}

// claimSecrets resolves a rule's secret to the claim's host-only env first, the node's value second.
type claimSecrets struct {
	h  *Host
	sb *types.Sandbox
}

func (c claimSecrets) Header(name string) (header, value string, ok bool) {
	secrets := c.h.o.View().secrets
	if header, value, ok = secrets.Header(name); !ok {
		return "", "", false
	}
	if own, set := c.sb.HiddenEnv(secrets.EnvName(name)); set {
		value = own
	}
	return header, value, true
}

func (c claimSecrets) Credentials(host string) []egress.Credential {
	var out []egress.Credential
	for name, v := range c.sb.Injections(host) {
		out = append(out, egress.Credential{Name: name, Header: v.Inject.Header, Query: v.Inject.Query, Body: v.Inject.Body, Value: v.Value, Placeholder: v.Inject.Placeholder})
	}
	return out
}

var _ egress.Router = claimRouter{}

// claimRouter sends a claim's egress through its upstream proxy, or direct when none applies.
type claimRouter struct {
	h  *Host
	sb *types.Sandbox
}

func (r claimRouter) Route() string {
	view := r.h.o.View()
	if view.upstreamEnv == "" {
		return ""
	}
	if v, set := r.sb.HiddenEnv(view.upstreamEnv); set {
		if v == upstreamDirect {
			return ""
		}
		return v
	}
	return cmp.Or(view.classUpstream[r.sb.EgressClass], view.poolUpstream[r.sb.PolicyKey()])
}

func (r claimRouter) Dial(ctx context.Context, route, network, addr string) (net.Conn, error) {
	h := r.h
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
