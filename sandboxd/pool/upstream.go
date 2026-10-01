package pool

import (
	"cmp"
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// upstreamDirect is the claim value that dials direct even where a pool or tenant names a default upstream.
const upstreamDirect = "direct"

var _ egress.Router = claimRouter{}

// claimRouter sends a claim's egress through its upstream proxy, or direct when none applies.
type claimRouter struct {
	m  *Manager
	sb *types.Sandbox
}

func (r claimRouter) Route() string {
	view := r.m.view.Load()
	if view.upstreamEnv == "" {
		return ""
	}
	r.m.mu.Lock()
	v, set := r.sb.Env.Hidden(view.upstreamEnv)
	r.m.mu.Unlock()
	if set {
		if v == upstreamDirect {
			return ""
		}
		return v
	}
	return cmp.Or(view.tenantUpstream[r.sb.Tenant], view.poolUpstream[r.sb.PolicyKey()])
}

func (r claimRouter) Dial(ctx context.Context, route, network, addr string) (net.Conn, error) {
	m := r.m
	if route == "" {
		return m.dial(ctx, network, addr)
	}
	u, err := egress.ParseUpstream(route)
	if err != nil {
		return nil, err
	}
	if !m.view.Load().upstreamAllow.Allows(u) {
		return nil, fmt.Errorf("egress: upstream %s is not allowed", u.Host)
	}
	target, internal, err := m.upstreamTarget(ctx, addr)
	if err != nil {
		return nil, err
	}
	if internal {
		return m.dial(ctx, network, target.String())
	}
	return egress.DialUpstream(ctx, u, target.String(), new(net.Dialer).DialContext)
}

// upstreamTarget checks every address addr resolves to: a re-admitted internal one dials direct, a blocked one fails, else the first is the tunnel target.
func (m *Manager) upstreamTarget(ctx context.Context, addr string) (netip.AddrPort, bool, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return netip.AddrPort{}, false, err
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return netip.AddrPort{}, false, fmt.Errorf("egress: bad port in %q: %w", addr, err)
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return netip.AddrPort{}, false, err
	}
	var target netip.AddrPort
	for _, ip := range ips {
		ap := netip.AddrPortFrom(ip.Unmap(), uint16(port))
		internal, err := m.destVerdict(ap.Addr(), ap.Port())
		if err != nil {
			return netip.AddrPort{}, false, err
		}
		if internal {
			return ap, true, nil
		}
		if !target.IsValid() {
			target = ap
		}
	}
	if !target.IsValid() {
		return netip.AddrPort{}, false, fmt.Errorf("egress: %s resolves to no address", host)
	}
	return target, false, nil
}

// checkUpstreamEnv admits a claim's upstream entry only host-side, as direct or an allowed upstream URL.
func (m *Manager) checkUpstreamEnv(env types.Env) error {
	view := m.view.Load()
	name := view.upstreamEnv
	if name == "" {
		return nil
	}
	v, ok := env[name]
	if !ok {
		return nil
	}
	if v.InGuest() {
		return fmt.Errorf("%w: %s must be guest: false, it carries the upstream's credentials", ErrBadEnv, name)
	}
	if v.Value == upstreamDirect {
		return nil
	}
	u, err := egress.ParseUpstream(v.Value)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrBadEnv, name, err)
	}
	if !view.upstreamAllow.Allows(u) {
		return fmt.Errorf("%w: %s: upstream %s is not in egress_upstream.allow", ErrBadEnv, name, u.Host)
	}
	return nil
}
