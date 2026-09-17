package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/netfilter"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// egressDoorConns bounds one sandbox's open connections per door; the guest's next dial waits in the backlog.
const egressDoorConns = 256

var (
	nat64Range = netip.MustParsePrefix("64:ff9b::/96") // RFC 6052 NAT64; the embedded v4 is checked instead

	// internalRanges lists the IANA special-purpose prefixes IsGlobalUnicast/IsPrivate leave in.
	internalRanges = []netip.Prefix{
		netip.MustParsePrefix("0.0.0.0/8"),       // "this network"
		netip.MustParsePrefix("100.64.0.0/10"),   // RFC 6598 CGNAT; some clouds host metadata here
		netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments
		netip.MustParsePrefix("192.0.2.0/24"),    // TEST-NET-1
		netip.MustParsePrefix("192.88.99.2/32"),  // 6a44-relay anycast
		netip.MustParsePrefix("198.18.0.0/15"),   // benchmarking
		netip.MustParsePrefix("198.51.100.0/24"), // TEST-NET-2
		netip.MustParsePrefix("203.0.113.0/24"),  // TEST-NET-3
		netip.MustParsePrefix("240.0.0.0/4"),     // reserved

		netip.MustParsePrefix("::/96"),          // deprecated IPv4-compatible; embeds a v4 unmapped
		netip.MustParsePrefix("64:ff9b:1::/48"), // RFC 8215 local-use NAT64; embeds a v4
		netip.MustParsePrefix("100::/64"),       // discard-only
		netip.MustParsePrefix("100:0:0:1::/64"), // dummy prefix
		netip.MustParsePrefix("2001::/23"),      // IETF protocol assignments (Teredo, benchmarking, ORCHID)
		netip.MustParsePrefix("2001:db8::/32"),  // documentation
		netip.MustParsePrefix("2002::/16"),      // 6to4; embeds a v4
		netip.MustParsePrefix("3fff::/20"),      // documentation
		netip.MustParsePrefix("5f00::/16"),      // SRv6 SIDs
	}
)

// egressListener is one sandbox's egress accept points over the per-sandbox UDS pair.
type egressListener struct {
	srv       *http.Server
	proxy     *egress.Proxy
	ln        net.Listener
	socks     net.Listener
	path      string
	socksPath string
}

func (e *egressListener) close() {
	if e.srv != nil {
		_ = e.srv.Close()
		e.proxy.Close()
	}
	_ = e.ln.Close()
	_ = os.Remove(e.path)
	if e.socks != nil {
		_ = e.socks.Close()
		_ = os.Remove(e.socksPath)
	}
}

// doorListener caps a door's live connections; the accepted conn stays a *net.UnixConn so splice and half-close keep working.
type doorListener struct {
	*net.UnixListener
	slots chan struct{}
	done  chan struct{}
	once  sync.Once
}

func limitDoor(ln *net.UnixListener) *doorListener {
	return &doorListener{UnixListener: ln, slots: make(chan struct{}, egressDoorConns), done: make(chan struct{})}
}

func (l *doorListener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}
	conn, err := l.AcceptUnix()
	if err != nil {
		<-l.slots
		return nil, err
	}
	return &doorConn{UnixConn: conn, slots: l.slots}, nil
}

func (l *doorListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return l.UnixListener.Close()
}

type doorConn struct {
	*net.UnixConn
	slots chan struct{}
	once  sync.Once
}

func (c *doorConn) Close() error {
	err := c.UnixConn.Close()
	c.once.Do(func() { <-c.slots })
	return err
}

// armEgress locks the egress-lane NIC then binds the proxy: fail-closed, never a free NIC.
func (m *Manager) armEgress(ctx context.Context, sb *types.Sandbox) error {
	if err := m.lockEgressNIC(ctx, sb); err != nil {
		return err
	}
	return m.armEgressProxy(ctx, sb)
}

// lockEgressNIC nft-locks the egress-lane NIC and records the tap for unlock.
func (m *Manager) lockEgressNIC(ctx context.Context, sb *types.Sandbox) error {
	if !m.locksNIC(sb.Key) {
		return nil
	}
	tap := sb.TAP
	if tap == "" {
		var err error
		if tap, err = m.tapOf(ctx, sb.VMName); err != nil {
			return err
		}
	}
	if err := netfilter.Lock(tap); err != nil {
		return fmt.Errorf("nft lock %s: %w", tap, err)
	}
	m.mu.Lock()
	m.egressTaps[sb.ID] = tap
	m.mu.Unlock()
	return nil
}

func (m *Manager) markLane(ctx context.Context, key types.PoolKey, sock string) error {
	lane := m.laneOf(key)
	if lane == "" {
		return nil
	}
	if err := m.eng.MarkLane(ctx, sock, lane); err != nil {
		return fmt.Errorf("mark lane: %w", err)
	}
	return nil
}

// laneOf is the verdict a guest receives; the none lane has no NIC to route through and gets none.
func (m *Manager) laneOf(key types.PoolKey) engine.Lane {
	switch {
	case key.Net != types.NetEgress:
		return ""
	case m.locksNIC(key):
		return engine.LaneRelay
	default:
		return engine.LaneDirect
	}
}

func (m *Manager) locksNIC(key types.PoolKey) bool {
	return m.lockEgress && key.Net == types.NetEgress
}

func (m *Manager) tapOf(ctx context.Context, vmName string) (string, error) {
	vm, ok, err := m.findVM(ctx, vmName)
	if err != nil {
		return "", fmt.Errorf("list %s: %w", vmName, err)
	}
	if !ok {
		return "", fmt.Errorf("no NIC config for %s", vmName)
	}
	if tap := vm.TapDevice(); tap != "" {
		return tap, nil
	}
	return "", fmt.Errorf("no tap for %s", vmName)
}

// armEgressProxy serves the egress proxy when the effective policy permits something, on the doors refill pre-bound where it could.
func (m *Manager) armEgressProxy(ctx context.Context, sb *types.Sandbox) error {
	if !m.guardedEgress || sb.VsockSocket == "" {
		return nil
	}
	policy, ok := m.effectivePolicy(sb)
	if !ok {
		m.closePrebound(sb.VMName)
		return nil
	}
	id, tenant := sb.ID, sb.Tenant
	evCtx := context.WithoutCancel(ctx)
	m.mu.Lock()
	el := m.egressPrebound[sb.VMName]
	delete(m.egressPrebound, sb.VMName)
	intercepts := m.poolEgress[sb.Key].Intercepts()
	m.mu.Unlock()
	// only an intercepting pool pays for the TLS transport and leaf cache
	ca := m.egressCA
	if !intercepts {
		ca = nil
	}
	proxy := egress.New(id, tenant, policy, m.egressSecrets, ca, m.dial,
		func(ev egress.Event) { m.recordEgress(evCtx, id, tenant, ev) }, sb)
	if el != nil && (reached(el.ln) || (el.socks != nil && reached(el.socks))) {
		el.close()
		el = nil
	}
	if el == nil {
		var err error
		if el, err = bindEgress(sb.VsockSocket, policy.ServesSocks()); err != nil {
			return err
		}
	}
	el.srv = &http.Server{Handler: proxy, ReadHeaderTimeout: 30 * time.Second}
	el.proxy = proxy
	m.mu.Lock()
	displaced := m.egressListeners[id]
	m.egressListeners[id] = el
	m.mu.Unlock()
	if displaced != nil {
		displaced.close()
	}
	go func() { _ = el.srv.Serve(el.ln) }()
	if el.socks != nil {
		go proxy.ServeSOCKS(evCtx, el.socks)
	}
	return nil
}

// prebindEgress binds a warm VM's doors at refill, so a claim only has to start serving them.
func (m *Manager) prebindEgress(ctx context.Context, sb *types.Sandbox) {
	m.mu.Lock()
	policy := m.poolEgress[sb.Key]
	m.mu.Unlock()
	if !m.guardedEgress || policy == nil || sb.VsockSocket == "" {
		return
	}
	el, err := bindEgress(sb.VsockSocket, policy.ServesSocks())
	if err != nil {
		log.WithFunc("pool.prebindEgress").Warnf(ctx, "pre-bind %s: %v; the claim binds instead", sb.VMName, err)
		return
	}
	m.mu.Lock()
	m.egressPrebound[sb.VMName] = el
	m.mu.Unlock()
}

// closePrebound drops the doors bound for a warm VM that is going away or will not be served.
func (m *Manager) closePrebound(name string) {
	m.mu.Lock()
	el := m.egressPrebound[name]
	delete(m.egressPrebound, name)
	m.mu.Unlock()
	if el != nil {
		el.close()
	}
}

// disarmIfReleased tears down a wake-path proxy if Release dropped the claim in the arm window.
func (m *Manager) disarmIfReleased(sb *types.Sandbox) bool {
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb
	m.mu.Unlock()
	if !live {
		m.disarmEgress(sb.ID, true)
	}
	return !live
}

// disarmEgress tears down a sandbox's egress listener, and its NIC lock only when removed.
func (m *Manager) disarmEgress(id string, removed bool) {
	if !m.guardedEgress && !m.lockEgress {
		return
	}
	m.mu.Lock()
	el := m.egressListeners[id]
	delete(m.egressListeners, id)
	var tap string
	if removed {
		tap = m.egressTaps[id]
		delete(m.egressTaps, id)
	}
	m.mu.Unlock()
	if el != nil {
		el.close()
	}
	if tap != "" {
		_ = netfilter.Unlock(tap)
	}
}

// poolIntercepts reports whether the configured pool for key HTTPS-intercepts.
func (m *Manager) poolIntercepts(key types.PoolKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.poolEgress[key].Intercepts()
}

// effectivePolicy resolves pool ∩ tenant; root has no tenant layer, an unpooled key no pool one.
func (m *Manager) effectivePolicy(sb *types.Sandbox) (egress.Evaluator, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	poolPol := m.poolEgress[sb.Key]
	tenantPol := m.tenantEgress[sb.Tenant]
	if poolPol == nil {
		if m.pools[sb.Key] != nil || sb.Tenant == "" || tenantPol == nil {
			return nil, false
		}
		return *tenantPol, true
	}
	if sb.Tenant == "" {
		return *poolPol, true
	}
	if tenantPol == nil {
		return nil, false
	}
	return egress.Compose(*poolPol, *tenantPol), true
}

// bindEgress binds the HTTP door and, when the policy serves it, the SOCKS5 door of one VM.
func bindEgress(vsock string, socks bool) (*egressListener, error) {
	el := &egressListener{path: engine.EgressSocketPath(vsock)}
	ln, err := listenUnix(el.path)
	if err != nil {
		return nil, fmt.Errorf("listen egress: %w", err)
	}
	el.ln = limitDoor(ln)
	if socks {
		el.socksPath = engine.SocksSocketPath(vsock)
		if ln, err = listenUnix(el.socksPath); err != nil {
			el.close()
			return nil, fmt.Errorf("listen socks: %w", err)
		}
		el.socks = limitDoor(ln)
	}
	return el, nil
}

// listenUnix binds path after clearing a stale socket from a prior life that would block the bind.
func listenUnix(path string) (*net.UnixListener, error) {
	_ = os.Remove(path)
	return net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
}

// reached reports whether a guest connected to a door before it was armed; only a clean EAGAIN says no.
func reached(ln net.Listener) bool {
	rc, err := ln.(syscall.Conn).SyscallConn()
	if err != nil {
		return true
	}
	waiting := true
	_ = rc.Control(func(fd uintptr) {
		syscall.ForkLock.RLock()
		defer syscall.ForkLock.RUnlock()
		nfd, _, err := syscall.Accept(int(fd)) //nolint:gosec // a descriptor, not arithmetic
		if err == nil {
			_ = syscall.Close(nfd)
			return
		}
		waiting = !errors.Is(err, syscall.EAGAIN)
	})
	return waiting
}

// newEgressDialer blocks internal targets, then re-admits exactly the node-named prefixes.
func newEgressDialer(allow []netip.Prefix) *net.Dialer {
	return &net.Dialer{Control: func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}
		ip, err := netip.ParseAddr(host)
		if err != nil {
			return fmt.Errorf("egress: unresolved address %q: %w", host, err)
		}
		ip = ip.Unmap()
		if nat64Range.Contains(ip) {
			b := ip.As16()
			ip = netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]})
		}
		// after the NAT64 unwrap and before the block, so a named prefix wins
		if slices.ContainsFunc(allow, func(p netip.Prefix) bool { return p.Contains(ip) }) {
			return nil
		}
		if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
			slices.ContainsFunc(internalRanges, func(p netip.Prefix) bool { return p.Contains(ip) }) {
			return fmt.Errorf("egress: blocked internal address %s", ip)
		}
		return nil
	}}
}

// parsePrefixes turns the allow-list into prefixes; config validation already rejected bad ones.
func parsePrefixes(cidrs []string) []netip.Prefix {
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		if p, err := netip.ParsePrefix(c); err == nil {
			out = append(out, p)
		}
	}
	return out
}
