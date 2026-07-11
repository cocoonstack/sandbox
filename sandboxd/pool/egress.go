package pool

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"slices"
	"syscall"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/netfilter"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var (
	cgnatRange = netip.MustParsePrefix("100.64.0.0/10") // RFC 6598; some clouds host metadata here
	nat64Range = netip.MustParsePrefix("64:ff9b::/96")  // RFC 6052; DNS64 embeds an IPv4 in the low 32 bits

	// egressDialer refuses connections to internal addresses so an allow-listed
	// host that resolves (or is rebound) to loopback, link-local (incl. cloud
	// metadata), or a private/CGNAT/sibling address cannot turn the proxy into
	// an SSRF.
	egressDialer = &net.Dialer{Control: func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}
		ip, err := netip.ParseAddr(host)
		if err != nil {
			return fmt.Errorf("egress: unresolved address %q", host)
		}
		ip = ip.Unmap() // fold ::ffff:127.0.0.1 so the v4 checks apply
		if nat64Range.Contains(ip) {
			b := ip.As16() // a DNS64-synthesized metadata/private v4 hides in the low 32 bits
			ip = netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]})
		}
		if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || cgnatRange.Contains(ip) {
			return fmt.Errorf("egress: blocked internal address %s", ip)
		}
		return nil
	}}
)

// egressListener is one sandbox's egress accept point: an http.Server serving
// egress.Proxy over the per-sandbox UDS the VMM connects when the guest dials
// CID2:egressPort.
type egressListener struct {
	srv  *http.Server
	ln   net.Listener
	path string
}

func (e *egressListener) close() {
	_ = e.srv.Close()
	_ = e.ln.Close()
	_ = os.Remove(e.path)
}

// armEgress locks the egress-lane NIC then binds the proxy: fail-closed (a
// lock error aborts the claim), and no policy still yields a locked NIC with
// no proxy — default-deny, never a free NIC.
func (m *Manager) armEgress(ctx context.Context, sb *types.Sandbox) error {
	if err := m.lockEgressNIC(ctx, sb); err != nil {
		return err
	}
	return m.armEgressProxy(ctx, sb)
}

// lockEgressNIC nft-locks the egress-lane NIC and records the tap for unlock;
// the engine lookup is the fallback for claims from pre-tap journals.
func (m *Manager) lockEgressNIC(ctx context.Context, sb *types.Sandbox) error {
	if !m.lockEgress || sb.Key.Net != types.NetEgress {
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

func (m *Manager) tapOf(ctx context.Context, vmName string) (string, error) {
	vms, err := m.eng.List(ctx, vmName)
	if err != nil {
		return "", fmt.Errorf("list %s: %w", vmName, err)
	}
	i := slices.IndexFunc(vms, func(vm types.VMRecord) bool { return vm.Config.Name == vmName })
	if i < 0 {
		return "", fmt.Errorf("no NIC config for %s", vmName)
	}
	if tap := vms[i].TapDevice(); tap != "" {
		return tap, nil
	}
	return "", fmt.Errorf("no tap for %s", vmName)
}

// armEgressProxy binds the vsock egress proxy for a claim whose effective policy
// permits something; a no-op otherwise, so no listener means default-deny (the
// egress-lane NIC stays nft-locked, the none lane's proxy dial is refused).
func (m *Manager) armEgressProxy(_ context.Context, sb *types.Sandbox) error {
	if !m.guardedEgress || sb.VsockSocket == "" {
		return nil
	}
	policy, ok := m.effectivePolicy(sb)
	if !ok {
		return nil
	}
	path := engine.EgressSocketPath(sb.VsockSocket)
	_ = os.Remove(path) // a stale socket from a prior life would block the bind
	ln, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listen egress %s: %w", sb.ID, err)
	}
	id, tenant := sb.ID, sb.Tenant
	proxy := egress.New(id, tenant, policy, m.egressSecrets, egressDialer.DialContext,
		func(ev egress.Event) { m.recordEgress(context.Background(), id, tenant, ev) })
	el := &egressListener{srv: &http.Server{Handler: proxy, ReadHeaderTimeout: 30 * time.Second}, ln: ln, path: path}
	m.mu.Lock()
	m.egressListeners[id] = el
	m.mu.Unlock()
	go func() { _ = el.srv.Serve(ln) }()
	return nil
}

// disarmIfReleased tears down a proxy armed on a wake path if Release dropped
// the claim in the arm window (Release skips the Transition lock); reports gone.
func (m *Manager) disarmIfReleased(sb *types.Sandbox) bool {
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb
	m.mu.Unlock()
	if !live {
		m.disarmEgress(sb.ID, true)
	}
	return !live
}

// disarmEgress tears down a sandbox's egress proxy listener, and its NIC lock
// only when removed is true — a failed VM removal keeps the lock (the guest is
// still running) but still stops the dropped claim's credential injection.
// Idempotent.
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

// effectivePolicy resolves a claim's egress evaluator: pool ∩ tenant (deny
// wins), or whichever single one is set; ok is false when neither applies.
func (m *Manager) effectivePolicy(sb *types.Sandbox) (egress.Evaluator, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var poolPol *egress.Policy
	if p := m.pools[sb.Key]; p != nil {
		poolPol = p.egressPolicy
	}
	tenantPol := m.tenantEgress[sb.Tenant]
	switch {
	case poolPol != nil && tenantPol != nil:
		return egress.Compose(*poolPol, *tenantPol), true
	case poolPol != nil:
		return *poolPol, true
	case tenantPol != nil:
		return *tenantPol, true
	default:
		return nil, false
	}
}
