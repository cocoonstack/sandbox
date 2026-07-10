package pool

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/netfilter"
	"github.com/cocoonstack/sandbox/sandboxd/types"
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

// armEgress locks the egress-lane NIC then binds the proxy: the lock is
// unconditional and fail-closed, so a lock error aborts the claim and no policy
// still yields a locked NIC with no proxy (default-deny, never a free NIC). Used
// on the claim path where both happen at once; wake/unarchive split the two to
// lock before the guest resumes.
func (m *Manager) armEgress(ctx context.Context, sb *types.Sandbox) error {
	if err := m.lockEgressNIC(ctx, sb.ID, sb.Key.Net, sb.VMName); err != nil {
		return err
	}
	return m.armEgressProxy(ctx, sb)
}

// lockEgressNIC drops the egress-lane VM's NIC egress via nft and records the
// tap for later unlock; a no-op off the egress lane or when guarded egress is
// off. vmName is passed explicitly because an unarchive locks before sb.VMName
// is republished.
func (m *Manager) lockEgressNIC(ctx context.Context, sbID string, lane types.NetShape, vmName string) error {
	if !m.guardedEgress || lane != types.NetEgress {
		return nil
	}
	vms, err := m.eng.List(ctx, vmName)
	if err != nil {
		return fmt.Errorf("list %s: %w", vmName, err)
	}
	for _, vm := range vms {
		if vm.Config.Name != vmName || len(vm.NetworkConfigs) == 0 {
			continue
		}
		tap := vm.NetworkConfigs[0].TAP
		if tap == "" {
			return fmt.Errorf("no tap for %s", vmName)
		}
		if err := netfilter.Lock(tap); err != nil {
			return fmt.Errorf("nft lock %s: %w", tap, err)
		}
		m.mu.Lock()
		m.egressTaps[sbID] = tap
		m.mu.Unlock()
		return nil
	}
	return fmt.Errorf("no NIC config for %s", vmName)
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
	proxy := egress.New(id, tenant, policy, m.egressSecrets, (&net.Dialer{}).DialContext,
		func(ev egress.Event) { m.recordEgress(context.Background(), id, tenant, ev) })
	el := &egressListener{srv: &http.Server{Handler: proxy, ReadHeaderTimeout: 30 * time.Second}, ln: ln, path: path}
	m.mu.Lock()
	m.egressListeners[id] = el
	m.mu.Unlock()
	go func() { _ = el.srv.Serve(ln) }()
	return nil
}

// disarmEgress tears down a sandbox's egress proxy and NIC lock; idempotent.
func (m *Manager) disarmEgress(id string) {
	if !m.guardedEgress {
		return
	}
	m.mu.Lock()
	el := m.egressListeners[id]
	delete(m.egressListeners, id)
	tap := m.egressTaps[id]
	delete(m.egressTaps, id)
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
