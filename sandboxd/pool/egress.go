package pool

import (
	"context"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// egressListener is one sandbox's none-lane egress accept point: an http.Server
// serving egress.Proxy over the per-sandbox UDS the VMM connects when the guest
// dials CID2:egressPort.
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

// armEgress starts the none-lane egress proxy for a claim whose effective
// policy is non-empty; a no-op otherwise, so no listener means default-deny
// (the guest's proxy dial is refused). Only the none lane is enforced by
// construction — the egress lane needs its own nftables lockdown.
func (m *Manager) armEgress(sb *types.Sandbox) {
	if !m.egressEnabled || sb.Key.Net != types.NetNone || sb.VsockSocket == "" {
		return
	}
	policy, ok := m.effectivePolicy(sb)
	if !ok {
		return
	}
	path := engine.EgressSocketPath(sb.VsockSocket)
	_ = os.Remove(path) // a stale socket from a prior life would block the bind
	ln, err := net.Listen("unix", path)
	if err != nil {
		log.WithFunc("pool.armEgress").Errorf(context.Background(), err, "listen egress %s", sb.ID)
		return
	}
	id, tenant := sb.ID, sb.Tenant
	proxy := egress.New(id, tenant, policy, m.egressSecrets, (&net.Dialer{}).DialContext,
		func(ev egress.Event) { m.recordEgress(context.Background(), id, tenant, ev) })
	el := &egressListener{srv: &http.Server{Handler: proxy, ReadHeaderTimeout: 30 * time.Second}, ln: ln, path: path}
	m.mu.Lock()
	m.egressListeners[id] = el
	m.mu.Unlock()
	go func() { _ = el.srv.Serve(ln) }()
}

// disarmEgress tears down a sandbox's egress accept point; idempotent.
func (m *Manager) disarmEgress(id string) {
	m.mu.Lock()
	el := m.egressListeners[id]
	delete(m.egressListeners, id)
	m.mu.Unlock()
	if el != nil {
		el.close()
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
