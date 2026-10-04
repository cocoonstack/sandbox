package pool

import (
	"context"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// NetRoute is how sb's guest reaches the network now: its own NIC, the proxy behind a bound door, or nothing.
func (m *Manager) NetRoute(sb *types.Sandbox) types.NetRoute {
	return m.out.Route(sb)
}

// disarmIfReleased tears down a wake-path proxy if Release dropped the claim in the arm window.
func (m *Manager) disarmIfReleased(sb *types.Sandbox) bool {
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb
	m.mu.Unlock()
	if !live {
		m.out.Disarm(sb.ID, true)
	}
	return !live
}

// policyOf resolves sb's effective egress policy against v; it takes m.mu only for a claim from before layers were recorded.
func (m *Manager) policyOf(v *configView, sb *types.Sandbox) (egress.Evaluator, bool) {
	return v.out.Resolve(sb, func() bool { return m.claimPooled(sb) })
}

func (m *Manager) policyOfLocked(v *configView, sb *types.Sandbox) (egress.Evaluator, bool) {
	return v.out.Resolve(sb, func() bool { return m.claimPooledLocked(sb) })
}

func (m *Manager) claimPooled(sb *types.Sandbox) bool {
	if sb.Layer != "" {
		return sb.Layer == types.LayerPooled
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.claimPooledLocked(sb)
}

func (m *Manager) claimPooledLocked(sb *types.Sandbox) bool {
	if sb.Layer != "" {
		return sb.Layer == types.LayerPooled
	}
	_, ok := m.activePool(sb.PolicyKey())
	return ok
}

func (m *Manager) outboundOptions(ca *egress.CA) outbound.Options {
	return outbound.Options{
		Engine: m.eng,
		View:   func() *outbound.View { return m.view.Load().out },
		Pooled: m.claimPooled,
		Record: m.recordEgress,
		Transfer: func(ctx context.Context, id, tenant string, ev egress.Event, sent, received int64) {
			m.recordUsage(ctx, usageEvent{Event: "egress_bytes", ID: id, Tenant: tenant, Reference: ev.Host, Upstream: ev.Upstream, Sent: sent, Received: received})
		},
		CA:      ca,
		LockNIC: len(m.cfg.Bridges) > 0,
	}
}
