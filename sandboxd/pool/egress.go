package pool

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// NetRoute reports sb's route as its outbound host sees it.
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

// deniedClaims counts per policy key the live claims v denies for lack of a pool policy.
func (m *Manager) deniedClaims(v *outbound.View) map[types.PoolKey]int {
	denied := map[types.PoolKey]int{}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sb := range m.claimed {
		key := sb.PolicyKey()
		if sb.NoEgress || v.HasPolicy(key) {
			continue
		}
		if _, ok := v.Resolve(sb, m.claimPooledLocked); !ok {
			denied[key]++
		}
	}
	return denied
}

func (m *Manager) warnDeniedClaims(ctx context.Context) {
	v := m.view.Load().out
	if !v.Guarded() {
		return
	}
	denied := m.deniedClaims(v)
	for _, key := range slices.SortedFunc(maps.Keys(denied), comparePoolKeys) {
		log.WithFunc("pool.warnDeniedClaims").Warnf(ctx, "pool %s has no egress policy: its %d live claims reach nothing", key.Template, denied[key])
	}
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

func comparePoolKeys(a, b types.PoolKey) int {
	return strings.Compare(a.Hash(), b.Hash())
}
