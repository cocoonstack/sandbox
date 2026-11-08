package pool

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

// SetEgressLimit replaces a claim's bandwidth cap without waking a hibernated or archived claim; open connections move onto it at once.
func (m *Manager) SetEgressLimit(ctx context.Context, id string, l types.EgressLimit) error {
	sb, ok := m.byID(id)
	if !ok {
		return ErrUnknownSandbox
	}
	m.mu.Lock()
	switch {
	case m.claimed[id] != sb:
		m.mu.Unlock()
		return ErrUnknownSandbox
	case m.pausedForMoveLocked(sb):
		m.mu.Unlock()
		return ErrEvacuating
	}
	prev := sb.EgressLimit
	sb.EgressLimit = l
	rowed := m.rowFollows(sb) || sb.ArchivePending != ""
	js := m.store.set(sb)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.mu.Lock()
		var rb claimSnapshot
		if m.claimed[id] == sb && sb.EgressLimit == l {
			sb.EgressLimit = prev
			rb = m.store.set(sb)
		}
		m.mu.Unlock()
		m.store.recommit(ctx, rb)
		return fmt.Errorf("set egress limit %s: persist claims: %w", id, err)
	}
	m.out.SetEgressLimit(sb)
	if rowed {
		if err := m.rewriteArchiveRow(ctx, sb, false); err != nil {
			return fmt.Errorf("set egress limit %s: %w", id, err)
		}
	}
	return nil
}

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
		if _, ok := v.Resolve(sb); !ok {
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
	for _, key := range slices.SortedFunc(maps.Keys(denied), types.PoolKey.Compare) {
		log.WithFunc("pool.warnDeniedClaims").Warnf(ctx, "pool %s has no egress policy: its %d live claims reach nothing", key.Template, denied[key])
	}
}

func (m *Manager) egressLimit(sb *types.Sandbox) types.EgressLimit {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sb.EgressLimit
}

func (m *Manager) outboundOptions(ca *egress.CA) outbound.Options {
	return outbound.Options{
		Engine:    m.eng,
		View:      func() *outbound.View { return m.view.Load().out },
		Record:    m.recordEgress,
		RecordDNS: m.recordDNS,
		Transfer: func(ctx context.Context, id, tenant string, ev egress.Event, sent, received int64) {
			m.recordUsage(ctx, usage.Event{Event: "egress_bytes", ID: id, Tenant: tenant, Reference: ev.Host, Upstream: ev.Upstream, Sent: sent, Received: received})
		},
		CA:          ca,
		LockNIC:     len(m.cfg.Bridges) > 0,
		RecordEnds:  m.audit != nil,
		NoRxSplice:  m.cfg.NoRxSplice,
		Router:      m.tunRouter,
		Flow:        m.recordTunFlow,
		TunBytes:    m.setTunBytes,
		TunState:    m.tunState,
		EgressLimit: m.egressLimit,
		Fenced:      m.isFenced,
	}
}
