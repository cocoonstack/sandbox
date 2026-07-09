package pool

import (
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestIdleOnceHibernatesPastThreshold(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, IdleHibernateSeconds: 1})
	sb := mustClaim(t, m, testKey)

	m.idleOnce(t.Context())
	if hibernated(m) != 0 {
		t.Fatal("fresh claim hibernated before its threshold")
	}

	backdate(m, sb, 2*time.Second)
	m.idleOnce(t.Context())
	waitFor(t, func() bool { return hibernated(m) == 1 })

	// Idempotent on an already-hibernated claim.
	m.idleOnce(t.Context())
	waitFor(t, func() bool { return !m.idleSweep.Load() })
	if hibernated(m) != 1 {
		t.Error("second sweep disturbed the hibernated claim")
	}
}

func TestIdleOncePolicyScope(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	m.idleDefault = time.Second // node default must NOT reach pooled keys
	m.idleEnabled = true

	sb := mustClaim(t, m, testKey)
	backdate(m, sb, time.Hour)
	m.idleOnce(t.Context())
	waitFor(t, func() bool { return !m.idleSweep.Load() })
	if hibernated(m) != 0 {
		t.Fatal("pooled key without the policy was idle-hibernated by the node default")
	}

	// An unpooled key (template claim shape) takes the node default.
	unpooled := types.PoolKey{Template: "tpl:v1", Net: types.NetNone, Size: types.SizeSmall}
	sb2, err := m.ClaimProvision(t.Context(), unpooled, time.Hour, "")
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	backdate(m, sb2, time.Hour)
	m.idleOnce(t.Context())
	waitFor(t, func() bool { return hibernated(m) == 1 })
}

func TestActivityStampsBlockIdleSweep(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, IdleHibernateSeconds: 1})
	sb := mustClaim(t, m, testKey)
	backdate(m, sb, 2*time.Second)

	// A data-plane connection refreshes the stamp; the sweep must spare it.
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("WakeAgentSocket: %v", err)
	}
	m.idleOnce(t.Context())
	waitFor(t, func() bool { return !m.idleSweep.Load() })
	if hibernated(m) != 0 {
		t.Fatal("active claim hibernated despite a fresh data-plane stamp")
	}
}

func backdate(m *Manager, sb *types.Sandbox, by time.Duration) {
	m.mu.Lock()
	sb.TouchAt(time.Now().Add(-by))
	m.mu.Unlock()
}

func hibernated(m *Manager) int {
	_, _, n := m.Info()
	return n
}
