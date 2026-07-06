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
	if sb.HibernateSnap != "" {
		t.Fatal("fresh claim hibernated before its threshold")
	}

	m.mu.Lock()
	sb.LastActivity = time.Now().Add(-2 * time.Second)
	m.mu.Unlock()
	m.idleOnce(t.Context())
	if sb.HibernateSnap == "" {
		t.Fatal("idle claim not hibernated")
	}

	// Idempotent on an already-hibernated claim.
	snap := sb.HibernateSnap
	m.idleOnce(t.Context())
	if sb.HibernateSnap != snap {
		t.Errorf("second sweep changed the snapshot: %q -> %q", snap, sb.HibernateSnap)
	}
}

func TestIdleOncePolicyScope(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	m.idleDefault = time.Second // node default must NOT reach pooled keys

	sb := mustClaim(t, m, testKey)
	m.mu.Lock()
	sb.LastActivity = time.Now().Add(-time.Hour)
	m.mu.Unlock()
	m.idleOnce(t.Context())
	if sb.HibernateSnap != "" {
		t.Fatal("pooled key without the policy was idle-hibernated by the node default")
	}

	// An unpooled key (template claim shape) takes the node default.
	unpooled := types.PoolKey{Template: "tpl:v1", Net: types.NetNone, Size: types.SizeSmall}
	sb2, err := m.ClaimProvision(t.Context(), unpooled, time.Hour)
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	m.mu.Lock()
	sb2.LastActivity = time.Now().Add(-time.Hour)
	m.mu.Unlock()
	m.idleOnce(t.Context())
	if sb2.HibernateSnap == "" {
		t.Fatal("unpooled claim not hibernated by the node default")
	}
}

func TestActivityStampsBlockIdleSweep(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, IdleHibernateSeconds: 1})
	sb := mustClaim(t, m, testKey)
	m.mu.Lock()
	sb.LastActivity = time.Now().Add(-2 * time.Second)
	m.mu.Unlock()

	// A data-plane connection refreshes the stamp; the sweep must spare it.
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("WakeAgentSocket: %v", err)
	}
	m.idleOnce(t.Context())
	if sb.HibernateSnap != "" {
		t.Fatal("active claim hibernated despite a fresh data-plane stamp")
	}
}
