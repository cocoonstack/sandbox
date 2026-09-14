package pool

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestIdleOnceHibernatesPastThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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

		m.idleOnce(t.Context())
		waitFor(t, func() bool { return !m.idleSweep.Load() })
		if hibernated(m) != 1 {
			t.Error("second sweep disturbed the hibernated claim")
		}
	})
}

func TestIdleOncePolicyScope(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
		m.idleDefault = time.Second
		m.idleEnabled.Store(true)

		sb := mustClaim(t, m, testKey)
		backdate(m, sb, time.Hour)
		m.idleOnce(t.Context())
		waitFor(t, func() bool { return !m.idleSweep.Load() })
		if hibernated(m) != 0 {
			t.Fatal("pooled key without the policy was idle-hibernated by the node default")
		}

		unpooled := types.PoolKey{Template: "tpl:v1", Net: types.NetNone, Size: types.SizeSmall}
		sb2, err := m.ClaimProvision(t.Context(), unpooled, time.Hour, "", "", nil)
		if err != nil {
			t.Fatalf("provision: %v", err)
		}
		backdate(m, sb2, time.Hour)
		m.idleOnce(t.Context())
		waitFor(t, func() bool { return hibernated(m) == 1 })
	})
}

func TestActivityStampsBlockIdleSweep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, IdleHibernateSeconds: 1})
		sb := mustClaim(t, m, testKey)
		backdate(m, sb, 2*time.Second)

		_, done, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
		if err != nil {
			t.Fatalf("WakeAgentSocket: %v", err)
		}
		done()
		m.idleOnce(t.Context())
		waitFor(t, func() bool { return !m.idleSweep.Load() })
		if hibernated(m) != 0 {
			t.Fatal("active claim hibernated despite a fresh data-plane stamp")
		}
	})
}

func TestHeldConnectionBlocksIdleSweep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, IdleHibernateSeconds: 1})
		sb := mustClaim(t, m, testKey)
		_, done, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
		if err != nil {
			t.Fatalf("WakeAgentSocket: %v", err)
		}

		backdate(m, sb, time.Hour)
		m.idleOnce(t.Context())
		waitFor(t, func() bool { return !m.idleSweep.Load() })
		if hibernated(m) != 0 {
			t.Fatal("claim with an open connection was idle-hibernated")
		}

		done()
		m.idleOnce(t.Context())
		waitFor(t, func() bool { return !m.idleSweep.Load() })
		if hibernated(m) != 0 {
			t.Fatal("idle clock did not restart when the connection closed")
		}

		backdate(m, sb, 2*time.Second)
		m.idleOnce(t.Context())
		waitFor(t, func() bool { return hibernated(m) == 1 })
	})
}

func TestEgressRequestInFlightBlocksIdleSweep(t *testing.T) {
	release := make(chan struct{})
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(origin.Close)
	pol := &egress.Policy{Allow: []egress.Rule{{Host: mustHostname(t, origin.URL)}}}
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, IdleHibernateSeconds: 1, Egress: pol})
	m.dial = (&net.Dialer{}).DialContext
	sb := vsockSandbox(t, "sb_inflight")
	sb.VMName = "sbx-inflight-1"
	m.mu.Lock()
	m.claimed[sb.ID] = sb
	m.mu.Unlock()
	if armErr := m.armEgressProxy(t.Context(), sb); armErr != nil {
		t.Fatalf("arm proxy: %v", armErr)
	}
	result := make(chan error, 1)
	go func() {
		resp, err := egressClient(engine.EgressSocketPath(sb.VsockSocket)).Get(origin.URL + "/")
		if err == nil {
			resp.Body.Close()
		}
		result <- err
	}()
	waitFor(t, sb.Busy)
	backdate(m, sb, time.Hour)
	m.idleOnce(t.Context())
	waitFor(t, func() bool { return !m.idleSweep.Load() })
	if hibernated(m) != 0 {
		t.Fatal("claim with an egress request in flight was idle-hibernated")
	}

	close(release)
	if err := <-result; err != nil {
		t.Fatalf("request through the proxy: %v", err)
	}
	waitFor(t, func() bool { return !sb.Busy() })
	m.idleOnce(t.Context())
	waitFor(t, func() bool { return !m.idleSweep.Load() })
	if hibernated(m) != 0 {
		t.Fatal("idle clock did not restart when the egress request ended")
	}
	backdate(m, sb, time.Hour)
	m.idleOnce(t.Context())
	waitFor(t, func() bool { return hibernated(m) == 1 })
}

func backdate(m *Manager, sb *types.Sandbox, by time.Duration) {
	m.mu.Lock()
	sb.TouchAt(time.Now().Add(-by))
	m.mu.Unlock()
}

func hibernated(m *Manager) int {
	_, g := m.Info()
	return g.Hibernated
}
