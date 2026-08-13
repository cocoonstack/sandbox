package pool

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

func TestKickRefillCoalescesAndNeverBlocks(t *testing.T) {
	m := &Manager{refillKick: make(chan struct{}, 1)}
	for range 3 {
		m.kickRefill()
	}
	if len(m.refillKick) != 1 {
		t.Fatalf("pending kicks = %d, want 1", len(m.refillKick))
	}
	<-m.refillKick
	m.kickRefill()
	if len(m.refillKick) != 1 {
		t.Fatalf("kick after drain not delivered")
	}
}

// TestNodeAtCapacityParksRefillInsteadOfRetrying pins the difference between
// "this VM failed to start" and "this node cannot hold another VM".
func TestNodeAtCapacityParksRefillInsteadOfRetrying(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.cloneErr = errors.New(`cocoon vm clone: exit status 1: Error: configure network: ` +
			`cni add A/eth0: plugin type="bridge" failed (add): ` +
			`failed to connect "veth3f2" to bridge cni0: exchange full`)
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 4})
		m.pools[testKey].goldenDir = "/goldens/x"

		m.refillOnce(t.Context())
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return infos[0].Refilling == 0
		})
		attempts := eng.cloneCount()
		if attempts == 0 {
			t.Fatal("no clone attempted, so the test proves nothing")
		}

		if _, g := m.Info(); !g.AtCapacity || g.AtCapacityReason == "" {
			t.Fatalf("at-capacity not published: %+v", g)
		}

		// Every later tick is a no-op until the backoff expires.
		for range 5 {
			m.refillOnce(t.Context())
		}
		if n := eng.cloneCount(); n != attempts {
			t.Errorf("clones=%d after parking, want %d: a parked node must not attempt again", n, attempts)
		}

		// A park, not a latch: once the backoff expires refill resumes.
		m.mu.Lock()
		m.atCapacityUntil = time.Now().Add(-time.Second)
		m.mu.Unlock()
		eng.cloneErr = nil
		m.refillOnce(t.Context())
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return infos[0].Warm == 4
		})
	})
}
