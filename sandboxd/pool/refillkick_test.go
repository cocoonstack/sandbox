package pool

import (
	"errors"
	"fmt"
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

		for range 5 {
			m.refillOnce(t.Context())
		}
		if n := eng.cloneCount(); n != attempts {
			t.Errorf("clones=%d after parking, want %d: a parked node must not attempt again", n, attempts)
		}

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

func TestRefillKeepsItsShareWhileTeardownsQueue(t *testing.T) {
	eng := newFakeEngine()
	eng.removeStall = make(chan struct{})
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	m.pools[testKey].goldenDir = "/goldens/x"
	names := make([]string, 2*cap(m.refillSem))
	for i := range names {
		names[i] = fmt.Sprintf("sbx-gone-%d", i)
	}
	teardown := m.destroyAll(t.Context(), names)
	defer func() {
		close(eng.removeStall)
		teardown.Wait()
	}()
	waitFor(t, func() bool { return len(m.maintSem) == cap(m.maintSem) })

	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Warm == 1
	})
}
