package pool

import (
	"context"
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
		t.Fatal("kick after drain not delivered")
	}
}

func TestAFreshGoldenStartsRefillWithoutWaitingForATick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
		ctx, cancel := context.WithCancel(t.Context())

		go m.Run(ctx)
		synctest.Wait()

		m.mu.Lock()
		golden := m.pools[testKey].goldenDir
		m.mu.Unlock()
		clones := eng.cloneCount()
		cancel()
		synctest.Wait()
		if golden == "" {
			t.Fatal("no golden built, so the test proves nothing")
		}
		if clones == 0 {
			t.Errorf("no warm clone before the %v refill tick", refillInterval)
		}
	})
}

func TestARefillAnOperatorDoorStartsStopsWithRun(t *testing.T) {
	spec := config.PoolSpec{PoolKey: testKey, Warm: 3}
	for _, tt := range []struct {
		name string
		shut func(*testing.T, *Manager) error
		open func(*testing.T, *Manager) error
	}{
		{
			"set pools",
			func(t *testing.T, m *Manager) error {
				return m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: testKey}})
			},
			func(t *testing.T, m *Manager) error { return m.SetPools(t.Context(), []config.PoolSpec{spec}) },
		},
		{
			"uncordon",
			func(t *testing.T, m *Manager) error { return m.Cordon(t.Context()) },
			func(t *testing.T, m *Manager) error { return m.Uncordon(t.Context()) },
		},
		{
			"undrain",
			func(t *testing.T, m *Manager) error { m.Drain(t.Context()); return nil },
			func(t *testing.T, m *Manager) error { m.Undrain(t.Context()); return nil },
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				eng := newFakeEngine()
				eng.cloneStall = make(chan struct{})
				m := newTestManager(t, eng, spec)
				m.refillSem = make(chan struct{}, 1)
				seedGolden(t, m, testKey, "")
				if err := tt.shut(t, m); err != nil {
					t.Fatalf("shut: %v", err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				go m.Run(ctx)
				synctest.Wait()
				if n := eng.cloneCount(); n != 0 {
					t.Fatalf("clones=%d before the door opened, so the test proves nothing", n)
				}

				if err := tt.open(t, m); err != nil {
					t.Fatalf("open: %v", err)
				}
				synctest.Wait()
				if n := eng.cloneCount(); n != 1 {
					t.Fatalf("clones=%d after the door opened, want the one the budget allows", n)
				}
				cancel()
				synctest.Wait()
				close(eng.cloneStall)
				synctest.Wait()

				if n := eng.cloneCount(); n != 1 {
					t.Errorf("clones=%d after Run stopped, want 1: the refill outlived it", n)
				}
			})
		})
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
		synctest.Wait()
		if n := eng.cloneCount(); n != attempts {
			t.Errorf("clones=%d after parking, want %d: a parked node must not attempt again", n, attempts)
		}

		m.mu.Lock()
		m.atCapacityUntil = time.Now().Add(-time.Second)
		m.mu.Unlock()
		eng.cloneErr = nil
		m.refillOnce(t.Context())
		waitWarm(t, m, 4)
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
	waitWarm(t, m, 1)
}
