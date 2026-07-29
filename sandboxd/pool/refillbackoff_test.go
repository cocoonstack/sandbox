package pool

import (
	"errors"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

func TestRefillBacksOffOnlyAfterAnUnbrokenRunOfFailures(t *testing.T) {
	p := &pool{}
	now := time.Now()

	for i := range refillFailStreak - 1 {
		if p.noteRefillResult(now, true) {
			t.Fatalf("backed off after %d failures, before the streak was reached", i+1)
		}
		if !p.nextRefill.IsZero() {
			t.Fatalf("nextRefill set after %d failures", i+1)
		}
	}
	if !p.noteRefillResult(now, true) {
		t.Fatal("the failure completing the streak did not start a backoff")
	}
	if !p.nextRefill.After(now) {
		t.Fatal("backoff started but nextRefill is not in the future")
	}

	p.noteRefillResult(now, false)
	if p.refillFails != 0 || !p.nextRefill.IsZero() {
		t.Fatalf("a success left fails=%d nextRefill=%v", p.refillFails, p.nextRefill)
	}

	if p.noteRefillResult(now, true) {
		t.Fatal("a single failure after a success backed the pool off")
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	p := &pool{}
	now := time.Now()
	var prev time.Duration
	for i := range refillFailStreak + 40 {
		p.noteRefillResult(now, true)
		if i < refillFailStreak-1 {
			continue
		}
		got := p.nextRefill.Sub(now)
		if got < prev {
			t.Fatalf("backoff shrank: %s then %s", prev, got)
		}
		if got > refillBackoffMax {
			t.Fatalf("backoff %s exceeds the %s cap", got, refillBackoffMax)
		}
		prev = got
	}
	if prev != refillBackoffMax {
		t.Errorf("backoff settled at %s, want the %s cap", prev, refillBackoffMax)
	}
}

// TestRefillOnceHonorsTheBackoff pins the property that saves the node:
// while a pool is backed off, the ticker spawns nothing.
func TestRefillOnceHonorsTheBackoff(t *testing.T) {
	eng := newFakeEngine()
	eng.cloneErr = errors.New("configure network: failed to connect veth to bridge cni0: exchange full")
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 50})
	m.pools[testKey].goldenDir = "/goldens/x"

	// Concurrency is bounded, so the streak accrues over several ticks.
	backedOff := false
	for range 50 {
		m.refillOnce(t.Context())
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return infos[0].Refilling == 0
		})
		m.mu.Lock()
		backedOff = !m.pools[testKey].nextRefill.IsZero()
		m.mu.Unlock()
		if backedOff {
			break
		}
	}
	attempts := eng.cloneCount()
	if !backedOff {
		t.Fatal("a pool whose every refill failed is not backed off")
	}

	// Pin the expiry far out so the no-spawn assertions cannot race a short
	// first backoff on a loaded machine.
	m.mu.Lock()
	m.pools[testKey].nextRefill = time.Now().Add(time.Hour)
	m.mu.Unlock()
	for range 5 {
		m.refillOnce(t.Context())
	}
	if n := eng.cloneCount(); n != attempts {
		t.Errorf("clones=%d after backing off, want %d: a backed-off pool must not attempt again", n, attempts)
	}

	// A pause, not a latch: once the wait expires a working engine fills.
	m.mu.Lock()
	m.pools[testKey].nextRefill = time.Now().Add(-time.Second)
	m.mu.Unlock()
	eng.cloneErr = nil
	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return infos[0].Warm == 50
	})
}
