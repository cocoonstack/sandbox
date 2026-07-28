package pool

import (
	"errors"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

// TestRefillBacksOffOnlyAfterAnUnbrokenRunOfFailures pins both halves of the
// discriminator. One VM failing to boot is ordinary and must not stall the
// pool; everything failing means the next attempt will fail too, and retrying
// that at ticker rate is what turns a capacity limit into an outage.
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

	// A success clears it: a pool must recover without waiting out a backoff
	// earned by a burst that has already passed.
	p.noteRefillResult(now, false)
	if p.refillFails != 0 || !p.nextRefill.IsZero() {
		t.Fatalf("a success left fails=%d nextRefill=%v", p.refillFails, p.nextRefill)
	}

	// And one straggler failure after that success starts from scratch.
	if p.noteRefillResult(now, true) {
		t.Fatal("a single failure after a success backed the pool off")
	}
}

// TestBackoffGrowsAndIsCapped keeps a persistent failure from being retried
// forever at the same rate, without letting the wait run away.
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

// TestRefillOnceHonorsTheBackoff is the property that actually saves the node:
// while a pool is backed off, the ticker must spawn nothing. Without it a pool
// short of target re-attempts every tick at full concurrency, and each attempt
// forks the engine and runs the CNI plugins before failing.
func TestRefillOnceHonorsTheBackoff(t *testing.T) {
	eng := newFakeEngine()
	eng.cloneErr = errors.New("configure network: failed to connect veth to bridge cni0: exchange full")
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 50})
	m.pools[testKey].goldenDir = "/goldens/x"

	// Concurrency is bounded, so the streak accrues over several ticks — the
	// same way it does on a node whose refills are all failing.
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

	for range 5 {
		m.refillOnce(t.Context())
	}
	if n := eng.cloneCount(); n != attempts {
		t.Errorf("clones=%d after backing off, want %d: a backed-off pool must not attempt again", n, attempts)
	}

	// It is a pause, not a latch: once the wait expires a working engine fills.
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
