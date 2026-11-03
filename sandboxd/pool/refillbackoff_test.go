package pool

import (
	"errors"
	"os"
	"testing"
	"testing/synctest"
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

func TestRefillOnceHonorsTheBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()

		eng.cloneErr = errors.New("clone: guest kernel panicked before vsock came up")
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 50})
		m.pools[testKey].goldenDir = "/goldens/x"
		m.pools[testKey].goldenRetiredAt = time.Now()

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

		m.mu.Lock()
		m.pools[testKey].nextRefill = time.Now().Add(time.Hour)
		m.mu.Unlock()
		for range 5 {
			m.refillOnce(t.Context())
		}
		if n := eng.cloneCount(); n != attempts {
			t.Errorf("clones=%d after backing off, want %d: a backed-off pool must not attempt again", n, attempts)
		}

		m.mu.Lock()
		m.pools[testKey].nextRefill = time.Now().Add(-time.Second)
		m.mu.Unlock()
		eng.cloneErr = nil
		m.refillOnce(t.Context())
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return infos[0].Warm == 50
		})
	})
}

func TestAGoldenThatCannotBeClonedIsRebuiltOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.cloneErr = errors.New(`vm.restore -> 500 "Memory manager error","Failed to allocate a memory range"`)
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
		seedGolden(t, m, "")

		waitFor(t, func() bool {
			m.refillOnce(t.Context())
			return eng.coldCount() == 1
		})
		eng.mu.Lock()
		eng.cloneErr = nil
		eng.mu.Unlock()
		waitFor(t, func() bool { return refilledTo(t, m, testKey, 2) })

		if n := eng.coldCount(); n != 1 {
			t.Errorf("golden builds %d, want one rebuild of the unclonable golden", n)
		}
	})
}

func TestAGoldenThatStaysUnclonableIsRebuiltAtMostOncePerRetryDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.cloneErr = errors.New(`vm.restore -> 500 "Memory manager error"`)
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
		seedGolden(t, m, "")
		start := time.Now()

		for time.Since(start) < 3*buildRetryDelay+buildRetryDelay/2 {
			m.refillOnce(t.Context())
			time.Sleep(100 * time.Millisecond)
		}
		waitPoolSettled(t, m, testKey)

		if n, limit := eng.coldCount(), 1+int(time.Since(start)/buildRetryDelay); n < 2 || n > limit {
			t.Errorf("golden builds %d over %v, want between 2 and %d", n, time.Since(start), limit)
		}
	})
}

func TestARebuiltGoldenSurvivesOneFailedRefill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.cloneErr = errors.New(`vm.restore -> 500 "Memory manager error"`)
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
		seedGolden(t, m, "")
		waitFor(t, func() bool {
			m.refillOnce(t.Context())
			return eng.coldCount() == 1
		})
		waitFor(t, func() bool {
			m.mu.Lock()
			defer m.mu.Unlock()
			p := m.pools[testKey]
			return p.goldenDir != "" && !p.building && p.refilling == 0
		})
		time.Sleep(buildRetryDelay)

		m.refillOnce(t.Context())
		waitPoolSettled(t, m, testKey)

		m.mu.Lock()
		golden, fails := m.pools[testKey].goldenDir, m.pools[testKey].refillFails
		m.mu.Unlock()
		if golden == "" || eng.coldCount() != 1 {
			t.Errorf("golden %q after %d builds: one failed refill of a rebuilt golden retired it again", golden, eng.coldCount())
		}
		if fails >= refillFailStreak {
			t.Errorf("refill fails %d, want the rebuilt golden's own streak, not the retired one's", fails)
		}
	})
}

func TestALateFailureOfARetiredGoldensCloneDoesNotCount(t *testing.T) {
	eng := newFakeEngine()
	eng.cloneErr = errors.New(`vm.restore -> 500 "Memory manager error"`)
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	m.mu.Lock()
	p := m.pools[testKey]
	p.goldenDir, p.goldenGen = "/goldens/new", 2
	p.refillFails, p.cloneFails = refillFailStreak-1, refillFailStreak-1
	p.refilling++
	m.mu.Unlock()
	m.refillSem <- struct{}{}

	m.refillOne(t.Context(), p, "/goldens/old", 1)

	m.mu.Lock()
	defer m.mu.Unlock()
	if p.refillFails != refillFailStreak-1 || p.cloneFails != refillFailStreak-1 || p.goldenDir != "/goldens/new" {
		t.Errorf("refill fails %d clone fails %d golden %q, want %d each and the new golden kept", p.refillFails, p.cloneFails, p.goldenDir, refillFailStreak-1)
	}
}

func TestAGoldenWhoseClonesSucceedIsNeverRetiredForFailedProbes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.probeErr = errors.New("silkd not ready")
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
		g := seedGolden(t, m, "")
		start := time.Now()

		for time.Since(start) < 2*buildRetryDelay {
			m.refillOnce(t.Context())
			time.Sleep(100 * time.Millisecond)
		}
		waitPoolSettled(t, m, testKey)

		m.mu.Lock()
		golden := m.pools[testKey].goldenDir
		m.mu.Unlock()
		if n := eng.probeCount(); n < refillFailStreak {
			t.Fatalf("%d failed probes, want at least %d so the streak is reached", n, refillFailStreak)
		}
		if golden != g || eng.coldCount() != 0 {
			t.Errorf("golden %q after %d builds, want %q kept: its clones succeed", golden, eng.coldCount(), g)
		}
	})
}

func TestRetiringAGoldenLeavesItsDirectoryForAClaimMidClone(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1})
	g := seedGolden(t, m, "")
	m.mu.Lock()
	p := m.pools[testKey]
	p.goldenDir, p.goldenGen, p.cloneFails = g, 1, refillFailStreak
	p.claimClones.Add(1)
	retired := m.retireUnclonableLocked(p, 1, time.Now())
	golden := p.goldenDir
	m.mu.Unlock()

	if !retired || golden != "" {
		t.Fatalf("retired %v golden %q, want the golden retired", retired, golden)
	}
	if _, err := os.Stat(g); err != nil {
		t.Errorf("golden directory under a claim clone: %v, want it left for the rebuild to replace", err)
	}
	if _, err := os.Stat(g + goldenStampSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("golden stamp after retirement: %v, want it removed so nothing adopts it", err)
	}
}
