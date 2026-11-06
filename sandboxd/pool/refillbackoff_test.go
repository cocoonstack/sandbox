package pool

import (
	"context"
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
		synctest.Wait()
		if n := eng.cloneCount(); n != attempts {
			t.Errorf("clones=%d after backing off, want %d: a backed-off pool must not attempt again", n, attempts)
		}

		m.mu.Lock()
		m.pools[testKey].nextRefill = time.Now().Add(-time.Second)
		m.mu.Unlock()
		eng.cloneErr = nil
		m.refillOnce(t.Context())
		waitWarm(t, m, 50)
	})
}

func TestAGoldenThatCannotBeClonedIsRebuiltOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.cloneErr = errors.New(`vm.restore -> 500 "Memory manager error","Failed to allocate a memory range"`)
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
		seedGolden(t, m, testKey, "")

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
		seedGolden(t, m, testKey, "")
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
		seedGolden(t, m, testKey, "")
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
		g := seedGolden(t, m, testKey, "")
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
	g := seedGolden(t, m, testKey, "")
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

func TestClaimClonesThatFailRetireTheGoldenOfAPoolWithNoRefill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.cloneErr = errors.New(`vm.restore -> 500 "Memory manager error"`)
		m, p, g := adoptedGoldenNoWarm(t, eng)

		for i := range refillFailStreak {
			if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); err == nil {
				t.Fatalf("claim %d on an unclonable golden succeeded", i)
			}
			if i < refillFailStreak-1 && goldenOf(m, p) != g {
				t.Fatalf("golden retired after %d failed claims, want %d", i+1, refillFailStreak)
			}
		}
		if goldenOf(m, p) != "" {
			t.Fatalf("golden %q after %d failed claims, want it retired", goldenOf(m, p), refillFailStreak)
		}
		if n := p.claimClones.Load(); n != 0 {
			t.Errorf("claim clones %d after the failed claims, want 0", n)
		}
		if _, err := os.Stat(g); err != nil {
			t.Errorf("golden directory after retirement: %v, want it left for the rebuild", err)
		}
		select {
		case <-m.refillKick:
		default:
			t.Error("retirement did not kick refill, so the rebuild waits for the next tick")
		}

		eng.mu.Lock()
		eng.cloneErr = nil
		eng.mu.Unlock()
		clones := eng.cloneCount()
		if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); err != nil {
			t.Fatalf("claim while no golden exists: %v", err)
		}
		if n := eng.coldCount(); n != 1 || eng.cloneCount() != clones {
			t.Errorf("cold boots %d clones %d, want the claim between golden builds to cold-boot once and clone nothing", n, eng.cloneCount()-clones)
		}

		m.refillOnce(t.Context())
		waitPoolSettled(t, m, testKey)
		if goldenOf(m, p) != g || eng.coldCount() != 2 {
			t.Fatalf("golden %q after %d cold boots, want %q rebuilt by a second one", goldenOf(m, p), eng.coldCount(), g)
		}
		clones = eng.cloneCount()
		if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); err != nil {
			t.Fatalf("claim on the rebuilt golden: %v", err)
		}
		if eng.cloneCount() != clones+1 {
			t.Errorf("clones %d, want the claim to clone the rebuilt golden", eng.cloneCount()-clones)
		}
	})
}

func TestClaimResultsThatSayNothingAboutTheGoldenDoNotCount(t *testing.T) {
	canceled := func(t *testing.T) context.Context {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		return ctx
	}
	for _, tt := range []struct {
		name string
		ctx  func(*testing.T) context.Context
		err  error
	}{
		{"canceled by its client", canceled, errors.New(`vm.restore -> 500 "Memory manager error"`)},
		{"capacity failure", (*testing.T).Context, errors.New("create tap: no space left on device")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			eng.cloneErr = tt.err
			m, p, g := adoptedGoldenNoWarm(t, eng)
			clones := eng.cloneCount()
			for range refillFailStreak + 1 {
				if _, err := m.ClaimProvision(tt.ctx(t), testKey, ClaimOptions{}); err == nil {
					t.Fatal("claim under the fault succeeded")
				}
			}
			if n := eng.cloneCount() - clones; n != refillFailStreak+1 {
				t.Fatalf("clones %d, want each claim to reach the clone", n)
			}
			if fails := cloneFailsOf(m, p); fails != 0 || goldenOf(m, p) != g {
				t.Errorf("clone fails %d golden %q, want 0 and %q kept", fails, goldenOf(m, p), g)
			}
		})
	}
}

func TestAClaimWhoseProbeFailsCountsItsCloneAsASuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.probeErr = errors.New("silkd not ready")
		m, p, g := adoptedGoldenNoWarm(t, eng)
		setCloneFails(m, p, refillFailStreak-1)

		for range 2 {
			if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); err == nil {
				t.Fatal("claim with a failing probe succeeded")
			}
		}
		if fails := cloneFailsOf(m, p); fails != 0 || goldenOf(m, p) != g {
			t.Errorf("clone fails %d golden %q, want 0 and %q kept: the clones succeeded", fails, goldenOf(m, p), g)
		}
	})
}

func TestAClaimCloneSuccessResetsTheStreak(t *testing.T) {
	m, p, g := adoptedGoldenNoWarm(t, newFakeEngine())
	setCloneFails(m, p, refillFailStreak-2)

	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if fails := cloneFailsOf(m, p); fails != 0 || goldenOf(m, p) != g {
		t.Errorf("clone fails %d golden %q, want the streak reset and %q kept", fails, goldenOf(m, p), g)
	}
}

func TestALateClaimCloneResultOfARetiredGoldenDoesNotCount(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{"failure", errors.New(`vm.restore -> 500 "Memory manager error"`)},
		{"success", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey})
			m.mu.Lock()
			p := m.pools[testKey]
			p.goldenDir, p.goldenGen, p.cloneFails = "/goldens/new", 2, refillFailStreak-1
			m.mu.Unlock()

			m.noteClaimClone(t.Context(), goldenResolution{dir: "/goldens/old", held: p, gen: 1}, tt.err)

			if fails := cloneFailsOf(m, p); fails != refillFailStreak-1 || goldenOf(m, p) != "/goldens/new" {
				t.Errorf("clone fails %d golden %q, want %d and the new golden kept", fails, goldenOf(m, p), refillFailStreak-1)
			}
		})
	}
}

func adoptedGoldenNoWarm(t *testing.T, eng *fakeEngine) (*Manager, *pool, string) {
	t.Helper()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey})
	g := seedGolden(t, m, testKey, "")
	m.refillOnce(t.Context())
	waitPoolSettled(t, m, testKey)
	m.mu.Lock()
	p := m.pools[testKey]
	got := p.goldenDir
	m.mu.Unlock()
	if got != g {
		t.Fatalf("adopted golden %q, want %q", got, g)
	}
	return m, p, g
}

func goldenOf(m *Manager, p *pool) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return p.goldenDir
}

func cloneFailsOf(m *Manager, p *pool) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return p.cloneFails
}

func setCloneFails(m *Manager, p *pool, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p.cloneFails = n
}
