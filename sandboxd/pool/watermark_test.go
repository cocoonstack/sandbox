package pool

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestEffectiveTargetTracksDemand(t *testing.T) {
	now := time.Now()
	p := &pool{key: types.PoolKey{}, floor: 2, warmMax: 8, lead: 500 * time.Millisecond}

	if got := p.effectiveTarget(now); got != 2 {
		t.Fatalf("quiet pool target %d, want the floor", got)
	}

	for i := range 60 {
		p.noteArrival(now.Add(time.Duration(i) * 100 * time.Millisecond))
	}
	sustainedEnd := now.Add(6 * time.Second)
	if got := p.effectiveTarget(sustainedEnd); got != 8 {
		t.Errorf("sustained 10/s target %d, want warmMax 8", got)
	}

	if got := p.effectiveTarget(sustainedEnd.Add(5 * time.Minute)); got != 2 {
		t.Errorf("post-silence target %d, want the floor", got)
	}
}

func TestEffectiveTargetIgnoresATwoClaimBurst(t *testing.T) {
	now := time.Now()
	p := &pool{key: types.PoolKey{}, floor: 2, warmMax: 20, lead: 500 * time.Millisecond}

	p.noteArrival(now)
	p.noteArrival(now.Add(time.Millisecond))
	if got := p.effectiveTarget(now.Add(time.Second)); got != 2 {
		t.Errorf("target %d after two claims 1 ms apart, want the floor", got)
	}

	p.noteArrival(now.Add(2 * time.Second))
	if got := p.effectiveTarget(now.Add(2 * time.Second)); got != 2 {
		t.Errorf("target %d after the burst's bin closed at 1/s, want the floor", got)
	}
}

func TestEffectiveTargetForgetsRateAcrossSilence(t *testing.T) {
	now := time.Now()
	p := &pool{key: types.PoolKey{}, floor: 2, warmMax: 8, lead: 500 * time.Millisecond}
	for i := range 60 {
		p.noteArrival(now.Add(time.Duration(i) * 100 * time.Millisecond))
	}
	if got := p.effectiveTarget(now.Add(6 * time.Second)); got != 8 {
		t.Fatalf("sustained target %d, want warmMax 8", got)
	}

	later := now.Add(10 * time.Minute)
	p.noteArrival(later)
	p.noteArrival(later.Add(time.Second))
	if got := p.effectiveTarget(later.Add(time.Second)); got != 2 {
		t.Errorf("target %d after two claims following ten minutes of silence, want the floor", got)
	}
}

func TestEffectiveTargetOffWithoutWarmMax(t *testing.T) {
	now := time.Now()
	p := &pool{floor: 1, lead: time.Second}
	for i := range 50 {
		p.noteArrival(now.Add(time.Duration(i) * 10 * time.Millisecond))
	}
	if got := p.effectiveTarget(now.Add(time.Second)); got != 1 {
		t.Errorf("target %d with warm_max unset, want the static floor", got)
	}
}

func TestEffectiveTargetGrowsAnEmptyPoolAndReturnsItToEmpty(t *testing.T) {
	now := time.Now()
	p := &pool{key: types.PoolKey{}, warmMax: 8}
	for i := range 60 {
		p.noteArrival(now.Add(time.Duration(i) * 100 * time.Millisecond))
	}
	busy := now.Add(6 * time.Second)
	if got := p.effectiveTarget(busy); got != 1 {
		t.Fatalf("target %d under demand with no measured lead, want 1 so a refill can measure it", got)
	}
	p.noteLead(500 * time.Millisecond)
	if got := p.effectiveTarget(busy); got != 8 {
		t.Errorf("target %d at 10/s with a 500 ms lead, want warmMax 8", got)
	}
	if got := p.effectiveTarget(busy.Add(10 * time.Minute)); got != 0 {
		t.Errorf("target %d after ten silent minutes, want the empty floor", got)
	}
}

func TestEffectiveTargetKeepsASlowPoolAheadOfSparseDemand(t *testing.T) {
	now := time.Now()
	p := &pool{key: types.PoolKey{}, floor: 1, warmMax: 3, lead: 2 * time.Minute, rate: 0.01, lastArrival: now}
	if got := p.effectiveTarget(now); got != 3 {
		t.Errorf("target %d at 0.01/s with a two minute lead, want 3: the empty-floor cutoff must not reach a sized pool", got)
	}
}

func TestRefillGrowsAnEmptyPoolOnDemand(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, WarmMax: 4})
	p := m.pools[testKey]
	p.goldenDir = "/goldens/x"
	start := time.Now().Add(-3 * time.Second)
	m.mu.Lock()
	for i := range 30 {
		p.noteArrival(start.Add(time.Duration(i) * 100 * time.Millisecond))
	}
	m.mu.Unlock()

	m.refillOnce(t.Context())
	waitFor(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return len(p.warm) > 0 && p.lead > 0
	})
}

func TestNoteLeadConverges(t *testing.T) {
	p := &pool{}
	p.noteLead(time.Second)
	if p.lead != time.Second {
		t.Fatalf("first lead %v", p.lead)
	}
	for range 20 {
		p.noteLead(100 * time.Millisecond)
	}
	if p.lead > 150*time.Millisecond {
		t.Errorf("lead %v did not converge toward the recent samples", p.lead)
	}
}

func TestShrinkWaitsOutTheDwell(t *testing.T) {
	now := time.Now()
	p := warmPool(4)

	if trimmed := p.shrink(now); trimmed != nil {
		t.Errorf("trimmed %v on the first tick above target, want the dwell to start", trimmed)
	}
	if trimmed := p.shrink(now.Add(warmShrinkDwell - time.Second)); trimmed != nil {
		t.Errorf("trimmed %v inside the dwell", trimmed)
	}

	trimmed := p.shrink(now.Add(warmShrinkDwell))
	if len(trimmed) != 2 || len(p.warm) != 2 {
		t.Errorf("trimmed %v leaving %d warm, want 2 and 2", trimmed, len(p.warm))
	}
	if !p.overTargetSince.IsZero() {
		t.Error("dwell still running after a trim")
	}
}

func TestShrinkRestartsTheDwellWhenDemandReturns(t *testing.T) {
	now := time.Now()
	p := warmPool(4)
	p.shrink(now)

	for i := range 60 {
		p.noteArrival(now.Add(time.Duration(i) * 100 * time.Millisecond))
	}
	hot := now.Add(6 * time.Second)
	if trimmed := p.shrink(hot); trimmed != nil || !p.overTargetSince.IsZero() {
		t.Fatalf("trimmed %v with the target back at warmMax, want the dwell cleared", trimmed)
	}

	quiet := hot.Add(10 * time.Minute)
	if trimmed := p.shrink(quiet); trimmed != nil {
		t.Errorf("trimmed %v on the first quiet tick, want a fresh dwell", trimmed)
	}
	trimmed := p.shrink(quiet.Add(warmShrinkDwell))
	if len(trimmed) != 2 || len(p.warm) != 2 {
		t.Errorf("trimmed %v leaving %d warm after the fresh dwell, want 2 and 2", trimmed, len(p.warm))
	}
}

func TestShrinkOnceDestroysWhatTheDecayedTargetDropped(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2, WarmMax: 8})
	p := m.pools[testKey]
	for i := range 4 {
		p.warm = append(p.warm, &types.Sandbox{VMName: fmt.Sprintf("sbx-warm-%d", i), Key: testKey})
	}

	m.shrinkOnce(t.Context())
	if removed := eng.removedNames(); len(p.warm) != 4 || len(removed) != 0 {
		t.Fatalf("warm %d and %d removes on the first sweep, want 4 and 0", len(p.warm), len(removed))
	}

	p.overTargetSince = time.Now().Add(-warmShrinkDwell - time.Second)
	m.shrinkOnce(t.Context())
	if len(p.warm) != 2 {
		t.Errorf("warm %d after the dwell, want the floor", len(p.warm))
	}
	waitFor(t, func() bool { return len(eng.removedNames()) == 2 })
	if got, want := slices.Sorted(slices.Values(eng.removedNames())), []string{"sbx-warm-2", "sbx-warm-3"}; !slices.Equal(got, want) {
		t.Errorf("removes %v, want %v", got, want)
	}
}

func warmPool(warm int) *pool {
	p := &pool{key: testKey, floor: 2, warmMax: 8, lead: 500 * time.Millisecond}
	for i := range warm {
		p.warm = append(p.warm, &types.Sandbox{VMName: fmt.Sprintf("sbx-warm-%d", i), Key: testKey})
	}
	return p
}
