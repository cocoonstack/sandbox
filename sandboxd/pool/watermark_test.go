package pool

import (
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestEffectiveTargetTracksDemand(t *testing.T) {
	now := time.Now()
	p := &pool{key: types.PoolKey{}, floor: 2, warmMax: 8, lead: 500 * time.Millisecond}

	if got := p.effectiveTarget(now); got != 2 {
		t.Fatalf("quiet pool target %d, want the floor", got)
	}

	for i := range 20 {
		p.noteArrival(now.Add(time.Duration(i) * 100 * time.Millisecond))
	}
	burstEnd := now.Add(2 * time.Second)
	if got := p.effectiveTarget(burstEnd); got != 8 {
		t.Errorf("burst target %d, want warmMax 8", got)
	}

	if got := p.effectiveTarget(burstEnd.Add(5 * time.Minute)); got != 2 {
		t.Errorf("post-silence target %d, want the floor", got)
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
