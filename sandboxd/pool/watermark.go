package pool

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	// ewmaAlpha weights the newest observation in the arrival-rate and provision-lead averages
	ewmaAlpha = 0.3
	rateBin   = time.Second
	// rateDecayTau halves the observed arrival rate roughly every 40s of silence
	rateDecayTau = 60 * time.Second
	// leadSafety over-provisions against the measured lead: arrivals are bursty, not uniform
	leadSafety = 2.0
	// warmShrinkDwell holds a decayed target for one decay period before trimming, so a fluctuating load does not churn the pool
	warmShrinkDwell = rateDecayTau
)

// pool fields are guarded by the manager mutex, except claimClones and the immutable key and hash.
type pool struct {
	key  types.PoolKey
	hash string

	// floor and warmMax bound the demand-adaptive target
	floor           int
	warmMax         int
	idle            time.Duration
	archiveAfter    time.Duration
	archiveDelete   time.Duration
	rate            float64
	lead            time.Duration
	lastArrival     time.Time
	binStart        time.Time
	binCount        int
	overTargetSince time.Time

	goldenDir string
	imageID   string
	// goldenGen counts installs, so a clone of a retired golden never lands after a rebuild at the same path.
	goldenGen   uint64
	building    bool
	nextBuild   time.Time
	removed     bool // dropped from the desired set while unsettled; swept by refillOnce
	warm        []*types.Sandbox
	refilling   int
	claimClones atomic.Int32

	// Without a streak gate a node-wide dead cause is retried every tick at full concurrency
	refillFails     int
	nextRefill      time.Time
	cloneFails      int
	goldenRetiredAt time.Time
}

func newPool(spec config.PoolSpec) *pool {
	p := &pool{key: spec.PoolKey, hash: spec.Hash()}
	p.applySpec(spec)
	return p
}

func (p *pool) settled() bool {
	return !p.building && p.refilling == 0 && p.claimClones.Load() == 0
}

// refillGated reports whether a refill may not spawn: backoff wait, or one probe in flight.
func (p *pool) refillGated(now time.Time) bool {
	return now.Before(p.nextRefill) || (p.refillFails >= refillFailStreak && p.refilling > 0)
}

// noteRefillResult records one refill outcome and reports whether it started a backoff.
func (p *pool) noteRefillResult(now time.Time, failed bool) bool {
	if !failed {
		p.refillFails, p.nextRefill = 0, time.Time{}
		return false
	}
	p.refillFails++
	if p.refillFails < refillFailStreak {
		return false
	}
	first := !now.Before(p.nextRefill)
	backoff := refillBackoffBase << min(p.refillFails-refillFailStreak, 16)
	p.nextRefill = now.Add(min(backoff, refillBackoffMax))
	return first
}

func (p *pool) countClone(ctx context.Context, gen uint64, cloneErr error, capReason string) bool {
	if ctx.Err() != nil || capReason != "" || gen != p.goldenGen || errors.Is(cloneErr, engine.ErrStopping) {
		return false
	}
	if cloneErr == nil {
		p.cloneFails = 0
	} else {
		p.cloneFails++
	}
	return true
}

func (p *pool) applySpec(spec config.PoolSpec) {
	p.floor = spec.Warm
	p.warmMax = spec.WarmMax
	p.idle = time.Duration(spec.IdleHibernateSeconds) * time.Second
	p.archiveAfter = time.Duration(spec.ArchiveAfterSeconds) * time.Second
	p.archiveDelete = time.Duration(spec.ArchiveDeleteAfterSeconds) * time.Second
}

func (p *pool) popWarm() *types.Sandbox {
	n := len(p.warm)
	if n == 0 {
		return nil
	}
	sb := p.warm[n-1]
	p.warm = p.warm[:n-1]
	return sb
}

func (p *pool) trimWarm(target int) []string {
	var trim []string
	for len(p.warm) > target {
		n := len(p.warm) - 1
		trim = append(trim, p.warm[n].VMName)
		p.warm = p.warm[:n]
	}
	return trim
}

// noteArrival counts one claim; a bin that has spanned rateBin folds its arrivals per second into the rate EWMA.
func (p *pool) noteArrival(now time.Time) {
	switch elapsed := now.Sub(p.binStart); {
	case p.binStart.IsZero():
		p.binStart = now
	case elapsed >= rateBin:
		decayed := p.rate * math.Exp(-elapsed.Seconds()/rateDecayTau.Seconds())
		p.rate = ewmaAlpha*(float64(p.binCount)/elapsed.Seconds()) + (1-ewmaAlpha)*decayed
		p.binStart, p.binCount = now, 0
	}
	p.binCount++
	p.lastArrival = now
}

func (p *pool) noteLead(d time.Duration) {
	if p.lead == 0 {
		p.lead = d
		return
	}
	p.lead = time.Duration(ewmaAlpha*float64(d) + (1-ewmaAlpha)*float64(p.lead))
}

// effectiveTarget raises the floor toward warmMax by rate × lead.
func (p *pool) effectiveTarget(now time.Time) int {
	if p.warmMax <= 0 {
		return p.floor
	}
	rate := p.rate
	if !p.lastArrival.IsZero() {
		rate *= math.Exp(-now.Sub(p.lastArrival).Seconds() / rateDecayTau.Seconds())
	}
	dynamic := int(math.Ceil(rate * p.lead.Seconds() * leadSafety))
	if p.lead == 0 && rate > 0 {
		dynamic = 1
	}
	if dynamic == 1 && rate*rateDecayTau.Seconds() < 1 {
		dynamic = 0
	}
	return max(p.floor, min(dynamic, p.warmMax))
}

// shrink trims the pool to its target once the warm count has stayed above it for warmShrinkDwell, returning the trimmed VM names.
func (p *pool) shrink(now time.Time) []string {
	target := p.effectiveTarget(now)
	switch {
	case len(p.warm) <= target:
		p.overTargetSince = time.Time{}
	case p.overTargetSince.IsZero():
		p.overTargetSince = now
	case now.Sub(p.overTargetSince) >= warmShrinkDwell:
		p.overTargetSince = time.Time{}
		return p.trimWarm(target)
	}
	return nil
}
