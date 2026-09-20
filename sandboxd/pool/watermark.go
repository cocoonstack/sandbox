package pool

import (
	"math"
	"time"
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

// noteArrival counts one claim; a bin that has spanned rateBin folds its arrivals per second into the rate EWMA. Caller holds the manager mutex.
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

// noteLead folds one provision duration into the lead EWMA. Caller holds the manager mutex.
func (p *pool) noteLead(d time.Duration) {
	if p.lead == 0 {
		p.lead = d
		return
	}
	p.lead = time.Duration(ewmaAlpha*float64(d) + (1-ewmaAlpha)*float64(p.lead))
}

// effectiveTarget raises the floor toward warmMax by rate × lead. Caller holds the manager mutex.
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

// shrink trims the pool to its target once the warm count has stayed above it for warmShrinkDwell, returning the trimmed VM names. Caller holds the manager mutex.
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
