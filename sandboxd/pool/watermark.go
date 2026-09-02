package pool

import (
	"math"
	"time"
)

const (
	// ewmaAlpha weights the newest observation in the arrival-rate and provision-lead averages
	ewmaAlpha = 0.3
	// rateDecayTau halves the observed arrival rate roughly every 40s of silence
	rateDecayTau = 60 * time.Second
	// leadSafety over-provisions against the measured lead: arrivals are bursty, not uniform
	leadSafety = 2.0
)

// noteArrival folds one claim into the arrival-rate EWMA. Caller holds the manager mutex.
func (p *pool) noteArrival(now time.Time) {
	if !p.lastArrival.IsZero() {
		if dt := now.Sub(p.lastArrival).Seconds(); dt > 0 {
			p.rate = ewmaAlpha*(1/dt) + (1-ewmaAlpha)*p.rate
		}
	}
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
	return max(p.floor, min(dynamic, p.warmMax))
}
