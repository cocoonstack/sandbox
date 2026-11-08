package tunnet

import (
	"net/netip"
	"slices"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestSetRatesPicksEachDirectionsBucket(t *testing.T) {
	tests := []struct {
		name           string
		node           float64
		setUp, setDown float64
		up, down       rate.Limit
	}{
		{"no node limit, no rates", 0, 0, 0, rate.Inf, rate.Inf},
		{"no node limit, up only", 0, 2000, 0, 2000, rate.Inf},
		{"no node limit, down only", 0, 0, 3000, rate.Inf, 3000},
		{"no node limit, both", 0, 2000, 3000, 2000, 3000},
		{"node limit, no rates", 1000, 0, 0, 1000, 1000},
		{"node limit, up only", 1000, 2000, 0, 2000, 1000},
		{"node limit, down only", 1000, 0, 3000, 1000, 3000},
		{"node limit, both", 1000, 2000, 3000, 2000, 3000},
	}
	t0 := time.Unix(1_700_000_000, 0)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFlows(Claim{Limits: Limits{BytesPerSecond: tt.node}})
			f.setRates(tt.setUp, tt.setDown)
			for _, d := range []struct {
				name string
				dir  *direction
				want rate.Limit
			}{{"up", f.up, tt.up}, {"down", f.down, tt.down}} {
				if got := limitOf(d.dir); got != d.want {
					t.Errorf("%s rate = %v, want %v", d.name, got, d.want)
				}
				admitsTwice := f.charge(d.dir, 1, 2*MaxFrame, t0) && f.charge(d.dir, 1, 1, t0)
				if admitsTwice != (d.want == rate.Inf) {
					t.Errorf("%s admits a burst and one more byte = %v, want %v", d.name, admitsTwice, d.want == rate.Inf)
				}
			}
		})
	}
}

func TestSetRatesToZeroRestoresTheNodeDefault(t *testing.T) {
	tests := []struct {
		name string
		node float64
		want rate.Limit
	}{
		{"node has no limit", 0, rate.Inf},
		{"node has a limit", 1000, 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFlows(Claim{Limits: Limits{BytesPerSecond: tt.node}})
			f.setRates(5000, 7000)
			if up, down := limitOf(f.up), limitOf(f.down); up != 5000 || down != 7000 {
				t.Fatalf("rates after set = %v up, %v down, want 5000 and 7000", up, down)
			}
			f.setRates(0, 0)
			if up, down := limitOf(f.up), limitOf(f.down); up != tt.want || down != tt.want {
				t.Errorf("rates after reset = %v up, %v down, want %v both", up, down, tt.want)
			}
		})
	}
}

func TestSetRatesKeepsTheBucketOfAnUnchangedRate(t *testing.T) {
	f := newFlows(Claim{})
	f.setRates(1000, 2000)
	t0 := time.Unix(1_700_000_000, 0)
	if !f.charge(f.up, 1, 2*MaxFrame, t0) || !f.charge(f.down, 1, 2*MaxFrame, t0) {
		t.Fatal("a fresh bucket refused its burst")
	}
	steps := []struct {
		name           string
		setUp, setDown float64
		wantUp         bool
		wantDown       bool
	}{
		{"same rates again", 1000, 2000, false, false},
		{"up changed, down kept", 1500, 2000, true, false},
		{"same rates after the change", 1500, 2000, false, false},
		{"down changed, up kept", 1500, 3000, false, true},
	}
	for _, s := range steps {
		f.setRates(s.setUp, s.setDown)
		if got := f.charge(f.up, 1, 2*MaxFrame, t0); got != s.wantUp {
			t.Errorf("%s: up burst admitted = %v, want %v", s.name, got, s.wantUp)
		}
		if got := f.charge(f.down, 1, 2*MaxFrame, t0); got != s.wantDown {
			t.Errorf("%s: down burst admitted = %v, want %v", s.name, got, s.wantDown)
		}
	}
}

func TestSetRatesDropsPastTheNewBucketAndKeepsFlowsRunning(t *testing.T) {
	var records []FlowRecord
	f := newFlows(Claim{
		Allow:  func(netip.Addr, uint16) bool { return true },
		Record: func(r FlowRecord) { records = append(records, r) },
	})
	t0 := time.Unix(1_700_000_000, 0)
	out := Packet{Proto: ProtoTCP, Dst: remote, DstPort: 443, SrcPort: 40000}
	if drop := f.outbound(out, 100, t0); drop != DropNone {
		t.Fatalf("opening packet dropped: %v", drop)
	}
	if !f.charge(f.up, 1, 4*MaxFrame, t0) || !f.charge(f.down, 1, 4*MaxFrame, t0) {
		t.Fatal("a claim with no byte limit refused a burst")
	}
	f.setRates(1000, 0)
	steps := []struct {
		name string
		dir  *direction
		n    int
		at   time.Time
		want bool
	}{
		{"up burst", f.up, 2 * MaxFrame, t0, true},
		{"up past the burst", f.up, 1, t0, false},
		{"up refilled one second", f.up, 1000, t0.Add(time.Second), true},
		{"up past the refill", f.up, 1, t0.Add(time.Second), false},
		{"down unaffected", f.down, 4 * MaxFrame, t0, true},
	}
	for _, s := range steps {
		if got := f.charge(s.dir, 1, s.n, s.at); got != s.want {
			t.Errorf("%s: charge of %d = %v, want %v", s.name, s.n, got, s.want)
		}
	}
	if drop := f.outbound(out, 50, t0.Add(2*time.Second)); drop != DropNone {
		t.Errorf("flow packet after SetRates dropped: %v", drop)
	}
	f.sweep(t0.Add(3*time.Second), true)
	if len(records) != 1 || records[0].Up != 150 {
		t.Errorf("records = %+v, want the one flow opened before SetRates with 150 up bytes", records)
	}
}

func TestRouterSetRatesDropsForwardedAndInboundPacketsPerDirection(t *testing.T) {
	u := &fakeUnder{out: make(chan sent, 64)}
	r := New(u, testOverlay(1450))
	r.Arm(Claim{ID: "c1", Addr: claimAddr, Allow: func(netip.Addr, uint16) bool { return true }})
	a := r.claims[claimAddr]
	l := &link{r: r, a: a, mtu: 1450}
	if !a.attach(l) {
		t.Fatal("attach failed")
	}
	inbound := func() []byte { return append(make([]byte, VnetHdrLen), reply(remote)[Overhead:]...) }
	forward := func() Drop { return l.forward(frame(ProtoTCP, GuestAddr, remote, false)) }
	admit := func() Drop { _, drop := r.admit(inbound()); return drop }
	drain := func(d *direction) { a.flows.charge(d, 1, 2*MaxFrame, time.Now()) }

	if got := forward(); got != DropNone {
		t.Fatalf("forward with no byte limit = %v, want none", got)
	}
	r.SetRates(claimAddr, 1, 0)
	drain(a.flows.up)
	if got := forward(); got != DropLimit {
		t.Errorf("forward past the up bucket = %v, want limit", got)
	}
	if got := admit(); got != DropNone {
		t.Errorf("inbound while only up is limited = %v, want none", got)
	}
	r.SetRates(claimAddr, 0, 1)
	drain(a.flows.down)
	if got := admit(); got != DropLimit {
		t.Errorf("inbound past the down bucket = %v, want limit", got)
	}
	if got := forward(); got != DropNone {
		t.Errorf("forward while only down is limited = %v, want none", got)
	}
	r.SetRates(claimAddr, 0, 0)
	if got, got2 := forward(), admit(); got != DropNone || got2 != DropNone {
		t.Errorf("after reset forward = %v and inbound = %v, want none for both", got, got2)
	}
}

func TestSetRatesOnAnUnknownAddressChangesNothing(t *testing.T) {
	u := &fakeUnder{out: make(chan sent, 64)}
	r := New(u, testOverlay(1450))
	stranger := netip.MustParseAddr("100.80.0.77")
	r.SetRates(claimAddr, 1, 1)
	r.Arm(Claim{ID: "c1", Addr: claimAddr})
	r.SetRates(stranger, 1, 1)
	a := r.claims[claimAddr]
	if up, down := limitOf(a.flows.up), limitOf(a.flows.down); up != rate.Inf || down != rate.Inf {
		t.Errorf("armed claim rates = %v up, %v down after SetRates on another address, want no limit", up, down)
	}
	r.Disarm(claimAddr)
	r.SetRates(claimAddr, 1, 1)
	if up, down := limitOf(a.flows.up), limitOf(a.flows.down); up != rate.Inf || down != rate.Inf {
		t.Errorf("disarmed claim rates = %v up, %v down after SetRates, want no limit", up, down)
	}
}

func TestSweepWritesPartialRowsForALiveFlowEvery(t *testing.T) {
	steps := []sweepStep{
		{at: 0, up: 100},
		{at: 4, down: 300},
		{at: 10, want: []rowWant{{partial: true, up: 100, down: 300, newUp: 100, newDown: 300}}},
		{at: 14, up: 5},
		{at: 20, want: []rowWant{{partial: true, up: 105, down: 300, newUp: 5}}},
		{at: 31},
		{at: 32, up: 7, down: 3, all: true, want: []rowWant{{up: 112, down: 303, newUp: 7, newDown: 3}}},
	}
	rows := runSweeps(t, 10*time.Second, ProtoTCP, steps)
	var newUp, newDown int64
	for _, r := range rows {
		newUp += r.newUp
		newDown += r.newDown
	}
	if last := rows[len(rows)-1]; newUp != last.up || newDown != last.down {
		t.Errorf("new bytes sum to %d up and %d down, want the final totals %d and %d", newUp, newDown, last.up, last.down)
	}
}

func TestSweepWithoutEveryWritesOneRowWhenTheFlowEnds(t *testing.T) {
	steps := []sweepStep{
		{at: 0, up: 100},
		{at: 10, down: 300},
		{at: 100},
		{at: 1000, up: 5},
		{at: 2000},
		{at: 2001, all: true, want: []rowWant{{up: 105, down: 300, newUp: 105, newDown: 300}}},
	}
	runSweeps(t, 0, ProtoTCP, steps)
}

func TestSweepIdleEndWritesTheRemainderAsTheFinalRow(t *testing.T) {
	steps := []sweepStep{
		{at: 0, up: 10},
		{at: 10, want: []rowWant{{partial: true, up: 10, newUp: 10}}},
		{at: 12, up: 4},
		{at: 13},
		{at: 12 + int(idleUDP/time.Second), want: []rowWant{{up: 14, newUp: 4}}},
	}
	runSweeps(t, 10*time.Second, ProtoUDP, steps)
}

func TestSweepReportsAnEndedTCPFlowAfterOneSweepPeriodWithoutEvery(t *testing.T) {
	steps := []sweepStep{
		{at: 0, up: 100, down: 50},
		{at: 1, up: 10, end: true},
		{at: 6},
		{at: 11, want: []rowWant{{partial: true, up: 110, down: 50, newUp: 110, newDown: 50}}},
		{at: 61},
		{at: 1 + int(idleTCPEnd/time.Second), want: []rowWant{{up: 110, down: 50}}},
	}
	runSweeps(t, 0, ProtoTCP, steps)
}

func TestSweepReportsLateBytesOfAnEndedTCPFlowInTheirOwnRow(t *testing.T) {
	steps := []sweepStep{
		{at: 0, up: 100, down: 50},
		{at: 1, up: 10, end: true},
		{at: 11, want: []rowWant{{partial: true, up: 110, down: 50, newUp: 110, newDown: 50}}},
		{at: 31, up: 7},
		{at: 36},
		{at: 41, want: []rowWant{{partial: true, up: 117, down: 50, newUp: 7}}},
		{at: 100},
		{at: 31 + int(idleTCPEnd/time.Second), want: []rowWant{{up: 117, down: 50}}},
	}
	runSweeps(t, 0, ProtoTCP, steps)
}

func TestSweepWritesNothingEarlyForAnUnendedTCPFlowWithoutEvery(t *testing.T) {
	steps := []sweepStep{
		{at: 0, up: 100, down: 50},
		{at: 10},
		{at: 60},
		{at: 600},
		{at: int(idleTCP/time.Second) - 1},
		{at: int(idleTCP / time.Second), want: []rowWant{{up: 100, down: 50, newUp: 100, newDown: 50}}},
	}
	runSweeps(t, 0, ProtoTCP, steps)
}

type rowWant struct {
	partial                  bool
	up, down, newUp, newDown int64
}

type sweepStep struct {
	at       int
	up, down int
	end, all bool
	want     []rowWant
}

func limitOf(d *direction) rate.Limit {
	if b := d.bytes.Load(); b != nil {
		return b.Limit()
	}
	return rate.Inf
}

func runSweeps(t *testing.T, every time.Duration, proto uint8, steps []sweepStep) []rowWant {
	t.Helper()
	var records []FlowRecord
	f := newFlows(Claim{
		Allow:  func(netip.Addr, uint16) bool { return true },
		Record: func(r FlowRecord) { records = append(records, r) },
		Every:  every,
	})
	t0 := time.Unix(1_700_000_000, 0)
	out := Packet{Proto: proto, Dst: remote, DstPort: 443, SrcPort: 40000}
	in := Packet{Proto: proto, Src: remote, SrcPort: 443, DstPort: 40000}
	var all []rowWant
	for i, s := range steps {
		now := t0.Add(time.Duration(s.at) * time.Second)
		if s.up > 0 {
			out.End = s.end
			if drop := f.outbound(out, s.up, now); drop != DropNone {
				t.Fatalf("step %d: outbound dropped: %v", i, drop)
			}
		}
		if s.down > 0 {
			if drop := f.inbound(in, nil, s.down, now); drop != DropNone {
				t.Fatalf("step %d: inbound dropped: %v", i, drop)
			}
		}
		records = nil
		f.sweep(now, s.all)
		got := rowsOf(records)
		if !slices.Equal(got, s.want) {
			t.Fatalf("step %d (sweep at +%ds, all=%v): rows %+v, want %+v", i, s.at, s.all, got, s.want)
		}
		all = append(all, got...)
	}
	return all
}

func rowsOf(records []FlowRecord) []rowWant {
	var rows []rowWant
	for _, r := range records {
		rows = append(rows, rowWant{partial: r.Partial, up: r.Up, down: r.Down, newUp: r.NewUp, newDown: r.NewDown})
	}
	return rows
}
