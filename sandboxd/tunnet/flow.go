package tunnet

import (
	"cmp"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	icmpEchoReply   = 0
	icmpUnreachable = 3
	icmpTimeExceed  = 11

	idleTCP    = time.Hour
	idleTCPEnd = 2 * time.Minute
	idleUDP    = 2 * time.Minute
	idleOther  = 30 * time.Second

	// maxSegs bounds the packets one GSO frame may put on the wire: 64 KiB at a 512-byte MSS; it is also the packet bucket's least burst.
	maxSegs = 128
)

// AllowFunc judges a new flow's destination; port 0 asks for a rule that covers every port.
type AllowFunc func(dst netip.Addr, port uint16) bool

// RecordFunc writes one flow's audit row.
type RecordFunc func(FlowRecord)

// Limits caps one claim's traffic on the tun path; zero leaves a dimension open.
type Limits struct {
	BytesPerSecond    float64 `json:"bytes_per_second,omitzero"`
	PacketsPerSecond  float64 `json:"packets_per_second,omitzero"`
	NewFlowsPerSecond float64 `json:"new_flows_per_second,omitzero"`
	MaxFlows          int     `json:"max_flows,omitzero"`
	QuotaBytes        int64   `json:"quota_bytes,omitzero"`
}

// FlowRecord is one flow's audit row, written when the flow ends; a Partial row reports the bytes since its last row of a flow whose entry still lives.
type FlowRecord struct {
	Proto   uint8
	Dst     netip.Addr
	Port    uint16
	Allowed bool
	Partial bool
	// Up and Down are the flow's totals; NewUp and NewDown are the bytes since its last row.
	Up, Down       int64
	NewUp, NewDown int64
	Start          time.Time
	End            time.Time
}

// flowKey is a flow in the outbound direction; the claim is the source.
type flowKey struct {
	proto uint8
	dst   netip.Addr
	dport uint16
	sport uint16
}

type peerKey struct {
	proto uint8
	addr  netip.Addr
}

type flow struct {
	allowed bool
	start   time.Time
	up      atomic.Int64
	down    atomic.Int64
	last    atomic.Int64
	ended   atomic.Bool

	// reported and the counts are guarded by flows.mu.
	reported                 time.Time
	reportedUp, reportedDown int64
}

func (fl *flow) carry(bytes *atomic.Int64, p Packet, n int, now time.Time) Drop {
	fl.last.Store(now.UnixNano())
	if !fl.allowed {
		return DropPolicy
	}
	bytes.Add(int64(n))
	if p.End {
		fl.ended.Store(true)
	}
	return DropNone
}

// record builds the flow's row and marks its bytes reported; the caller holds flows.mu.
func (fl *flow) record(key flowKey, now time.Time, partial bool) FlowRecord {
	up, down := fl.up.Load(), fl.down.Load()
	r := FlowRecord{
		Proto: key.proto, Dst: key.dst, Port: key.dport, Allowed: fl.allowed, Partial: partial,
		Up: up, Down: down, NewUp: up - fl.reportedUp, NewDown: down - fl.reportedDown,
		Start: fl.start, End: time.Unix(0, fl.last.Load()),
	}
	fl.reported, fl.reportedUp, fl.reportedDown = now, up, down
	return r
}

type direction struct {
	bytes   atomic.Pointer[rate.Limiter]
	packets *rate.Limiter
}

func newDirection(l Limits) *direction {
	d := &direction{}
	d.setBytes(l.BytesPerSecond)
	if l.PacketsPerSecond > 0 {
		d.packets = rate.NewLimiter(rate.Limit(l.PacketsPerSecond), max(int(l.PacketsPerSecond), maxSegs))
	}
	return d
}

// setBytes keeps the bucket of an unchanged rate, so a repeated cap grants no new burst.
func (d *direction) setBytes(perSecond float64) {
	cur := d.bytes.Load()
	switch {
	case perSecond <= 0:
		d.bytes.Store(nil)
	case cur == nil || float64(cur.Limit()) != perSecond:
		d.bytes.Store(rate.NewLimiter(rate.Limit(perSecond), max(int(perSecond), 2*MaxFrame)))
	}
}

func (d *direction) admit(now time.Time, packets, bytes int) bool {
	b := d.bytes.Load()
	return (d.packets == nil || d.packets.AllowN(now, packets)) && (b == nil || b.AllowN(now, bytes))
}

// flows is one claim's flow table and limits: the claim's reader adds, the VXLAN receiver looks up.
type flows struct {
	limits Limits
	used   *atomic.Int64
	allow  AllowFunc
	record RecordFunc
	every  time.Duration

	newFlows *rate.Limiter
	up, down *direction

	mu    sync.RWMutex
	table map[flowKey]*flow
	peers map[peerKey]int
}

func newFlows(c Claim) *flows {
	f := &flows{
		limits: c.Limits,
		used:   c.Used,
		allow:  c.Allow,
		record: c.Record,
		every:  c.Every,
		up:     newDirection(c.Limits),
		down:   newDirection(c.Limits),
		table:  map[flowKey]*flow{},
		peers:  map[peerKey]int{},
	}
	if r := c.Limits.NewFlowsPerSecond; r > 0 {
		f.newFlows = rate.NewLimiter(rate.Limit(r), max(int(r), 1))
	}
	if f.used == nil {
		f.used = new(atomic.Int64)
	}
	return f
}

// outbound decides a guest packet the caller already charged, opening its flow on first sight.
func (f *flows) outbound(p Packet, n int, now time.Time) Drop {
	key := flowKey{proto: p.Proto, dst: p.Dst}
	switch p.Proto {
	case ProtoTCP, ProtoUDP:
		key.dport, key.sport = p.DstPort, p.SrcPort
	case ProtoICMP:
	default:
		return DropProto
	}
	for range 2 {
		f.mu.RLock()
		if fl := f.table[key]; fl != nil {
			drop := fl.carry(&fl.up, p, n, now)
			f.mu.RUnlock()
			return drop
		}
		f.mu.RUnlock()
		if f.open(key, now) == nil {
			break
		}
	}
	return DropLimit
}

// inbound passes a packet only for a flow the claim opened, and ICMP errors about one, then charges it.
func (f *flows) inbound(p Packet, frame []byte, n int, now time.Time) Drop {
	for resumed := false; ; resumed = true {
		f.mu.RLock()
		fl, ok := f.match(p, frame)
		if ok || fl != nil || resumed || p.Proto != ProtoTCP || p.Open {
			drop := f.admitIn(fl, ok, p, n, now)
			f.mu.RUnlock()
			return drop
		}
		f.mu.RUnlock()
		if !f.allow(p.Src, p.SrcPort) {
			return DropPolicy
		}
		if f.open(flowKey{proto: ProtoTCP, dst: p.Src, dport: p.SrcPort, sport: p.DstPort}, now) == nil {
			return DropLimit
		}
	}
}

// admitIn charges an inbound packet match found and counts it on its flow; the caller holds mu, so a sweep never records the flow before the count.
func (f *flows) admitIn(fl *flow, ok bool, p Packet, n int, now time.Time) Drop {
	if !ok {
		return DropPolicy
	}
	if !f.charge(f.down, 1, n, now) {
		return DropLimit
	}
	if fl != nil {
		_ = fl.carry(&fl.down, p, n, now)
	}
	return DropNone
}

// match finds the allowed flow an inbound packet answers; an ICMP error about a peer passes without one.
func (f *flows) match(p Packet, frame []byte) (*flow, bool) {
	key := flowKey{proto: p.Proto, dst: p.Src}
	switch p.Proto {
	case ProtoTCP, ProtoUDP:
		key.dport, key.sport = p.SrcPort, p.DstPort
	case ProtoICMP:
		if icmpError(p) {
			inner, ok := innerDst(frame)
			return nil, ok && (f.peers[peerKey{ProtoTCP, inner}] > 0 || f.peers[peerKey{ProtoUDP, inner}] > 0 || f.peers[peerKey{ProtoICMP, inner}] > 0)
		}
		if p.ICMPType != icmpEchoReply {
			return nil, false
		}
	default:
		return nil, false
	}
	fl := f.table[key]
	return fl, fl != nil && fl.allowed
}

// charge takes packets and bytes from the direction's buckets and the bytes from the quota.
func (f *flows) charge(d *direction, packets, n int, now time.Time) bool {
	if !d.admit(now, packets, n) {
		return false
	}
	if q := f.limits.QuotaBytes; q > 0 && f.used.Load()+int64(n) > q {
		return false
	}
	f.used.Add(int64(n))
	return true
}

// setRates swaps both byte buckets; a zero rate falls back to the node's.
func (f *flows) setRates(up, down float64) {
	f.up.setBytes(cmp.Or(up, f.limits.BytesPerSecond))
	f.down.setBytes(cmp.Or(down, f.limits.BytesPerSecond))
}

func (f *flows) open(key flowKey, now time.Time) *flow {
	f.mu.Lock()
	defer f.mu.Unlock()
	if fl := f.table[key]; fl != nil {
		return fl
	}
	if f.limits.MaxFlows > 0 && len(f.table) >= f.limits.MaxFlows {
		return nil
	}
	if f.newFlows != nil && !f.newFlows.AllowN(now, 1) {
		return nil
	}
	fl := &flow{allowed: f.allow(key.dst, key.dport), start: now, reported: now}
	fl.last.Store(now.UnixNano())
	f.table[key] = fl
	if fl.allowed {
		f.peers[peerKey{key.proto, key.dst}]++
	}
	return fl
}

func (f *flows) sweep(now time.Time, all bool) {
	var rows []FlowRecord
	f.mu.Lock()
	for key, fl := range f.table {
		idle := now.Sub(time.Unix(0, fl.last.Load()))
		if !all && idle < idleFor(key.proto, fl) {
			due := (f.every > 0 && now.Sub(fl.reported) >= f.every) || (fl.ended.Load() && idle >= sweepEvery)
			if due && fl.allowed && fl.up.Load()+fl.down.Load() > fl.reportedUp+fl.reportedDown {
				rows = append(rows, fl.record(key, now, true))
			}
			continue
		}
		delete(f.table, key)
		if fl.allowed {
			if pk := (peerKey{key.proto, key.dst}); f.peers[pk] <= 1 {
				delete(f.peers, pk)
			} else {
				f.peers[pk]--
			}
		}
		rows = append(rows, fl.record(key, now, false))
	}
	f.mu.Unlock()
	if f.record != nil {
		for _, r := range rows {
			f.record(r)
		}
	}
}

func idleFor(proto uint8, fl *flow) time.Duration {
	switch {
	case proto == ProtoTCP && fl.ended.Load():
		return idleTCPEnd
	case proto == ProtoTCP && fl.allowed:
		return idleTCP
	case proto == ProtoUDP && fl.allowed:
		return idleUDP
	default:
		return idleOther
	}
}

func icmpError(p Packet) bool {
	return p.Proto == ProtoICMP && (p.ICMPType == icmpUnreachable || p.ICMPType == icmpTimeExceed)
}

// innerDst is the destination of the packet an ICMP error quotes.
func innerDst(frame []byte) (netip.Addr, bool) {
	ip := frame[VnetHdrLen:]
	inner := int(ip[0]&0x0f)*4 + 8
	if len(ip) < inner+20 || ip[inner]>>4 != 4 {
		return netip.Addr{}, false
	}
	return netip.AddrFrom4([4]byte(ip[inner+dstAt : inner+dstAt+4])), true
}
