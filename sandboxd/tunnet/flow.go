package tunnet

import (
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

	// packetBurst lets one GSO frame of small segments through a low packet rate.
	packetBurst = 128
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

// FlowRecord is one flow's audit row, written when the flow ends.
type FlowRecord struct {
	Proto   uint8
	Dst     netip.Addr
	Port    uint16
	Allowed bool
	Up      int64
	Down    int64
	Start   time.Time
	End     time.Time
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
}

type direction struct {
	bytes, packets *rate.Limiter
}

func newDirection(l Limits) *direction {
	d := &direction{}
	if l.BytesPerSecond > 0 {
		d.bytes = rate.NewLimiter(rate.Limit(l.BytesPerSecond), max(int(l.BytesPerSecond), 2*MaxFrame))
	}
	if l.PacketsPerSecond > 0 {
		d.packets = rate.NewLimiter(rate.Limit(l.PacketsPerSecond), max(int(l.PacketsPerSecond), packetBurst))
	}
	return d
}

func (d *direction) admit(now time.Time, packets, bytes int) bool {
	return (d.packets == nil || d.packets.AllowN(now, packets)) && (d.bytes == nil || d.bytes.AllowN(now, bytes))
}

// flows is one claim's flow table and limits: the claim's reader adds, the VXLAN receiver looks up.
type flows struct {
	limits Limits
	used   *atomic.Int64
	allow  AllowFunc
	record RecordFunc

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
	f.mu.RLock()
	fl := f.table[key]
	f.mu.RUnlock()
	if fl == nil {
		if fl = f.open(key, now); fl == nil {
			return DropLimit
		}
	}
	fl.last.Store(now.UnixNano())
	if !fl.allowed {
		return DropPolicy
	}
	fl.up.Add(int64(n))
	if p.End {
		fl.ended.Store(true)
	}
	return DropNone
}

// inbound passes a packet only for a flow the claim opened, and ICMP errors about one, then charges it.
func (f *flows) inbound(p Packet, frame []byte, n int, now time.Time) Drop {
	fl, ok := f.match(p, frame)
	if !ok {
		return DropPolicy
	}
	if !f.charge(f.down, 1, n, now) {
		return DropLimit
	}
	if fl != nil {
		fl.down.Add(int64(n))
		fl.last.Store(now.UnixNano())
		if p.End {
			fl.ended.Store(true)
		}
	}
	return DropNone
}

// match finds the allowed flow an inbound packet answers; an ICMP error about a peer passes without one.
func (f *flows) match(p Packet, frame []byte) (*flow, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	key := flowKey{proto: p.Proto, dst: p.Src}
	switch p.Proto {
	case ProtoTCP, ProtoUDP:
		key.dport, key.sport = p.SrcPort, p.DstPort
	case ProtoICMP:
		if p.ICMPType == icmpUnreachable || p.ICMPType == icmpTimeExceed {
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
	fl := &flow{allowed: f.allow(key.dst, key.dport), start: now}
	fl.last.Store(now.UnixNano())
	f.table[key] = fl
	if fl.allowed {
		f.peers[peerKey{key.proto, key.dst}]++
	}
	return fl
}

// sweep ends idle flows, or every flow when all is set, and writes their records.
func (f *flows) sweep(now time.Time, all bool) {
	var ended []FlowRecord
	f.mu.Lock()
	for key, fl := range f.table {
		idle := now.Sub(time.Unix(0, fl.last.Load()))
		if !all && idle < idleFor(key.proto, fl) {
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
		ended = append(ended, FlowRecord{
			Proto: key.proto, Dst: key.dst, Port: key.dport, Allowed: fl.allowed,
			Up: fl.up.Load(), Down: fl.down.Load(), Start: fl.start, End: time.Unix(0, fl.last.Load()),
		})
	}
	f.mu.Unlock()
	if f.record != nil {
		for _, r := range ended {
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

// innerDst is the destination of the packet an ICMP error quotes.
func innerDst(frame []byte) (netip.Addr, bool) {
	ip := frame[VnetHdrLen:]
	inner := int(ip[0]&0x0f)*4 + 8
	if len(ip) < inner+20 || ip[inner]>>4 != 4 {
		return netip.Addr{}, false
	}
	return netip.AddrFrom4([4]byte(ip[inner+dstAt : inner+dstAt+4])), true
}
