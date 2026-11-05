package tunnet

import (
	"bufio"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"maps"
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Drop reasons, one counter each.
	DropNone Drop = iota
	DropMalformed
	DropNotIPv4
	DropOptions
	DropFragment
	DropProto
	DropGSO
	DropPolicy
	DropNoExit
	DropUnknownClaim
	DropQueueFull
	DropSize
	DropLimit
	DropSend
	DropSender
	dropKinds

	helloTimeout = 5 * time.Second
	sweepEvery   = 10 * time.Second
	refreshEvery = 45 * time.Second
	// sendQueue bounds the frames waiting for one guest, about 64 MiB at worst; a full queue drops, as a congested link would.
	sendQueue = 1024
	readBuf   = 256 << 10
	// segmentable is what the segmenter takes from a guest.
	segmentable = OffloadCSUM | OffloadTSO4 | OffloadTSOECN | OffloadUSO4
)

var (
	// GuestRange holds the fixed guest address; no overlay subnet may overlap it.
	GuestRange = netip.MustParsePrefix("100.64.0.0/30")

	seed      = maphash.MakeSeed()
	dropNames = [dropKinds]string{"", "malformed", "not_ipv4", "options", "fragment", "proto", "gso", "policy", "no_exit", "unknown_claim", "queue_full", "size", "limit", "send", "sender"}
)

// Drop names why a packet was not carried.
type Drop uint8

// String is the reason's metric label; DropNone has none.
func (d Drop) String() string {
	if d >= dropKinds {
		return ""
	}
	return dropNames[d]
}

// Underlay carries VXLAN datagrams: the transport sandboxd's exits listen on, kept apart from identity, policy and rewrite.
type Underlay interface {
	// Send writes the datagrams packed in b, each seg bytes but the last, to dst; flow picks the outer source port.
	Send(b []byte, seg int, dst netip.AddrPort, flow uint64) error
	// Recv reads one batch and calls each per datagram with its sender.
	Recv(each func(d []byte, from netip.AddrPort)) error
}

// Exit is one exit's VTEP and the MAC of its VXLAN device.
type Exit struct {
	VTEP netip.Addr
	MAC  [6]byte
}

// Overlay is the reloadable part of the node's tun config.
type Overlay struct {
	Subnet netip.Prefix
	VNI    uint32
	Port   uint16
	MTU    uint16
	// Exits maps a profile label to its exit; a claim without a label uses "default".
	Exits map[string]Exit
}

func (o *Overlay) exit(profile string) (Exit, bool) {
	e, ok := o.Exits[cmp.Or(profile, "default")]
	return e, ok
}

// overlay is a loaded Overlay with the set of exit VTEPs the router accepts VXLAN from.
type overlay struct {
	*Overlay
	vteps map[netip.Addr]bool
}

func load(o *Overlay) *overlay {
	v := &overlay{Overlay: o, vteps: make(map[netip.Addr]bool, len(o.Exits))}
	for _, e := range o.Exits {
		v.vteps[e.VTEP] = true
	}
	return v
}

// Claim is one tun-enabled claim as the router sees it.
type Claim struct {
	ID   string
	Addr netip.Addr
	// Enabled is asked once per packet stream; false answers the guest off.
	Enabled func() bool
	// Profile is the claim's exit label; empty means "default".
	Profile string
	Allow   AllowFunc
	Limits  Limits
	// Used counts the claim's bytes both ways; the caller persists it.
	Used   *atomic.Int64
	Record RecordFunc
}

// Stats counts what crossed each hop and why packets were dropped.
type Stats struct {
	FramesIn, FramesOut  uint64
	DgramsOut, DgramsIn  uint64
	ARPAnswered, ARPSent uint64
	Drops                [dropKinds]uint64
}

// Router moves packets between claims' vsock streams and the VXLAN underlay.
type Router struct {
	under Underlay
	conf  atomic.Pointer[overlay]
	pool  sync.Pool

	framesIn, framesOut  atomic.Uint64
	dgramsOut, dgramsIn  atomic.Uint64
	arpAnswered, arpSent atomic.Uint64
	drops                [dropKinds]atomic.Uint64

	gro coalescer

	mu     sync.RWMutex
	claims map[netip.Addr]*armed
}

// New builds a router over under with the overlay o.
func New(under Underlay, o *Overlay) *Router {
	r := &Router{under: under, claims: map[netip.Addr]*armed{}}
	r.conf.Store(load(o))
	r.pool.New = func() any { b := make([]byte, 4+MaxFrame); return &b }
	return r
}

// Reload swaps the overlay config and announces every armed claim again; live streams pick the new exits up on their next packet.
func (r *Router) Reload(o *Overlay) {
	r.conf.Store(load(o))
	r.announceAll()
}

// Arm makes c's address answer ARP on this node and announces it to its exit.
func (r *Router) Arm(c Claim) {
	a := &armed{c: c, flows: newFlows(c)}
	r.mu.Lock()
	old := r.claims[c.Addr]
	r.claims[c.Addr] = a
	r.mu.Unlock()
	if old != nil {
		old.close()
	}
	r.announce(a)
}

// Disarm stops answering for addr, ends its packet stream and closes its flows.
func (r *Router) Disarm(addr netip.Addr) {
	r.mu.Lock()
	a := r.claims[addr]
	delete(r.claims, addr)
	r.mu.Unlock()
	if a != nil {
		a.close()
	}
}

// Serve runs one packet stream for the claim armed at addr until it ends; a newer stream replaces it.
func (r *Router) Serve(conn net.Conn, addr netip.Addr) error {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(helloTimeout))
	guest, err := ReadHello(conn)
	if err != nil {
		return err
	}
	r.mu.RLock()
	a := r.claims[addr]
	r.mu.RUnlock()
	on := a != nil && (a.c.Enabled == nil || a.c.Enabled())
	o := r.conf.Load()
	hello := Hello{Version: Version, Offloads: segmentable, MTU: o.MTU}
	if on {
		hello.Flags = FlagOn
	}
	b := hello.Encode()
	if guest.Version < Version {
		return fmt.Errorf("tun hello: guest speaks version %d", guest.Version)
	}
	if _, err = conn.Write(b[:]); err != nil || !on {
		return err
	}
	_ = conn.SetDeadline(time.Time{})
	l := &link{r: r, a: a, conn: conn, send: make(chan *[]byte, sendQueue), done: make(chan struct{})}
	if !a.attach(l) {
		return nil
	}
	go l.write()
	go l.sweep()
	err = l.read(bufio.NewReaderSize(conn, readBuf))
	if a.detach(l) {
		a.flows.sweep(time.Now(), true)
	}
	l.close()
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

// Run receives VXLAN until ctx ends or the underlay fails, and refreshes every armed claim's announcement.
func (r *Router) Run(ctx context.Context) error {
	go func() {
		tick := time.Tick(refreshEvery)
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick:
				r.announceAll()
			}
		}
	}()
	for ctx.Err() == nil {
		err := r.under.Recv(r.receive)
		r.gro.flush(r)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("vxlan receive: %w", err)
		}
	}
	return nil
}

// Stats snapshots the counters.
func (r *Router) Stats() Stats {
	s := Stats{
		FramesIn: r.framesIn.Load(), FramesOut: r.framesOut.Load(),
		DgramsOut: r.dgramsOut.Load(), DgramsIn: r.dgramsIn.Load(),
		ARPAnswered: r.arpAnswered.Load(), ARPSent: r.arpSent.Load(),
	}
	for i := range s.Drops {
		s.Drops[i] = r.drops[i].Load()
	}
	return s
}

func (r *Router) receive(d []byte, from netip.AddrPort) {
	r.dgramsIn.Add(1)
	o := r.conf.Load()
	if !o.vteps[from.Addr()] {
		r.drops[DropSender].Add(1)
		return
	}
	inner, etype, ok := parseVXLAN(d, o.VNI)
	if !ok {
		r.drops[DropMalformed].Add(1)
		return
	}
	switch etype {
	case etherARP:
		sender, mac, target, ok := arpRequest(inner)
		if !ok {
			return
		}
		r.mu.RLock()
		a := r.claims[target]
		r.mu.RUnlock()
		if a == nil {
			return
		}
		reply := arpReplyFor(o.VNI, target, sender, mac)
		if r.under.Send(reply, len(reply), netip.AddrPortFrom(from.Addr(), o.Port), 0) == nil {
			r.arpAnswered.Add(1)
		}
	case etherIPv4:
		if drop := r.deliver(inner); drop != DropNone {
			r.drops[drop].Add(1)
		}
	}
}

// deliver hands an inbound packet to its claim's stream, rewritten to the guest address.
func (r *Router) deliver(inner []byte) Drop {
	if len(inner) > MaxFrame-VnetHdrLen {
		return DropMalformed
	}
	bp := r.pool.Get().(*[]byte)
	b := (*bp)[:4+VnetHdrLen+len(inner)]
	clear(b[4 : 4+VnetHdrLen])
	copy(b[4+VnetHdrLen:], inner)
	frame := b[4:]
	l, drop := r.admit(frame)
	if drop == DropNone {
		*bp = b
		if r.gro.add(r, l, bp, frame) {
			return DropNone
		}
		binary.LittleEndian.PutUint32(b, uint32(len(frame))) //nolint:gosec // bounded by MaxFrame
		if l.enqueue(bp) {
			return DropNone
		}
		drop = DropQueueFull
	}
	r.pool.Put(bp)
	return drop
}

// admit parses an inbound frame, checks it against its claim's flows and rewrites it to the guest.
func (r *Router) admit(frame []byte) (*link, Drop) {
	p, drop := Parse(frame)
	if drop != DropNone {
		return nil, drop
	}
	r.mu.RLock()
	a := r.claims[p.Dst]
	r.mu.RUnlock()
	l := a.live()
	if l == nil {
		return nil, DropUnknownClaim
	}
	if drop = a.flows.inbound(p, frame, len(frame), time.Now()); drop != DropNone {
		return nil, drop
	}
	RewriteDst(frame, GuestAddr)
	if p.Proto == ProtoICMP && p.ICMPType != icmpEchoReply {
		rewriteQuotedSrc(frame, p.Dst, GuestAddr)
	}
	return l, DropNone
}

func (r *Router) announceAll() {
	r.mu.RLock()
	all := slices.Collect(maps.Values(r.claims))
	r.mu.RUnlock()
	for _, a := range all {
		r.announce(a)
	}
}

func (r *Router) announce(a *armed) {
	o := r.conf.Load()
	exit, ok := o.exit(a.c.Profile)
	if !ok {
		return
	}
	g := gratuitousARP(o.VNI, a.c.Addr)
	if r.under.Send(g, len(g), netip.AddrPortFrom(exit.VTEP, o.Port), 0) == nil {
		r.arpSent.Add(1)
	}
}

// armed is a claim this node answers for, with its flows and limits, which outlive a stream, and its live stream if one runs.
type armed struct {
	c     Claim
	flows *flows
	mu    sync.Mutex
	l     *link
	gone  bool
}

func (a *armed) live() *link {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.l
}

func (a *armed) attach(l *link) bool {
	a.mu.Lock()
	old, gone := a.l, a.gone
	if !gone {
		a.l = l
	}
	a.mu.Unlock()
	if old != nil {
		old.close()
	}
	return !gone
}

// detach drops l as the live stream and reports whether the claim was disarmed meanwhile.
func (a *armed) detach(l *link) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.l == l {
		a.l = nil
	}
	return a.gone
}

func (a *armed) close() {
	a.mu.Lock()
	l := a.l
	a.l, a.gone = nil, true
	a.mu.Unlock()
	if l != nil {
		l.close()
	}
	a.flows.sweep(time.Now(), true)
}

// link is one claim's live packet stream.
type link struct {
	r    *Router
	a    *armed
	conn net.Conn
	out  []byte
	send chan *[]byte
	done chan struct{}
	once sync.Once
}

func (l *link) read(br *bufio.Reader) error {
	var head [4]byte
	frame := make([]byte, MaxFrame)
	for {
		if _, err := io.ReadFull(br, head[:]); err != nil {
			return err
		}
		n, err := FrameLen(head)
		if err != nil {
			return err
		}
		pkt := frame[:n]
		if _, err = io.ReadFull(br, pkt); err != nil {
			return err
		}
		l.r.framesIn.Add(1)
		if drop := l.forward(pkt); drop != DropNone {
			l.r.drops[drop].Add(1)
		}
	}
}

// forward carries one guest frame out: parse, size, charge and policy, then rewrite, segment and send.
func (l *link) forward(frame []byte) Drop {
	p, drop := Parse(frame)
	if drop != DropNone {
		return drop
	}
	o := l.r.conf.Load()
	segs, wire, ok := wireCost(frame, int(o.MTU))
	if !ok {
		return DropSize
	}
	now, f := time.Now(), l.a.flows
	if !f.charge(f.up, segs, wire, now) {
		return DropLimit
	}
	if o.Subnet.Contains(p.Dst) || GuestRange.Contains(p.Dst) {
		return DropPolicy
	}
	if drop = f.outbound(p, wire, now); drop != DropNone {
		return drop
	}
	exit, ok := o.exit(l.a.c.Profile)
	if !ok {
		return DropNoExit
	}
	RewriteSrc(frame, l.a.c.Addr)
	var hdr [Overhead]byte
	appendHeader(hdr[:0], o.VNI, exit.MAC, MAC(l.a.c.Addr), etherIPv4)
	out, stride, n := appendSegments(l.out[:0], hdr[:], frame)
	l.out = out
	if n == 0 {
		return DropGSO
	}
	flow := maphash.Comparable(seed, [3]uint64{uint64(p.Proto), uint64(p.SrcPort)<<16 | uint64(p.DstPort), dst4(p.Dst)})
	if l.r.under.Send(out, stride, netip.AddrPortFrom(exit.VTEP, o.Port), flow) != nil {
		return DropSend
	}
	l.r.dgramsOut.Add(uint64(n)) //nolint:gosec // a count
	return DropNone
}

func (l *link) enqueue(bp *[]byte) bool {
	select {
	case l.send <- bp:
		return true
	default:
		return false
	}
}

func (l *link) write() {
	batch := make(net.Buffers, 0, 64)
	held := make([]*[]byte, 0, 64)
	for {
		select {
		case <-l.done:
			return
		case bp := <-l.send:
			batch, held = append(batch[:0], *bp), append(held[:0], bp)
		drain:
			for len(held) < cap(held) {
				select {
				case bp = <-l.send:
					batch, held = append(batch, *bp), append(held, bp)
				default:
					break drain
				}
			}
			bufs := batch
			_, err := bufs.WriteTo(l.conn)
			l.r.framesOut.Add(uint64(len(held)))
			for _, bp := range held {
				l.r.pool.Put(bp)
			}
			if err != nil {
				l.close()
				return
			}
		}
	}
}

func (l *link) sweep() {
	tick := time.Tick(sweepEvery)
	for {
		select {
		case <-l.done:
			return
		case now := <-tick:
			l.a.flows.sweep(now, false)
		}
	}
}

func (l *link) close() {
	l.once.Do(func() {
		close(l.done)
		_ = l.conn.Close()
	})
}

func dst4(a netip.Addr) uint64 {
	b := a.As4()
	return uint64(binary.BigEndian.Uint32(b[:]))
}
