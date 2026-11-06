package tunnet

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	vtep    = netip.MustParseAddr("10.0.0.9")
	exitMAC = [6]byte{0x02, 0xee, 0, 0, 0, 1}
	subnet  = netip.MustParsePrefix("100.80.0.0/12")
)

func TestRouterAnnouncesRewritesAndPassesOnlyReplies(t *testing.T) {
	u, r := newRouter()
	var records []FlowRecord
	var mu sync.Mutex
	r.Arm(Claim{
		ID: "c1", Addr: claimAddr,
		Allow:  func(_ netip.Addr, port uint16) bool { return port == 443 },
		Record: func(fr FlowRecord) { mu.Lock(); records = append(records, fr); mu.Unlock() },
	})
	if inner, etype, to := u.sent(t); etype != etherARP || to.Addr() != vtep {
		t.Fatalf("arm sent %v to %v, want a gratuitous ARP to the exit", etype, to)
	} else if _, _, target, ok := arpRequest(inner); !ok || target != claimAddr {
		t.Errorf("gratuitous ARP announces %v", target)
	}
	guest := serve(t, r, claimAddr)
	if h := guestHello(t, guest); h.Flags&FlagOn == 0 || h.MTU != 1450 || h.Offloads&OffloadTSO4 == 0 {
		t.Fatalf("host hello = %+v", h)
	}
	sendFrame(t, guest, frame(ProtoTCP, netip.MustParseAddr("100.80.0.99"), remote, true))
	inner, etype, _ := u.sent(t)
	pkt := append(make([]byte, VnetHdrLen), inner...)
	if p, _ := Parse(pkt); etype != etherIPv4 || p.Src != claimAddr || !validIP(pkt) || !validL4(pkt) {
		t.Fatalf("sent src %v, want the claim address with full checksums", p.Src)
	}
	denied := frame(ProtoTCP, GuestAddr, remote, false)
	binary.BigEndian.PutUint16(denied[VnetHdrLen+20+2:], 22)
	sendFrame(t, guest, denied)
	sendFrame(t, guest, frame(ProtoTCP, GuestAddr, netip.MustParseAddr("100.80.0.5"), false))
	stranger := reply(netip.MustParseAddr("198.51.100.1"))
	binary.BigEndian.PutUint16(stranger[Overhead+20:], 22)
	u.inject(t, stranger)
	u.inject(t, reply(remote))
	got := readFrame(t, guest)
	if p, _ := Parse(got); p.Dst != GuestAddr || p.Src != remote || !validIP(got) {
		t.Fatalf("delivered %+v, want the reply rewritten to the guest", p)
	}
	waitFor(t, func() bool { return r.Stats().Drops[DropPolicy] == 3 })
	if n := len(u.out); n != 0 {
		t.Errorf("%d more datagrams sent, want none for denied traffic", n)
	}
	_ = guest.Close()
	r.Disarm(claimAddr)
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(records) == 2 })
}

func TestRouterAnswersARPOnlyForArmedClaims(t *testing.T) {
	u, r := newRouter()
	r.Arm(Claim{ID: "c1", Addr: claimAddr})
	u.sent(t)
	asker := netip.MustParseAddr("100.80.0.1")
	for _, target := range []netip.Addr{netip.MustParseAddr("100.80.0.8"), claimAddr} {
		b := appendHeader(nil, 7, broadcast, exitMAC, etherARP)
		u.inject(t, appendARP(b, arpReq, exitMAC, asker, [6]byte{}, target))
	}
	inner, _, to := u.sent(t)
	if to.Addr() != vtep || binary.BigEndian.Uint16(inner[6:]) != arpReply || netip.AddrFrom4([4]byte(inner[14:18])) != claimAddr {
		t.Fatalf("arp reply to %v: %x", to, inner)
	}
	b := appendHeader(nil, 7, broadcast, exitMAC, etherARP)
	u.injectFrom(t, appendARP(b, arpReq, exitMAC, asker, [6]byte{}, claimAddr), netip.MustParseAddrPort("10.0.0.77:4789"))
	waitFor(t, func() bool { return r.Stats().Drops[DropSender] == 1 })
	r.Disarm(claimAddr)
	b = appendHeader(nil, 7, broadcast, exitMAC, etherARP)
	u.inject(t, appendARP(b, arpReq, exitMAC, asker, [6]byte{}, claimAddr))
	time.Sleep(50 * time.Millisecond)
	if len(u.out) != 0 {
		t.Error("a disarmed claim, or a sender that is no exit, got an ARP answer")
	}
}

func TestRouterSegmentsGSOIntoOneBatch(t *testing.T) {
	u, r := newRouter()
	r.Arm(Claim{ID: "c1", Addr: claimAddr, Allow: func(netip.Addr, uint16) bool { return true }})
	u.sent(t)
	guest := serve(t, r, claimAddr)
	guestHello(t, guest)
	sendFrame(t, guest, gsoFrame(ProtoTCP, 4000, 1400))
	for i := range 3 {
		inner, _, _ := u.sent(t)
		if pkt := append(make([]byte, VnetHdrLen), inner...); !validIP(pkt) || !validL4(pkt) {
			t.Errorf("segment %d invalid", i)
		}
	}
	if b := u.batches.Load(); b != 2 {
		t.Errorf("sends = %d, want the ARP and one batch for the GSO frame", b)
	}
}

func TestRouterRefusesUnarmedAndDisabledAndDisarmEndsStream(t *testing.T) {
	_, r := newRouter()
	if h := guestHello(t, serve(t, r, claimAddr)); h.Flags&FlagOn != 0 {
		t.Error("an unarmed address got on")
	}
	r.Arm(Claim{ID: "c1", Addr: claimAddr, Enabled: func() bool { return false }})
	off := serve(t, r, claimAddr)
	if h := guestHello(t, off); h.Flags&FlagOn != 0 {
		t.Error("a disabled claim got on")
	}
	if _, err := off.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Errorf("stream after off: %v", err)
	}
	r.Arm(Claim{ID: "c1", Addr: claimAddr, Allow: func(netip.Addr, uint16) bool { return true }})
	on := serve(t, r, claimAddr)
	guestHello(t, on)
	waitFor(t, func() bool { return r.claims[claimAddr].live() != nil })
	r.Disarm(claimAddr)
	if _, err := on.Read(make([]byte, 1)); err == nil {
		t.Error("stream still open after disarm")
	}
}

func TestRouterQuotaStopsTraffic(t *testing.T) {
	u, r := newRouter()
	used := new(atomic.Int64)
	r.Arm(Claim{ID: "c1", Addr: claimAddr, Used: used, Limits: Limits{QuotaBytes: 100}, Allow: func(netip.Addr, uint16) bool { return true }})
	u.sent(t)
	guest := serve(t, r, claimAddr)
	guestHello(t, guest)
	f := frame(ProtoTCP, GuestAddr, remote, false)
	for range 3 {
		sendFrame(t, guest, f)
	}
	u.sent(t)
	waitFor(t, func() bool { return r.Stats().Drops[DropLimit] == 2 })
	if used.Load() > 100 {
		t.Errorf("used %d past the quota", used.Load())
	}
}

func TestDeliverCoalescesInOrderSegmentsIntoOneGSOFrame(t *testing.T) {
	u, r := newRouter()
	r.Arm(Claim{ID: "c1", Addr: claimAddr, Allow: func(netip.Addr, uint16) bool { return true }})
	u.sent(t)
	guest := serve(t, r, claimAddr)
	guestHello(t, guest)
	sendFrame(t, guest, frame(ProtoTCP, GuestAddr, remote, false))
	u.sent(t)
	bad := dataSegment(5000, 1000)
	bad[len(bad)-1] ^= 0xff
	batch := [][]byte{dataSegment(1000, 1000), dataSegment(2000, 1000), dataSegment(3000, 1000), dataSegment(4000, 400), bad}
	for _, d := range batch {
		u.inject(t, d)
	}
	merged := readFrame(t, guest)
	ip := merged[VnetHdrLen:]
	if merged[1] != gsoTCPv4 || binary.LittleEndian.Uint16(merged[4:]) != 1000 || len(ip) != 40+3400 {
		t.Fatalf("frame gso %d size %d len %d, want one TCPv4 GSO frame of 3400 payload bytes at 1000", merged[1], binary.LittleEndian.Uint16(merged[4:]), len(ip))
	}
	if got, want := binary.BigEndian.Uint16(ip[36:]), fold(pseudoSum(ip, len(ip)-20)); got != want || !validIP(merged) {
		t.Errorf("merged checksum %#x want the pseudo-header sum %#x, ip valid %v", got, want, validIP(merged))
	}
	if p, _ := Parse(merged); p.Dst != GuestAddr {
		t.Errorf("merged frame goes to %v", p.Dst)
	}
	if lone := readFrame(t, guest); lone[1] != gsoNone || validL4(lone) {
		t.Error("a segment with a bad checksum was merged or repaired instead of passed through")
	}
}

func TestRouterChargesSegmentsAndRefusesOversizedPackets(t *testing.T) {
	u, r := newRouter()
	r.Arm(Claim{ID: "c1", Addr: claimAddr, Limits: Limits{PacketsPerSecond: 10}, Allow: func(netip.Addr, uint16) bool { return true }})
	u.sent(t)
	go func() {
		for s := range u.out {
			_ = s
		}
	}()
	guest := serve(t, r, claimAddr)
	guestHello(t, guest)
	sendFrame(t, guest, gsoFrame(ProtoTCP, 4000, 1420))
	big := gsoFrame(ProtoUDP, 1500, 1500)
	big[1] = gsoNone
	sendFrame(t, guest, big)
	sendFrame(t, guest, gsoFrame(ProtoTCP, 4000, 8))
	sendFrame(t, guest, gsoFrame(ProtoTCP, 4000, 40))
	sendFrame(t, guest, gsoFrame(ProtoTCP, 4000, 40))
	waitFor(t, func() bool { s := r.Stats(); return s.Drops[DropSize] == 3 && s.Drops[DropLimit] == 1 })
	if s := r.Stats(); s.DgramsOut != 100 {
		t.Errorf("sent %d datagrams, want the 100 segments of the one frame the packet bucket admitted", s.DgramsOut)
	}
}

func TestRouterLimitsOutliveAReconnect(t *testing.T) {
	u, r := newRouter()
	r.Arm(Claim{ID: "c1", Addr: claimAddr, Limits: Limits{NewFlowsPerSecond: 0.01}, Allow: func(netip.Addr, uint16) bool { return true }})
	u.sent(t)
	first := serve(t, r, claimAddr)
	guestHello(t, first)
	sendFrame(t, first, frame(ProtoTCP, GuestAddr, remote, false))
	u.sent(t)
	_ = first.Close()
	second := serve(t, r, claimAddr)
	guestHello(t, second)
	other := frame(ProtoTCP, GuestAddr, remote, false)
	binary.BigEndian.PutUint16(other[VnetHdrLen+20:], 40001)
	sendFrame(t, second, other)
	waitFor(t, func() bool { return r.Stats().Drops[DropLimit] == 1 })
}

func TestRouterHandsAnICMPErrorBackWithTheQuotedGuestAddress(t *testing.T) {
	u, r := newRouter()
	r.Arm(Claim{ID: "c1", Addr: claimAddr, Allow: func(netip.Addr, uint16) bool { return true }})
	u.sent(t)
	guest := serve(t, r, claimAddr)
	guestHello(t, guest)
	sendFrame(t, guest, frame(ProtoTCP, GuestAddr, remote, false))
	inner, _, _ := u.sent(t)
	icmp := make([]byte, 20+8+28)
	icmp[0], icmp[8], icmp[9] = 0x45, 64, ProtoICMP
	binary.BigEndian.PutUint16(icmp[2:], uint16(len(icmp)))
	s, d := remote.As4(), claimAddr.As4()
	copy(icmp[srcAt:], s[:])
	copy(icmp[dstAt:], d[:])
	binary.BigEndian.PutUint16(icmp[10:], ^fold(onesSum(icmp[:20], 0)))
	icmp[20], icmp[21] = icmpUnreachable, 4
	copy(icmp[28:], inner[:28])
	binary.BigEndian.PutUint16(icmp[22:], ^fold(onesSum(icmp[20:], 0)))
	u.inject(t, append(appendHeader(nil, 7, MAC(claimAddr), exitMAC, etherIPv4), icmp...))
	got := readFrame(t, guest)
	ip := got[VnetHdrLen:]
	if netip.AddrFrom4([4]byte(ip[28+srcAt:])) != GuestAddr || fold(onesSum(ip[20:], 0)) != 0xffff || fold(onesSum(ip[28:48], 0)) != 0xffff || !validIP(got) {
		t.Fatalf("quoted source %v, checksums icmp %#x quoted %#x", netip.AddrFrom4([4]byte(ip[28+srcAt:])), fold(onesSum(ip[20:], 0)), fold(onesSum(ip[28:48], 0)))
	}
}

func TestRouterResumesAStreamOpenedBeforeTheArmButRefusesAnOpener(t *testing.T) {
	u, r := newRouter()
	r.Arm(Claim{ID: "c1", Addr: claimAddr, Allow: func(_ netip.Addr, port uint16) bool { return port == 443 }})
	u.sent(t)
	guest := serve(t, r, claimAddr)
	guestHello(t, guest)
	waitFor(t, func() bool { return r.claims[claimAddr].live() != nil })
	syn := reply(remote)
	syn[Overhead+20+13] = tcpSyn
	u.inject(t, syn)
	waitFor(t, func() bool { return r.Stats().Drops[DropPolicy] == 1 })
	u.inject(t, reply(remote))
	if p, _ := Parse(readFrame(t, guest)); p.Src != remote || p.Dst != GuestAddr {
		t.Fatalf("delivered %+v, want the stream's next segment", p)
	}
}

func TestRouterKeepsTheMTUAStreamWasToldUntilItsNextHello(t *testing.T) {
	u, r := newRouter()
	r.Arm(Claim{ID: "c1", Addr: claimAddr, Allow: func(netip.Addr, uint16) bool { return true }})
	u.sent(t)
	guest := serve(t, r, claimAddr)
	guestHello(t, guest)
	r.Reload(&Overlay{Subnet: subnet, VNI: 7, Port: 4789, MTU: 1410, Exits: map[string]Exit{"default": {VTEP: vtep, MAC: exitMAC}}})
	u.sent(t)
	full := gsoFrame(ProtoUDP, 1412, 1412)
	full[0], full[1] = 0, gsoNone
	sendFrame(t, guest, full)
	if inner, _, _ := u.sent(t); len(inner) != 1440 {
		t.Errorf("sent %d bytes, want the 1440-byte packet the stream's MTU allows", len(inner))
	}
}

func TestEndFlowsRecordsLiveFlowsWithIPBytes(t *testing.T) {
	u, r := newRouter()
	var mu sync.Mutex
	var records []FlowRecord
	r.Arm(Claim{
		ID: "c1", Addr: claimAddr, Allow: func(netip.Addr, uint16) bool { return true },
		Record: func(fr FlowRecord) { mu.Lock(); records = append(records, fr); mu.Unlock() },
	})
	u.sent(t)
	guest := serve(t, r, claimAddr)
	guestHello(t, guest)
	out := frame(ProtoTCP, GuestAddr, remote, false)
	sendFrame(t, guest, out)
	u.sent(t)
	in := reply(remote)
	u.inject(t, in)
	readFrame(t, guest)
	r.EndFlows()
	mu.Lock()
	defer mu.Unlock()
	if len(records) != 1 || records[0].Up != int64(len(out)-VnetHdrLen) || records[0].Down != int64(len(in)-Overhead) {
		t.Fatalf("records %+v, want one flow with %d up and %d down IP bytes", records, len(out)-VnetHdrLen, len(in)-Overhead)
	}
}

func TestDeliverMergesOnlyForAGuestThatOfferedTSO(t *testing.T) {
	u, r := newRouter()
	r.Arm(Claim{ID: "c1", Addr: claimAddr, Allow: func(netip.Addr, uint16) bool { return true }})
	u.sent(t)
	host, guest := net.Pipe()
	t.Cleanup(func() { _ = guest.Close() })
	go func() { _ = r.Serve(host, claimAddr) }()
	h := Hello{Version: Version, Offloads: OffloadCSUM}.Encode()
	if _, err := guest.Write(h[:]); err != nil {
		t.Fatal(err)
	}
	guestHello(t, guest)
	sendFrame(t, guest, frame(ProtoTCP, GuestAddr, remote, false))
	u.sent(t)
	u.inject(t, dataSegment(1000, 1000))
	u.inject(t, dataSegment(2000, 1000))
	for range 2 {
		if f := readFrame(t, guest); f[1] != gsoNone || len(f) != VnetHdrLen+1040 {
			t.Fatalf("frame gso %d len %d, want each segment alone", f[1], len(f))
		}
	}
}

func TestRouterPointsAGuestICMPErrorAtTheClaimAddress(t *testing.T) {
	u, r := newRouter()
	r.Arm(Claim{ID: "c1", Addr: claimAddr, Allow: func(netip.Addr, uint16) bool { return true }})
	u.sent(t)
	guest := serve(t, r, claimAddr)
	guestHello(t, guest)
	quoted := frame(ProtoUDP, remote, GuestAddr, false)[VnetHdrLen:][:28]
	icmp := make([]byte, VnetHdrLen+20+8+28)
	ip := icmp[VnetHdrLen:]
	ip[0], ip[8], ip[9] = 0x45, 64, ProtoICMP
	binary.BigEndian.PutUint16(ip[2:], uint16(len(ip)))
	s, d := GuestAddr.As4(), remote.As4()
	copy(ip[srcAt:], s[:])
	copy(ip[dstAt:], d[:])
	binary.BigEndian.PutUint16(ip[10:], ^fold(onesSum(ip[:20], 0)))
	ip[20], ip[21] = icmpUnreachable, 3
	copy(ip[28:], quoted)
	binary.BigEndian.PutUint16(ip[22:], ^fold(onesSum(ip[20:], 0)))
	sendFrame(t, guest, icmp)
	inner, _, _ := u.sent(t)
	if got := netip.AddrFrom4([4]byte(inner[28+dstAt:])); got != claimAddr || fold(onesSum(inner[20:], 0)) != 0xffff {
		t.Errorf("quoted destination %v, icmp checksum %#x; want the claim address and a valid checksum", got, fold(onesSum(inner[20:], 0)))
	}
}

func newRouter() (*fakeUnder, *Router) {
	u := &fakeUnder{out: make(chan sent, 64), in: make(chan sent, 16)}
	r := New(u, &Overlay{Subnet: subnet, VNI: 7, Port: 4789, MTU: 1450, Exits: map[string]Exit{"default": {VTEP: vtep, MAC: exitMAC}}})
	go func() { _ = r.Run(context.Background()) }()
	return u, r
}

func serve(t *testing.T, r *Router, addr netip.Addr) net.Conn {
	t.Helper()
	host, guest := net.Pipe()
	go func() { _ = r.Serve(host, addr) }()
	h := Hello{Version: Version, Offloads: OffloadCSUM | OffloadTSO4 | OffloadTSO6 | OffloadTSOECN}.Encode()
	if _, err := guest.Write(h[:]); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = guest.Close() })
	return guest
}

func guestHello(t *testing.T, guest net.Conn) Hello {
	t.Helper()
	h, err := ReadHello(guest)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func sendFrame(t *testing.T, guest net.Conn, f []byte) {
	t.Helper()
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(f)))
	if _, err := guest.Write(append(b, f...)); err != nil {
		t.Fatal(err)
	}
}

func readFrame(t *testing.T, guest net.Conn) []byte {
	t.Helper()
	_ = guest.SetReadDeadline(time.Now().Add(2 * time.Second))
	var head [4]byte
	if _, err := io.ReadFull(guest, head[:]); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, binary.LittleEndian.Uint32(head[:]))
	if _, err := io.ReadFull(guest, b); err != nil {
		t.Fatal(err)
	}
	return b
}

func reply(from netip.Addr) []byte {
	f := frame(ProtoTCP, from, claimAddr, false)
	seg := f[VnetHdrLen+20:]
	binary.BigEndian.PutUint16(seg, 443)
	binary.BigEndian.PutUint16(seg[2:], 40000)
	ip := f[VnetHdrLen:]
	binary.BigEndian.PutUint16(seg[16:], 0)
	binary.BigEndian.PutUint16(seg[16:], ^fold(onesSum(seg, pseudoSum(ip, len(ip)-20))))
	b := appendHeader(nil, 7, MAC(claimAddr), exitMAC, etherIPv4)
	return append(b, ip...)
}

func dataSegment(seq uint32, payload int) []byte {
	ip := make([]byte, 40+payload)
	ip[0], ip[8], ip[9] = 0x45, 64, ProtoTCP
	binary.BigEndian.PutUint16(ip[2:], uint16(len(ip)))
	s, d := remote.As4(), claimAddr.As4()
	copy(ip[srcAt:], s[:])
	copy(ip[dstAt:], d[:])
	binary.BigEndian.PutUint16(ip[10:], ^fold(onesSum(ip[:20], 0)))
	tcp := ip[20:]
	binary.BigEndian.PutUint16(tcp, 443)
	binary.BigEndian.PutUint16(tcp[2:], 40000)
	binary.BigEndian.PutUint32(tcp[4:], seq)
	binary.BigEndian.PutUint32(tcp[8:], 77)
	tcp[12], tcp[13] = 5<<4, tcpAck
	for i := 20; i < len(tcp); i++ {
		tcp[i] = byte(seq) + byte(i)
	}
	binary.BigEndian.PutUint16(tcp[16:], ^fold(onesSum(tcp, pseudoSum(ip, len(tcp)))))
	return append(appendHeader(nil, 7, MAC(claimAddr), exitMAC, etherIPv4), ip...)
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatal("condition not reached")
}

type sent struct {
	d  []byte
	to netip.AddrPort
}

type fakeUnder struct {
	out     chan sent
	in      chan sent
	batches atomic.Int64
}

func (u *fakeUnder) Send(b []byte, seg int, dst netip.AddrPort, _ uint64) error {
	u.batches.Add(1)
	for len(b) > 0 {
		n := min(seg, len(b))
		u.out <- sent{d: bytes.Clone(b[:n]), to: dst}
		b = b[n:]
	}
	return nil
}

func (u *fakeUnder) Recv(each func(d []byte, from netip.AddrPort)) error {
	s := <-u.in
	each(s.d, s.to)
	for {
		select {
		case s = <-u.in:
			each(s.d, s.to)
		default:
			return nil
		}
	}
}

func (u *fakeUnder) inject(t *testing.T, d []byte) {
	t.Helper()
	u.injectFrom(t, d, netip.AddrPortFrom(vtep, 50000))
}

func (u *fakeUnder) injectFrom(t *testing.T, d []byte, from netip.AddrPort) {
	t.Helper()
	u.in <- sent{d: d, to: from}
}

func (u *fakeUnder) sent(t *testing.T) ([]byte, uint16, netip.AddrPort) {
	t.Helper()
	select {
	case s := <-u.out:
		inner, etype, ok := parseVXLAN(s.d, 7)
		if !ok {
			t.Fatalf("sent a datagram that is not VXLAN for vni 7: %x", s.d)
		}
		return inner, etype, s.to
	case <-time.After(2 * time.Second):
		t.Fatal("nothing sent")
		return nil, 0, netip.AddrPort{}
	}
}
