package tunnet

import (
	"bytes"
	"encoding/binary"
	"net/netip"
	"testing"
)

var (
	claimAddr = netip.AddrFrom4([4]byte{100, 80, 0, 7})
	remote    = netip.AddrFrom4([4]byte{203, 0, 113, 9})
)

func TestRewriteKeepsFullChecksumsValid(t *testing.T) {
	for _, proto := range []uint8{ProtoTCP, ProtoUDP} {
		f := frame(proto, GuestAddr, remote, false)
		RewriteSrc(f, claimAddr)
		p, drop := Parse(f)
		if drop != DropNone || p.Src != claimAddr || p.Dst != remote {
			t.Fatalf("proto %d: parse after rewrite = %+v %v", proto, p, drop)
		}
		if !validIP(f) || !validL4(f) {
			t.Errorf("proto %d: checksum broken by the source rewrite", proto)
		}
		RewriteDst(f, GuestAddr)
		if !validIP(f) {
			t.Errorf("proto %d: ip checksum broken by the destination rewrite", proto)
		}
	}
}

func TestRewriteMovesAPartialChecksumAsAnUncomplementedSum(t *testing.T) {
	f := frame(ProtoTCP, GuestAddr, remote, true)
	RewriteSrc(f, claimAddr)
	ip := f[VnetHdrLen:]
	got := binary.BigEndian.Uint16(ip[20+16:])
	if want := pseudo(ip); got != want {
		t.Errorf("partial checksum = %#x, want the pseudo-header sum %#x", got, want)
	}
}

func TestRewriteLeavesAZeroUDPChecksumZero(t *testing.T) {
	f := frame(ProtoUDP, GuestAddr, remote, false)
	ip := f[VnetHdrLen:]
	binary.BigEndian.PutUint16(ip[20+6:], 0)
	RewriteSrc(f, claimAddr)
	if binary.BigEndian.Uint16(ip[20+6:]) != 0 {
		t.Error("a UDP checksum of 0 means none and must stay 0")
	}
	if !validIP(f) {
		t.Error("ip checksum broken")
	}
}

func TestParseReadsPortsAndRefusesWhatV1DoesNotCarry(t *testing.T) {
	f := frame(ProtoTCP, GuestAddr, remote, false)
	f[VnetHdrLen+20+13] = tcpFin
	p, drop := Parse(f)
	if drop != DropNone || p.SrcPort != 40000 || p.DstPort != 443 || !p.End {
		t.Fatalf("Parse = %+v %v", p, drop)
	}
	cases := map[Drop]func([]byte) []byte{
		DropFragment:  func(b []byte) []byte { b[VnetHdrLen+6] = 0x20; return b },
		DropOptions:   func(b []byte) []byte { b[VnetHdrLen] = 0x46; return b },
		DropNotIPv4:   func(b []byte) []byte { b[VnetHdrLen] = 0x65; return b },
		DropMalformed: func(b []byte) []byte { return b[:len(b)-1] },
		DropProto:     func(b []byte) []byte { b[VnetHdrLen+9] = 47; return b },
	}
	for want, mutate := range cases {
		if _, got := Parse(mutate(frame(ProtoTCP, GuestAddr, remote, false))); got != want {
			t.Errorf("Parse = %v, want %v", got, want)
		}
	}
	spoof := frame(ProtoTCP, GuestAddr, remote, true)
	binary.LittleEndian.PutUint16(spoof[8:], 0)
	binary.LittleEndian.PutUint16(spoof[6:], 16)
	if _, got := Parse(spoof); got != DropGSO {
		t.Errorf("a partial checksum aimed at the IP header: %v, want refused", got)
	}
	mismatch := gsoFrame(ProtoTCP, 3000, 1400)
	mismatch[1] = gsoUDPL4
	if _, got := Parse(mismatch); got != DropGSO {
		t.Errorf("a UDP GSO type on TCP: %v, want refused", got)
	}
	if _, got := Parse(f[:VnetHdrLen+19]); got != DropMalformed {
		t.Errorf("truncated header: %v", got)
	}
}

func TestHelloRoundTripsAndFrameLenBounds(t *testing.T) {
	h := Hello{Version: Version, Offloads: OffloadCSUM | OffloadTSO4, MTU: 1460, Flags: FlagOn}
	b := h.Encode()
	got, err := ReadHello(bytes.NewReader(b[:]))
	if err != nil || got != h {
		t.Fatalf("ReadHello = %+v %v", got, err)
	}
	b[0] = 'X'
	if _, err = ReadHello(bytes.NewReader(b[:])); err == nil {
		t.Error("bad magic accepted")
	}
	for n, ok := range map[uint32]bool{9: false, 10: true, MaxFrame: true, MaxFrame + 1: false} {
		var head [4]byte
		binary.LittleEndian.PutUint32(head[:], n)
		if _, err := FrameLen(head); (err == nil) != ok {
			t.Errorf("FrameLen(%d) err = %v", n, err)
		}
	}
}

func frame(proto uint8, src, dst netip.Addr, partial bool) []byte {
	l4 := 20
	if proto == ProtoUDP {
		l4 = 8
	}
	f := make([]byte, VnetHdrLen+20+l4+16)
	ip := f[VnetHdrLen:]
	ip[0] = 0x45
	binary.BigEndian.PutUint16(ip[2:], uint16(len(ip)))
	binary.BigEndian.PutUint16(ip[4:], 7)
	ip[8], ip[9] = 64, proto
	s, d := src.As4(), dst.As4()
	copy(ip[srcAt:], s[:])
	copy(ip[dstAt:], d[:])
	binary.BigEndian.PutUint16(ip[10:], ^fold(onesSum(ip[:20], 0)))
	seg := ip[20:]
	binary.BigEndian.PutUint16(seg, 40000)
	binary.BigEndian.PutUint16(seg[2:], 443)
	for i := l4; i < len(seg); i++ {
		seg[i] = byte(i)
	}
	at := 16
	if proto == ProtoUDP {
		binary.BigEndian.PutUint16(seg[4:], uint16(len(seg)))
		at = 6
	} else {
		seg[12] = 5 << 4
	}
	if partial {
		f[0] = needsCsum
		binary.LittleEndian.PutUint16(f[6:], 20)
		binary.LittleEndian.PutUint16(f[8:], uint16(at))
		binary.BigEndian.PutUint16(seg[at:], pseudo(ip))
		return f
	}
	binary.BigEndian.PutUint16(seg[at:], ^fold(onesSum(seg, pseudoSum(ip, len(ip)-20))))
	return f
}

func validIP(f []byte) bool {
	return fold(onesSum(f[VnetHdrLen:VnetHdrLen+20], 0)) == 0xffff
}

func validL4(f []byte) bool {
	ip := f[VnetHdrLen:]
	return fold(onesSum(ip[20:], pseudoSum(ip, len(ip)-20))) == 0xffff
}

func pseudo(ip []byte) uint16 {
	return fold(pseudoSum(ip, len(ip)-20))
}
