package tunnet

import (
	"encoding/binary"
	"net/netip"
	"testing"
)

func FuzzFrameLen(f *testing.F) {
	f.Add(uint32(10))
	f.Add(uint32(MaxFrame + 1))
	f.Fuzz(func(t *testing.T, n uint32) {
		var head [4]byte
		binary.LittleEndian.PutUint32(head[:], n)
		got, err := FrameLen(head)
		if err == nil && (got < VnetHdrLen || got > MaxFrame) {
			t.Fatalf("FrameLen accepted %d", got)
		}
	})
}

func FuzzParseAndRewrite(f *testing.F) {
	f.Add(frame(ProtoTCP, GuestAddr, remote, true))
	f.Add(frame(ProtoUDP, GuestAddr, remote, false))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxFrame {
			return
		}
		p, drop := Parse(b)
		if drop != DropNone {
			return
		}
		if p.Proto != ProtoTCP && p.Proto != ProtoUDP && p.Proto != ProtoICMP {
			t.Fatalf("Parse passed protocol %d", p.Proto)
		}
		RewriteSrc(b, claimAddr)
		RewriteDst(b, GuestAddr)
		if q, _ := Parse(b); q.Src != claimAddr || q.Dst != GuestAddr {
			t.Fatalf("rewrite left %v -> %v", q.Src, q.Dst)
		}
	})
}

func FuzzSegment(f *testing.F) {
	f.Add(gsoFrame(ProtoTCP, 3000, 1400))
	f.Add(gsoFrame(ProtoUDP, 3000, 1000))
	f.Add(frame(ProtoTCP, GuestAddr, remote, true))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxFrame {
			return
		}
		out, stride, n := appendSegments(nil, nil, b)
		if n > 0 && (stride > len(b) || len(out) > n*stride) {
			t.Fatalf("%d segments of stride %d, %d bytes, from a %d-byte frame", n, stride, len(out), len(b))
		}
	})
}

func FuzzVXLANAndARP(f *testing.F) {
	f.Add(gratuitousARP(7, claimAddr))
	f.Add(arpReplyFor(7, claimAddr, netip.MustParseAddr("100.80.0.1"), broadcast))
	f.Fuzz(func(_ *testing.T, d []byte) {
		inner, etype, ok := parseVXLAN(d, 7)
		if !ok {
			return
		}
		if etype == etherARP {
			_, _, _, _ = arpRequest(inner)
		}
		_, _ = Parse(append(make([]byte, VnetHdrLen), inner...))
	})
}
