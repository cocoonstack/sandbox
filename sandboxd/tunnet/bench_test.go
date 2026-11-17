package tunnet

import (
	"net/netip"
	"testing"
)

func BenchmarkForwardGSO(b *testing.B) {
	l := benchLink(b)
	f := gsoFrame(ProtoTCP, 63000, 1398)
	buf := make([]byte, len(f))
	b.SetBytes(int64(len(f)))
	b.ReportAllocs()
	for b.Loop() {
		copy(buf, f)
		if d := l.forward(buf); d != DropNone {
			b.Fatal(d)
		}
	}
}

func BenchmarkForwardSmall(b *testing.B) {
	l := benchLink(b)
	f := frame(ProtoUDP, GuestAddr, remote, false)
	buf := make([]byte, len(f))
	b.SetBytes(int64(len(f)))
	b.ReportAllocs()
	for b.Loop() {
		copy(buf, f)
		if d := l.forward(buf); d != DropNone {
			b.Fatal(d)
		}
	}
}

func BenchmarkDeliver(b *testing.B) {
	l := benchLink(b)
	l.forward(frame(ProtoTCP, GuestAddr, remote, false))
	d := reply(remote)
	inner, _, _ := parseVXLAN(d, 7)
	go func() {
		for bp := range l.send {
			l.r.put(bp)
		}
	}()
	b.Cleanup(func() { close(l.send) })
	b.SetBytes(int64(len(inner)))
	b.ReportAllocs()
	for b.Loop() {
		if drop := l.r.deliver(inner); drop != DropNone && drop != DropQueueFull {
			b.Fatal(drop)
		}
	}
}

func BenchmarkDeliverTrain(b *testing.B) {
	l := benchLink(b)
	l.forward(frame(ProtoTCP, GuestAddr, remote, false))
	segs := make([][]byte, 45)
	n := 0
	for i := range segs {
		segs[i], _, _ = parseVXLAN(dataSegment(uint32(1000+1398*i), 1398), 7)
		n += len(segs[i])
	}
	b.SetBytes(int64(n))
	b.ReportAllocs()
	for b.Loop() {
		for _, s := range segs {
			if drop := l.r.deliver(s); drop != DropNone {
				b.Fatal(drop)
			}
		}
		l.r.gro.flush(l.r)
		l.large.Add(-1)
		l.r.put(<-l.send)
	}
}

func benchLink(b *testing.B) *link {
	b.Helper()
	r := New(nopUnder{}, testOverlay(1450))
	c := Claim{ID: "c1", Addr: claimAddr, Allow: func(netip.Addr, uint16) bool { return true }}
	r.Arm(c)
	a := r.claims[claimAddr]
	l := &link{r: r, a: a, mtu: 1450, merge: true, send: make(chan *[]byte, sendQueue), done: make(chan struct{})}
	if !a.attach(l) {
		b.Fatal("the bench claim was disarmed")
	}
	return l
}

type nopUnder struct{}

func (nopUnder) Send([]byte, int, netip.AddrPort, uint64) error { return nil }

func (nopUnder) Recv(func([]byte, netip.AddrPort)) error { select {} }
