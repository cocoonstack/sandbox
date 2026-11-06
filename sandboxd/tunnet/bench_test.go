package tunnet

import (
	"net/netip"
	"testing"
)

func BenchmarkForwardGSO(b *testing.B) {
	l := benchLink()
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
	l := benchLink()
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
	l := benchLink()
	l.forward(frame(ProtoTCP, GuestAddr, remote, false))
	d := reply(remote)
	inner, _, _ := parseVXLAN(d, 7)
	go func() {
		for bp := range l.send {
			l.r.pool.Put(bp)
		}
	}()
	b.SetBytes(int64(len(inner)))
	b.ReportAllocs()
	for b.Loop() {
		if drop := l.r.deliver(inner); drop != DropNone && drop != DropQueueFull {
			b.Fatal(drop)
		}
	}
}

func benchLink() *link {
	r := New(nopUnder{}, testOverlay(1450))
	c := Claim{ID: "c1", Addr: claimAddr, Allow: func(netip.Addr, uint16) bool { return true }}
	r.Arm(c)
	a := r.claims[claimAddr]
	l := &link{r: r, a: a, mtu: 1450, merge: true, send: make(chan *[]byte, sendQueue), done: make(chan struct{})}
	a.attach(l)
	return l
}

type nopUnder struct{}

func (nopUnder) Send([]byte, int, netip.AddrPort, uint64) error { return nil }

func (nopUnder) Recv(func([]byte, netip.AddrPort)) error { select {} }
