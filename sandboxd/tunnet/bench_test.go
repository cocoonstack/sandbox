package tunnet

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
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

func BenchmarkReadGSO(b *testing.B) {
	benchRead(b, gsoFrame(ProtoTCP, 63000, 1398))
}

func BenchmarkReadSmall(b *testing.B) {
	benchRead(b, frame(ProtoUDP, GuestAddr, remote, false))
}

func BenchmarkDeliver(b *testing.B) {
	l := benchLink(b)
	l.forward(frame(ProtoTCP, GuestAddr, remote, false))
	d := reply(remote)
	inner, _, _ := parseVXLAN(d, 7)
	go func() {
		for bp := range l.send {
			l.r.pool.Put(bp)
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

func benchRead(b *testing.B, f []byte) {
	b.Helper()
	l := benchLink(b)
	s := &frameStream{b: b, f: append(binary.LittleEndian.AppendUint32(nil, uint32(len(f))), f...)}
	b.SetBytes(int64(len(f)))
	b.ReportAllocs()
	if err := l.read(bufio.NewReaderSize(s, readBuf)); !errors.Is(err, io.EOF) {
		b.Fatal(err)
	}
}

type frameStream struct {
	b    *testing.B
	f    []byte
	off  int
	done bool
}

func (s *frameStream) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) && !s.done {
		if s.off == 0 && !s.b.Loop() {
			s.done = true
			break
		}
		c := copy(p[n:], s.f[s.off:])
		n += c
		s.off = (s.off + c) % len(s.f)
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

type nopUnder struct{}

func (nopUnder) Send([]byte, int, netip.AddrPort, uint64) error { return nil }

func (nopUnder) Recv(func([]byte, netip.AddrPort)) error { select {} }
