package udp

import (
	"net"
	"net/netip"
	"testing"
)

func TestSendBatchArrivesAsSegments(t *testing.T) {
	u, err := Listen(netip.MustParseAddrPort("127.0.0.1:0"), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	dst := u.recv.LocalAddr().(*net.UDPAddr).AddrPort()
	batch := make([]byte, 3*1000+400)
	if err = u.Send(batch, 1000, dst, 7); err != nil {
		t.Fatal(err)
	}
	var sizes []int
	for len(sizes) < 4 {
		if err = u.Recv(func(d []byte, _ netip.AddrPort) { sizes = append(sizes, len(d)) }); err != nil {
			t.Fatal(err)
		}
	}
	if sizes[0] != 1000 || sizes[3] != 400 {
		t.Errorf("datagram sizes %v, want 1000 x3 then 400", sizes)
	}
}

func BenchmarkSendGSO(b *testing.B) {
	u, err := Listen(netip.MustParseAddrPort("127.0.0.1:0"), 1)
	if err != nil {
		b.Fatal(err)
	}
	defer u.Close()
	dst := u.recv.LocalAddr().(*net.UDPAddr).AddrPort()
	go func() {
		for {
			if u.Recv(func([]byte, netip.AddrPort) {}) != nil {
				return
			}
		}
	}()
	batch := make([]byte, 44*1472)
	b.SetBytes(int64(len(batch)))
	b.ReportAllocs()
	for b.Loop() {
		if err := u.Send(batch, 1472, dst, 1); err != nil {
			b.Fatal(err)
		}
	}
}
