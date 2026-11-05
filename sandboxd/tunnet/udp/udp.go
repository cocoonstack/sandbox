// Package udp is the VXLAN underlay on plain UDP sockets: one send call per segmented batch, one receive per coalesced batch, and several send sockets so flows leave from different source ports.
package udp

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"

	"golang.org/x/net/ipv4"

	"github.com/cocoonstack/sandbox/sandboxd/tunnet"
)

const (
	// maxBatch keeps one GSO send under the kernel's 64 KiB and 64-segment caps.
	maxBatch    = 65000
	maxSegments = 64
	sockBuf     = 8 << 20
	recvBuf     = 1 << 16
	// recvBatch is the datagrams one receive call can return, each up to a GRO-coalesced 64 KiB.
	recvBatch = 32
	oobLen    = 64
)

var cmsgPool = sync.Pool{New: func() any { return new([cmsgCap]byte) }}

var _ tunnet.Underlay = (*Underlay)(nil)

// Underlay is a receive socket on the VXLAN port and a set of send sockets on ephemeral ports.
type Underlay struct {
	recv  *net.UDPConn
	batch *ipv4.PacketConn
	msgs  []ipv4.Message
	sends []*net.UDPConn
	gso   atomic.Bool
}

// Listen binds the VXLAN port at addr and senders send sockets on the same address.
func Listen(addr netip.AddrPort, senders int) (*Underlay, error) {
	recv, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(addr))
	if err != nil {
		return nil, fmt.Errorf("listen vxlan %s: %w", addr, err)
	}
	u := &Underlay{recv: recv, batch: ipv4.NewPacketConn(recv), msgs: make([]ipv4.Message, recvBatch)}
	for i := range u.msgs {
		u.msgs[i].Buffers = [][]byte{make([]byte, recvBuf)}
		u.msgs[i].OOB = make([]byte, oobLen)
	}
	_ = recv.SetReadBuffer(sockBuf)
	enableGRO(recv)
	for range max(senders, 1) {
		c, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(addr.Addr(), 0)))
		if err != nil {
			u.Close()
			return nil, fmt.Errorf("listen vxlan sender: %w", err)
		}
		_ = c.SetWriteBuffer(sockBuf)
		u.sends = append(u.sends, c)
	}
	u.gso.Store(gsoSupported)
	return u, nil
}

func (u *Underlay) Send(b []byte, seg int, dst netip.AddrPort, flow uint64) error {
	c := u.sends[flow%uint64(len(u.sends))]
	per := min(maxBatch/seg, maxSegments) * seg
	for len(b) > 0 {
		n := min(len(b), per)
		var err error
		switch {
		case n <= seg:
			_, err = c.WriteToUDPAddrPort(b[:n], dst)
		case u.gso.Load():
			oob := cmsgPool.Get().(*[cmsgCap]byte)
			_, _, err = c.WriteMsgUDPAddrPort(b[:n], segmentCmsg(oob, seg), dst)
			cmsgPool.Put(oob)
			if errors.Is(err, errGSORefused) {
				u.gso.Store(false)
				continue
			}
		default:
			for off := 0; off < n && err == nil; off += seg {
				_, err = c.WriteToUDPAddrPort(b[off:min(off+seg, n)], dst)
			}
		}
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

func (u *Underlay) Recv(each func(d []byte, from netip.AddrPort)) error {
	got, err := u.batch.ReadBatch(u.msgs, 0)
	if err != nil {
		return err
	}
	for i := range got {
		m := &u.msgs[i]
		ua, ok := m.Addr.(*net.UDPAddr)
		if !ok {
			continue
		}
		from, buf := ua.AddrPort(), m.Buffers[0][:m.N]
		seg := groSize(m.OOB[:m.NN])
		if seg <= 0 {
			seg = len(buf)
		}
		for off := 0; off < len(buf); off += seg {
			each(buf[off:min(off+seg, len(buf))], from)
		}
	}
	return nil
}

// Close closes every socket.
func (u *Underlay) Close() {
	if u.recv != nil {
		_ = u.recv.Close()
	}
	for _, c := range u.sends {
		_ = c.Close()
	}
}
