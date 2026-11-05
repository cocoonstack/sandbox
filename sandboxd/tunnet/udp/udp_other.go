//go:build !linux

package udp

import (
	"errors"
	"net"
	"net/netip"
)

const (
	gsoSupported = false
	cmsgCap      = 32
)

var errGSORefused = errors.ErrUnsupported

// receiver reads one datagram per call off Linux, where no build serves traffic.
type receiver struct {
	c   *net.UDPConn
	buf []byte
}

func newReceiver(c *net.UDPConn) (*receiver, error) {
	return &receiver{c: c, buf: make([]byte, recvBuf)}, nil
}

func (r *receiver) recv(each func(d []byte, from netip.AddrPort)) error {
	n, from, err := r.c.ReadFromUDPAddrPort(r.buf)
	if err != nil {
		return err
	}
	each(r.buf[:n], from)
	return nil
}

func segmentCmsg(*[cmsgCap]byte, int) []byte { return nil }

func groSize([]byte) int { return 0 }

func enableGRO(*net.UDPConn) {}
