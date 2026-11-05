//go:build linux

package udp

import (
	"encoding/binary"
	"net"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	gsoSupported = true
	cmsgCap      = 32
)

var errGSORefused = unix.EIO

// segmentCmsg fills b with a control message asking the kernel to cut one send into seg-byte datagrams.
func segmentCmsg(b *[cmsgCap]byte, seg int) []byte {
	h := (*unix.Cmsghdr)(unsafe.Pointer(&b[0])) //nolint:gosec // b holds a whole Cmsghdr
	h.Level, h.Type = unix.SOL_UDP, unix.UDP_SEGMENT
	h.SetLen(unix.CmsgLen(2))
	binary.NativeEndian.PutUint16(b[unix.CmsgLen(0):], uint16(seg)) //nolint:gosec // a datagram size fits
	return b[:unix.CmsgSpace(2)]
}

// groSize is the segment size the kernel coalesced a receive at, 0 when it did not.
func groSize(oob []byte) int {
	for len(oob) >= unix.SizeofCmsghdr {
		h := (*unix.Cmsghdr)(unsafe.Pointer(&oob[0])) //nolint:gosec // oob holds a whole Cmsghdr
		l := int(h.Len)                               //nolint:gosec // a control message length fits
		if l < unix.CmsgLen(0) || l > len(oob) {
			return 0
		}
		if h.Level == unix.SOL_UDP && h.Type == unix.UDP_GRO && l >= unix.CmsgLen(4) {
			return int(binary.NativeEndian.Uint32(oob[unix.CmsgLen(0):]))
		}
		oob = oob[min(unix.CmsgSpace(l-unix.CmsgLen(0)), len(oob)):]
	}
	return 0
}

func enableGRO(c *net.UDPConn) {
	rc, err := c.SyscallConn()
	if err != nil {
		return
	}
	_ = rc.Control(func(fd uintptr) {
		_ = unix.SetsockoptInt(int(fd), unix.SOL_UDP, unix.UDP_GRO, 1)
	})
}
