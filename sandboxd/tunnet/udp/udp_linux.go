//go:build linux

package udp

import (
	"encoding/binary"
	"net"
	"net/netip"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	gsoSupported = true
	cmsgCap      = 32
)

var errGSORefused = unix.EIO

// receiver reads up to recvBatch datagrams per recvmmsg into buffers it owns, without allocating.
type receiver struct {
	rc    syscall.RawConn
	read  func(fd uintptr) bool
	got   uintptr
	errno syscall.Errno
	hdrs  [recvBatch]mmsghdr
	iovs  [recvBatch]unix.Iovec
	names [recvBatch]unix.RawSockaddrInet4
	oobs  [recvBatch][oobLen]byte
	bufs  [recvBatch][recvBuf]byte
}

func newReceiver(c *net.UDPConn) (*receiver, error) {
	rc, err := c.SyscallConn()
	if err != nil {
		return nil, err
	}
	r := &receiver{rc: rc}
	r.read = r.recvmmsg
	for i := range r.hdrs {
		r.iovs[i].Base = &r.bufs[i][0]
		h := &r.hdrs[i].hdr
		h.Name = (*byte)(unsafe.Pointer(&r.names[i])) //nolint:gosec // the kernel writes a sockaddr_in here
		h.Iov, h.Control = &r.iovs[i], &r.oobs[i][0]
		h.SetIovlen(1)
	}
	return r, nil
}

func (r *receiver) recv(each func(d []byte, from netip.AddrPort)) error {
	for i := range r.hdrs {
		r.iovs[i].SetLen(recvBuf)
		r.hdrs[i].hdr.Namelen = unix.SizeofSockaddrInet4
		r.hdrs[i].hdr.SetControllen(oobLen)
	}
	err := r.rc.Read(r.read)
	switch {
	case err != nil:
		return err
	case r.errno != 0:
		return r.errno
	}
	for i := range int(r.got) { //nolint:gosec // at most recvBatch
		h, name := &r.hdrs[i], &r.names[i]
		port := binary.BigEndian.Uint16((*[2]byte)(unsafe.Pointer(&name.Port))[:]) //nolint:gosec // Port holds network byte order
		split(r.bufs[i][:h.n], groSize(r.oobs[i][:h.hdr.Controllen]), netip.AddrPortFrom(netip.AddrFrom4(name.Addr), port), each)
	}
	return nil
}

func (r *receiver) recvmmsg(fd uintptr) bool {
	for {
		r.got, _, r.errno = unix.Syscall6(unix.SYS_RECVMMSG, fd, uintptr(unsafe.Pointer(&r.hdrs[0])), recvBatch, 0, 0, 0) //nolint:gosec // hdrs outlives the call
		if r.errno != unix.EINTR {
			return r.errno != unix.EAGAIN
		}
	}
}

// mmsghdr is the kernel's struct mmsghdr.
type mmsghdr struct {
	hdr unix.Msghdr
	n   uint32
	_   [4]byte
}

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
