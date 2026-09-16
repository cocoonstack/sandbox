//go:build unix

package sandbox

import (
	"errors"
	"net"
	"syscall"
)

const canProbe = true

// peerQuiet peeks at a parked connection without consuming: a peer that hung up, or spoke unprompted, fails it.
func peerQuiet(conn net.Conn) bool {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return false
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return false
	}
	quiet := false
	err = rc.Read(func(fd uintptr) bool {
		var b [1]byte
		_, _, rerr := syscall.Recvfrom(int(fd), b[:], syscall.MSG_PEEK)
		quiet = errors.Is(rerr, syscall.EAGAIN)
		return true
	})
	return err == nil && quiet
}
