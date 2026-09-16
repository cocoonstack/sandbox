//go:build unix

package sandbox

import "syscall"

const canProbe = true

// peek looks at the socket's next byte without consuming it; peeked means there is none and the peer is still there.
func (c *agentConn) peek(fd uintptr) bool {
	var b [1]byte
	_, _, err := syscall.Recvfrom(int(fd), b[:], syscall.MSG_PEEK)
	c.peeked = err == syscall.EAGAIN
	return true
}
