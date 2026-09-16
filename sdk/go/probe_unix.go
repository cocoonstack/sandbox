//go:build unix

package sandbox

import "syscall"

const canProbe = true

// peek looks at a socket's next byte without consuming it; quiet means there is none and the peer is still there.
type peek struct{ quiet bool }

func (p *peek) read(fd uintptr) bool {
	var b [1]byte
	_, _, err := syscall.Recvfrom(int(fd), b[:], syscall.MSG_PEEK)
	p.quiet = err == syscall.EAGAIN
	return true
}
