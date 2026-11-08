//go:build linux

package egress

import (
	"net"

	"golang.org/x/sys/unix"
)

func clampWindow(c *net.TCPConn, n int) {
	raw, err := c.SyscallConn()
	if err != nil {
		return
	}
	_ = raw.Control(func(fd uintptr) {
		_ = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_WINDOW_CLAMP, n)
	})
}
