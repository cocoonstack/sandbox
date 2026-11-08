//go:build linux

package egress

import (
	"net"
	"syscall"
)

func clampWindow(c *net.TCPConn, n int) {
	raw, err := c.SyscallConn()
	if err != nil {
		return
	}
	_ = raw.Control(func(fd uintptr) {
		_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_WINDOW_CLAMP, n)
	})
}
