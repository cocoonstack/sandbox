//go:build !linux

package egress

import "net"

func clampWindow(*net.TCPConn, int) {}
