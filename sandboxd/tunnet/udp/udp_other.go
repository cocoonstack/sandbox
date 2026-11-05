//go:build !linux

package udp

import (
	"errors"
	"net"
)

const (
	gsoSupported = false
	cmsgCap      = 32
)

var errGSORefused = errors.ErrUnsupported

func segmentCmsg(*[cmsgCap]byte, int) []byte { return nil }

func groSize([]byte) int { return 0 }

func enableGRO(*net.UDPConn) {}
