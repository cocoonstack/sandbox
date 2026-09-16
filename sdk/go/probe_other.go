//go:build !unix

package sandbox

import "net"

const canProbe = false

func peerQuiet(net.Conn) bool { return false }
