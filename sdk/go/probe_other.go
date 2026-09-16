//go:build !unix

package sandbox

const canProbe = false

func (*agentConn) peek(uintptr) bool { return true }
