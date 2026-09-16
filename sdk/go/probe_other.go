//go:build !unix

package sandbox

const canProbe = false

type peek struct{ quiet bool }

func (*peek) read(uintptr) bool { return true }
