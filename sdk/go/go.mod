module github.com/cocoonstack/sandbox/sdk/go

go 1.27.1

replace github.com/cocoonstack/sandbox/protocol/wire => ../../protocol/wire

// Consumers ignore the replace: pin the protocol/wire tag cut with each sdk/go release.
require github.com/cocoonstack/sandbox/protocol/wire v0.1.32
