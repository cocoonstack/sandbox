module github.com/cocoonstack/sandbox/sdk/go

go 1.26.5

replace github.com/cocoonstack/sandbox/protocol/wire => ../../protocol/wire

// Downstream consumers ignore the local replace, so this must name a real,
// resolvable version: cut a protocol/wire tag alongside every sdk/go release
// and keep this pinned to it.
require github.com/cocoonstack/sandbox/protocol/wire v0.1.6
