module github.com/cocoonstack/sandbox/sdk/go

go 1.26.4

replace github.com/cocoonstack/sandbox/protocol/wire => ../../protocol/wire

// Real pseudo-version so downstream `go get` resolves without the local
// replace (dependency-module replaces are ignored). Cut a protocol/wire tag
// alongside every sdk/go release and bump this to it.
require github.com/cocoonstack/sandbox/protocol/wire v0.0.0-20260718024729-0689a7475e64
