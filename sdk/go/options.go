package sandbox

import "time"

const (
	// NetNone is the hardened default: no NIC at all, vsock-only I/O.
	NetNone NetShape = "none"
	// NetEgress attaches the node's bridge or CNI network.
	NetEgress NetShape = "egress"

	Small  Size = "small"
	Medium Size = "medium"
	Large  Size = "large"
	XLarge Size = "xlarge"
)

// NetShape selects the sandbox network lane.
type NetShape string

// Size is a T-shirt resource tier; free-form CPU/memory would fragment the
// node's warm pools.
type Size string

// Option configures a New claim.
type Option func(*claimRequest)

// WithNetwork selects the network lane.
func WithNetwork(n NetShape) Option {
	return func(r *claimRequest) { r.Net = string(n) }
}

// WithSize selects the resource tier.
func WithSize(s Size) Option {
	return func(r *claimRequest) { r.Size = string(s) }
}

// WithTimeout bounds the sandbox's lifetime: the owning node reaps it after
// d (rounded up to seconds) even if the client vanishes.
func WithTimeout(d time.Duration) Option {
	return func(r *claimRequest) {
		r.TTLSeconds = ttlSeconds(d)
	}
}

// ttlSeconds rounds a lease up to whole wire seconds.
func ttlSeconds(d time.Duration) int {
	return int((d + time.Second - 1) / time.Second)
}
