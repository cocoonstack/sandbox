package sandbox

import (
	"maps"
	"slices"
	"time"
)

const (
	// NetNone is the hardened default: no NIC at all, vsock-only I/O.
	NetNone NetShape = "none"
	// NetEgress attaches the node's bridge or CNI network.
	NetEgress NetShape = "egress"

	Small    Size = "small"
	Medium   Size = "medium"
	Medium4G Size = "medium-4g"
	Large    Size = "large"
	XLarge   Size = "xlarge"
	XXLarge  Size = "2xlarge"
)

// NetShape selects the sandbox network lane.
type NetShape string

// Size is a T-shirt resource tier; free-form CPU/memory would fragment the node's warm pools.
type Size string

// Volume requests one catalog entry at an optional guest mount path and mode.
type Volume struct {
	Name  string `json:"name"`
	Mount string `json:"mount,omitempty"`
	// Mode is "rw" for a writable mount; empty (or "ro") means read-only.
	Mode string `json:"mode,omitempty"`
	// AttachOnly leaves mounting and unmounting to the caller.
	AttachOnly bool `json:"attach_only,omitzero"`
}

// VolumeInfo describes one visible fleet catalog entry.
type VolumeInfo struct {
	Name         string `json:"name"`
	DefaultMount string `json:"default_mount"`
	SizeBytes    int64  `json:"size_bytes"`
	Available    bool   `json:"available"`
	Nodes        int    `json:"nodes"`
	Writable     bool   `json:"writable,omitzero"`
}

// EnvInject makes the node's egress proxy send a host-only entry's value to Hosts, exact or "*." suffix, as Header or as the Query parameter, where the guest sent a set Placeholder.
type EnvInject struct {
	Hosts       []string `json:"hosts"`
	Header      string   `json:"header,omitempty"`
	Query       string   `json:"query,omitempty"`
	Body        bool     `json:"body,omitzero"`
	Placeholder string   `json:"placeholder,omitempty"`
}

// EnvVar is one entry of a claim's env; a nil Guest delivers it into the guest, false keeps it host-side for the node's egress.
type EnvVar struct {
	Value  string     `json:"value"`
	Guest  *bool      `json:"guest,omitempty"`
	Inject *EnvInject `json:"inject,omitempty"`
	// Missing, on a read, marks a host-only entry whose value the node no longer holds; a write that sets it is refused.
	Missing bool `json:"missing,omitzero"`
}

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

// WithVolumes requests catalog volumes for a claim; each entry defaults to read-only.
func WithVolumes(volumes ...Volume) Option {
	volumes = slices.Clone(volumes)
	return func(r *claimRequest) { r.Volumes = volumes }
}

// WithTimeout makes the owning node reap the sandbox after d, rounded up to seconds.
func WithTimeout(d time.Duration) Option {
	return func(r *claimRequest) {
		r.TTLSeconds = ttlSeconds(d)
	}
}

// WithClaimRef records an opaque caller reference on the claim, echoed by Client.Sandboxes.
func WithClaimRef(ref string) Option {
	return func(r *claimRequest) { r.ClaimRef = ref }
}

// WithMetadata records key-value labels on the claim, echoed by Client.Sandboxes.
func WithMetadata(md map[string]string) Option {
	md = maps.Clone(md)
	return func(r *claimRequest) { r.Metadata = md }
}

// WithEnv sets the claim's own env.
func WithEnv(env map[string]EnvVar) Option {
	env = maps.Clone(env)
	return func(r *claimRequest) { r.Env = env }
}

// WithArchiveOnExpire makes the node hibernate and archive the sandbox when its lease ends, instead of destroying it; a wake restores it.
func WithArchiveOnExpire() Option {
	return func(r *claimRequest) { r.OnExpire = "archive" }
}

func ttlSeconds(d time.Duration) int {
	return int((d + time.Second - 1) / time.Second)
}
