package sandbox

import (
	"slices"
	"time"
)

const (
	// NetNone is the hardened default: no NIC at all, vsock-only I/O.
	NetNone NetShape = "none"
	// NetEgress attaches the node's bridge or CNI network.
	NetEgress NetShape = "egress"

	Small  Size = "small"
	Medium Size = "medium"
	Large  Size = "large"
	XLarge Size = "xlarge"

	volumeModeRO = "ro"
	volumeModeRW = "rw"
)

// NetShape selects the sandbox network lane.
type NetShape string

// Size is a T-shirt resource tier; free-form CPU/memory would fragment the
// node's warm pools.
type Size string

// Volume requests one catalog entry at an optional guest mount path and mode.
type Volume struct {
	Name  string `json:"name"`
	Mount string `json:"mount,omitempty"`
	// Mode is "rw" for a writable mount; empty (or "ro") means read-only.
	Mode string `json:"mode,omitempty"`
}

// VolumeInfo describes one visible fleet catalog entry.
type VolumeInfo struct {
	Name         string `json:"name"`
	DefaultMount string `json:"default_mount"`
	SizeBytes    int64  `json:"size_bytes"`
	Available    bool   `json:"available"`
	Nodes        int    `json:"nodes"`
	Writable     bool   `json:"writable,omitempty"`
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

// WithVolumes requests catalog volumes for a claim; each entry defaults to
// read-only, set Volume.Mode to "rw" for a writable mount.
func WithVolumes(volumes ...Volume) Option {
	volumes = slices.Clone(volumes)
	for i, v := range volumes {
		if v.Mode == volumeModeRO {
			volumes[i].Mode = ""
		}
	}
	return func(r *claimRequest) { r.Volumes = volumes }
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

// WithWorkspace binds the sandbox to a shared workspace: the owning node keeps
// <workspace-root>/<name> synced with the guest workspace dir under a
// multi-writer, close-to-open contract. Ignored by nodes with no workspace
// root configured.
func WithWorkspace(name string) Option {
	return func(r *claimRequest) { r.Workspace = name }
}

// WithNoRedirect pins the claim to the addressed node instead of following a
// warm-pool redirect to a peer — useful for driving a specific node.
func WithNoRedirect() Option {
	return func(r *claimRequest) { r.NoRedirect = true }
}
