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

	// EngineCH is the default hypervisor; EngineFC cold-boots under Firecracker.
	EngineCH Engine = "ch"
	EngineFC Engine = "fc"

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

// Engine is the pool key's hypervisor axis.
type Engine string

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

// WithVolumesAttachOnly attaches the claim's volumes without mounting them;
// the workload finds each device by its serial and owns the mount — and its
// flush — from there: release without a clean self-umount discards unsynced
// guest pages, since the sandbox performs no sync of its own.
func WithVolumesAttachOnly() Option {
	return func(r *claimRequest) { r.VolumesAttachOnly = true }
}

// WithTimeout bounds the sandbox's lifetime: the owning node reaps it after
// d (rounded up to seconds) even if the client vanishes.
func WithTimeout(d time.Duration) Option {
	return func(r *claimRequest) {
		r.TTLSeconds = ttlSeconds(d)
	}
}

// WithClaimRef records an opaque caller reference on the claim (the aggregated
// apiserver passes its k8s "<namespace>/<name>"), echoed by Client.Sandboxes.
func WithClaimRef(ref string) Option {
	return func(r *claimRequest) { r.ClaimRef = ref }
}

// ttlSeconds rounds a lease up to whole wire seconds.
func ttlSeconds(d time.Duration) int {
	return int((d + time.Second - 1) / time.Second)
}
