package types

import (
	"math"
	"time"
)

const (
	VolumeAttached  VolumePhase = "attached"
	VolumePending   VolumePhase = "pending"
	VolumeDetaching VolumePhase = "detaching"
)

// VolumeInfo is the caller-visible, host-path-free catalog projection.
type VolumeInfo struct {
	Name         string `json:"name"`
	DefaultMount string `json:"default_mount"`
	SizeBytes    int64  `json:"size_bytes"`
	Available    bool   `json:"available"`
	Nodes        int    `json:"nodes"`
	// Writable reports whether the operator allows rw claims of this name.
	Writable bool `json:"writable,omitzero"`
}

// VolumeRegistration binds a backing path to a runtime catalog name.
type VolumeRegistration struct {
	Path     string   `json:"path"`
	DirectIO string   `json:"directio,omitempty"`
	Writable bool     `json:"writable,omitzero"`
	Tenants  []string `json:"tenants,omitempty"`
}

// VolumePatch is the wire body of PATCH /v1/sandboxes/{id}/volumes.
type VolumePatch struct {
	Add    []Volume `json:"add,omitzero"`
	Remove []string `json:"remove,omitzero"`
}

// VolumePhase is how far one held volume has converged.
type VolumePhase string

// VolumeState reports one held volume and its last failure.
type VolumeState struct {
	Name  string      `json:"name"`
	State VolumePhase `json:"state"`
	Error string      `json:"error,omitempty"`
}

// SandboxVolumesResponse is the wire reply of the sandbox volume routes, failures included.
type SandboxVolumesResponse struct {
	VolumesWant []Volume      `json:"volumes_want"`
	Volumes     []Volume      `json:"volumes"`
	States      []VolumeState `json:"states"`
	Converging  bool          `json:"converging"`
	Error       string        `json:"error,omitempty"`
}

// Seconds converts wire seconds to a duration: a negative count is zero and a huge one saturates instead of wrapping.
func Seconds(n int) time.Duration {
	return time.Duration(min(max(int64(n), 0), math.MaxInt64/int64(time.Second))) * time.Second
}
