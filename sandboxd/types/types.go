// Package types defines the shared vocabulary of the sandbox control plane:
// pool keys with their derived backend lane, size tiers, and the sandbox
// record tracked by a node.
package types

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

const (
	NetNone   NetShape = "none"
	NetEgress NetShape = "egress"

	SizeSmall  Size = "small"
	SizeMedium Size = "medium"
	SizeLarge  Size = "large"

	BackendCH Backend = "ch"
	BackendFC Backend = "fc"
)

var sizeSpecs = map[Size]SizeSpec{
	SizeSmall:  {CPU: 1, Memory: "512M"},
	SizeMedium: {CPU: 2, Memory: "1G"},
	SizeLarge:  {CPU: 4, Memory: "4G"},
}

// NetShape selects the sandbox network lane and derives the backend.
type NetShape string

// Size is a T-shirt resource tier. Free-form CPU/memory would fragment the
// warm pools, so only tiers are accepted.
type Size string

// Backend names the hypervisor serving a lane.
type Backend string

// PoolKey identifies one warm pool. Every New() parameter that changes VM
// construction is an axis here.
type PoolKey struct {
	Template string   `json:"template"`
	Net      NetShape `json:"net"`
	Size     Size     `json:"size"`
}

// Backend derives the hypervisor lane: FC serves the no-network lane (faster
// restore, minimal device model, and none of FC's clone restrictions apply
// without a NIC); CH serves the full-featured egress lane.
func (k PoolKey) Backend() Backend {
	if k.Net == NetNone {
		return BackendFC
	}
	return BackendCH
}

// Hash is a short stable digest used in VM, snapshot, and golden-dir naming.
func (k PoolKey) Hash() string {
	sum := sha256.Sum256([]byte(k.Template + "|" + string(k.Net) + "|" + string(k.Size)))
	return hex.EncodeToString(sum[:4])
}

// Validate checks that every axis holds a known value.
func (k PoolKey) Validate() error {
	if k.Template == "" {
		return fmt.Errorf("template must not be empty")
	}
	switch k.Net {
	case NetNone, NetEgress:
	default:
		return fmt.Errorf("unknown net shape %q", k.Net)
	}
	if _, ok := k.Size.Spec(); !ok {
		return fmt.Errorf("unknown size %q", k.Size)
	}
	return nil
}

// SizeSpec is the concrete allocation behind a tier, in cocoon flag units.
type SizeSpec struct {
	CPU    int
	Memory string
}

// Spec resolves a tier to its allocation; ok is false for unknown tiers.
func (s Size) Spec() (SizeSpec, bool) {
	spec, ok := sizeSpecs[s]
	return spec, ok
}

// Sandbox is the node-local record of one pooled or claimed VM.
type Sandbox struct {
	ID     string  `json:"id"`
	VMName string  `json:"vm_name"`
	Key    PoolKey `json:"key"`

	Token    string    `json:"token,omitempty"`
	Deadline time.Time `json:"deadline,omitzero"`

	VsockSocket string `json:"vsock_socket,omitempty"`
}

// VMRecord is the subset of cocoon's `vm list --format json` output the
// control plane reads.
type VMRecord struct {
	State       string   `json:"state"`
	VsockSocket string   `json:"vsock_socket"`
	Config      VMConfig `json:"config"`
}

// VMConfig is the config subset of VMRecord.
type VMConfig struct {
	Name string `json:"name"`
}
