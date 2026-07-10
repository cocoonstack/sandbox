// Package types defines the shared vocabulary of the sandbox control plane:
// pool keys with their derived backend lane, size tiers, and the sandbox
// record tracked by a node.
package types

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

const (
	NetNone   NetShape = "none"
	NetEgress NetShape = "egress"

	SizeSmall  Size = "small"
	SizeMedium Size = "medium"
	SizeLarge  Size = "large"
	SizeXLarge Size = "xlarge"

	BackendCH Backend = "ch"
	BackendFC Backend = "fc"
)

var (
	// NameRe pins caller-chosen names (templates, checkpoints, tenants),
	// which ride in journal fields and metric labels: one conservative
	// charset, also accepted by cocoon's snapshot naming.
	NameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,62}$`)

	sizeSpecs = map[Size]SizeSpec{
		SizeSmall:  {CPU: 1, Memory: "512M"},
		SizeMedium: {CPU: 2, Memory: "1G"},
		SizeLarge:  {CPU: 4, Memory: "4G"},
		SizeXLarge: {CPU: 4, Memory: "8G"},
	}
)

// NetShape selects the sandbox network lane and derives the backend.
type NetShape string

// Backend names the hypervisor serving a lane.
type Backend string

// Size is a T-shirt resource tier. Free-form CPU/memory would fragment the
// warm pools, so only tiers are accepted.
type Size string

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

// Hash is a stable digest used in VM, snapshot, and golden-dir naming.
// 128 bits, not a short tag: Promote keys goldens by caller-chosen template
// names, so a targeted collision with a configured pool's hash must stay a
// second-preimage problem, never a brute-forceable one.
func (k PoolKey) Hash() string {
	sum := sha256.Sum256([]byte(k.Template + "|" + string(k.Net) + "|" + string(k.Size)))
	return hex.EncodeToString(sum[:16])
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

// Sandbox is the node-local record of one pooled or claimed VM.
type Sandbox struct {
	ID     string  `json:"id"`
	VMName string  `json:"vm_name"`
	Key    PoolKey `json:"key"`

	Token    string    `json:"token,omitempty"`
	Deadline time.Time `json:"deadline,omitzero"`

	// Tenant names the owning tenant; empty means the operator (root)
	// claimed it. Fork children inherit it from the parent.
	Tenant string `json:"tenant,omitempty"`

	VsockSocket string `json:"vsock_socket,omitempty"`
	// HibernateSnap names the memory snapshot while the VM is hibernated;
	// empty means running.
	HibernateSnap string `json:"hibernate_snap,omitempty"`
	// PendingSnap is the journaled intent of a hibernate in flight: written
	// before the engine stops the VM, cleared by the commit. Reconcile
	// trusts it to adopt a hibernate whose commit never landed.
	PendingSnap string `json:"pending_snap,omitempty"`
	// ArchiveCk names the store checkpoint holding this sandbox's state while
	// archived; empty means live or hibernated. While set, VMName/VsockSocket/
	// HibernateSnap are empty (no local VM) and Deadline is the retention deadline.
	ArchiveCk string `json:"archive_ck,omitempty"`

	// FromCheckpoint names the checkpoint this sandbox branched from, for
	// lineage; empty for pool and template claims.
	FromCheckpoint string `json:"from_checkpoint,omitempty"`

	// StaleSnap names a consumed wake snapshot a lagging journal still
	// references; dropped once a later write lands. Guarded by Transition.
	StaleSnap string `json:"-"`

	// lastActivity is unix-nanos of the last data-plane connection, for the
	// idle policy; lock-free, stamped on the relay hot path. Runtime-only, a
	// restart resets it to adoption time.
	lastActivity atomic.Int64

	// Transition serializes hibernate/wake so concurrent wakes collapse onto
	// one restore. Lock it before (never under) the manager mutex.
	Transition sync.Mutex `json:"-"`
}

// Touch stamps last data-plane activity; lock-free, called on the relay hot path.
func (s *Sandbox) Touch() { s.lastActivity.Store(time.Now().UnixNano()) }

// TouchAt stamps last-activity at a caller-supplied instant (batch claim/adoption, tests).
func (s *Sandbox) TouchAt(t time.Time) { s.lastActivity.Store(t.UnixNano()) }

// LastSeen returns the last data-plane activity time.
func (s *Sandbox) LastSeen() time.Time { return time.Unix(0, s.lastActivity.Load()) }

// Checkpoint is the record of a captured sandbox state: claims cloned from
// it branch off the exact captured moment. Node-local, like a template.
type Checkpoint struct {
	ID        string    `json:"id"`
	Name      string    `json:"name,omitempty"`
	SandboxID string    `json:"sandbox_id"`
	Key       PoolKey   `json:"key"`
	Tenant    string    `json:"tenant,omitempty"`
	CreatedAt time.Time `json:"created_at"`

	// Archive marks a lifecycle-internal wake image: hidden from listings
	// and undeletable on every node sharing the store, not just the owner.
	Archive bool `json:"archive,omitempty"`
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
