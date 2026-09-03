// Package types defines the shared vocabulary of the sandbox control plane.
package types

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
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

	RestoreCopy     RestoreMode = "copy"
	RestoreOnDemand RestoreMode = "ondemand"
	RestoreMmap     RestoreMode = "mmap"

	MaxClaimVolumes = 8

	// Also the guest `mount -o` option literals: renaming them changes the mount flags.
	VolumeModeRO = "ro"
	VolumeModeRW = "rw"

	DirectIOOn   = "on"
	DirectIOOff  = "off"
	DirectIOAuto = "auto"
)

var (
	// NameRe pins caller-chosen names to one conservative charset cocoon also accepts.
	NameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,62}$`)
	// VolumeNameRe is the virtio disk serial grammar shared with cocoon.
	VolumeNameRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,19}$`)

	sizeSpecs = map[Size]SizeSpec{
		SizeSmall:  {CPU: 1, Memory: "512M", MemoryBytes: 512 << 20},
		SizeMedium: {CPU: 2, Memory: "1G", MemoryBytes: 1 << 30},
		SizeLarge:  {CPU: 4, Memory: "4G", MemoryBytes: 4 << 30},
		SizeXLarge: {CPU: 4, Memory: "8G", MemoryBytes: 8 << 30},
	}

	guestOSMountRoots = []string{
		"/bin", "/boot", "/dev", "/etc", "/lib", "/lib64", "/proc",
		"/run", "/sbin", "/sys", "/usr", "/var",
	}
)

// NetShape selects whether the Cloud Hypervisor guest has a NIC.
type NetShape string

// RestoreMode selects cocoon's clone memory-restore strategy; empty is cocoon's default.
type RestoreMode string

// Validate accepts the empty default plus cocoon's known restore modes.
func (m RestoreMode) Validate() error {
	switch m {
	case "", RestoreCopy, RestoreOnDemand, RestoreMmap:
		return nil
	default:
		return fmt.Errorf("unknown restore mode %q", m)
	}
}

// Size is a T-shirt resource tier.
type Size string

// SizeSpec is the concrete allocation behind a tier; Memory is in cocoon flag units.
type SizeSpec struct {
	CPU         int
	Memory      string
	MemoryBytes int64
}

// Spec resolves a tier to its allocation; ok is false for unknown tiers.
func (s Size) Spec() (SizeSpec, bool) {
	spec, ok := sizeSpecs[s]
	return spec, ok
}

// PoolKey identifies one warm pool.
type PoolKey struct {
	Template string   `json:"template"`
	Net      NetShape `json:"net"`
	Size     Size     `json:"size"`
}

// Capturable is false on the egress lane: a resumed capture opens an unlocked-NIC window.
func (k PoolKey) Capturable() bool {
	return k.Net != NetEgress
}

// Defaulted fills the wire defaults: the no-NIC lane and the smallest tier.
func (k PoolKey) Defaulted() PoolKey {
	k.Net = cmp.Or(k.Net, NetNone)
	k.Size = cmp.Or(k.Size, SizeSmall)
	return k
}

// Hash is a stable 128-bit digest for naming; a shorter tag would be brute-forceable.
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

	// Tenant names the owning tenant; empty means the operator claimed it.
	Tenant string `json:"tenant,omitempty"`

	// ClaimRef is the opaque caller reference recorded at claim time; empty for pool claims.
	ClaimRef string `json:"claim_ref,omitempty"`
	// Volumes records the volumes successfully applied to this claim.
	Volumes []Volume `json:"volumes,omitempty"`

	VsockSocket string `json:"vsock_socket,omitempty"`
	// TAP is the egress-lane NIC's host tap; empty on the none lane.
	TAP string `json:"tap,omitempty"`
	// HibernateSnap names the memory snapshot while the VM is hibernated; empty means running.
	HibernateSnap string `json:"hibernate_snap,omitempty"`
	// PendingSnap is the journaled intent of a hibernate in flight, cleared by its commit.
	PendingSnap string `json:"pending_snap,omitempty"`
	// ArchiveCk names the store checkpoint holding an archived sandbox; empty means local.
	ArchiveCk string `json:"archive_ck,omitempty"`

	// FromCheckpoint names the checkpoint this sandbox branched from; empty otherwise.
	FromCheckpoint string `json:"from_checkpoint,omitempty"`
	// TemplateDigest identifies the promoted-template export this sandbox was cloned from.
	TemplateDigest string `json:"template_digest,omitempty"`

	// StaleSnap names a consumed wake snapshot a lagging journal references; guarded by Transition.
	StaleSnap string `json:"-"`

	// lastActivity is unix-nanos of the last data-plane connection, stamped lock-free.
	lastActivity atomic.Int64

	// Transition serializes hibernate/wake; lock it before (never under) the manager mutex.
	Transition sync.Mutex `json:"-"`
}

// Touch stamps last data-plane activity; lock-free, called on the relay hot path.
func (s *Sandbox) Touch() { s.lastActivity.Store(time.Now().UnixNano()) }

// TouchAt stamps last-activity at a caller-supplied instant (batch claim/adoption, tests).
func (s *Sandbox) TouchAt(t time.Time) { s.lastActivity.Store(t.UnixNano()) }

// LastSeen returns the last data-plane activity time.
func (s *Sandbox) LastSeen() time.Time { return time.Unix(0, s.lastActivity.Load()) }

// Checkpoint is the record of a captured sandbox state; node-local, like a template.
type Checkpoint struct {
	ID        string    `json:"id"`
	Name      string    `json:"name,omitempty"`
	SandboxID string    `json:"sandbox_id"`
	Key       PoolKey   `json:"key"`
	Tenant    string    `json:"tenant,omitempty"`
	CreatedAt time.Time `json:"created_at"`

	// Archive marks a lifecycle-internal wake image: hidden from listings and undeletable.
	Archive bool `json:"archive,omitempty"`
}

// VMNetConfig is the per-NIC host tap the egress-lane nft lock binds.
type VMNetConfig struct {
	TAP string `json:"tap"`
}

// VMConfig is the config subset of VMRecord.
type VMConfig struct {
	Name string `json:"name"`
}

// VMRecord is the subset of cocoon's VM records the control plane reads.
type VMRecord struct {
	State          string        `json:"state"`
	PID            int           `json:"pid"`
	VsockSocket    string        `json:"vsock_socket"`
	NetworkConfigs []VMNetConfig `json:"network_configs,omitempty"`
	Config         VMConfig      `json:"config"`
}

// TapDevice returns the first NIC's host tap; empty when the record carries none.
func (r VMRecord) TapDevice() string {
	if len(r.NetworkConfigs) == 0 {
		return ""
	}
	return r.NetworkConfigs[0].TAP
}

// Volume is one dataset mount; Mode is normalized to "" (read-only) or VolumeModeRW.
type Volume struct {
	Name  string `json:"name"`
	Mount string `json:"mount,omitempty"`
	Mode  string `json:"mode,omitempty"`
	// AttachOnly carries the claim's flag into validation; past it the empty Mount is the signal.
	AttachOnly bool `json:"-"`
}

// RW reports whether the entry asks for write access.
func (v Volume) RW() bool { return v.Mode == VolumeModeRW }

// TemplateGossipHash scopes a key hash to its owner so the tenant never travels the wire.
func TemplateGossipHash(keyHash, tenant string) string {
	sum := sha256.Sum256([]byte(keyHash + "|" + tenant))
	return hex.EncodeToString(sum[:16])
}

// ValidVolumeName reports whether name is a legal cocoon data-disk serial.
func ValidVolumeName(name string) bool {
	return VolumeNameRe.MatchString(name) && !strings.HasPrefix(name, "cocoon-")
}

// DefaultVolumeMount returns the guest mount used when a request omits one.
func DefaultVolumeMount(name string) string {
	return "/volumes/" + name
}

// ValidDirectIO reports whether mode is a legal volume direct-I/O setting.
func ValidDirectIO(mode string) bool {
	return mode == DirectIOOn || mode == DirectIOOff || mode == DirectIOAuto
}

// VolumeNames projects the entries' names in order; nil for none.
func VolumeNames(volumes []Volume) []string {
	if len(volumes) == 0 {
		return nil
	}
	names := make([]string, len(volumes))
	for i, volume := range volumes {
		names[i] = volume.Name
	}
	return names
}

// VolumeRWNames projects the names of the write-enabled entries; nil for none.
func VolumeRWNames(volumes []Volume) []string {
	var names []string
	for _, volume := range volumes {
		if volume.RW() {
			names = append(names, volume.Name)
		}
	}
	return names
}

// VolumesAttachOnly reports whether the caller mounts the devices itself.
func VolumesAttachOnly(volumes []Volume) bool {
	return slices.ContainsFunc(volumes, func(v Volume) bool { return v.AttachOnly })
}

// ValidateVolumes returns detached entries with defaults filled and modes normalized.
func ValidateVolumes(volumes []Volume, attachOnly bool) ([]Volume, error) {
	if len(volumes) > MaxClaimVolumes {
		return nil, fmt.Errorf("volumes must contain at most %d entries, got %d", MaxClaimVolumes, len(volumes))
	}
	if attachOnly && len(volumes) == 0 {
		return nil, errors.New("volumes_attach_only requires at least one volume")
	}
	applied := make([]Volume, len(volumes))
	names := make(map[string]struct{}, len(volumes))
	for i, volume := range volumes {
		if !ValidVolumeName(volume.Name) {
			return nil, fmt.Errorf("volumes[%d] name %q must match %s and not start with cocoon-", i, volume.Name, VolumeNameRe)
		}
		if _, ok := names[volume.Name]; ok {
			return nil, fmt.Errorf("volumes[%d] duplicates name %q", i, volume.Name)
		}
		names[volume.Name] = struct{}{}
		mode := volume.Mode
		switch mode {
		case VolumeModeRO:
			mode = ""
		case "", VolumeModeRW:
		default:
			return nil, fmt.Errorf("volumes[%d] mode %q must be %s or %s", i, mode, VolumeModeRO, VolumeModeRW)
		}
		if attachOnly {
			if volume.Mount != "" {
				return nil, fmt.Errorf("volumes[%d] mount %q is meaningless when the caller mounts the device itself", i, volume.Mount)
			}
			applied[i] = Volume{Name: volume.Name, Mode: mode, AttachOnly: true}
			continue
		}
		mount := volume.Mount
		if mount == "" {
			mount = DefaultVolumeMount(volume.Name)
		}
		if err := validateVolumeMount(mount); err != nil {
			return nil, fmt.Errorf("volumes[%d] mount %q: %w", i, mount, err)
		}
		for j := range i {
			other := applied[j].Mount
			switch {
			case mount == other:
				return nil, fmt.Errorf("volumes[%d] mount %q duplicates volumes[%d]", i, mount, j)
			case pathWithin(other, mount), pathWithin(mount, other):
				return nil, fmt.Errorf("volumes[%d] mount %q nests with volumes[%d] mount %q", i, mount, j, other)
			}
		}
		applied[i] = Volume{Name: volume.Name, Mount: mount, Mode: mode}
	}
	return applied, nil
}

func validateVolumeMount(mount string) error {
	if !filepath.IsAbs(mount) {
		return errors.New("must be absolute")
	}
	if filepath.Clean(mount) != mount {
		return errors.New("must be clean")
	}
	if mount == "/" {
		return errors.New("must be outside the guest OS tree")
	}
	for _, root := range guestOSMountRoots {
		if mount == root || pathWithin(root, mount) {
			return errors.New("must be outside the guest OS tree")
		}
	}
	return nil
}

func pathWithin(parent, child string) bool {
	return strings.HasPrefix(child, parent+"/")
}
