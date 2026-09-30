// Package types defines the shared vocabulary of the sandbox control plane.
package types

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/net/http/httpguts"
)

const (
	NetNone   NetShape = "none"
	NetEgress NetShape = "egress"

	SizeSmall   Size = "small"
	SizeMedium  Size = "medium"
	SizeLarge   Size = "large"
	SizeXLarge  Size = "xlarge"
	Size2XLarge Size = "2xlarge"

	LayerPooled   PolicyLayer = "pooled"
	LayerUnpooled PolicyLayer = "unpooled"

	NetRouteRelay  NetRoute = "relay"
	NetRouteDirect NetRoute = "direct"
	NetRouteNone   NetRoute = "none"

	RestoreCopy     RestoreMode = "copy"
	RestoreOnDemand RestoreMode = "ondemand"
	RestoreMmap     RestoreMode = "mmap"

	ExpireDestroy ExpireAction = "destroy"
	ExpireArchive ExpireAction = "archive"

	MaxClaimVolumes = 8

	// Also the guest `mount -o` option literals: renaming them changes the mount flags.
	VolumeModeRO = "ro"
	VolumeModeRW = "rw"

	DirectIOOn   = "on"
	DirectIOOff  = "off"
	DirectIOAuto = "auto"

	maxMetadataPairs      = 16
	maxMetadataKeyBytes   = 128
	maxMetadataValueBytes = 512
	maxMetadataBytes      = 4 << 10

	maxEnvVars       = 64
	maxEnvValueBytes = 8 << 10
)

var (
	// NameRe pins caller-chosen names to one conservative charset cocoon also accepts.
	NameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,62}$`)
	// VolumeNameRe is the virtio disk serial grammar shared with cocoon.
	VolumeNameRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,19}$`)
	// EnvNameRe is the portable shell variable name.
	EnvNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

	// guestFileEscaper covers the characters systemd unescapes inside a double-quoted EnvironmentFile value.
	guestFileEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "`", "\\`", `$`, `\$`)

	sizeSpecs = map[Size]SizeSpec{
		SizeSmall:   {CPU: 1, Memory: "512M", MemoryBytes: 512 << 20},
		SizeMedium:  {CPU: 2, Memory: "1G", MemoryBytes: 1 << 30},
		SizeLarge:   {CPU: 4, Memory: "4G", MemoryBytes: 4 << 30},
		SizeXLarge:  {CPU: 4, Memory: "8G", MemoryBytes: 8 << 30},
		Size2XLarge: {CPU: 8, Memory: "16G", MemoryBytes: 16 << 30},
	}

	guestOSMountRoots = []string{
		"/bin", "/boot", "/dev", "/etc", "/lib", "/lib64", "/proc",
		"/run", "/sbin", "/sys", "/usr", "/var",
	}
)

// NetShape selects whether the Cloud Hypervisor guest has a NIC.
type NetShape string

// PolicyLayer pins, at claim time, whether the key had a pool layer in its egress policy.
type PolicyLayer string

// NetRoute is how a claimed guest reaches the network: relay through the host proxy, a direct NIC, or none.
type NetRoute string

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

// ExpireAction is what the node does with a claim whose lease ends; empty means destroy.
type ExpireAction string

// Validate accepts the empty default plus the known actions.
func (a ExpireAction) Validate() error {
	switch a {
	case "", ExpireDestroy, ExpireArchive:
		return nil
	default:
		return fmt.Errorf("on_expire %q must be %s or %s", a, ExpireDestroy, ExpireArchive)
	}
}

// Or resolves a requested action against current: empty keeps current, destroy clears it.
func (a ExpireAction) Or(current ExpireAction) ExpireAction {
	switch a {
	case "":
		return current
	case ExpireDestroy:
		return ""
	default:
		return a
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

// Metadata is the caller's key-value labels on a claim.
type Metadata map[string]string

// Validate enforces the pair count, key and value charsets, value size, and JSON size, in which a quote or backslash is the only escaped byte.
func (md Metadata) Validate() error {
	if len(md) > maxMetadataPairs {
		return fmt.Errorf("metadata must contain at most %d pairs, got %d", maxMetadataPairs, len(md))
	}
	for k, v := range md {
		if !validMetadataKey(k) {
			return fmt.Errorf("metadata key %q must be 1..%d bytes of printable ASCII without = or &", k, maxMetadataKeyBytes)
		}
		if len(v) > maxMetadataValueBytes {
			return fmt.Errorf("metadata value of key %q must be at most %d bytes, got %d", k, maxMetadataValueBytes, len(v))
		}
		if !validMetadataValue(v) {
			return fmt.Errorf("metadata value of key %q must be printable UTF-8", k)
		}
	}
	if size := md.encodedSize(); size > maxMetadataBytes {
		return fmt.Errorf("metadata must be at most %d bytes as JSON, got %d", maxMetadataBytes, size)
	}
	return nil
}

// Matches reports whether md holds every pair of filter.
func (md Metadata) Matches(filter Metadata) bool {
	for k, v := range filter {
		if got, ok := md[k]; !ok || got != v {
			return false
		}
	}
	return true
}

func (md Metadata) encodedSize() int {
	size := 2 + max(len(md)-1, 0)
	for k, v := range md {
		size += escapedLen(k) + escapedLen(v) + 1
	}
	return size
}

// EnvVar is one entry of a claim's environment; a nil Guest means the entry reaches the guest.
type EnvVar struct {
	Value string `json:"value"`
	Guest *bool  `json:"guest,omitempty"`
}

// InGuest reports whether the entry is delivered into the guest.
func (v EnvVar) InGuest() bool { return v.Guest == nil || *v.Guest }

// UnmarshalJSON rejects unknown members and a null guest, so a mistyped host-only flag never falls back to the guest.
func (v *EnvVar) UnmarshalJSON(b []byte) error {
	var raw struct {
		Value string         `json:"value"`
		Guest jsontext.Value `json:"guest,omitzero"`
	}
	if err := json.Unmarshal(b, &raw, json.RejectUnknownMembers(true)); err != nil {
		return fmt.Errorf("env entry: %w", err)
	}
	*v = EnvVar{Value: raw.Value}
	if raw.Guest == nil {
		return nil
	}
	var guest bool
	if raw.Guest.Kind() == 'n' || json.Unmarshal(raw.Guest, &guest) != nil {
		return errors.New("env entry: guest must be true or false")
	}
	v.Guest = &guest
	return nil
}

func (v EnvVar) same(other EnvVar) bool {
	return v.Value == other.Value && v.InGuest() == other.InGuest()
}

// Env is a claim's own environment: guest entries reach the guest, the others stay host-side for egress secrets.
type Env map[string]EnvVar

// Validate enforces the entry count, the name grammar, and a bounded value a header and an env file both carry.
func (e Env) Validate() error {
	if len(e) > maxEnvVars {
		return fmt.Errorf("env must contain at most %d entries, got %d", maxEnvVars, len(e))
	}
	for name, v := range e {
		if !EnvNameRe.MatchString(name) {
			return fmt.Errorf("env name %q must match %s", name, EnvNameRe)
		}
		if len(v.Value) > maxEnvValueBytes {
			return fmt.Errorf("env value of %s must be at most %d bytes, got %d", name, maxEnvValueBytes, len(v.Value))
		}
		if !httpguts.ValidHeaderFieldValue(v.Value) {
			return fmt.Errorf("env value of %s must not hold control characters other than tab", name)
		}
	}
	return nil
}

// Hidden returns the value of a host-only entry.
func (e Env) Hidden(name string) (string, bool) {
	v, ok := e[name]
	if !ok || v.InGuest() {
		return "", false
	}
	return v.Value, true
}

// HasGuest reports whether any entry reaches the guest.
func (e Env) HasGuest() bool {
	for _, v := range e {
		if v.InGuest() {
			return true
		}
	}
	return false
}

// Equal reports whether e and other hold the same entries.
func (e Env) Equal(other Env) bool {
	return maps.EqualFunc(e, other, EnvVar.same)
}

// SameGuest reports whether e and other deliver the same guest entries.
func (e Env) SameGuest(other Env) bool {
	return maps.EqualFunc(e.guest(), other.guest(), EnvVar.same)
}

// Redacted copies e with every host-only value dropped, so a read names them without serving them.
func (e Env) Redacted() Env {
	if e == nil {
		return nil
	}
	out := make(Env, len(e))
	for name, v := range e {
		if !v.InGuest() {
			v.Value = ""
		}
		out[name] = v
	}
	return out
}

// GuestFile renders the guest entries as a systemd EnvironmentFile, sorted by name.
func (e Env) GuestFile() []byte {
	var b strings.Builder
	for _, name := range slices.Sorted(maps.Keys(e.guest())) {
		fmt.Fprintf(&b, "%s=\"%s\"\n", name, guestFileEscaper.Replace(e[name].Value))
	}
	return []byte(b.String())
}

func (e Env) guest() Env {
	out := Env{}
	for name, v := range e {
		if v.InGuest() {
			out[name] = v
		}
	}
	return out
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
	// ClaimedAt is the first grant; renew and wake move Deadline, never this.
	ClaimedAt time.Time `json:"claimed_at,omitzero"`
	// LeaseSeconds is the lease the claim was granted; a wake from the archive grants it again.
	LeaseSeconds int `json:"lease_seconds,omitzero"`
	// Layer is empty on a record older than the field, which resolves against the live pool set.
	Layer PolicyLayer `json:"policy_layer,omitempty"`
	// PolicySource is the pool whose egress policy a clone of a promoted template or its checkpoint inherits.
	PolicySource PoolKey `json:"policy_source,omitzero"`
	// NoEgress pins a claim that asked for no egress policy.
	NoEgress bool `json:"no_egress,omitzero"`

	// Tenant names the owning tenant; empty means the operator claimed it.
	Tenant string `json:"tenant,omitempty"`

	// ClaimRef is the opaque caller reference; empty for checkpoint branches and unprefixed forks.
	ClaimRef string `json:"claim_ref,omitempty"`
	// Metadata is immutable after the claim: forks, the journal, and summaries share it by reference.
	Metadata Metadata     `json:"metadata,omitempty"`
	OnExpire ExpireAction `json:"on_expire,omitempty"`
	// Volumes records the volumes successfully applied to this claim.
	Volumes []Volume `json:"volumes,omitempty"`
	// Env is replaced whole under the manager mutex; host-only values are never served back.
	Env Env `json:"env,omitempty"`

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
	// open counts the data-plane connections held right now.
	open atomic.Int32

	// Transition serializes hibernate/wake; lock it before (never under) the manager mutex.
	Transition sync.Mutex `json:"-"`
}

// Touch stamps last data-plane activity; lock-free, called on the relay hot path.
func (s *Sandbox) Touch() { s.lastActivity.Store(time.Now().UnixNano()) }

// TouchAt stamps last-activity at a caller-supplied instant (batch claim/adoption, tests).
func (s *Sandbox) TouchAt(t time.Time) { s.lastActivity.Store(t.UnixNano()) }

// LastSeen returns the last data-plane activity time.
func (s *Sandbox) LastSeen() time.Time { return time.Unix(0, s.lastActivity.Load()) }

// Hold registers an open data-plane connection; the idle sweep leaves a held sandbox alone.
func (s *Sandbox) Hold() { s.open.Add(1) }

// Unhold restarts the idle clock, then ends the Hold; a sweep that sees the hold gone also sees the fresh stamp.
func (s *Sandbox) Unhold() {
	s.Touch()
	s.open.Add(-1)
}

// UnholdIdle ends a Hold without restarting the idle clock.
func (s *Sandbox) UnholdIdle() { s.open.Add(-1) }

// Busy reports whether a data-plane connection is held.
func (s *Sandbox) Busy() bool { return s.open.Load() > 0 }

// PolicyKey is the pool key whose egress policy applies: PolicySource when set, else Key.
func (s *Sandbox) PolicyKey() PoolKey { return cmp.Or(s.PolicySource, s.Key) }

// Checkpoint is the record of a captured sandbox state; node-local, like a template.
type Checkpoint struct {
	ID        string    `json:"id"`
	Name      string    `json:"name,omitempty"`
	SandboxID string    `json:"sandbox_id"`
	Key       PoolKey   `json:"key"`
	Tenant    string    `json:"tenant,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	// PolicySource and NoEgress carry the source sandbox's egress to a branch.
	PolicySource PoolKey `json:"policy_source,omitzero"`
	NoEgress     bool    `json:"no_egress,omitzero"`
	// GuestEnv marks a capture whose guest holds an env file a branch must replace.
	GuestEnv bool `json:"guest_env,omitzero"`

	// Archive marks a lifecycle-internal wake image: hidden from listings and undeletable.
	Archive bool `json:"archive,omitzero"`
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

func escapedLen(s string) int {
	return len(s) + 2 + strings.Count(s, `"`) + strings.Count(s, `\`)
}

func validMetadataValue(v string) bool {
	return utf8.ValidString(v) && !strings.ContainsFunc(v, func(r rune) bool { return !unicode.IsPrint(r) })
}

func validMetadataKey(k string) bool {
	if k == "" || len(k) > maxMetadataKeyBytes {
		return false
	}
	for i := range len(k) {
		if c := k[i]; c < ' ' || c > '~' || c == '=' || c == '&' {
			return false
		}
	}
	return true
}

func pathWithin(parent, child string) bool {
	return strings.HasPrefix(child, parent+"/")
}
