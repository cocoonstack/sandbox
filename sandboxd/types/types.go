// Package types defines the shared vocabulary of the sandbox control plane.
package types

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"net/netip"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	NetNone   NetShape = "none"
	NetEgress NetShape = "egress"

	SizeSmall    Size = "small"
	SizeMedium   Size = "medium"
	SizeMedium4G Size = "medium-4g"
	SizeLarge    Size = "large"
	SizeXLarge   Size = "xlarge"
	Size2XLarge  Size = "2xlarge"

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

	VMMRestartCold VMMRestart = "cold"
	VMMRestartNone VMMRestart = "none"

	BalloonInflated    BalloonState = "inflated"
	BalloonUnknown     BalloonState = "unknown"
	BalloonUnsupported BalloonState = "unsupported"

	TemplateRootScope = "*"

	maxMetadataPairs      = 16
	maxMetadataKeyBytes   = 128
	maxMetadataValueBytes = 512
	maxMetadataBytes      = 4 << 10
)

var (
	// NameRe pins caller-chosen names to one conservative charset cocoon also accepts.
	NameRe    = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,62}$`)
	sizeSpecs = map[Size]SizeSpec{
		SizeSmall:    {CPU: 1, Memory: "512M", MemoryBytes: 512 << 20},
		SizeMedium:   {CPU: 2, Memory: "1G", MemoryBytes: 1 << 30},
		SizeMedium4G: {CPU: 2, Memory: "4G", MemoryBytes: 4 << 30},
		SizeLarge:    {CPU: 4, Memory: "4G", MemoryBytes: 4 << 30},
		SizeXLarge:   {CPU: 4, Memory: "8G", MemoryBytes: 8 << 30},
		Size2XLarge:  {CPU: 8, Memory: "16G", MemoryBytes: 16 << 30},
	}
)

// NetShape selects whether the guest has a NIC.
type NetShape string

// PolicyLayer pins, at claim time, whether the key had a pool layer in its egress policy.
type PolicyLayer string

// NetRoute is how a claimed guest reaches the network.
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

// VMMRestart is what the node does with a claim whose VMM exited outside a transition; empty means cold.
type VMMRestart string

// Validate accepts the empty default plus the known policies.
func (r VMMRestart) Validate() error {
	switch r {
	case "", VMMRestartCold, VMMRestartNone:
		return nil
	default:
		return fmt.Errorf("vmm_restart %q must be %s or %s", r, VMMRestartCold, VMMRestartNone)
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

// Compare orders keys by Hash, for a deterministic walk over a key set.
func (k PoolKey) Compare(o PoolKey) int {
	return strings.Compare(k.Hash(), o.Hash())
}

// Hash is a stable 128-bit digest for naming; a shorter tag would be brute-forceable.
func (k PoolKey) Hash() string {
	sum := sha256.Sum256([]byte(k.Template + "|" + string(k.Net) + "|" + string(k.Size)))
	return hex.EncodeToString(sum[:16])
}

// Validate checks that every axis holds a known value.
func (k PoolKey) Validate() error {
	if k.Template == "" {
		return errors.New("template must not be empty")
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

// MoveOut is the source's journal of a live move: the record's first epoch, cocoon's series and the target.
type MoveOut struct {
	Epoch  int64  `json:"epoch"`
	Series string `json:"series,omitempty"`
	Addr   string `json:"addr,omitempty"`
	To     string `json:"to,omitempty"`
}

// EgressLimit caps a claim's egress bytes per second each way; zero leaves a direction unlimited.
type EgressLimit struct {
	TxBytesPerSecond int64 `json:"tx_bytes_per_second,omitzero"`
	RxBytesPerSecond int64 `json:"rx_bytes_per_second,omitzero"`
}

// Validate refuses a negative rate.
func (l EgressLimit) Validate() error {
	if l.TxBytesPerSecond < 0 || l.RxBytesPerSecond < 0 {
		return fmt.Errorf("egress limit rates must not be negative, got tx %d and rx %d", l.TxBytesPerSecond, l.RxBytesPerSecond)
	}
	return nil
}

// BalloonState is what the node knows of a VM's balloon target; empty means the boot default.
type BalloonState string

// envView is an immutable env with its inject entries in name order.
type envView struct {
	env     Env
	injects []namedVar
}

type namedVar struct {
	name string
	v    EnvVar
}

// Sandbox is the node-local record of one pooled or claimed VM.
type Sandbox struct {
	ID     string  `json:"id"`
	VMName string  `json:"vm_name"`
	Key    PoolKey `json:"key"`

	Token string `json:"token,omitempty"`
	// TokenSHA256 authenticates a claim the root adopted without its token; it is never served.
	TokenSHA256 string    `json:"token_sha256,omitempty"`
	Deadline    time.Time `json:"deadline,omitzero"`
	// ClaimedAt is the first grant; renew and wake move Deadline, never this.
	ClaimedAt time.Time `json:"claimed_at,omitzero"`
	// LeaseSeconds is the lease the claim was granted; a wake from the archive grants it again.
	LeaseSeconds int         `json:"lease_seconds,omitzero"`
	Layer        PolicyLayer `json:"policy_layer,omitempty"`
	// PolicySource is the pool whose egress policy a clone of a promoted template or its checkpoint inherits.
	PolicySource PoolKey `json:"policy_source,omitzero"`
	// NoEgress pins a claim that asked for no egress policy.
	NoEgress bool `json:"no_egress,omitzero"`

	// Tenant names the owning tenant; empty means the operator claimed it.
	Tenant string `json:"tenant,omitempty"`
	// EgressClass is the tenant's egress class at claim time; the claim keeps it for life.
	EgressClass string `json:"egress_class,omitempty"`

	// ClaimRef is the opaque caller reference; empty for checkpoint branches and unprefixed forks.
	ClaimRef string `json:"claim_ref,omitempty"`
	// Metadata is immutable after the claim: forks, the journal, and summaries share it by reference.
	Metadata Metadata     `json:"metadata,omitempty"`
	OnExpire ExpireAction `json:"on_expire,omitempty"`
	// Volumes records the volumes successfully applied to this claim.
	Volumes []Volume `json:"volumes,omitempty"`
	// VolumesWant is the durable target, including additions not yet confirmed.
	VolumesWant []Volume `json:"volumes_want"`
	// Env is replaced whole through SetEnv under the manager mutex; host-only values are never served back.
	Env Env `json:"env,omitempty"`

	VsockSocket string `json:"vsock_socket,omitempty"`
	// TAP is the egress-lane NIC's host tap; empty on the none lane.
	TAP string `json:"tap,omitempty"`
	// TunAddr is the claim's overlay address in the advanced network mode; zero when its pool has no tun.
	TunAddr netip.Addr `json:"tun_addr,omitzero"`
	// TunOff disables the claim's advanced network mode; only the root sets it.
	TunOff bool `json:"tun_off,omitzero"`
	// TunProfile labels the claim's exit; empty means "default".
	TunProfile string `json:"tun_profile,omitempty"`
	// TunBytes counts the claim's tun bytes both ways against its quota.
	TunBytes int64 `json:"tun_bytes,omitzero"`
	// EgressLimit is the claim's proxy and tun bandwidth cap; only the root sets it.
	EgressLimit EgressLimit `json:"egress_limit,omitzero"`
	// HibernateSnap names the memory snapshot while the VM is hibernated; empty means running.
	HibernateSnap string `json:"hibernate_snap,omitempty"`
	// PendingSnap is the journaled intent of a hibernate in flight, cleared by its commit.
	PendingSnap string `json:"pending_snap,omitempty"`
	SaveSnap    string `json:"save_snap,omitempty"`
	ReaderSafe  bool   `json:"reader_safe,omitzero"`
	// ArchiveCk names the store checkpoint holding an archived sandbox; empty means local.
	ArchiveCk string `json:"archive_ck,omitempty"`
	// ArchiveEpoch is the archive registry epoch this node last held for the claim; zero means no row.
	ArchiveEpoch   int64  `json:"archive_epoch,omitzero"`
	ArchivePending string `json:"archive_pending,omitempty"`
	// ReplicaCk names the checkpoint the claim's registry row points at while the claim runs; empty means unreplicated.
	ReplicaCk string `json:"replica_ck,omitempty"`
	// ReplicaPending is the checkpoint a replication round is about to register; the row may already name it.
	ReplicaPending string `json:"replica_pending,omitempty"`

	FromCheckpoint string `json:"from_checkpoint,omitempty"`
	// TemplateDigest identifies the promoted-template export this sandbox was cloned from.
	TemplateDigest string `json:"-"`

	// Restarts counts the cold boots that replaced an exited VMM; each loses guest memory.
	Restarts    int       `json:"restarts,omitzero"`
	RestartedAt time.Time `json:"restarted_at,omitzero"`
	// Failed names why the VMM is down and not restarted; empty means the claim is serviceable.
	Failed string `json:"failed,omitempty"`
	// MoveOut journals a live move this node runs as the source; nil when none.
	MoveOut *MoveOut `json:"move_out,omitempty"`
	// MoveIn marks a claim a live move delivered here and this node has not started yet.
	MoveIn bool `json:"move_in,omitzero"`
	// RestartLog holds the recent restart attempts that the loop budget counts; guarded by Transition.
	RestartLog []time.Time `json:"-"`
	// ExitRecorded marks a VMM exit already journaled as vmm_exit; guarded by Transition.
	ExitRecorded bool `json:"-"`

	// StaleSnap names a consumed wake snapshot a lagging journal references; guarded by Transition.
	StaleSnap string `json:"-"`

	// Waking marks a wake in flight, which counts as running; guarded by the manager mutex.
	Waking bool `json:"-"`
	// Replicated marks a claim of a replicating pool; it is set before the claim is published, written only on a change, and read without a lock.
	Replicated bool `json:"-"`

	ArchiveRetryAt time.Time `json:"-"`

	// Balloon, BalloonTarget (the idle target applied), BalloonDefault (the boot target in bytes), BalloonRetryAt and BalloonBusy (a call in flight) are guarded by the manager mutex.
	Balloon        BalloonState `json:"-"`
	BalloonTarget  string       `json:"-"`
	BalloonDefault int64        `json:"-"`
	BalloonRetryAt time.Time    `json:"-"`
	BalloonBusy    bool         `json:"-"`

	// lastActivity is unix-nanos of the last data-plane connection, stamped lock-free.
	lastActivity atomic.Int64
	// open counts the data-plane connections held right now.
	open atomic.Int32
	// live is the env as of the last SetEnv, read by the egress proxy without the manager mutex.
	live atomic.Pointer[envView]

	// Transition serializes captures and live volume changes; lock it before (never under) the manager mutex.
	Transition sync.Mutex `json:"-"`
}

// Touch stamps last data-plane activity; lock-free, called on the relay hot path.
func (s *Sandbox) Touch() { s.lastActivity.Store(time.Now().UnixNano()) }

// TouchAt stamps the last data-plane activity at t.
func (s *Sandbox) TouchAt(t time.Time) { s.lastActivity.Store(t.UnixNano()) }

// LastSeen returns the last data-plane activity time.
func (s *Sandbox) LastSeen() time.Time { return time.Unix(0, s.lastActivity.Load()) }

// HasReplica reports whether the claim's registry row may name a replica.
func (s *Sandbox) HasReplica() bool { return s.ReplicaCk != "" || s.ReplicaPending != "" }

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

// SetEnv replaces the env and publishes it to the egress proxy.
func (s *Sandbox) SetEnv(e Env) {
	s.Env = e
	view := &envView{env: e}
	for name, v := range e {
		if v.Inject != nil && !v.InGuest() && !v.Missing {
			view.injects = append(view.injects, namedVar{name, v})
		}
	}
	slices.SortFunc(view.injects, func(a, b namedVar) int { return strings.Compare(a.name, b.name) })
	s.live.Store(view)
}

// HiddenEnv returns a host-only entry's value as last published.
func (s *Sandbox) HiddenEnv(name string) (string, bool) {
	if view := s.live.Load(); view != nil {
		if v, ok := view.env[name]; ok && !v.InGuest() {
			return v.Value, true
		}
	}
	return "", false
}

// Injections yields, in name order, the published host-only entries that inject into host.
func (s *Sandbox) Injections(host string) iter.Seq2[string, EnvVar] {
	return func(yield func(string, EnvVar) bool) {
		view := s.live.Load()
		if view == nil {
			return
		}
		for _, e := range view.injects {
			if e.v.Inject.Matches(host) && !yield(e.name, e.v) {
				return
			}
		}
	}
}

// PolicyKey is the pool key whose egress policy applies: PolicySource when set, else Key.
func (s *Sandbox) PolicyKey() PoolKey { return cmp.Or(s.PolicySource, s.Key) }

// Checkpoint is the record of a captured sandbox state; node-local, like a template.
type Checkpoint struct {
	ID        string    `json:"id"`
	Name      string    `json:"name,omitempty"`
	SandboxID string    `json:"sandbox_id"`
	Node      string    `json:"node,omitempty"`
	Key       PoolKey   `json:"key"`
	Tenant    string    `json:"tenant,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	// PolicySource and NoEgress carry the source sandbox's egress to a branch.
	PolicySource PoolKey `json:"policy_source,omitzero"`
	NoEgress     bool    `json:"no_egress,omitzero"`
	// TunOff and TunProfile carry the operator's tun switch and exit label to a branch, so a branch cannot turn tun back on.
	TunOff     bool   `json:"tun_off,omitzero"`
	TunProfile string `json:"tun_profile,omitempty"`
	// EgressLimit carries the operator's bandwidth cap to a branch, so a branch cannot shed it.
	EgressLimit EgressLimit `json:"egress_limit,omitzero"`
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

// VMDisk is one hot-attached disk that `cocoon vm inspect` reports.
type VMDisk struct {
	Name     string `json:"name"`
	ReadOnly bool   `json:"readonly,omitzero"`
}

// VMDevices is the attached-device subset that `cocoon vm inspect` fills only for a running VM.
type VMDevices struct {
	Disks []VMDisk `json:"disks,omitempty"`
}

// VMBalloon is a VM's balloon target, the bytes it holds now when the hypervisor reports them, and its boot target.
type VMBalloon struct {
	Target  int64 `json:"target"`
	Actual  int64 `json:"actual,omitzero"`
	Default int64 `json:"default"`
}

// VMRecord is the subset of cocoon's VM records the control plane reads.
type VMRecord struct {
	State           string        `json:"state"`
	PID             int           `json:"pid"`
	VsockSocket     string        `json:"vsock_socket"`
	NetworkConfigs  []VMNetConfig `json:"network_configs,omitempty"`
	Config          VMConfig      `json:"config"`
	AttachedDevices VMDevices     `json:"attached_devices,omitzero"`
	Balloon         *VMBalloon    `json:"balloon,omitempty"`
}

// TapDevice returns the first NIC's host tap; empty when the record carries none.
func (r VMRecord) TapDevice() string {
	if len(r.NetworkConfigs) == 0 {
		return ""
	}
	return r.NetworkConfigs[0].TAP
}

// MigrateFile is one file a migrate round wrote, named relative to the round's Dir.
type MigrateFile struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// MigrateRound is what `cocoon vm migrate capture` prints for one round.
type MigrateRound struct {
	Series string        `json:"series"`
	Dir    string        `json:"dir"`
	Files  []MigrateFile `json:"files"`
}

// TemplateGossipHash scopes a key hash to its owner; TemplateRootScope covers every tenant.
func TemplateGossipHash(keyHash, tenant string) string {
	sum := sha256.Sum256([]byte(keyHash + "|" + tenant))
	return hex.EncodeToString(sum[:16])
}

// ValidateClaimRef bounds a caller reference named field like a metadata value.
func ValidateClaimRef(field, ref string) error {
	if len(ref) > maxMetadataValueBytes || !validMetadataValue(ref) {
		return fmt.Errorf("%s must be at most %d bytes of printable UTF-8", field, maxMetadataValueBytes)
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
	return k != "" && len(k) <= maxMetadataKeyBytes && !strings.ContainsFunc(k, func(r rune) bool { return r < ' ' || r > '~' || r == '=' || r == '&' })
}
