// Package config loads the sandboxd node configuration.
package config

import (
	"cmp"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/docker/go-units"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/tunnet"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const (
	DefaultMaxForkCount = 16

	defaultListen    = ":7777"
	defaultDataDir   = "/var/lib/sandboxd"
	defaultCocoonBin = "cocoon"
	defaultWarm      = 4
	refillFloor      = 4
	refillCeiling    = 256
	minStorageBytes  = 10 << 30

	defaultTunVNI     = 1
	defaultTunPort    = 4789
	defaultTunMTU     = 1450
	defaultTunSenders = 4
	defaultTunReuse   = 5 * 24 * 3600
	defaultTunFlows   = 16384
	defaultTunNewRate = 1000
)

// PoolSpec declares one warm pool and its target of claim-ready VMs.
type PoolSpec struct {
	types.PoolKey
	Warm    int `json:"warm"`
	WarmMax int `json:"warm_max,omitzero"`

	Egress *egress.Policy `json:"egress,omitempty"`
	Warmup []string       `json:"warmup,omitempty"`
	// EgressUpstreamEnv names the node env holding this pool's default upstream proxy URL.
	EgressUpstreamEnv string `json:"egress_upstream_env,omitempty"`
	// CaptureTrim trims the guest's copy-on-write disk before a promote or checkpoint of this pool's sandboxes.
	CaptureTrim bool `json:"capture_trim,omitzero"`
	// Storage sizes the golden's copy-on-write disk in cocoon's --storage spelling; every clone inherits it, and empty keeps cocoon's default.
	Storage string `json:"storage,omitempty"`
	// ReplicateSeconds snapshots each running claim of the pool to the checkpoint store at this interval; zero turns replication off.
	ReplicateSeconds int `json:"replicate_seconds,omitzero"`
	// Reader serves read-only file calls on this pool's hibernated or archived claims from a warm VM, without waking them.
	Reader bool `json:"reader,omitzero"`
	// Tun gives this pool's claims the advanced network mode; the node's tun block must be set.
	Tun bool `json:"tun,omitzero"`

	IdleHibernateSeconds      int `json:"idle_hibernate_seconds,omitzero"`
	ArchiveAfterSeconds       int `json:"archive_after_seconds,omitzero"`
	ArchiveDeleteAfterSeconds int `json:"archive_delete_after_seconds,omitzero"`

	warmSet bool
}

// UnmarshalJSONFrom records whether the object named warm, so an explicit 0 survives the config default.
func (s *PoolSpec) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	raw, err := dec.ReadValue()
	if err != nil {
		return err
	}
	type plain PoolSpec
	if err := json.Unmarshal(raw, (*plain)(s), dec.Options()); err != nil {
		return err
	}
	var probe struct {
		Warm *int `json:"warm"`
	}
	_ = json.Unmarshal(raw, &probe)
	s.warmSet = probe.Warm != nil
	return nil
}

// ValidateLimits checks the warm/watermark/idle bounds shared by config and PUT /v1/pools.
func (s PoolSpec) ValidateLimits() error {
	if s.Warm < 0 {
		return fmt.Errorf("warm must not be negative")
	}
	if s.WarmMax != 0 && s.WarmMax < s.Warm {
		return fmt.Errorf("warm_max %d below warm %d", s.WarmMax, s.Warm)
	}
	if s.IdleHibernateSeconds < 0 {
		return fmt.Errorf("idle_hibernate_seconds must not be negative")
	}
	if s.Net == types.NetEgress && s.IdleHibernateSeconds > 0 {
		return fmt.Errorf("idle_hibernate_seconds is not supported for egress pools")
	}
	if s.Reader && s.Warm < 1 {
		return fmt.Errorf("reader needs warm of at least 1")
	}
	if slices.Contains(s.Warmup, "") {
		return fmt.Errorf("warmup must not contain an empty argument")
	}
	if s.Storage != "" {
		n, err := StorageBytes(s.Storage)
		if err != nil {
			return fmt.Errorf("storage %q: %w", s.Storage, err)
		}
		if n < minStorageBytes {
			return fmt.Errorf("storage %q is below cocoon's default of 10G", s.Storage)
		}
	}
	return validateArchiveWindow(s.IdleHibernateSeconds, s.ArchiveAfterSeconds, s.ArchiveDeleteAfterSeconds)
}

// StoreConfig selects a checkpoint backend.
type StoreConfig struct {
	Kind string          `json:"kind"`
	S3   *store.S3Config `json:"s3,omitempty"`
}

func (s *StoreConfig) validate() error {
	switch s.Kind {
	case "", "dir":
	case "s3":
		if s.S3 == nil || s.S3.Bucket == "" {
			return fmt.Errorf("checkpoint_store s3 needs a bucket")
		}
		if min(s.S3.UploadConcurrency, s.S3.FetchConcurrency, s.S3.PublishConcurrency, s.S3.MetaReadConcurrency) < 0 {
			return fmt.Errorf("checkpoint_store s3 concurrency must not be negative")
		}
	default:
		return fmt.Errorf("checkpoint_store kind %q: want dir or s3", s.Kind)
	}
	return nil
}

// EgressUpstreamConfig lets a claim's host-only ClaimEnv entry name the proxy its egress leaves through, from the Allow list.
type EgressUpstreamConfig struct {
	ClaimEnv string   `json:"claim_env"`
	Allow    []string `json:"allow"`
}

// EgressConnRate caps the new upstream connections each claim's proxy opens, as a token bucket.
type EgressConnRate struct {
	PerSecond float64 `json:"per_second"`
	Burst     int     `json:"burst"`
}

// TunExit is one exit: its VTEP address and the MAC of its VXLAN device.
type TunExit struct {
	VTEP string `json:"vtep"`
	MAC  string `json:"mac"`
}

// TunConfig turns on the advanced network mode: the overlay subnet, the VXLAN socket and the exits claims leave through.
type TunConfig struct {
	// Subnet holds the claims' overlay addresses; under a meta store every node of the cell sets the same subnet.
	Subnet string `json:"subnet"`
	// Listen is the ip:port the VXLAN socket binds; exits reach the node at this address.
	Listen string `json:"listen"`
	VNI    uint32 `json:"vni,omitzero"`
	// Port is the exits' VXLAN port.
	Port uint16 `json:"port,omitzero"`
	// MTU is the guest tun0 MTU.
	MTU     int `json:"mtu,omitzero"`
	Senders int `json:"senders,omitzero"`
	// Exits maps a claim's profile label to its exit; "default" serves a claim without one.
	Exits  map[string]TunExit `json:"exits"`
	Limits tunnet.Limits      `json:"limits,omitzero"`
	// ReuseAfterSeconds holds a freed address back until exit conntrack entries for it can have expired.
	ReuseAfterSeconds int `json:"reuse_after_seconds,omitzero"`
}

// Prefix is the parsed overlay subnet; validation already rejected a bad one.
func (t *TunConfig) Prefix() netip.Prefix {
	p, _ := netip.ParsePrefix(t.Subnet)
	return p.Masked()
}

func (t *TunConfig) ListenAddr() netip.AddrPort {
	ap, _ := netip.ParseAddrPort(t.Listen)
	return ap
}

func (t *TunConfig) ReuseAfter() time.Duration {
	return time.Duration(t.ReuseAfterSeconds) * time.Second
}

// Overlay is the router's reloadable view of the block.
func (t *TunConfig) Overlay() *tunnet.Overlay {
	o, _ := t.overlay()
	return o
}

func (t *TunConfig) applyDefaults() {
	t.VNI = cmp.Or(t.VNI, defaultTunVNI)
	t.Port = cmp.Or(t.Port, defaultTunPort)
	t.MTU = cmp.Or(t.MTU, defaultTunMTU)
	t.Senders = cmp.Or(t.Senders, defaultTunSenders)
	t.ReuseAfterSeconds = cmp.Or(t.ReuseAfterSeconds, defaultTunReuse)
	t.Limits.MaxFlows = cmp.Or(t.Limits.MaxFlows, defaultTunFlows)
	t.Limits.NewFlowsPerSecond = cmp.Or(t.Limits.NewFlowsPerSecond, defaultTunNewRate)
}

func (t *TunConfig) validate() error {
	listen, err := netip.ParseAddrPort(t.Listen)
	l := t.Limits
	switch {
	case err != nil || !listen.Addr().Is4() || listen.Addr().IsUnspecified() || listen.Port() == 0:
		return fmt.Errorf("tun.listen %q must be a specific IPv4 address and port", t.Listen)
	case t.VNI >= 1<<24:
		return fmt.Errorf("tun.vni %d does not fit 24 bits", t.VNI)
	case t.MTU < 576 || t.MTU > 9000:
		return fmt.Errorf("tun.mtu %d must be between 576 and 9000", t.MTU)
	case t.Senders < 1:
		return fmt.Errorf("tun.senders must be at least 1, got %d", t.Senders)
	case t.ReuseAfterSeconds < 0:
		return fmt.Errorf("tun.reuse_after_seconds must not be negative")
	case l.BytesPerSecond < 0 || l.PacketsPerSecond < 0 || l.NewFlowsPerSecond < 0 || l.MaxFlows < 0 || l.QuotaBytes < 0:
		return fmt.Errorf("tun.limits must not be negative")
	}
	_, err = t.overlay()
	return err
}

func (t *TunConfig) overlay() (*tunnet.Overlay, error) {
	subnet, err := netip.ParsePrefix(t.Subnet)
	switch {
	case err != nil || !subnet.Addr().Is4():
		return nil, fmt.Errorf("tun.subnet %q must be an IPv4 prefix", t.Subnet)
	case subnet.Bits() < 4 || subnet.Bits() > 24:
		return nil, fmt.Errorf("tun.subnet %q must be between /4 and /24", t.Subnet)
	case subnet.Overlaps(tunnet.GuestRange):
		return nil, fmt.Errorf("tun.subnet %q overlaps the guest range %s", t.Subnet, tunnet.GuestRange)
	}
	if _, ok := t.Exits["default"]; !ok {
		return nil, fmt.Errorf(`tun.exits needs a "default" exit`)
	}
	exits := make(map[string]tunnet.Exit, len(t.Exits))
	for label, e := range t.Exits {
		vtep, vtepErr := netip.ParseAddr(e.VTEP)
		mac, macErr := net.ParseMAC(e.MAC)
		switch {
		case !types.NameRe.MatchString(label):
			return nil, fmt.Errorf("tun.exits label %q must match %s", label, types.NameRe)
		case vtepErr != nil || !vtep.Is4() || vtep.IsUnspecified():
			return nil, fmt.Errorf("tun.exits %q vtep %q must be an IPv4 address", label, e.VTEP)
		case macErr != nil || len(mac) != 6 || mac[0]&1 != 0 || [6]byte(mac) == [6]byte{}:
			return nil, fmt.Errorf("tun.exits %q mac %q must be a unicast 6-byte MAC", label, e.MAC)
		}
		exits[label] = tunnet.Exit{VTEP: vtep, MAC: [6]byte(mac)}
	}
	return &tunnet.Overlay{
		Subnet: subnet.Masked(),
		VNI:    t.VNI,
		Port:   t.Port,
		MTU:    uint16(t.MTU), //nolint:gosec // validate bounds it to 576..9000
		Exits:  exits,
	}, nil
}

// EgressCAConfig provisions HTTPS interception; the root private key never appears here.
type EgressCAConfig struct {
	RootCert         string `json:"root_cert"`
	IntermediateCert string `json:"intermediate_cert"`
	IntermediateKey  string `json:"intermediate_key"`
}

// Set reports whether every path is provided.
func (e *EgressCAConfig) Set() bool {
	return e != nil && e.RootCert != "" && e.IntermediateCert != "" && e.IntermediateKey != ""
}

// EgressClass is a named tenant egress layer; a tenant names its class, so the API never defines a policy.
type EgressClass struct {
	Name   string         `json:"name"`
	Egress *egress.Policy `json:"egress"`
	// EgressUpstreamEnv names the node env holding the class's default upstream proxy URL.
	EgressUpstreamEnv string `json:"egress_upstream_env,omitempty"`
}

// MetaStoreConfig points every node at one shared PostgreSQL for its tenant set and, with a cell, its pool set; absent keeps both in each node's files.
type MetaStoreConfig struct {
	Kind string `json:"kind"`
	// DSNEnv names the node env holding the connection string, so no credential sits in the file.
	DSNEnv string `json:"dsn_env"`
	// Cell opts the node into one pool set for every node of the cell; empty keeps the pool set per node.
	Cell string `json:"cell,omitempty"`
	// Usage lands the node's usage journal in the shared usage table and serves GET /v1/usage from it.
	Usage bool `json:"usage,omitzero"`
}

// VolumeSpec declares one operator-managed dataset disk, held by exactly one node.
type VolumeSpec struct {
	Name     string   `json:"name"`
	Path     string   `json:"path"`
	DirectIO string   `json:"directio,omitempty"`
	Writable bool     `json:"writable,omitzero"`
	Tenants  []string `json:"tenants,omitempty"`
}

// DiskReserve refuses claims, wakes and refills while a listed directory's filesystem has less than Free left.
type DiskReserve struct {
	Free string   `json:"free"`
	Dirs []string `json:"dirs,omitempty"`
}

// MemoryLimit refuses claims and refills while the node's anon memory or memory stall share is above a limit.
type MemoryLimit struct {
	Anon         string  `json:"anon,omitempty"`
	PSISomeAvg10 float64 `json:"psi_some_avg10,omitzero"`
}

// MeshConfig configures cluster membership; every node shares one APIToken and tenant set.
type MeshConfig struct {
	NodeID     string   `json:"node_id"`
	Bind       string   `json:"bind"`           // memberlist host:port
	Join       []string `json:"join,omitempty"` // seed members; empty = mesh of one
	ClusterKey string   `json:"cluster_key,omitempty"`
}

// ID is the node's mesh name: NodeID, else Bind.
func (mc *MeshConfig) ID() string {
	return cmp.Or(mc.NodeID, mc.Bind)
}

// ParsedBind splits Bind into host and port, rejecting a wildcard host.
func (mc *MeshConfig) ParsedBind() (string, int, error) {
	host, portStr, err := net.SplitHostPort(mc.Bind)
	if err != nil {
		return "", 0, fmt.Errorf("mesh bind %q: %w", mc.Bind, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("mesh bind port %q: %w", portStr, err)
	}
	if !RoutableHost(host) {
		return "", 0, fmt.Errorf("mesh bind needs an explicit host (got %q); a wildcard advertises an unroutable address", mc.Bind)
	}
	return host, port, nil
}

// DecodedKey returns the gossip-encryption key bytes; empty means unencrypted.
func (mc *MeshConfig) DecodedKey() ([]byte, error) {
	if mc.ClusterKey == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(mc.ClusterKey)
	if err != nil {
		return nil, fmt.Errorf("mesh cluster_key: not valid base64: %w", err)
	}
	switch len(key) {
	case 16, 24, 32:
		return key, nil
	default:
		return nil, fmt.Errorf("mesh cluster_key decodes to %d bytes, want 16, 24, or 32 (AES-128/192/256)", len(key))
	}
}

// Config is the sandboxd node configuration.
type Config struct {
	Listen    string `json:"listen"`
	DataDir   string `json:"data_dir"`
	CocoonBin string `json:"cocoon_bin"`

	AdvertiseAddr   string   `json:"advertise_addr,omitempty"`
	ClientAdvertise string   `json:"client_advertise,omitempty"`
	Bridges         []string `json:"bridges,omitempty"`
	Networks        []string `json:"networks,omitempty"`

	RestoreMode types.RestoreMode `json:"restore_mode,omitempty"`
	NoDirectIO  bool              `json:"no_direct_io,omitzero"`
	NoBalloon   bool              `json:"no_balloon,omitzero"`
	NoRxSplice  bool              `json:"no_rx_splice,omitzero"`
	SyncClaims  bool              `json:"sync_claims,omitzero"`
	VMMRestart  types.VMMRestart  `json:"vmm_restart,omitempty"`
	// CocoondSocket is cocoon daemon's API socket; its event stream detects VMM exits without polling.
	CocoondSocket string `json:"cocoond_socket,omitempty"`

	APIToken  string              `json:"api_token,omitempty"`
	MetaStore *MetaStoreConfig    `json:"meta_store,omitempty"`
	Secrets   []egress.SecretSpec `json:"secrets,omitempty"`

	IdleHibernateSeconds      int `json:"idle_hibernate_seconds,omitzero"`
	ArchiveAfterSeconds       int `json:"archive_after_seconds,omitzero"`
	ArchiveDeleteAfterSeconds int `json:"archive_delete_after_seconds,omitzero"`
	// ArchiveFailover lets another node adopt this node's archived and replicated claims once its node lease expires.
	ArchiveFailover bool `json:"archive_failover,omitzero"`

	PreviewListen    string `json:"preview_listen,omitempty"`
	PreviewSecret    string `json:"preview_secret,omitempty"`
	PreviewAdvertise string `json:"preview_advertise,omitempty"`

	CheckpointDir      string       `json:"checkpoint_dir,omitempty"`
	CheckpointStore    *StoreConfig `json:"checkpoint_store,omitempty"`
	CheckpointPeerHeal bool         `json:"checkpoint_peer_heal,omitzero"`
	CheckpointTTLHours int          `json:"checkpoint_ttl_hours,omitzero"`

	EgressClasses       []EgressClass         `json:"egress_classes,omitempty"`
	EgressInternalAllow []string              `json:"egress_internal_allow,omitempty"`
	EgressCA            *EgressCAConfig       `json:"egress_ca,omitempty"`
	EgressUpstream      *EgressUpstreamConfig `json:"egress_upstream,omitempty"`
	EgressUsageBytes    bool                  `json:"egress_usage_bytes,omitzero"`
	EgressConnRate      *EgressConnRate       `json:"egress_conn_rate,omitempty"`

	MaxClaims  int `json:"max_claims,omitzero"`
	MaxRunning int `json:"max_running,omitzero"`
	// MoveConcurrency caps the live moves this node runs as source or target at once; zero takes the default.
	MoveConcurrency     int  `json:"move_concurrency,omitzero"`
	MaxForkCount        int  `json:"max_fork_count,omitzero"`
	RefillConcurrency   int  `json:"refill_concurrency,omitzero"`
	ReleaseDelaySeconds int  `json:"release_delay_seconds,omitzero"`
	AuditLog            bool `json:"audit_log,omitzero"`

	DiskReserve *DiskReserve `json:"disk_reserve,omitempty"`
	MemoryLimit *MemoryLimit `json:"memory_limit,omitempty"`

	Tun *TunConfig `json:"tun,omitempty"`

	Volumes []VolumeSpec `json:"volumes,omitempty"`
	// VolumeRoots bounds runtime registration paths only; static volumes stay unchecked.
	VolumeRoots []string    `json:"volume_roots,omitempty"`
	Mesh        *MeshConfig `json:"mesh,omitempty"`
	Pools       []PoolSpec  `json:"pools"`
}

// HasEgress reports whether the node can attach egress-lane VMs.
func (c *Config) HasEgress() bool {
	return len(c.Bridges) > 0 || len(c.Networks) > 0
}

// Movable reports whether the node can run live moves: they need a meta store cell for the move record and an encrypted mesh.
func (c *Config) Movable() bool {
	return c.MetaStore != nil && c.MetaStore.Cell != "" && c.Mesh != nil && c.Mesh.ClusterKey != ""
}

func (c *Config) S3Checkpoints() bool {
	return c.CheckpointStore != nil && c.CheckpointStore.Kind == "s3"
}

func (c *Config) guardsEgressLane() bool {
	return len(c.EgressClasses) > 0 || slices.ContainsFunc(c.Pools, func(p PoolSpec) bool { return p.Net == types.NetEgress && p.Egress != nil })
}

func (c *Config) applyDefaults() {
	c.Listen = cmp.Or(c.Listen, defaultListen)
	c.DataDir = cmp.Or(c.DataDir, defaultDataDir)
	c.CocoonBin = cmp.Or(c.CocoonBin, defaultCocoonBin)
	c.AdvertiseAddr = cmp.Or(c.AdvertiseAddr, c.Listen)
	c.PreviewAdvertise = cmp.Or(c.PreviewAdvertise, c.PreviewListen)
	c.MaxForkCount = cmp.Or(c.MaxForkCount, DefaultMaxForkCount)
	if c.RefillConcurrency == 0 {
		c.RefillConcurrency = autoRefillConcurrency(runtime.NumCPU())
	}
	for i := range c.Pools {
		if !c.Pools[i].warmSet && c.Pools[i].Warm == 0 && c.Pools[i].WarmMax == 0 {
			c.Pools[i].Warm = defaultWarm
		}
		c.Pools[i].PoolKey = c.Pools[i].Defaulted()
	}
	for i := range c.Volumes {
		c.Volumes[i].DirectIO = cmp.Or(c.Volumes[i].DirectIO, types.DirectIOOff)
	}
	if c.Tun != nil {
		c.Tun.applyDefaults()
	}
}

func (c *Config) validate() error {
	if err := c.validateClientAdvertise(); err != nil {
		return err
	}
	for _, field := range []struct {
		name  string
		value int
	}{
		{"idle_hibernate_seconds", c.IdleHibernateSeconds},
		{"checkpoint_ttl_hours", c.CheckpointTTLHours},
		{"max_claims", c.MaxClaims},
		{"max_running", c.MaxRunning},
		{"move_concurrency", c.MoveConcurrency},
		{"refill_concurrency", c.RefillConcurrency},
		{"release_delay_seconds", c.ReleaseDelaySeconds},
	} {
		if field.value < 0 {
			return fmt.Errorf("%s must not be negative, got %d", field.name, field.value)
		}
	}
	if err := c.validateAttachment(); err != nil {
		return err
	}
	if len(c.Networks) > 0 && c.guardsEgressLane() {
		return fmt.Errorf("guarded egress needs a bridge lane, not a CNI network: the tap lives in the VM netns and cannot be locked")
	}
	if c.MaxForkCount < 1 {
		return fmt.Errorf("max_fork_count must be at least 1, got %d", c.MaxForkCount)
	}
	if err := c.RestoreMode.Validate(); err != nil {
		return fmt.Errorf("restore_mode: %w", err)
	}
	if err := c.VMMRestart.Validate(); err != nil {
		return err
	}
	if err := c.validatePreview(); err != nil {
		return err
	}
	if err := c.validatePoolKeys(); err != nil {
		return err
	}
	if err := validateArchiveWindow(c.IdleHibernateSeconds, c.ArchiveAfterSeconds, c.ArchiveDeleteAfterSeconds); err != nil {
		return err
	}
	if cs := c.CheckpointStore; cs != nil {
		if err := cs.validate(); err != nil {
			return err
		}
	}
	if c.CheckpointPeerHeal && (c.Mesh == nil || c.Mesh.ClusterKey == "") {
		return fmt.Errorf("checkpoint_peer_heal requires an encrypted mesh (set mesh.cluster_key)")
	}
	if c.CheckpointPeerHeal && c.CheckpointTTLHours == 0 {
		return fmt.Errorf("checkpoint_peer_heal requires checkpoint_ttl_hours > 0: the ttl is what ages out a healed replica a delete broadcast missed")
	}
	if c.CheckpointPeerHeal && c.APIToken == "" {
		return fmt.Errorf("checkpoint_peer_heal requires api_token: without it resolveScope leaves the raw checkpoint blob GET reachable with no credential")
	}
	if c.MoveConcurrency > 0 && !c.Movable() {
		return fmt.Errorf("move_concurrency requires meta_store with a cell and an encrypted mesh (set mesh.cluster_key)")
	}
	for _, check := range []func() error{c.validateNodeEgress, c.validateDiskReserve, c.validateMemoryLimit, c.validateMetaStore, c.validateVolumes, c.validateMesh, c.validateReplication, c.validateTun} {
		if err := check(); err != nil {
			return err
		}
	}
	secrets, err := c.validateSecrets()
	if err != nil {
		return err
	}
	return c.validateEgress(secrets)
}

func (c *Config) validateReplication() error {
	for _, p := range c.Pools {
		switch {
		case p.ReplicateSeconds < 0:
			return fmt.Errorf("pool %q: replicate_seconds must not be negative", p.Template)
		case p.ReplicateSeconds > 0 && !c.ArchiveFailover:
			return fmt.Errorf("pool %q: replicate_seconds requires archive_failover", p.Template)
		case p.ReplicateSeconds > 0 && p.Net != types.NetNone:
			return fmt.Errorf("pool %q: replicate_seconds needs net %q: only the none lane captures", p.Template, types.NetNone)
		}
	}
	return nil
}

func (c *Config) validateTun() error {
	for _, p := range c.Pools {
		switch {
		case !p.Tun:
		case c.Tun == nil:
			return fmt.Errorf("pool %q: tun needs the node tun block", p.Template)
		case p.Net != types.NetNone:
			return fmt.Errorf("pool %q: tun needs net %q", p.Template, types.NetNone)
		}
	}
	if c.Tun == nil {
		return nil
	}
	if c.Mesh != nil && (c.MetaStore == nil || c.MetaStore.Cell == "") {
		return fmt.Errorf("tun with a mesh requires meta_store with a cell: overlay addresses must be unique across the cell")
	}
	return c.Tun.validate()
}

func (c *Config) validateVolumes() error {
	for i, root := range c.VolumeRoots {
		if !filepath.IsAbs(root) {
			return fmt.Errorf("volume_roots %q must be an absolute path", root)
		}
		c.VolumeRoots[i] = filepath.Clean(root)
	}
	return ValidateVolumeCatalog(c.Volumes)
}

func (c *Config) validatePreview() error {
	if c.PreviewListen == "" {
		return nil
	}
	if c.PreviewSecret == "" {
		return fmt.Errorf("preview_listen needs preview_secret")
	}
	if !namesHost(c.PreviewAdvertise) {
		return fmt.Errorf("preview_advertise %q is minted into every preview URL and must name a routable host", c.PreviewAdvertise)
	}
	return nil
}

func (c *Config) validatePoolKeys() error {
	pools := make(map[types.PoolKey]struct{}, len(c.Pools))
	for _, p := range c.Pools {
		if _, ok := pools[p.PoolKey]; ok {
			return fmt.Errorf("duplicate pool %s %s/%s", p.Template, p.Net, p.Size)
		}
		pools[p.PoolKey] = struct{}{}
	}
	return nil
}

func (c *Config) validateClientAdvertise() error {
	if c.ClientAdvertise == "" {
		return nil
	}
	u, err := url.Parse(c.ClientAdvertise)
	if err != nil {
		return fmt.Errorf("client_advertise: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil ||
		(u.Path != "" && u.Path != "/") || strings.ContainsAny(c.ClientAdvertise, "?#") {
		return fmt.Errorf("client_advertise must be an http or https origin")
	}
	if !RoutableHost(u.Hostname()) {
		return fmt.Errorf("client_advertise must name a routable host")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("client_advertise port must be between 1 and 65535")
		}
	}
	c.ClientAdvertise = strings.TrimSuffix(c.ClientAdvertise, "/")
	return nil
}

func (c *Config) validateMesh() error {
	if c.Mesh == nil {
		return nil
	}
	if _, _, err := c.Mesh.ParsedBind(); err != nil {
		return err
	}
	if _, err := c.Mesh.DecodedKey(); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(c.AdvertiseAddr)
	if err != nil {
		return fmt.Errorf("advertise_addr: %w", err)
	}
	if !RoutableHost(host) {
		return fmt.Errorf("advertise_addr %q is gossiped to peers and must name a routable host", c.AdvertiseAddr)
	}
	return nil
}

func (c *Config) validateEgress(secrets map[string]struct{}) error {
	for _, p := range c.Pools {
		if err := p.Validate(); err != nil {
			return fmt.Errorf("pool %q: %w", p.Template, err)
		}
		if p.Net == types.NetEgress && !c.HasEgress() {
			return fmt.Errorf("pool %q: egress lane needs bridge or network", p.Template)
		}
		if err := p.ValidateLimits(); err != nil {
			return fmt.Errorf("pool %q: %w", p.Template, err)
		}
		if err := validatePolicy(p.Egress, secrets); err != nil {
			return fmt.Errorf("pool %q egress: %w", p.Template, err)
		}
		if p.Egress.Intercepts() && !c.EgressCA.Set() {
			return fmt.Errorf("pool %q: intercept needs egress_ca (root_cert + this node's intermediate_cert/intermediate_key)", p.Template)
		}
	}
	names := make(map[string]struct{}, len(c.EgressClasses))
	for _, ec := range c.EgressClasses {
		switch _, dup := names[ec.Name]; {
		case !types.NameRe.MatchString(ec.Name):
			return fmt.Errorf("egress class name %q must match %s", ec.Name, types.NameRe)
		case dup:
			return fmt.Errorf("duplicate egress class %q", ec.Name)
		case ec.Egress == nil:
			return fmt.Errorf("egress class %q needs egress", ec.Name)
		case ec.Egress.Intercepts():
			return fmt.Errorf("egress class %q: intercept may only be set on a pool rule", ec.Name)
		}
		if err := validatePolicy(ec.Egress, secrets); err != nil {
			return fmt.Errorf("egress class %q: %w", ec.Name, err)
		}
		names[ec.Name] = struct{}{}
	}
	return nil
}

func (c *Config) validateSecrets() (map[string]struct{}, error) {
	names := make(map[string]struct{}, len(c.Secrets))
	for _, s := range c.Secrets {
		if err := s.Validate(); err != nil {
			return nil, err
		}
		if _, ok := names[s.Name]; ok {
			return nil, fmt.Errorf("duplicate secret name %q", s.Name)
		}
		names[s.Name] = struct{}{}
	}
	return names, nil
}

func (c *Config) validateAttachment() error {
	if len(c.Bridges) > 0 && len(c.Networks) > 0 {
		return fmt.Errorf("bridges and networks are mutually exclusive")
	}
	if err := validateShards(c.Bridges, "bridges", "bridge device"); err != nil {
		return err
	}
	return validateShards(c.Networks, "networks", "conflist")
}

func (c *Config) validateNodeEgress() error {
	if r := c.EgressConnRate; r != nil && (r.PerSecond <= 0 || r.Burst < 1) {
		return fmt.Errorf("egress_conn_rate needs per_second above 0 and burst of at least 1, got %v and %d", r.PerSecond, r.Burst)
	}
	for _, entry := range c.EgressInternalAllow {
		if _, err := egress.ParseInternalAllow(entry); err != nil {
			return fmt.Errorf("egress_internal_allow %q: %w", entry, err)
		}
	}
	var defaults []string
	for _, p := range c.Pools {
		if p.EgressUpstreamEnv != "" {
			defaults = append(defaults, p.EgressUpstreamEnv)
		}
	}
	for _, ec := range c.EgressClasses {
		if ec.EgressUpstreamEnv != "" {
			defaults = append(defaults, ec.EgressUpstreamEnv)
		}
	}
	u := c.EgressUpstream
	if u == nil {
		if len(defaults) > 0 {
			return fmt.Errorf("egress_upstream_env needs egress_upstream: its allow list governs every upstream")
		}
		return nil
	}
	if !types.EnvNameRe.MatchString(u.ClaimEnv) {
		return fmt.Errorf("egress_upstream.claim_env %q must match %s", u.ClaimEnv, types.EnvNameRe)
	}
	if len(u.Allow) == 0 {
		return fmt.Errorf("egress_upstream.allow must name at least one upstream")
	}
	allow, err := egress.ParseUpstreamAllow(u.Allow)
	if err != nil {
		return fmt.Errorf("egress_upstream.allow: %w", err)
	}
	for _, name := range defaults {
		parsed, err := egress.ParseUpstream(os.Getenv(name))
		if err != nil {
			return fmt.Errorf("egress_upstream_env %s: %w", name, err)
		}
		if !allow.Allows(parsed) {
			return fmt.Errorf("egress_upstream_env %s: upstream %s is not in egress_upstream.allow", name, parsed.Host)
		}
	}
	return nil
}

func (c *Config) validateDiskReserve() error {
	r := c.DiskReserve
	if r == nil {
		return nil
	}
	if n, err := StorageBytes(r.Free); err != nil || n <= 0 {
		return fmt.Errorf("disk_reserve.free %q must be a positive size like 100G", r.Free)
	}
	for _, d := range r.Dirs {
		if !filepath.IsAbs(d) {
			return fmt.Errorf("disk_reserve.dirs %q must be an absolute path", d)
		}
	}
	return nil
}

func (c *Config) validateMemoryLimit() error {
	l := c.MemoryLimit
	if l == nil {
		return nil
	}
	if l.Anon == "" && l.PSISomeAvg10 == 0 {
		return fmt.Errorf("memory_limit needs anon or psi_some_avg10")
	}
	if l.Anon != "" {
		if n, err := StorageBytes(l.Anon); err != nil || n <= 0 {
			return fmt.Errorf("memory_limit.anon %q must be a positive size like 300G", l.Anon)
		}
	}
	if l.PSISomeAvg10 < 0 || l.PSISomeAvg10 > 100 {
		return fmt.Errorf("memory_limit.psi_some_avg10 %v must be a percentage", l.PSISomeAvg10)
	}
	return nil
}

func (c *Config) validateMetaStore() error {
	ms := c.MetaStore
	switch {
	case c.ArchiveFailover && (ms == nil || ms.Cell == "" || !c.S3Checkpoints() || c.Mesh == nil):
		return fmt.Errorf("archive_failover requires meta_store with a cell, an s3 checkpoint_store and a mesh")
	case ms == nil:
		return nil
	case ms.Kind != "pg":
		return fmt.Errorf("meta_store.kind %q is not supported; use pg", ms.Kind)
	case !types.EnvNameRe.MatchString(ms.DSNEnv) || os.Getenv(ms.DSNEnv) == "":
		return fmt.Errorf("meta_store.dsn_env %q must name a set node env", ms.DSNEnv)
	case ms.Cell != "" && !types.NameRe.MatchString(ms.Cell):
		return fmt.Errorf("meta_store.cell %q must match %s", ms.Cell, types.NameRe)
	case c.APIToken == "":
		return fmt.Errorf("meta_store needs api_token: the tenant API is root-only")
	}
	return nil
}

// ValidateVolumeCatalog checks the shared static and runtime catalog rules without I/O.
func ValidateVolumeCatalog(volumes []VolumeSpec) error {
	names := make(map[string]struct{}, len(volumes))
	paths := make(map[string]string, len(volumes))
	for _, volume := range volumes {
		if !types.ValidVolumeName(volume.Name) {
			return fmt.Errorf("volume name %q must match %s and not start with cocoon-", volume.Name, types.VolumeNameRe)
		}
		if _, ok := names[volume.Name]; ok {
			return fmt.Errorf("duplicate volume name %q", volume.Name)
		}
		names[volume.Name] = struct{}{}
		if !filepath.IsAbs(volume.Path) {
			return fmt.Errorf("volume %q path must be absolute", volume.Name)
		}
		if other, ok := paths[filepath.Clean(volume.Path)]; ok {
			return fmt.Errorf("volume %q shares its path with volume %q: admission is keyed by name", volume.Name, other)
		}
		paths[filepath.Clean(volume.Path)] = volume.Name
		if !types.ValidDirectIO(volume.DirectIO) {
			return fmt.Errorf("volume %q directio must be on, off, or auto, got %q", volume.Name, volume.DirectIO)
		}
	}
	return nil
}

// Load reads a JSON config file, applies defaults, and validates.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // path is the operator-supplied -config flag
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := &Config{}
	if err := utils.DecodeStrictJSON(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

// StorageBytes parses a size the way cocoon parses --storage: Docker/Kubernetes binary units, with "Gi" read as "GiB".
func StorageBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(strings.ToLower(s), "i") {
		s += "B"
	}
	return units.RAMInBytes(s)
}

func RoutableHost(host string) bool {
	ip, _ := netip.ParseAddr(host)
	return host != "" && !ip.IsUnspecified()
}

func validatePolicy(p *egress.Policy, secrets map[string]struct{}) error {
	if p == nil {
		return nil
	}
	if err := p.Validate(); err != nil {
		return err
	}
	for i, r := range p.Allow {
		if r.Secret == "" {
			continue
		}
		if _, ok := secrets[r.Secret]; !ok {
			return fmt.Errorf("allow[%d]: unknown secret %q", i, r.Secret)
		}
	}
	return nil
}

func validateArchiveWindow(idle, after, del int) error {
	if after < 0 || del < 0 {
		return fmt.Errorf("archive seconds must not be negative")
	}
	if after > 0 && (idle <= 0 || after <= idle) {
		return fmt.Errorf("archive_after_seconds %d requires idle_hibernate_seconds>0 and a larger value", after)
	}
	return nil
}

func autoRefillConcurrency(cpus int) int {
	return min(max(refillFloor, cpus*2/3), refillCeiling)
}

func validateShards(names []string, field, kind string) error {
	seen := make(map[string]struct{}, len(names))
	for _, n := range names {
		if n == "" {
			return fmt.Errorf("%s must not contain an empty %s name", field, kind)
		}
		if _, ok := seen[n]; ok {
			return fmt.Errorf("duplicate %s %q", kind, n)
		}
		seen[n] = struct{}{}
	}
	return nil
}

// namesHost reads addr the way Mint does and reports whether it carries a host a client could dial.
func namesHost(addr string) bool {
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	return err == nil && RoutableHost(u.Hostname())
}
