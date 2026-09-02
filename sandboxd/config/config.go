// Package config loads the sandboxd node configuration.
package config

import (
	"cmp"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/store/s3"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const (
	defaultListen       = ":7777"
	defaultDataDir      = "/var/lib/sandboxd"
	defaultCocoonBin    = "cocoon"
	defaultWarm         = 4
	defaultMaxForkCount = 16
	refillFloor         = 4
	refillCeiling       = 256
)

// PoolSpec declares one warm pool and its target of claim-ready VMs.
type PoolSpec struct {
	types.PoolKey
	Warm int `json:"warm"`

	// WarmMax, when >0, lets the warm target rise from Warm toward it under demand.
	WarmMax int `json:"warm_max,omitempty"`

	// Egress is this pool's allow-list, intersected with the tenant's; nil denies all egress.
	Egress *egress.Policy `json:"egress,omitempty"`

	// IdleHibernateSeconds, when >0, hibernates idle claims after that many seconds.
	IdleHibernateSeconds int `json:"idle_hibernate_seconds,omitempty"`

	// ArchiveAfterSeconds, when >0, archives a hibernated claim; must exceed IdleHibernateSeconds.
	ArchiveAfterSeconds int `json:"archive_after_seconds,omitempty"`

	// ArchiveDeleteAfterSeconds, when >0, purges the checkpoint that long after archiving.
	ArchiveDeleteAfterSeconds int `json:"archive_delete_after_seconds,omitempty"`
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
	return validateArchiveWindow(s.IdleHibernateSeconds, s.ArchiveAfterSeconds, s.ArchiveDeleteAfterSeconds)
}

// StoreConfig selects a checkpoint backend.
type StoreConfig struct {
	Kind string     `json:"kind"`
	S3   *s3.Config `json:"s3,omitempty"`
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

// TenantSpec declares one tenant: its bearer token and its live-claim quota.
type TenantSpec struct {
	Name      string `json:"name"`
	Token     string `json:"token"` //nolint:gosec // config field, not a hardcoded credential
	MaxClaims int    `json:"max_claims,omitempty"`

	// Egress is the tenant's allow-list (see PoolSpec.Egress).
	Egress *egress.Policy `json:"egress,omitempty"`
}

// VolumeSpec declares one operator-managed dataset disk, held by exactly one node.
type VolumeSpec struct {
	Name     string   `json:"name"`
	Path     string   `json:"path"`
	DirectIO string   `json:"directio,omitempty"`
	Writable bool     `json:"writable,omitempty"`
	Tenants  []string `json:"tenants,omitempty"`
}

// MeshConfig configures cluster membership; every node shares one APIToken and tenant set.
type MeshConfig struct {
	NodeID     string   `json:"node_id"`               // unique name; defaults to Bind
	Bind       string   `json:"bind"`                  // memberlist host:port
	Join       []string `json:"join,omitempty"`        // seed members; empty = mesh of one
	ClusterKey string   `json:"cluster_key,omitempty"` // base64 gossip-encryption key
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
	if host == "" {
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

	// AdvertiseAddr is the host:port the data plane reaches this node at; defaults to Listen.
	AdvertiseAddr string `json:"advertise_addr,omitempty"`

	// Bridges shards egress-lane VMs over host bridges; their taps stay lockable in the root netns.
	Bridges []string `json:"bridges,omitempty"`

	// Networks shards egress-lane VMs over CNI conflists; a bridge holds at most 1024 ports.
	Networks []string `json:"networks,omitempty"`

	// RestoreMode is cocoon's --restore-mode; an older CH silently eager-copies an mmap restore.
	RestoreMode types.RestoreMode `json:"restore_mode,omitempty"`

	// NoDirectIO enables buffered writable disks for cold boots and clones.
	NoDirectIO bool `json:"no_direct_io,omitempty"`

	// APIToken, when set, guards claim and info.
	APIToken string `json:"api_token,omitempty"` //nolint:gosec // config field, not a hardcoded credential

	// Tenants adds per-tenant bearer tokens next to APIToken.
	Tenants []TenantSpec `json:"tenants,omitempty"`

	// Secrets registers node-side credentials; values come from value_env, never this file.
	Secrets []egress.SecretSpec `json:"secrets,omitempty"`

	// IdleHibernateSeconds is the idle policy for unpooled claims; per-pool settings override.
	IdleHibernateSeconds int `json:"idle_hibernate_seconds,omitempty"`

	// ArchiveAfterSeconds and ArchiveDeleteAfterSeconds are the archive policy for unpooled keys.
	ArchiveAfterSeconds       int `json:"archive_after_seconds,omitempty"`
	ArchiveDeleteAfterSeconds int `json:"archive_delete_after_seconds,omitempty"`

	PreviewListen string `json:"preview_listen,omitempty"`
	PreviewSecret string `json:"preview_secret,omitempty"` //nolint:gosec // config field, not a hardcoded credential
	// PreviewAdvertise is the browser-facing base, shareable fleet-wide behind one proxy.
	PreviewAdvertise string `json:"preview_advertise,omitempty"`

	// CheckpointDir is where checkpoints live; defaults to <data_dir>/checkpoints.
	CheckpointDir string `json:"checkpoint_dir,omitempty"`

	// CheckpointStore selects the checkpoint backend; absent means the dir backend.
	CheckpointStore *StoreConfig `json:"checkpoint_store,omitempty"`

	// CheckpointPeerHeal lets a node pull a checkpoint it lacks from a peer; off by default.
	CheckpointPeerHeal bool `json:"checkpoint_peer_heal,omitempty"`

	// EgressInternalAllow re-admits CIDRs through the proxy's SSRF guard, node-wide.
	EgressInternalAllow []string `json:"egress_internal_allow,omitempty"`

	// CheckpointTTLHours ages out checkpoints; 0 keeps them forever.
	CheckpointTTLHours int `json:"checkpoint_ttl_hours,omitempty"`

	// MaxClaims caps live claims node-wide; 0 means unlimited.
	MaxClaims int `json:"max_claims,omitempty"`

	// AuditLog, when true, appends relayed request ops, never payloads, to audit.jsonl.
	AuditLog bool `json:"audit_log,omitempty"`

	// MaxForkCount caps children per fork call; each child is a full-RAM VM.
	MaxForkCount int `json:"max_fork_count,omitempty"`

	// Volumes is the node-local catalog of operator-managed dataset images.
	Volumes []VolumeSpec `json:"volumes,omitempty"`

	// RefillConcurrency caps concurrent VM provisioning node-wide; 0 auto-scales with CPUs.
	RefillConcurrency int `json:"refill_concurrency,omitempty"`

	// Mesh, when set, joins this node to a memberlist cluster; nil is a mesh of one.
	Mesh *MeshConfig `json:"mesh,omitempty"`

	// EgressCA provisions HTTPS interception; required for any intercept rule.
	EgressCA *EgressCAConfig `json:"egress_ca,omitempty"`

	Pools []PoolSpec `json:"pools"`
}

// HasEgress reports whether the node can attach egress-lane VMs.
func (c *Config) HasEgress() bool {
	return len(c.Bridges) > 0 || len(c.Networks) > 0
}

// ClusterDigest fingerprints the must-match config; without cluster_key it omits tokens.
func (c *Config) ClusterDigest(caFingerprint string) string {
	names := make([]string, len(c.Tenants))
	for i, t := range c.Tenants {
		names[i] = t.Name
	}
	slices.Sort(names)
	if c.Mesh != nil && c.Mesh.ClusterKey != "" {
		if key, err := base64.StdEncoding.DecodeString(c.Mesh.ClusterKey); err == nil {
			type auth struct{ Name, Token string }
			tenants := make([]auth, len(c.Tenants))
			for i, t := range c.Tenants {
				tenants[i] = auth{t.Name, t.Token}
			}
			slices.SortFunc(tenants, func(a, b auth) int { return strings.Compare(a.Name, b.Name) })
			raw, _ := json.Marshal([]any{c.APIToken, c.PreviewSecret, caFingerprint, tenants, c.CheckpointTTLHours})
			mac := hmac.New(sha256.New, key)
			mac.Write(raw)
			return hex.EncodeToString(mac.Sum(nil))
		}
	}
	raw, _ := json.Marshal([]any{caFingerprint, names, c.CheckpointTTLHours})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// guardsEgressLane counts any tenant policy, but only an egress-lane pool policy.
func (c *Config) guardsEgressLane() bool {
	return slices.ContainsFunc(c.Tenants, func(t TenantSpec) bool { return t.Egress != nil }) ||
		slices.ContainsFunc(c.Pools, func(p PoolSpec) bool { return p.Net == types.NetEgress && p.Egress != nil })
}

func (c *Config) applyDefaults() {
	c.Listen = cmp.Or(c.Listen, defaultListen)
	c.DataDir = cmp.Or(c.DataDir, defaultDataDir)
	c.CocoonBin = cmp.Or(c.CocoonBin, defaultCocoonBin)
	c.AdvertiseAddr = cmp.Or(c.AdvertiseAddr, c.Listen)
	c.PreviewAdvertise = cmp.Or(c.PreviewAdvertise, c.PreviewListen)
	c.MaxForkCount = cmp.Or(c.MaxForkCount, defaultMaxForkCount)
	if c.RefillConcurrency == 0 {
		c.RefillConcurrency = autoRefillConcurrency(runtime.NumCPU())
	}
	for i := range c.Pools {
		c.Pools[i].Warm = cmp.Or(c.Pools[i].Warm, defaultWarm)
		c.Pools[i].PoolKey = c.Pools[i].Defaulted()
	}
	for i := range c.Volumes {
		c.Volumes[i].DirectIO = cmp.Or(c.Volumes[i].DirectIO, types.DirectIOOff)
	}
}

func (c *Config) validate() error {
	if err := c.validateAttachment(); err != nil {
		return err
	}
	// a CNI network's tap lives in the VM netns, unreachable from the root-netns nft lock.
	if len(c.Networks) > 0 && c.guardsEgressLane() {
		return fmt.Errorf("guarded egress needs a bridge lane, not a CNI network: the tap lives in the VM netns and cannot be locked")
	}
	if c.MaxForkCount < 1 {
		return fmt.Errorf("max_fork_count must be at least 1, got %d", c.MaxForkCount)
	}
	if c.RefillConcurrency < 0 {
		return fmt.Errorf("refill_concurrency must not be negative, got %d", c.RefillConcurrency)
	}
	if err := c.RestoreMode.Validate(); err != nil {
		return fmt.Errorf("restore_mode: %w", err)
	}
	if c.MaxClaims < 0 {
		return fmt.Errorf("max_claims must not be negative, got %d", c.MaxClaims)
	}
	if c.PreviewListen != "" && c.PreviewSecret == "" {
		return fmt.Errorf("preview_listen needs preview_secret")
	}
	if c.IdleHibernateSeconds < 0 {
		return fmt.Errorf("idle_hibernate_seconds must not be negative, got %d", c.IdleHibernateSeconds)
	}
	if err := validateArchiveWindow(c.IdleHibernateSeconds, c.ArchiveAfterSeconds, c.ArchiveDeleteAfterSeconds); err != nil {
		return err
	}
	if cs := c.CheckpointStore; cs != nil {
		switch cs.Kind {
		case "", "dir":
		case "s3":
			if cs.S3 == nil || cs.S3.Bucket == "" {
				return fmt.Errorf("checkpoint_store s3 needs a bucket")
			}
		default:
			return fmt.Errorf("checkpoint_store kind %q: want dir or s3", cs.Kind)
		}
	}
	if c.CheckpointTTLHours < 0 {
		return fmt.Errorf("checkpoint_ttl_hours must not be negative")
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
	if err := c.validateEgressAllow(); err != nil {
		return err
	}
	if err := c.validateTenants(); err != nil {
		return err
	}
	if err := c.validateVolumes(); err != nil {
		return err
	}
	if err := c.validateMesh(); err != nil {
		return err
	}
	secrets, err := c.validateSecrets()
	if err != nil {
		return err
	}
	return c.validateEgress(secrets)
}

func (c *Config) validateVolumes() error {
	names := make(map[string]struct{}, len(c.Volumes))
	tenants := make(map[string]struct{}, len(c.Tenants))
	for _, tenant := range c.Tenants {
		tenants[tenant.Name] = struct{}{}
	}
	for _, volume := range c.Volumes {
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
		if !types.ValidDirectIO(volume.DirectIO) {
			return fmt.Errorf("volume %q directio must be on, off, or auto, got %q", volume.Name, volume.DirectIO)
		}
		for _, tenant := range volume.Tenants {
			if _, ok := tenants[tenant]; !ok {
				return fmt.Errorf("volume %q references unknown tenant %q", volume.Name, tenant)
			}
		}
	}
	return nil
}

// validateMesh fails at load what would otherwise only surface at startMesh.
func (c *Config) validateMesh() error {
	if c.Mesh == nil {
		return nil
	}
	if _, _, err := c.Mesh.ParsedBind(); err != nil {
		return err
	}
	_, err := c.Mesh.DecodedKey()
	return err
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
	for _, tn := range c.Tenants {
		if err := validatePolicy(tn.Egress, secrets); err != nil {
			return fmt.Errorf("tenant %q egress: %w", tn.Name, err)
		}
		if tn.Egress.Intercepts() {
			return fmt.Errorf("tenant %q egress: intercept may only be set on a pool rule", tn.Name)
		}
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

// validateAttachment checks the egress-lane attachment; a repeated shard fills one bridge.
func (c *Config) validateAttachment() error {
	if len(c.Bridges) > 0 && len(c.Networks) > 0 {
		return fmt.Errorf("bridges and networks are mutually exclusive")
	}
	if err := validateShards(c.Bridges, "bridges", "bridge device"); err != nil {
		return err
	}
	return validateShards(c.Networks, "networks", "conflist")
}

// validateEgressAllow rejects a malformed prefix at load, not at a later refused dial.
func (c *Config) validateEgressAllow() error {
	for _, cidr := range c.EgressInternalAllow {
		if _, err := netip.ParsePrefix(cidr); err != nil {
			return fmt.Errorf("egress_internal_allow %q: %w", cidr, err)
		}
	}
	return nil
}

func (c *Config) validateTenants() error {
	if len(c.Tenants) > 0 && c.APIToken == "" {
		return fmt.Errorf("tenants require api_token: the operator surfaces are unreachable without it")
	}
	names := make(map[string]struct{}, len(c.Tenants))
	tokens := make(map[string]struct{}, len(c.Tenants))
	for _, tn := range c.Tenants {
		if !types.NameRe.MatchString(tn.Name) {
			return fmt.Errorf("tenant name %q must match %s", tn.Name, types.NameRe)
		}
		if _, ok := names[tn.Name]; ok {
			return fmt.Errorf("duplicate tenant name %q", tn.Name)
		}
		names[tn.Name] = struct{}{}
		switch tn.Token {
		case "":
			return fmt.Errorf("tenant %q needs a token", tn.Name)
		case c.APIToken:
			return fmt.Errorf("tenant %q token must differ from api_token", tn.Name)
		}
		if _, ok := tokens[tn.Token]; ok {
			return fmt.Errorf("tenant %q token reused by another tenant", tn.Name)
		}
		tokens[tn.Token] = struct{}{}
		if tn.MaxClaims < 0 {
			return fmt.Errorf("tenant %q max_claims must not be negative, got %d", tn.Name, tn.MaxClaims)
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
	// Hand-edited file: a typo must fail load, not silently change policy.
	if err := utils.DecodeStrictJSON(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

// validatePolicy checks the rules and secret refs; a nil policy is deny-all.
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

// validateArchiveWindow requires archive_after past idle_hibernate, both non-negative.
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
