// Package config loads the sandboxd node configuration.
package config

import (
	"cmp"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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

	"github.com/docker/go-units"

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
	// cocoon builds a VM with a 10G disk by default, so a pool never asks for less
	minStorageBytes = 10 << 30
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
	_ = json.Unmarshal(raw, &probe, json.MatchCaseInsensitiveNames(true))
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
	Kind string     `json:"kind"`
	S3   *s3.Config `json:"s3,omitempty"`
}

// EgressUpstreamConfig lets a claim's host-only ClaimEnv entry name the proxy its egress leaves through, from the Allow list.
type EgressUpstreamConfig struct {
	ClaimEnv string   `json:"claim_env"`
	Allow    []string `json:"allow"`
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
}

// TenantSpec is one tenant as the tenant API takes it: its bearer token, its live-claim quota and its egress class.
type TenantSpec struct {
	Name        string `json:"name"`
	Token       string `json:"token"`
	MaxClaims   int    `json:"max_claims,omitzero"`
	EgressClass string `json:"egress_class,omitempty"`
}

// TenantRecord stores a tenant's identity, quota and egress class without its plaintext token.
type TenantRecord struct {
	Name        string `json:"name"`
	TokenSHA256 string `json:"token_sha256"`
	MaxClaims   int    `json:"max_claims,omitzero"`
	EgressClass string `json:"egress_class,omitempty"`
}

// VolumeSpec declares one operator-managed dataset disk, held by exactly one node.
type VolumeSpec struct {
	Name     string   `json:"name"`
	Path     string   `json:"path"`
	DirectIO string   `json:"directio,omitempty"`
	Writable bool     `json:"writable,omitzero"`
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

	AdvertiseAddr   string   `json:"advertise_addr,omitempty"`
	ClientAdvertise string   `json:"client_advertise,omitempty"`
	Bridges         []string `json:"bridges,omitempty"`
	Networks        []string `json:"networks,omitempty"`

	RestoreMode types.RestoreMode `json:"restore_mode,omitempty"`
	NoDirectIO  bool              `json:"no_direct_io,omitzero"`
	NoBalloon   bool              `json:"no_balloon,omitzero"`
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

	MaxClaims           int  `json:"max_claims,omitzero"`
	MaxForkCount        int  `json:"max_fork_count,omitzero"`
	RefillConcurrency   int  `json:"refill_concurrency,omitzero"`
	ReleaseDelaySeconds int  `json:"release_delay_seconds,omitzero"`
	AuditLog            bool `json:"audit_log,omitzero"`

	Volumes []VolumeSpec `json:"volumes,omitempty"`
	Mesh    *MeshConfig  `json:"mesh,omitempty"`
	Pools   []PoolSpec   `json:"pools"`
}

// HasEgress reports whether the node can attach egress-lane VMs.
func (c *Config) HasEgress() bool {
	return len(c.Bridges) > 0 || len(c.Networks) > 0
}

// ClusterDigest fingerprints the must-match config, its egress layer and the live tenant records; without cluster_key it omits token hashes.
func (c *Config) ClusterDigest(caFingerprint string, tenants []TenantRecord) string {
	tenants = slices.Clone(tenants)
	slices.SortFunc(tenants, func(a, b TenantRecord) int { return strings.Compare(a.Name, b.Name) })
	eg := c.egressFingerprint()
	if c.Mesh != nil {
		if key, _ := c.Mesh.DecodedKey(); key != nil {
			raw, _ := utils.DigestJSON([]any{c.APIToken, c.PreviewSecret, caFingerprint, tenants, c.CheckpointTTLHours, eg})
			mac := hmac.New(sha256.New, key)
			mac.Write(raw)
			return hex.EncodeToString(mac.Sum(nil))
		}
	}
	for i := range tenants {
		tenants[i].TokenSHA256 = ""
	}
	return utils.DigestHex([]any{caFingerprint, tenants, c.CheckpointTTLHours, eg})
}

type egressFingerprint struct {
	Classes  []EgressClass         `json:"classes,omitempty"`
	Secrets  []egress.SecretSpec   `json:"secrets,omitempty"`
	Internal []string              `json:"internal,omitempty"`
	Upstream *EgressUpstreamConfig `json:"upstream,omitempty"`
	Pools    map[string]PoolSpec   `json:"pools,omitempty"`
}

// egressFingerprint is the egress config every node must share, in an order-free form; secret values stay out.
func (c *Config) egressFingerprint() egressFingerprint {
	fp := egressFingerprint{
		Classes:  slices.SortedFunc(slices.Values(c.EgressClasses), func(a, b EgressClass) int { return strings.Compare(a.Name, b.Name) }),
		Internal: slices.Sorted(slices.Values(c.EgressInternalAllow)),
		Secrets:  slices.SortedFunc(slices.Values(c.Secrets), func(a, b egress.SecretSpec) int { return strings.Compare(a.Name, b.Name) }),
		Pools:    map[string]PoolSpec{},
	}
	if u := c.EgressUpstream; u != nil {
		fp.Upstream = &EgressUpstreamConfig{ClaimEnv: u.ClaimEnv, Allow: slices.Sorted(slices.Values(u.Allow))}
	}
	for _, p := range c.Pools {
		if p.Egress != nil || p.EgressUpstreamEnv != "" {
			fp.Pools[poolLabel(p.PoolKey)] = PoolSpec{Egress: p.Egress, EgressUpstreamEnv: p.EgressUpstreamEnv}
		}
	}
	return fp
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
	c.MaxForkCount = cmp.Or(c.MaxForkCount, defaultMaxForkCount)
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
	if c.CheckpointPeerHeal && (c.Mesh == nil || c.Mesh.ClusterKey == "") {
		return fmt.Errorf("checkpoint_peer_heal requires an encrypted mesh (set mesh.cluster_key)")
	}
	if c.CheckpointPeerHeal && c.CheckpointTTLHours == 0 {
		return fmt.Errorf("checkpoint_peer_heal requires checkpoint_ttl_hours > 0: the ttl is what ages out a healed replica a delete broadcast missed")
	}
	if c.CheckpointPeerHeal && c.APIToken == "" {
		return fmt.Errorf("checkpoint_peer_heal requires api_token: without it resolveScope leaves the raw checkpoint blob GET reachable with no credential")
	}
	if err := c.validateEgressRouting(); err != nil {
		return err
	}
	if err := c.validateMetaStore(); err != nil {
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
	paths := make(map[string]string, len(c.Volumes))
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
	if !routableHost(u.Hostname()) {
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
	if !routableHost(host) {
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

func (c *Config) validateEgressRouting() error {
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

func (c *Config) validateMetaStore() error {
	ms := c.MetaStore
	switch {
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

// TokenSHA256 is the hex SHA-256 a tenant token is kept as outside config.json.
func TokenSHA256(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
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
	return err == nil && routableHost(u.Hostname())
}

func routableHost(host string) bool {
	ip, _ := netip.ParseAddr(host)
	return host != "" && !ip.IsUnspecified()
}
