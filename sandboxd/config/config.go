// Package config loads the sandboxd node configuration.
package config

import (
	"fmt"
	"os"

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
)

// PoolSpec declares one warm pool and its target of claim-ready VMs.
type PoolSpec struct {
	types.PoolKey
	Warm int `json:"warm"`

	// WarmMax, when >0, turns on the demand-adaptive watermark: the warm
	// target may rise from Warm (the floor) toward WarmMax while claims
	// arrive faster than the pool provisions, and decays back in silence.
	WarmMax int `json:"warm_max,omitempty"`

	// Egress is this pool's allow-list, intersected with the tenant's for the
	// effective policy (a request must pass both); when both matched rules
	// name a secret, the pool's is injected. Nil denies all egress.
	Egress *egress.Policy `json:"egress,omitempty"`

	// IdleHibernateSeconds, when >0, hibernates this pool's idle claims
	// after that many seconds without a data-plane connection; the next
	// call wakes them transparently. Zero disables.
	IdleHibernateSeconds int `json:"idle_hibernate_seconds,omitempty"`

	// ArchiveAfterSeconds, when >0, checkpoints a hibernated claim to the
	// store after that many idle seconds and drops its local VM; a later call
	// restores it. Requires IdleHibernateSeconds>0 and must exceed it.
	ArchiveAfterSeconds int `json:"archive_after_seconds,omitempty"`

	// ArchiveDeleteAfterSeconds, when >0, purges an archived claim's store
	// checkpoint that many seconds after archiving. Zero keeps it forever.
	ArchiveDeleteAfterSeconds int `json:"archive_delete_after_seconds,omitempty"`
}

// ValidateLimits checks the warm/watermark/idle bounds — shared by the
// config file path and the PUT /v1/pools path so the two cannot drift.
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
	return validateArchiveWindow(s.IdleHibernateSeconds, s.ArchiveAfterSeconds, s.ArchiveDeleteAfterSeconds)
}

// StoreConfig selects a checkpoint backend.
type StoreConfig struct {
	Kind string     `json:"kind"`
	S3   *s3.Config `json:"s3,omitempty"`
}

// TenantSpec declares one tenant: its bearer token and its live-claim quota.
// APIToken stays the operator (root) credential with full access; tenant
// tokens reach the resource-creating verbs only, and everything a tenant
// creates is stamped with its name.
type TenantSpec struct {
	Name      string `json:"name"`
	Token     string `json:"token"` //nolint:gosec // config field, not a hardcoded credential
	MaxClaims int    `json:"max_claims,omitempty"`

	// Egress is the tenant's allow-list (see PoolSpec.Egress).
	Egress *egress.Policy `json:"egress,omitempty"`
}

// MeshConfig configures cluster membership. Two v1 constraints: all nodes
// must share the same APIToken AND the same Tenants set (the SDK replays
// whichever token authorized a claim across a redirect, so a peer missing
// that tenant answers 401), and a
// node serving the egress lane can only redirect egress claims to peers if it
// too has an egress attachment (a no-egress node answers 409 rather than
// redirecting). Both are acceptable for a homogeneous cluster.
type MeshConfig struct {
	NodeID     string   `json:"node_id"`               // unique name; defaults to Bind
	Bind       string   `json:"bind"`                  // memberlist host:port
	Join       []string `json:"join,omitempty"`        // seed members; empty = mesh of one
	ClusterKey string   `json:"cluster_key,omitempty"` // base64 gossip-encryption key
}

// Config is the sandboxd node configuration.
type Config struct {
	Listen    string `json:"listen"`
	DataDir   string `json:"data_dir"`
	CocoonBin string `json:"cocoon_bin"`

	// AdvertiseAddr is the host:port the data plane reaches this node at; it
	// is returned as a claim's owner address (and, at M2c, gossiped). Defaults
	// to Listen, which is correct when Listen is a routable host:port.
	AdvertiseAddr string `json:"advertise_addr,omitempty"`

	// Bridge and Network pick the egress-lane attachment (TAP-on-bridge vs
	// CNI conflist); mutually exclusive. With neither set the node serves
	// only the no-network lane.
	Bridge  string `json:"bridge,omitempty"`
	Network string `json:"network,omitempty"`

	// APIToken, when set, guards claim and info; per-sandbox tokens guard
	// sandbox-scoped calls regardless.
	APIToken string `json:"api_token,omitempty"` //nolint:gosec // config field, not a hardcoded credential

	// Tenants adds per-tenant bearer tokens next to APIToken; empty keeps
	// the single-token behavior.
	Tenants []TenantSpec `json:"tenants,omitempty"`

	// Secrets registers node-side credentials the egress proxy injects by
	// name; values come from the environment (value_env), never this file.
	Secrets []egress.SecretSpec `json:"secrets,omitempty"`

	// IdleHibernateSeconds is the idle policy for claims of unpooled keys
	// (template and checkpoint claims); per-pool settings override it for
	// pooled keys. Zero disables.
	IdleHibernateSeconds int `json:"idle_hibernate_seconds,omitempty"`

	// ArchiveAfterSeconds / ArchiveDeleteAfterSeconds are the archive policy
	// for unpooled keys; per-pool settings override them. Same rules as
	// PoolSpec (archive_after must exceed idle_hibernate).
	ArchiveAfterSeconds       int `json:"archive_after_seconds,omitempty"`
	ArchiveDeleteAfterSeconds int `json:"archive_delete_after_seconds,omitempty"`

	// PreviewListen, when set, starts a preview HTTP server on that address
	// serving guest ports under signed URLs. PreviewSecret (cluster-shared)
	// signs the tokens; PreviewAdvertise is the base a browser/proxy reaches
	// this node's preview server at, defaulting to PreviewListen.
	PreviewListen    string `json:"preview_listen,omitempty"`
	PreviewSecret    string `json:"preview_secret,omitempty"` //nolint:gosec // config field, not a hardcoded credential
	PreviewAdvertise string `json:"preview_advertise,omitempty"`

	// CheckpointDir is where checkpoints live; defaults to
	// <data_dir>/checkpoints. Point it at a shared FUSE mount (JuiceFS over
	// object storage, NFS) and every node sharing the mount resolves every
	// checkpoint — the path's filesystem is the operator's choice.
	CheckpointDir string `json:"checkpoint_dir,omitempty"`

	// CheckpointStore selects the checkpoint backend; absent means the
	// dir backend at CheckpointDir. Kind "s3" stores checkpoints in object
	// storage (credentials from the AWS chain, never this file).
	CheckpointStore *StoreConfig `json:"checkpoint_store,omitempty"`

	// CheckpointTTLHours ages out checkpoints (0 = keep forever); the
	// sweep runs hourly and on startup.
	CheckpointTTLHours int `json:"checkpoint_ttl_hours,omitempty"`

	// MaxClaims caps live claims node-wide; 0 means unlimited. Claim,
	// fork, and checkpoint-branch requests beyond it answer 429.
	MaxClaims int `json:"max_claims,omitempty"`

	// AuditLog, when true, appends every relayed request frame's op and
	// addressing fields (never payloads) to <data_dir>/audit.jsonl.
	AuditLog bool `json:"audit_log,omitempty"`

	// MaxForkCount caps children per fork call — each child is a full-RAM VM,
	// so this bounds a single request's memory blast radius to the node's
	// capacity. Defaults to 16.
	MaxForkCount int `json:"max_fork_count,omitempty"`

	// Mesh, when set, joins this node to a memberlist cluster for redirect
	// placement; nil is a single node (mesh of one, no gossip).
	Mesh *MeshConfig `json:"mesh,omitempty"`

	Pools []PoolSpec `json:"pools"`
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

// HasEgress reports whether the node can attach egress-lane VMs.
func (c *Config) HasEgress() bool {
	return c.Bridge != "" || c.Network != ""
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = defaultListen
	}
	if c.DataDir == "" {
		c.DataDir = defaultDataDir
	}
	if c.CocoonBin == "" {
		c.CocoonBin = defaultCocoonBin
	}
	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.Listen
	}
	if c.PreviewListen != "" && c.PreviewAdvertise == "" {
		c.PreviewAdvertise = c.PreviewListen
	}
	if c.MaxForkCount == 0 {
		c.MaxForkCount = defaultMaxForkCount
	}
	for i := range c.Pools {
		if c.Pools[i].Warm == 0 {
			c.Pools[i].Warm = defaultWarm
		}
	}
}

func (c *Config) validate() error {
	if c.Bridge != "" && c.Network != "" {
		return fmt.Errorf("bridge and network are mutually exclusive")
	}
	if c.MaxForkCount < 1 {
		return fmt.Errorf("max_fork_count must be at least 1, got %d", c.MaxForkCount)
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
	if err := c.validateTenants(); err != nil {
		return err
	}
	secrets, err := c.validateSecrets()
	if err != nil {
		return err
	}
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
	}
	for _, tn := range c.Tenants {
		if err := validatePolicy(tn.Egress, secrets); err != nil {
			return fmt.Errorf("tenant %q egress: %w", tn.Name, err)
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

// validatePolicy checks the rules and that each secret ref is registered; a
// nil policy is valid (deny-all).
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

// validateArchiveWindow checks the archive thresholds shared by PoolSpec and
// the node Config: non-negative, and archive_after must sit past idle_hibernate.
func validateArchiveWindow(idle, after, del int) error {
	if after < 0 || del < 0 {
		return fmt.Errorf("archive seconds must not be negative")
	}
	if after > 0 && (idle <= 0 || after <= idle) {
		return fmt.Errorf("archive_after_seconds %d requires idle_hibernate_seconds>0 and a larger value", after)
	}
	return nil
}
