// Package config loads the sandboxd node configuration.
package config

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	defaultListen    = ":7777"
	defaultDataDir   = "/var/lib/sandboxd"
	defaultCocoonBin = "cocoon"
	defaultWarm      = 4
)

// PoolSpec declares one warm pool and its target of claim-ready VMs.
type PoolSpec struct {
	types.PoolKey
	Warm int `json:"warm"`
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

	// Mesh, when set, joins this node to a memberlist cluster for redirect
	// placement; nil is a single node (mesh of one, no gossip).
	Mesh *MeshConfig `json:"mesh,omitempty"`

	Pools []PoolSpec `json:"pools"`
}

// MeshConfig configures cluster membership. Two v1 constraints: all nodes
// must share the same APIToken (the SDK replays it across a redirect), and a
// node serving the egress lane can only redirect egress claims to peers if it
// too has an egress attachment (a no-egress node answers 409 rather than
// redirecting). Both are acceptable for a homogeneous cluster.
type MeshConfig struct {
	NodeID     string   `json:"node_id"`               // unique name; defaults to Bind
	Bind       string   `json:"bind"`                  // memberlist host:port
	Join       []string `json:"join,omitempty"`        // seed members; empty = mesh of one
	ClusterKey string   `json:"cluster_key,omitempty"` // base64 gossip-encryption key
}

// HasEgress reports whether the node can attach egress-lane VMs.
func (c *Config) HasEgress() bool {
	return c.Bridge != "" || c.Network != ""
}

// Load reads a JSON config file, applies defaults, and validates.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // path is the operator-supplied -config flag
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := &Config{}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
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
	for _, p := range c.Pools {
		if err := p.Validate(); err != nil {
			return fmt.Errorf("pool %q: %w", p.Template, err)
		}
		if p.Net == types.NetEgress && !c.HasEgress() {
			return fmt.Errorf("pool %q: egress lane needs bridge or network", p.Template)
		}
		if p.Warm < 0 {
			return fmt.Errorf("pool %q: warm must not be negative", p.Template)
		}
	}
	return nil
}
