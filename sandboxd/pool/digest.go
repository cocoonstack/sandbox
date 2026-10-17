package pool

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

// clusterDigest fingerprints the must-match config, its egress layer and the live tenant records; without cluster_key it omits token hashes.
func clusterDigest(c *config.Config, caFingerprint string, tenants []config.TenantRecord) string {
	tenants = slices.Clone(tenants)
	slices.SortFunc(tenants, func(a, b config.TenantRecord) int { return strings.Compare(a.Name, b.Name) })
	eg := egressFingerprintOf(c)
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
	Classes  []config.EgressClass         `json:"classes,omitempty"`
	Secrets  []egress.SecretSpec          `json:"secrets,omitempty"`
	Internal []string                     `json:"internal,omitempty"`
	Upstream *config.EgressUpstreamConfig `json:"upstream,omitempty"`
	Pools    map[string]config.PoolSpec   `json:"pools,omitempty"`
}

// egressFingerprintOf is the egress config every node must share, in an order-free form; secret values stay out.
func egressFingerprintOf(c *config.Config) egressFingerprint {
	fp := egressFingerprint{
		Classes:  slices.SortedFunc(slices.Values(c.EgressClasses), func(a, b config.EgressClass) int { return strings.Compare(a.Name, b.Name) }),
		Internal: slices.Sorted(slices.Values(c.EgressInternalAllow)),
		Secrets:  slices.SortedFunc(slices.Values(c.Secrets), func(a, b egress.SecretSpec) int { return strings.Compare(a.Name, b.Name) }),
		Pools:    map[string]config.PoolSpec{},
	}
	if u := c.EgressUpstream; u != nil {
		fp.Upstream = &config.EgressUpstreamConfig{ClaimEnv: u.ClaimEnv, Allow: slices.Sorted(slices.Values(u.Allow))}
	}
	for _, p := range c.Pools {
		if p.Egress != nil || p.EgressUpstreamEnv != "" {
			fp.Pools[fmt.Sprintf("pools[%s %s %s]", p.Template, p.Net, p.Size)] = config.PoolSpec{Egress: p.Egress, EgressUpstreamEnv: p.EgressUpstreamEnv}
		}
	}
	return fp
}
