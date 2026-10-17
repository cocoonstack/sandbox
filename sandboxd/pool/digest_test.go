package pool

import (
	"encoding/base64"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestClusterDigest(t *testing.T) {
	acme := []config.TenantSpec{{Name: "acme", Token: "t1"}}
	rotated := []config.TenantSpec{{Name: "acme", Token: "rotated"}}
	base := &config.Config{APIToken: "tok", PreviewSecret: "ps"}
	digest := func(c *config.Config, fp string, tenants []config.TenantSpec) string {
		return clusterDigest(c, fp, tenantRecords(tenants))
	}
	d := digest(base, "ca-fp", acme)
	if digest(base, "ca-fp", acme) != d {
		t.Fatal("digest is not stable for identical config")
	}
	if digest(base, "ca-fp", []config.TenantSpec{{Name: "beta", Token: "t1"}}) == d {
		t.Error("a tenant-name change is not reflected")
	}
	if digest(base, "other-fp", acme) == d {
		t.Error("an egress CA root change is not reflected")
	}

	if digest(&config.Config{APIToken: "other", PreviewSecret: "ps"}, "ca-fp", acme) != d {
		t.Error("api_token leaked into the keyless digest")
	}
	if digest(base, "ca-fp", rotated) != d {
		t.Error("a tenant token leaked into the keyless digest")
	}
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	keyed := &config.Config{APIToken: "tok", PreviewSecret: "ps", Mesh: &config.MeshConfig{ClusterKey: key}}
	keyedDiff := &config.Config{APIToken: "other", PreviewSecret: "ps", Mesh: &config.MeshConfig{ClusterKey: key}}
	if digest(keyed, "ca-fp", acme) == digest(keyedDiff, "ca-fp", acme) {
		t.Error("with a cluster_key the api_token must be covered by the digest")
	}
	if digest(keyed, "ca-fp", acme) == digest(keyed, "ca-fp", rotated) {
		t.Error("with a cluster_key a tenant token rotation must change the digest")
	}
	withVolume := *base
	withVolume.Volumes = []config.VolumeSpec{{Name: "imagenet", Path: "/srv/datasets/imagenet.img", DirectIO: types.DirectIOOff}}
	if digest(&withVolume, "ca-fp", acme) != d {
		t.Error("node-local volume catalog must not change the cluster digest")
	}
	for _, cfg := range []*config.Config{base, keyed} {
		records := tenantRecords(acme)
		before := clusterDigest(cfg, "ca-fp", records)
		records[0].MaxClaims = 1
		if clusterDigest(cfg, "ca-fp", records) == before {
			t.Error("a quota-only change must change the cluster digest")
		}
		if records[0].TokenSHA256 != config.TokenSHA256("t1") {
			t.Error("digest changed the caller's token hash")
		}
	}
}

func TestClusterDigestBytesArePinned(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	tenants := []config.TenantSpec{{Name: "acme", Token: "t<1>&\u2028"}, {Name: "beta", Token: "t2"}}
	for _, tt := range []struct {
		name    string
		cfg     *config.Config
		tenants []config.TenantSpec
		fp      string
		want    string
	}{
		{"keyless", &config.Config{APIToken: "t<o>k", PreviewSecret: "p&s", CheckpointTTLHours: 24}, tenants, "ca<&>\u2028fp", "ad397b4d6752991094c96a38e2e3a18bc220a5e68b315a625236f5147d9d0924"},
		{"keyed", &config.Config{APIToken: "t<o>k&\u2029", PreviewSecret: "p&s<", CheckpointTTLHours: 24, Mesh: &config.MeshConfig{ClusterKey: key}}, tenants, "ca<&>fp", "848ffb9bbcaa14b59074f0753cc2aff33f72494722059f27587d6fe61eb74ad2"},
		{"no tenants", &config.Config{}, nil, "fp", "4bf6489cb7c229d99dad45b7c8eb8a070a4fd7040cf43c1b49f103ae3107b10b"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := clusterDigest(tt.cfg, tt.fp, tenantRecords(tt.tenants)); got != tt.want {
				t.Errorf("digest %s, want the pinned digest %s: a change splits a rolling cluster", got, tt.want)
			}
		})
	}
}

func TestClusterDigestCoversTheEgressLayerInAnyOrder(t *testing.T) {
	key := types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall}
	desk := config.EgressClass{Name: "desk", Egress: &egress.Policy{Allow: []egress.Rule{{Host: "api.example.com"}}}}
	ops := config.EgressClass{Name: "ops", Egress: &egress.Policy{Allow: []egress.Rule{{Host: "ops.example.com"}}}}
	base := func() *config.Config {
		return &config.Config{
			EgressClasses:       []config.EgressClass{desk, ops},
			Secrets:             []egress.SecretSpec{{Name: "gh", Header: "Authorization", ValueEnv: "GH_A"}, {Name: "gw", Header: "X-Key"}},
			EgressInternalAllow: []string{"10.0.0.0/8", "172.16.0.0/12"},
			EgressUpstream:      &config.EgressUpstreamConfig{ClaimEnv: "EGRESS_UPSTREAM", Allow: []string{"a.example", "b.example"}},
			Pools:               []config.PoolSpec{{PoolKey: key, Warm: 2, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "api.example.com", Intercept: egress.InterceptAlways}}}}},
		}
	}
	d := clusterDigest(base(), "fp", nil)
	same := base()
	same.EgressClasses = []config.EgressClass{ops, desk}
	same.Secrets = []egress.SecretSpec{{Name: "gw", Header: "X-Key"}, {Name: "gh", Header: "Authorization", ValueEnv: "GH_A"}}
	same.EgressInternalAllow = []string{"172.16.0.0/12", "10.0.0.0/8"}
	same.EgressUpstream.Allow = []string{"b.example", "a.example"}
	same.Pools[0].Warm = 9
	if clusterDigest(same, "fp", nil) != d {
		t.Error("order or a pool target changed the digest")
	}
	for name, change := range map[string]func(c *config.Config){
		"class rule":     func(c *config.Config) { c.EgressClasses[0].Egress = &egress.Policy{} },
		"secret header":  func(c *config.Config) { c.Secrets[0].Header = "X-Other" },
		"secret env":     func(c *config.Config) { c.Secrets[0].ValueEnv = "GH_B" },
		"internal allow": func(c *config.Config) { c.EgressInternalAllow = nil },
		"upstream allow": func(c *config.Config) { c.EgressUpstream.Allow = []string{"a.example"} },
		"pool egress":    func(c *config.Config) { c.Pools[0].Egress = &egress.Policy{} },
	} {
		c := base()
		change(c)
		if clusterDigest(c, "fp", nil) == d {
			t.Errorf("a %s change left the digest unchanged", name)
		}
	}
}

func tenantRecords(tenants []config.TenantSpec) []config.TenantRecord {
	out := make([]config.TenantRecord, len(tenants))
	for i, t := range tenants {
		out[i] = config.TenantRecord{Name: t.Name, TokenSHA256: config.TokenSHA256(t.Token), MaxClaims: t.MaxClaims}
	}
	return out
}
