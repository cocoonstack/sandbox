package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestClusterDigest(t *testing.T) {
	acme := []TenantSpec{{Name: "acme", Token: "t1"}}
	rotated := []TenantSpec{{Name: "acme", Token: "rotated"}}
	base := &Config{APIToken: "tok", PreviewSecret: "ps"}
	digest := func(c *Config, fp string, tenants []TenantSpec) string {
		return c.ClusterDigest(fp, tenantRecords(tenants))
	}
	d := digest(base, "ca-fp", acme)
	if digest(base, "ca-fp", acme) != d {
		t.Fatal("digest is not stable for identical config")
	}
	if digest(base, "ca-fp", []TenantSpec{{Name: "beta", Token: "t1"}}) == d {
		t.Error("a tenant-name change is not reflected")
	}
	if digest(base, "other-fp", acme) == d {
		t.Error("an egress CA root change is not reflected")
	}

	if digest(&Config{APIToken: "other", PreviewSecret: "ps"}, "ca-fp", acme) != d {
		t.Error("api_token leaked into the keyless digest")
	}
	if digest(base, "ca-fp", rotated) != d {
		t.Error("a tenant token leaked into the keyless digest")
	}
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	keyed := &Config{APIToken: "tok", PreviewSecret: "ps", Mesh: &MeshConfig{ClusterKey: key}}
	keyedDiff := &Config{APIToken: "other", PreviewSecret: "ps", Mesh: &MeshConfig{ClusterKey: key}}
	if digest(keyed, "ca-fp", acme) == digest(keyedDiff, "ca-fp", acme) {
		t.Error("with a cluster_key the api_token must be covered by the digest")
	}
	if digest(keyed, "ca-fp", acme) == digest(keyed, "ca-fp", rotated) {
		t.Error("with a cluster_key a tenant token rotation must change the digest")
	}
	withVolume := *base
	withVolume.Volumes = []VolumeSpec{{Name: "imagenet", Path: "/srv/datasets/imagenet.img", DirectIO: types.DirectIOOff}}
	if digest(&withVolume, "ca-fp", acme) != d {
		t.Error("node-local volume catalog must not change the cluster digest")
	}
	for _, cfg := range []*Config{base, keyed} {
		records := tenantRecords(acme)
		before := cfg.ClusterDigest("ca-fp", records)
		records[0].MaxClaims = 1
		if cfg.ClusterDigest("ca-fp", records) == before {
			t.Error("a quota-only change must change the cluster digest")
		}
		if records[0].TokenSHA256 != TokenSHA256("t1") {
			t.Error("digest changed the caller's token hash")
		}
	}
}

func TestClusterDigestBytesArePinned(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	tenants := []TenantSpec{{Name: "acme", Token: "t<1>&\u2028"}, {Name: "beta", Token: "t2"}}
	for _, tt := range []struct {
		name    string
		cfg     *Config
		tenants []TenantSpec
		fp      string
		want    string
	}{
		{"keyless", &Config{APIToken: "t<o>k", PreviewSecret: "p&s", CheckpointTTLHours: 24}, tenants, "ca<&>\u2028fp", "ad397b4d6752991094c96a38e2e3a18bc220a5e68b315a625236f5147d9d0924"},
		{"keyed", &Config{APIToken: "t<o>k&\u2029", PreviewSecret: "p&s<", CheckpointTTLHours: 24, Mesh: &MeshConfig{ClusterKey: key}}, tenants, "ca<&>fp", "848ffb9bbcaa14b59074f0753cc2aff33f72494722059f27587d6fe61eb74ad2"},
		{"no tenants", &Config{}, nil, "fp", "4bf6489cb7c229d99dad45b7c8eb8a070a4fd7040cf43c1b49f103ae3107b10b"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.ClusterDigest(tt.fp, tenantRecords(tt.tenants)); got != tt.want {
				t.Errorf("digest %s, want the pinned digest %s: a change splits a rolling cluster", got, tt.want)
			}
		})
	}
}

func TestClusterDigestCoversTheEgressLayerInAnyOrder(t *testing.T) {
	key := types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall}
	desk := EgressClass{Name: "desk", Egress: &egress.Policy{Allow: []egress.Rule{{Host: "api.example.com"}}}}
	ops := EgressClass{Name: "ops", Egress: &egress.Policy{Allow: []egress.Rule{{Host: "ops.example.com"}}}}
	base := func() *Config {
		return &Config{
			EgressClasses:       []EgressClass{desk, ops},
			Secrets:             []egress.SecretSpec{{Name: "gh", Header: "Authorization", ValueEnv: "GH_A"}, {Name: "gw", Header: "X-Key"}},
			EgressInternalAllow: []string{"10.0.0.0/8", "172.16.0.0/12"},
			EgressUpstream:      &EgressUpstreamConfig{ClaimEnv: "EGRESS_UPSTREAM", Allow: []string{"a.example", "b.example"}},
			Pools:               []PoolSpec{{PoolKey: key, Warm: 2, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "api.example.com", Intercept: true}}}}},
		}
	}
	d := base().ClusterDigest("fp", nil)
	same := base()
	same.EgressClasses = []EgressClass{ops, desk}
	same.Secrets = []egress.SecretSpec{{Name: "gw", Header: "X-Key"}, {Name: "gh", Header: "Authorization", ValueEnv: "GH_A"}}
	same.EgressInternalAllow = []string{"172.16.0.0/12", "10.0.0.0/8"}
	same.EgressUpstream.Allow = []string{"b.example", "a.example"}
	same.Pools[0].Warm = 9
	if same.ClusterDigest("fp", nil) != d {
		t.Error("order or a pool target changed the digest")
	}
	for name, change := range map[string]func(c *Config){
		"class rule":     func(c *Config) { c.EgressClasses[0].Egress = &egress.Policy{} },
		"secret header":  func(c *Config) { c.Secrets[0].Header = "X-Other" },
		"secret env":     func(c *Config) { c.Secrets[0].ValueEnv = "GH_B" },
		"internal allow": func(c *Config) { c.EgressInternalAllow = nil },
		"upstream allow": func(c *Config) { c.EgressUpstream.Allow = []string{"a.example"} },
		"pool egress":    func(c *Config) { c.Pools[0].Egress = &egress.Policy{} },
	} {
		c := base()
		change(c)
		if c.ClusterDigest("fp", nil) == d {
			t.Errorf("a %s change left the digest unchanged", name)
		}
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	path := writeConfig(t, `{"pools":[{"template":"rt:24.04","net":"none","size":"small"}]}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Listen == "" || cfg.DataDir == "" || cfg.CocoonBin == "" {
		t.Errorf("defaults not applied: %+v", cfg)
	}
	if cfg.AdvertiseAddr != cfg.Listen {
		t.Errorf("advertise %q, want the listen default %q", cfg.AdvertiseAddr, cfg.Listen)
	}
	if cfg.MaxForkCount < 1 {
		t.Errorf("max_fork_count default missing: %d", cfg.MaxForkCount)
	}
	if cfg.RefillConcurrency < 1 {
		t.Errorf("refill_concurrency default missing: %d", cfg.RefillConcurrency)
	}
	if cfg.NoDirectIO {
		t.Error("no_direct_io defaulted on; want existing direct-I/O behavior")
	}
	if cfg.Pools[0].Warm < 1 {
		t.Errorf("pool warm default missing: %d", cfg.Pools[0].Warm)
	}
	if got := cfg.Pools[0].PoolKey; got != got.Defaulted() {
		t.Errorf("pool key %+v not defaulted; claims key pools via Defaulted and would miss this pool's warm set", got)
	}
}

func TestLoadReadsCaptureTrim(t *testing.T) {
	cfg, err := Load(writeConfig(t, `{"pools":[{"template":"rt:24.04","capture_trim":true},{"template":"py:3.12"}]}`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Pools[0].CaptureTrim || cfg.Pools[1].CaptureTrim {
		t.Errorf("capture_trim %v/%v, want on for the pool that names it and off by default", cfg.Pools[0].CaptureTrim, cfg.Pools[1].CaptureTrim)
	}
}

func TestPoolStorageFollowsCocoonsSpelling(t *testing.T) {
	for _, tt := range []struct {
		storage string
		wantOK  bool
	}{
		{"", true},
		{"10G", true},
		{"40G", true},
		{"40GiB", true},
		{"40Gi", true},
		{"1T", true},
		{"9G", false},
		{"9Gi", false},
		{"forty", false},
	} {
		_, err := Load(writeConfig(t, `{"pools":[{"template":"rt:24.04","storage":"`+tt.storage+`"}]}`))
		if (err == nil) != tt.wantOK {
			t.Errorf("storage %q: err = %v, want ok=%v", tt.storage, err, tt.wantOK)
		}
	}
}

func TestStorageBytesMatchesCocoonsUnits(t *testing.T) {
	for _, storage := range []string{"40G", "40GiB", "40Gi", "40g", " 40G "} {
		if n, err := StorageBytes(storage); err != nil || n != 40<<30 {
			t.Errorf("StorageBytes(%q) = %d, %v, want %d", storage, n, err, int64(40<<30))
		}
	}
}

func TestAutoRefillConcurrency(t *testing.T) {
	for _, tt := range []struct {
		cpus int
		want int
	}{
		{cpus: 1, want: 4},
		{cpus: 6, want: 4},
		{cpus: 24, want: 16},
		{cpus: 384, want: 256},
		{cpus: 768, want: 256},
	} {
		t.Run(strconv.Itoa(tt.cpus)+" cpus", func(t *testing.T) {
			if got := autoRefillConcurrency(tt.cpus); got != tt.want {
				t.Errorf("autoRefillConcurrency(%d) = %d, want %d", tt.cpus, got, tt.want)
			}
		})
	}
}

func TestLoadRejectsInvalid(t *testing.T) {
	for _, tt := range []struct {
		name, body, want string
	}{
		{"bad json", `{`, "config"},
		{"bad fork count", `{"max_fork_count":-1,"pools":[]}`, "max_fork_count"},
		{"negative refill concurrency", `{"refill_concurrency":-1,"pools":[]}`, "refill_concurrency"},
		{"bad restore mode", `{"restore_mode":"Mmap","pools":[]}`, "restore_mode"},
		{"bad pool key", `{"pools":[{"template":"","net":"none","size":"small"}]}`, "pool"},
		{"egress without attachment", `{"pools":[{"template":"rt:24.04","net":"egress","size":"small"}]}`, "egress lane needs"},
		{"egress idle hibernate", `{"bridges":["br0"],"pools":[{"template":"rt:24.04","net":"egress","size":"small","idle_hibernate_seconds":1}]}`, "not supported for egress"},
		{"negative warm", `{"pools":[{"template":"rt:24.04","net":"none","size":"small","warm":-2}]}`, "negative"},
		{"secret without header", `{"secrets":[{"name":"gh"}],"pools":[]}`, "not a valid header name"},
		{"secret bad header", `{"secrets":[{"name":"gh","header":"Bad Header:","value_env":"GH_TOKEN"}],"pools":[]}`, "not a valid header name"},
		{"secret with inline value", `{"secrets":[{"name":"gh","header":"Authorization","value":"tok"}],"pools":[]}`, "value is not supported"},
		{"secret with value and value_env", `{"secrets":[{"name":"gh","header":"Authorization","value":"tok","value_env":"GH_TOKEN"}],"pools":[]}`, "value is not supported"},
		{"secret hop-by-hop header", `{"secrets":[{"name":"gh","header":"Connection","value_env":"GH_TOKEN"}],"pools":[]}`, "not injectable"},
		{"egress methods typo", `{"pools":[{"template":"rt:24.04","net":"none","size":"small","egress":{"allow":[{"host":"x","method":["GET"]}]}}]}`, "unknown object member name"},
		{"egress key typo", `{"pools":[{"template":"rt:24.04","net":"none","size":"small","egres":{"allow":[{"host":"x"}]}}]}`, "unknown object member name"},
		{"duplicate methods key", `{"pools":[{"template":"rt:24.04","net":"none","size":"small","egress":{"allow":[{"host":"x","methods":["GET"],"methods":[]}]}}]}`, "duplicate object member name"},
		{"trailing data", `{"pools":[]} {"api_token":"x"}`, "after top-level value"},
		{"duplicate secret name", `{"secrets":[{"name":"gh","header":"A","value_env":"X"},{"name":"gh","header":"B","value_env":"Y"}],"pools":[]}`, "duplicate secret"},
		{"pool egress empty host", `{"pools":[{"template":"rt:24.04","net":"none","size":"small","egress":{"allow":[{"host":""}]}}]}`, "must not be empty"},
		{"pool egress unknown secret", `{"pools":[{"template":"rt:24.04","net":"none","size":"small","egress":{"allow":[{"host":"api.github.com","secret":"gh"}]}}]}`, "unknown secret"},
		{"meta_store unknown kind", `{"api_token":"r","pools":[],"meta_store":{"kind":"redis","dsn_env":"HOME"}}`, "not supported"},
		{"meta_store dsn env unset", `{"api_token":"r","pools":[],"meta_store":{"kind":"pg","dsn_env":"SANDBOX_TEST_UNSET_DSN"}}`, "must name a set node env"},
		{"egress class unknown secret", `{"pools":[],"egress_classes":[{"name":"desk","egress":{"allow":[{"host":"x","secret":"gh"}]}}]}`, "unknown secret"},
		{"egress class intercepts", `{"pools":[],"egress_classes":[{"name":"desk","egress":{"allow":[{"host":"x","intercept":true}]}}]}`, "only be set on a pool rule"},
		{"egress class without egress", `{"pools":[],"egress_classes":[{"name":"desk"}]}`, "needs egress"},
		{"duplicate egress class", `{"pools":[],"egress_classes":[{"name":"desk","egress":{}},{"name":"desk","egress":{}}]}`, "duplicate egress class"},
		{"tenants in config.json", `{"api_token":"root","pools":[],"tenants":[{"name":"acme","token":"t1"}]}`, "unknown object member"},
		{"guarded egress on cni pool", `{"networks":["cni"],"pools":[{"template":"rt:24.04","net":"egress","size":"small","egress":{"allow":[{"host":"x"}]}}]}`, "needs a bridge lane"},
		{"guarded egress on cni class", `{"networks":["cni"],"pools":[{"template":"rt:24.04","net":"egress","size":"small"}],"egress_classes":[{"name":"desk","egress":{"allow":[{"host":"x"}]}}]}`, "needs a bridge lane"},
		{"cni class no egress pool", `{"networks":["cni"],"pools":[{"template":"rt:24.04","net":"none","size":"small"}],"egress_classes":[{"name":"desk","egress":{"allow":[{"host":"x"}]}}]}`, "needs a bridge lane"},
		{"mesh with wildcard advertise", `{"listen":":7777","pools":[],"mesh":{"bind":"node1:7946"}}`, "routable host"},
		{"mesh with unspecified advertise", `{"advertise_addr":"0.0.0.0:7777","pools":[],"mesh":{"bind":"node1:7946"}}`, "routable host"},
		{"mesh bind missing port", `{"pools":[],"mesh":{"bind":"node1"}}`, "mesh bind"},
		{"mesh bind wildcard host", `{"pools":[],"mesh":{"bind":":7946"}}`, "explicit host"},
		{"mesh cluster key not base64", `{"pools":[],"mesh":{"bind":"node1:7946","cluster_key":"not!base64"}}`, "not valid base64"},
		{"mesh cluster key wrong length", `{"pools":[],"mesh":{"bind":"node1:7946","cluster_key":"YWJj"}}`, "want 16, 24, or 32"},
		{"checkpoint peer heal without mesh", `{"checkpoint_peer_heal":true,"pools":[]}`, "requires an encrypted mesh"},
		{"checkpoint peer heal without cluster key", `{"checkpoint_peer_heal":true,"pools":[],"mesh":{"bind":"node1:7946"}}`, "requires an encrypted mesh"},
		{"checkpoint peer heal without ttl", `{"checkpoint_peer_heal":true,"pools":[],"mesh":{"bind":"node1:7946","cluster_key":"MDEyMzQ1Njc4OWFiY2RlZg=="}}`, "requires checkpoint_ttl_hours"},
		{"checkpoint peer heal without api_token", `{"checkpoint_peer_heal":true,"pools":[],"mesh":{"bind":"node1:7946","cluster_key":"MDEyMzQ1Njc4OWFiY2RlZg=="},"checkpoint_ttl_hours":1}`, "requires api_token"},
		{"duplicate pool key", `{"pools":[{"template":"rt:24.04","warm":2,"egress":{"allow":[{"host":"x"}]}},{"template":"rt:24.04","warm":8}]}`, "duplicate pool"},
		{"preview advertise defaults to a wildcard listen", `{"preview_listen":":8443","preview_secret":"s","pools":[]}`, "routable host"},
		{"preview advertise names no host", `{"preview_listen":"127.0.0.1:8443","preview_secret":"s","preview_advertise":"https://:8443","pools":[]}`, "routable host"},
		{"preview advertise unspecified bare host", `{"preview_listen":"127.0.0.1:8443","preview_secret":"s","preview_advertise":"0.0.0.0","pools":[]}`, "routable host"},
		{"preview advertise unspecified bare ipv6", `{"preview_listen":"127.0.0.1:8443","preview_secret":"s","preview_advertise":"[::]","pools":[]}`, "routable host"},
		{"internal allow port out of range", `{"egress_internal_allow":["10.8.0.1/32:70000"],"pools":[]}`, `egress_internal_allow "10.8.0.1/32:70000"`},
		{"internal allow empty port", `{"egress_internal_allow":["fdc8::/16:"],"pools":[]}`, `egress_internal_allow "fdc8::/16:"`},
		{"upstream default without egress_upstream", `{"pools":[{"template":"rt:24.04","egress_upstream_env":"RES"}]}`, "needs egress_upstream"},
		{"upstream claim env name", `{"egress_upstream":{"claim_env":"bad name","allow":["res.example"]},"pools":[]}`, "claim_env"},
		{"upstream empty allow", `{"egress_upstream":{"claim_env":"UP","allow":[]},"pools":[]}`, "at least one upstream"},
		{"upstream bad allow entry", `{"egress_upstream":{"claim_env":"UP","allow":["res example"]},"pools":[]}`, "egress_upstream.allow"},
		{"upstream default env unset", `{"egress_upstream":{"claim_env":"UP","allow":["res.example"]},"egress_classes":[{"name":"desk","egress":{},"egress_upstream_env":"SANDBOX_TEST_UNSET_UPSTREAM"}],"pools":[]}`, "SANDBOX_TEST_UNSET_UPSTREAM"},
		{"duplicate volume path", `{"pools":[],"volumes":[{"name":"models","path":"/srv/models.img"},{"name":"models-rw","path":"/srv/models.img","writable":true}]}`, "shares its path"},
		{"bad volume name", `{"pools":[],"volumes":[{"name":"ImageNet","path":"/srv/datasets/a.img"}]}`, "volume name"},
		{"reserved volume name", `{"pools":[],"volumes":[{"name":"cocoon-data","path":"/srv/datasets/a.img"}]}`, "not start with cocoon-"},
		{"duplicate volume name", `{"pools":[],"volumes":[{"name":"data","path":"/srv/datasets/a.img"},{"name":"data","path":"/srv/datasets/b.img"}]}`, "duplicate volume"},
		{"relative volume path", `{"pools":[],"volumes":[{"name":"data","path":"datasets/a.img"}]}`, "path must be absolute"},
		{"bad volume directio", `{"pools":[],"volumes":[{"name":"data","path":"/srv/datasets/a.img","directio":"yes"}]}`, "directio must be"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.body))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Load: %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestLoadMetaStore(t *testing.T) {
	t.Setenv("SANDBOX_TEST_DSN", "postgres://sandboxd@db/sandboxd")
	ms := `"meta_store":{"kind":"pg","dsn_env":"SANDBOX_TEST_DSN"}`
	cfg, err := Load(writeConfig(t, `{"api_token":"r","pools":[],`+ms+`}`))
	if err != nil || cfg.MetaStore.DSNEnv != "SANDBOX_TEST_DSN" {
		t.Fatalf("Load: %+v %v", cfg, err)
	}
	if _, err := Load(writeConfig(t, `{"pools":[],`+ms+`}`)); err == nil || !strings.Contains(err.Error(), "needs api_token") {
		t.Errorf("meta_store without api_token: %v, want a refusal", err)
	}
	next := *cfg
	next.MetaStore = nil
	if _, _, err := cfg.ReloadDiff(&next); err == nil || !strings.Contains(err.Error(), "meta_store") {
		t.Errorf("a reload that drops meta_store: %v, want a restart refusal", err)
	}
}

func TestLoadEgressUpstream(t *testing.T) {
	t.Setenv("SANDBOX_TEST_RES", "http://user:pw@res.example:3128")
	t.Setenv("SANDBOX_TEST_DC", "socks5://10.1.0.5:1080")
	body := `{"egress_upstream":{"claim_env":"EGRESS_UPSTREAM","allow":["res.example","10.1.0.0/16"]},"egress_usage_bytes":true,
		"api_token":"r","egress_classes":[{"name":"desk","egress":{},"egress_upstream_env":"SANDBOX_TEST_DC"}],
		"pools":[{"template":"rt:24.04","egress_upstream_env":"SANDBOX_TEST_RES"}]}`
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.EgressUpstream.ClaimEnv != "EGRESS_UPSTREAM" || !cfg.EgressUsageBytes || cfg.Pools[0].EgressUpstreamEnv != "SANDBOX_TEST_RES" {
		t.Errorf("loaded %+v / %+v", cfg.EgressUpstream, cfg.Pools[0])
	}
	t.Setenv("SANDBOX_TEST_RES", "http://user:pw@elsewhere.example:3128")
	if _, err := Load(writeConfig(t, body)); err == nil || !strings.Contains(err.Error(), "not in egress_upstream.allow") || strings.Contains(err.Error(), "pw") {
		t.Errorf("unlisted default: %v, want an allow error without the password", err)
	}
}

func TestLoadAcceptsPreviewAdvertiseShapes(t *testing.T) {
	for _, advertise := range []string{"preview.example.com", "preview.example.com:8443", "https://preview.example.com", "10.0.0.5:8443"} {
		t.Run(advertise, func(t *testing.T) {
			path := writeConfig(t, `{"preview_listen":":8443","preview_secret":"s","preview_advertise":"`+advertise+`","pools":[]}`)
			if _, err := Load(path); err != nil {
				t.Errorf("Load: %v, want the shape Mint accepts to pass validation", err)
			}
		})
	}
}

func TestLoadAcceptsVolumes(t *testing.T) {
	path := writeConfig(t, `{"pools":[],"volumes":[
		{"name":"imagenet","path":"/srv/datasets/imagenet.img"},
		{"name":"weights-llama","path":"/srv/datasets/llama.img","directio":"on"}]}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Volumes) != 2 || cfg.Volumes[0].DirectIO != types.DirectIOOff ||
		cfg.Volumes[1].DirectIO != types.DirectIOOn {
		t.Errorf("volumes = %+v", cfg.Volumes)
	}
}

func TestLoadAcceptsVolumeTenantAccessList(t *testing.T) {
	path := writeConfig(t, `{"api_token":"root","pools":[],
		"volumes":[{"name":"corpus","path":"/srv/datasets/corpus.img","tenants":["acme"]}]}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Volumes[0].Tenants; !slices.Equal(got, []string{"acme"}) {
		t.Errorf("volume tenants = %v, want [acme]", got)
	}
}

func TestLoadAcceptsEgressPolicy(t *testing.T) {
	path := writeConfig(t, `{"secrets":[{"name":"gh","header":"Authorization","value_env":"GH_TOKEN"},{"name":"gw","header":"Authorization"}],
		"pools":[{"template":"rt:24.04","net":"none","size":"small",
			"egress":{"allow":[{"host":"api.github.com","methods":["GET"],"secret":"gh"},{"host":"*.googleapis.com","secret":"gw"}]}}]}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	pol := cfg.Pools[0].Egress
	if pol == nil || len(pol.Allow) != 2 || pol.Allow[0].Secret != "gh" {
		t.Errorf("egress policy %+v", pol)
	}
	if len(cfg.Secrets) != 2 || cfg.Secrets[0].Name != "gh" || cfg.Secrets[1].ValueEnv != "" {
		t.Errorf("secrets %+v", cfg.Secrets)
	}
}

func TestLoadAcceptsUnguardedCNINetwork(t *testing.T) {
	path := writeConfig(t, `{"networks":["cni"],"pools":[{"template":"rt:24.04","net":"egress","size":"small"}]}`)
	if _, err := Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
}

func TestLoadAcceptsNoneLanePolicyOnCNI(t *testing.T) {
	for _, body := range []string{
		`{"networks":["cni"],"pools":[{"template":"rt:24.04","net":"none","size":"small","egress":{"allow":[{"host":"x"}]}}]}`,
		`{"networks":["cni"],"pools":[{"template":"rt:24.04","net":"egress","size":"small"},{"template":"rt:24.04","net":"none","size":"medium","egress":{"allow":[{"host":"x"}]}}]}`,
	} {
		if _, err := Load(writeConfig(t, body)); err != nil {
			t.Errorf("Load rejected a none-lane policy on CNI: %v", err)
		}
	}
}

func TestLoadAcceptsPoolIntercept(t *testing.T) {
	path := writeConfig(t, `{"egress_ca":{"root_cert":"/x/root.crt","intermediate_cert":"/x/n.crt","intermediate_key":"/x/n.key"},
		"pools":[{"template":"rt:24.04","net":"none","size":"small","egress":{"allow":[{"host":"api.github.com","intercept":true}]}}]}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if pol := cfg.Pools[0].Egress; pol == nil || len(pol.Allow) != 1 || !pol.Allow[0].Intercept {
		t.Errorf("intercept rule not parsed: %+v", cfg.Pools[0].Egress)
	}
}

func TestLoadRejectsInterceptWithoutCA(t *testing.T) {
	path := writeConfig(t, `{"pools":[{"template":"rt:24.04","net":"none","size":"small","egress":{"allow":[{"host":"x","intercept":true}]}}]}`)
	if _, err := Load(path); err == nil {
		t.Error("Load accepted an intercept pool without egress_ca; want rejection")
	}
}

func TestHasEgress(t *testing.T) {
	if (&Config{}).HasEgress() {
		t.Error("no attachment must mean no egress")
	}
	if !(&Config{Bridges: []string{"br0", "br1"}}).HasEgress() || !(&Config{Networks: []string{"cni"}}).HasEgress() {
		t.Error("bridges or networks must enable egress")
	}
}

func TestLoadDefaultsWarmOnlyWhenOmitted(t *testing.T) {
	tests := []struct {
		name string
		pool string
		want int
	}{
		{"omitted", `{"template":"rt:24.04","net":"none","size":"small"}`, defaultWarm},
		{"explicit zero", `{"template":"rt:24.04","net":"none","size":"small","warm":0}`, 0},
		{"explicit zero under warm_max", `{"template":"rt:24.04","net":"none","size":"small","warm":0,"warm_max":8}`, 0},
		{"explicit zero, other case", `{"template":"rt:24.04","net":"none","size":"small","WARM":0}`, 0},
		{"explicit three", `{"template":"rt:24.04","net":"none","size":"small","warm":3}`, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, `{"pools":[`+tt.pool+`]}`))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Pools[0].Warm != tt.want {
				t.Errorf("warm %d, want %d", cfg.Pools[0].Warm, tt.want)
			}
		})
	}
	if _, err := Load(writeConfig(t, `{"pools":[{"template":"rt:24.04","net":"none","size":"small","warmth":2}]}`)); err == nil {
		t.Error("an unknown pool member loaded")
	}
}

func TestLoadKeepsExplicitValues(t *testing.T) {
	path := writeConfig(t, `{"listen":"0.0.0.0:9999","advertise_addr":"10.0.0.5:9999","max_fork_count":4,
		"refill_concurrency":8,"no_direct_io":true,"bridges":["br0"],"pools":[{"template":"rt:24.04","net":"egress","size":"small","warm":3}]}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AdvertiseAddr != "10.0.0.5:9999" || cfg.MaxForkCount != 4 || cfg.RefillConcurrency != 8 || !cfg.NoDirectIO || cfg.Pools[0].Warm != 3 {
		t.Errorf("explicit values overridden: %+v", cfg)
	}
	if cfg.Pools[0].Net != types.NetEgress {
		t.Errorf("pool net %q", cfg.Pools[0].Net)
	}
}

func TestLoadRejectsUnusableNetworkLists(t *testing.T) {
	for name, body := range map[string]string{
		"bridges and networks together": `{"bridges":["br0"],"networks":["a"],"pools":[]}`,
		"retired scalar network key":    `{"network":"a","pools":[]}`,
		"retired scalar bridge key":     `{"bridge":"br0","pools":[]}`,
		"empty conflist name":           `{"networks":["a",""],"pools":[]}`,
		"empty bridge name":             `{"bridges":["br0",""],"pools":[]}`,
		"repeated conflist":             `{"networks":["a","b","a"],"pools":[]}`,
		"repeated bridge":               `{"bridges":["br0","br1","br0"],"pools":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, body)); err == nil {
				t.Error("Load accepted an unusable network list; want rejection")
			}
		})
	}
}

func TestLoadAcceptsGuardedEgressOnABridgesList(t *testing.T) {
	path := writeConfig(t, `{"bridges":["sbx0","sbx1"],"pools":[{"template":"rt:24.04","net":"egress","size":"small","egress":{"allow":[{"host":"x"}]}}]}`)
	if _, err := Load(path); err != nil {
		t.Fatalf("guarded egress on a bridges list must load (taps stay in the root netns): %v", err)
	}
}

func TestLoadAcceptsAShardedNetworkList(t *testing.T) {
	path := writeConfig(t, `{"networks":["cocoon-sbx0","cocoon-sbx1","cocoon-sbx2","cocoon-sbx3"],"pools":[]}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Networks) != 4 || !cfg.HasEgress() {
		t.Errorf("Networks = %v, HasEgress = %v", cfg.Networks, cfg.HasEgress())
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	return path
}

func tenantRecords(tenants []TenantSpec) []TenantRecord {
	out := make([]TenantRecord, len(tenants))
	for i, t := range tenants {
		out[i] = TenantRecord{Name: t.Name, TokenSHA256: TokenSHA256(t.Token), MaxClaims: t.MaxClaims}
	}
	return out
}
