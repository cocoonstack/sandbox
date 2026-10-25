package config

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

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
		{"egress conn rate without a rate", `{"egress_conn_rate":{"per_second":0,"burst":10},"pools":[]}`, "egress_conn_rate"},
		{"egress conn rate without a burst", `{"egress_conn_rate":{"per_second":30},"pools":[]}`, "egress_conn_rate"},
		{"disk reserve without a size", `{"disk_reserve":{"free":""},"pools":[]}`, "disk_reserve.free"},
		{"disk reserve relative dir", `{"disk_reserve":{"free":"100G","dirs":["run"]},"pools":[]}`, "disk_reserve.dirs"},
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
		{"meta_store bad cell", `{"api_token":"r","pools":[],"meta_store":{"kind":"pg","dsn_env":"HOME","cell":"Cell A"}}`, "meta_store.cell"},
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
		{"reader without warm", `{"pools":[{"template":"rt:24.04","warm":0,"reader":true}]}`, "reader needs warm"},
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
	if err != nil || cfg.MetaStore.DSNEnv != "SANDBOX_TEST_DSN" || cfg.MetaStore.Cell != "" {
		t.Fatalf("Load: %+v %v, want no cell unless one is set", cfg, err)
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

func TestLoadArchiveFailover(t *testing.T) {
	t.Setenv("SANDBOX_TEST_DSN", "postgres://sandboxd@db/sandboxd")
	parts := map[string]string{
		"meta_store":       `"meta_store":{"kind":"pg","dsn_env":"SANDBOX_TEST_DSN","cell":"c1"}`,
		"checkpoint_store": `"checkpoint_store":{"kind":"s3","s3":{"bucket":"b"}}`,
		"mesh":             `"mesh":{"bind":"node1:7946"}`,
	}
	body := func(skip string) string {
		b := `{"api_token":"r","advertise_addr":"node1:7777","archive_failover":true,"pools":[]`
		for name, part := range parts {
			if name != skip {
				b += "," + part
			}
		}
		return b + "}"
	}
	cfg, err := Load(writeConfig(t, body("")))
	if err != nil || !cfg.ArchiveFailover || cfg.Mesh.ID() != "node1:7946" {
		t.Fatalf("Load: %+v %v, want failover on with the bind as node id", cfg, err)
	}
	for name := range parts {
		if _, err := Load(writeConfig(t, body(name))); err == nil || !strings.Contains(err.Error(), "archive_failover requires") {
			t.Errorf("archive_failover without %s: %v, want a refusal", name, err)
		}
	}
	if _, err := Load(writeConfig(t, strings.Replace(body(""), `"kind":"s3","s3":{"bucket":"b"}`, `"kind":"dir"`, 1))); err == nil || !strings.Contains(err.Error(), "archive_failover requires") {
		t.Errorf("archive_failover on a dir checkpoint_store: %v, want a refusal", err)
	}
	if _, err := Load(writeConfig(t, strings.Replace(body(""), `,"cell":"c1"`, "", 1))); err == nil || !strings.Contains(err.Error(), "archive_failover requires") {
		t.Errorf("archive_failover without meta_store.cell: %v, want a refusal", err)
	}
}

func TestLoadReplicateSeconds(t *testing.T) {
	t.Setenv("SANDBOX_TEST_DSN", "postgres://sandboxd@db/sandboxd")
	failover := `"archive_failover":true,"meta_store":{"kind":"pg","dsn_env":"SANDBOX_TEST_DSN","cell":"c1"},"checkpoint_store":{"kind":"s3","s3":{"bucket":"b"}},"mesh":{"bind":"node1:7946"},`
	body := func(on, pool string) string {
		return `{"api_token":"r","advertise_addr":"node1:7777",` + on + `"pools":[` + pool + `]}`
	}
	cfg, err := Load(writeConfig(t, body(failover, `{"template":"rt:24.04","replicate_seconds":60}`)))
	if err != nil || cfg.Pools[0].ReplicateSeconds != 60 {
		t.Fatalf("Load: %+v %v, want replicate_seconds 60", cfg, err)
	}
	for name, tt := range map[string]struct{ on, pool, want string }{
		"no failover":  {"", `{"template":"rt:24.04","replicate_seconds":60}`, "requires archive_failover"},
		"negative":     {failover, `{"template":"rt:24.04","replicate_seconds":-1}`, "must not be negative"},
		"egress lane":  {failover + `"bridges":["br0"],`, `{"template":"rt:24.04","net":"egress","replicate_seconds":60}`, "needs net"},
		"off is valid": {"", `{"template":"rt:24.04","replicate_seconds":0}`, ""},
	} {
		_, err := Load(writeConfig(t, body(tt.on, tt.pool)))
		if tt.want == "" && err != nil || tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)) {
			t.Errorf("%s: %v, want %q", name, err, tt.want)
		}
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

func TestLoadReadsReader(t *testing.T) {
	cfg, err := Load(writeConfig(t, `{"pools":[{"template":"rt:24.04","reader":true},{"template":"py:3.12"}]}`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Pools[0].Reader || cfg.Pools[1].Reader {
		t.Errorf("reader %v/%v, want on for the pool that names it and off by default", cfg.Pools[0].Reader, cfg.Pools[1].Reader)
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
	for mode, want := range map[string]egress.InterceptMode{"true": egress.InterceptAlways, `"inject"`: egress.InterceptInject} {
		path := writeConfig(t, `{"egress_ca":{"root_cert":"/x/root.crt","intermediate_cert":"/x/n.crt","intermediate_key":"/x/n.key"},
		"pools":[{"template":"rt:24.04","net":"none","size":"small","egress":{"allow":[{"host":"api.github.com","intercept":`+mode+`}]}}]}`)
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load intercept %s: %v", mode, err)
		}
		if pol := cfg.Pools[0].Egress; pol == nil || len(pol.Allow) != 1 || pol.Allow[0].Intercept != want {
			t.Errorf("intercept %s rule not parsed: %+v", mode, cfg.Pools[0].Egress)
		}
	}
}

func TestLoadRejectsInterceptWithoutCA(t *testing.T) {
	for _, mode := range []string{"true", `"inject"`} {
		path := writeConfig(t, `{"pools":[{"template":"rt:24.04","net":"none","size":"small","egress":{"allow":[{"host":"x","intercept":`+mode+`}]}}]}`)
		if _, err := Load(path); err == nil {
			t.Errorf("Load accepted an intercept %s pool without egress_ca; want rejection", mode)
		}
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

func TestValidateVolumeCatalogTenantNames(t *testing.T) {
	for _, tenant := range []string{"", "has space", "_leading", strings.Repeat("a", 64)} {
		spec := VolumeSpec{Name: "data", Path: "/images/data", DirectIO: types.DirectIOOff, Tenants: []string{tenant}}
		if err := ValidateVolumeCatalog([]VolumeSpec{spec}); err == nil {
			t.Errorf("invalid tenant %q accepted", tenant)
		}
	}
	spec := VolumeSpec{Name: "data", Path: "/images/data", DirectIO: types.DirectIOOff, Tenants: []string{"future-tenant", "team/project:v1"}}
	if err := ValidateVolumeCatalog([]VolumeSpec{spec}); err != nil {
		t.Fatalf("syntactically valid future tenant rejected: %v", err)
	}
}

func TestLoadPreservesRestrictedACLWithInvalidVolumeTenants(t *testing.T) {
	for _, tenants := range []string{`["acme", "has space", "", "beta"]`, `["has space"]`} {
		cfg, err := Load(writeConfig(t, `{"pools":[],"volumes":[{"name":"data","path":"/images/data","tenants":`+tenants+`}]}`))
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"has space"}
		if strings.Contains(tenants, "acme") {
			want = []string{"acme", "has space", "", "beta"}
		}
		if !slices.Equal(cfg.Volumes[0].Tenants, want) {
			t.Fatalf("tenants = %v, want %v", cfg.Volumes[0].Tenants, want)
		}
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
