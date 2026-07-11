package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	if cfg.Pools[0].Warm < 1 {
		t.Errorf("pool warm default missing: %d", cfg.Pools[0].Warm)
	}
}

func TestLoadRejectsInvalid(t *testing.T) {
	for _, tt := range []struct {
		name, body, want string
	}{
		{"bad json", `{`, "config"},
		{"bridge and network", `{"bridge":"br0","network":"cni","pools":[]}`, "mutually exclusive"},
		{"bad fork count", `{"max_fork_count":-1,"pools":[]}`, "max_fork_count"},
		{"bad pool key", `{"pools":[{"template":"","net":"none","size":"small"}]}`, "pool"},
		{"egress without attachment", `{"pools":[{"template":"rt:24.04","net":"egress","size":"small"}]}`, "egress lane needs"},
		{"negative warm", `{"pools":[{"template":"rt:24.04","net":"none","size":"small","warm":-2}]}`, "negative"},
		{"tenants without api_token", `{"pools":[],"tenants":[{"name":"acme","token":"t1"}]}`, "require api_token"},
		{"empty tenant name", `{"api_token":"root","pools":[],"tenants":[{"name":"","token":"t1"}]}`, "tenant name"},
		{"bad tenant name", `{"api_token":"root","pools":[],"tenants":[{"name":"_bad","token":"t1"}]}`, "tenant name"},
		{"duplicate tenant name", `{"api_token":"root","pools":[],"tenants":[{"name":"acme","token":"t1"},{"name":"acme","token":"t2"}]}`, "duplicate tenant"},
		{"empty tenant token", `{"api_token":"root","pools":[],"tenants":[{"name":"acme","token":""}]}`, "needs a token"},
		{"tenant token equals api token", `{"api_token":"root","pools":[],"tenants":[{"name":"acme","token":"root"}]}`, "differ from api_token"},
		{"duplicate tenant token", `{"api_token":"root","pools":[],"tenants":[{"name":"acme","token":"t1"},{"name":"beta","token":"t1"}]}`, "token reused"},
		{"negative tenant max_claims", `{"api_token":"root","pools":[],"tenants":[{"name":"acme","token":"t1","max_claims":-1}]}`, "max_claims"},
		{"secret without header", `{"secrets":[{"name":"gh"}],"pools":[]}`, "not a valid header name"},
		{"secret bad header", `{"secrets":[{"name":"gh","header":"Bad Header:","value_env":"GH_TOKEN"}],"pools":[]}`, "not a valid header name"},
		{"secret without value_env", `{"secrets":[{"name":"gh","header":"Authorization"}],"pools":[]}`, "needs value_env"},
		{"secret with inline value", `{"secrets":[{"name":"gh","header":"Authorization","value":"tok"}],"pools":[]}`, "value is not supported"},
		{"secret with value and value_env", `{"secrets":[{"name":"gh","header":"Authorization","value":"tok","value_env":"GH_TOKEN"}],"pools":[]}`, "value is not supported"},
		{"secret hop-by-hop header", `{"secrets":[{"name":"gh","header":"Connection","value_env":"GH_TOKEN"}],"pools":[]}`, "not injectable"},
		{"egress methods typo", `{"pools":[{"template":"rt:24.04","net":"none","size":"small","egress":{"allow":[{"host":"x","method":["GET"]}]}}]}`, "unknown field"},
		{"egress key typo", `{"pools":[{"template":"rt:24.04","net":"none","size":"small","egres":{"allow":[{"host":"x"}]}}]}`, "unknown field"},
		{"duplicate methods key", `{"pools":[{"template":"rt:24.04","net":"none","size":"small","egress":{"allow":[{"host":"x","methods":["GET"],"methods":[]}]}}]}`, "duplicate key"},
		{"trailing data", `{"pools":[]} {"api_token":"x"}`, "trailing data"},
		{"duplicate secret name", `{"secrets":[{"name":"gh","header":"A","value_env":"X"},{"name":"gh","header":"B","value_env":"Y"}],"pools":[]}`, "duplicate secret"},
		{"pool egress empty host", `{"pools":[{"template":"rt:24.04","net":"none","size":"small","egress":{"allow":[{"host":""}]}}]}`, "must not be empty"},
		{"pool egress unknown secret", `{"pools":[{"template":"rt:24.04","net":"none","size":"small","egress":{"allow":[{"host":"api.github.com","secret":"gh"}]}}]}`, "unknown secret"},
		{"tenant egress unknown secret", `{"api_token":"root","pools":[],"tenants":[{"name":"acme","token":"t1","egress":{"allow":[{"host":"x","secret":"gh"}]}}]}`, "unknown secret"},
		{"guarded egress on cni pool", `{"network":"cni","pools":[{"template":"rt:24.04","net":"egress","size":"small","egress":{"allow":[{"host":"x"}]}}]}`, "needs a bridge lane"},
		{"guarded egress on cni tenant", `{"api_token":"root","network":"cni","pools":[{"template":"rt:24.04","net":"egress","size":"small"}],"tenants":[{"name":"acme","token":"t1","egress":{"allow":[{"host":"x"}]}}]}`, "needs a bridge lane"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.body))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Load: %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestLoadAcceptsTenants(t *testing.T) {
	path := writeConfig(t, `{"api_token":"root","pools":[],
		"tenants":[{"name":"acme","token":"t1","max_claims":50},{"name":"beta","token":"t2"}]}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Tenants) != 2 || cfg.Tenants[0].Name != "acme" || cfg.Tenants[0].MaxClaims != 50 {
		t.Errorf("tenants %+v", cfg.Tenants)
	}
	if cfg.Tenants[1].MaxClaims != 0 {
		t.Errorf("beta max_claims %d, want 0 (unlimited)", cfg.Tenants[1].MaxClaims)
	}
}

func TestLoadAcceptsEgressPolicy(t *testing.T) {
	path := writeConfig(t, `{"secrets":[{"name":"gh","header":"Authorization","value_env":"GH_TOKEN"}],
		"pools":[{"template":"rt:24.04","net":"none","size":"small",
			"egress":{"allow":[{"host":"api.github.com","methods":["GET"],"secret":"gh"},{"host":"*.googleapis.com"}]}}]}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	pol := cfg.Pools[0].Egress
	if pol == nil || len(pol.Allow) != 2 || pol.Allow[0].Secret != "gh" {
		t.Errorf("egress policy %+v", pol)
	}
	if len(cfg.Secrets) != 1 || cfg.Secrets[0].Name != "gh" {
		t.Errorf("secrets %+v", cfg.Secrets)
	}
}

func TestLoadAcceptsUnguardedCNINetwork(t *testing.T) {
	// A CNI network lane is fine without an egress policy: guarded egress (the
	// root-netns nft lock) is what a VM-netns tap can't support, not the lane.
	path := writeConfig(t, `{"network":"cni","pools":[{"template":"rt:24.04","net":"egress","size":"small"}]}`)
	if _, err := Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
}

func TestHasEgress(t *testing.T) {
	if (&Config{}).HasEgress() {
		t.Error("no attachment must mean no egress")
	}
	if !(&Config{Bridge: "br0"}).HasEgress() || !(&Config{Network: "cni"}).HasEgress() {
		t.Error("bridge or network must enable egress")
	}
}

func TestLoadKeepsExplicitValues(t *testing.T) {
	path := writeConfig(t, `{"listen":"0.0.0.0:9999","advertise_addr":"10.0.0.5:9999","max_fork_count":4,
		"bridge":"br0","pools":[{"template":"rt:24.04","net":"egress","size":"small","warm":3}]}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AdvertiseAddr != "10.0.0.5:9999" || cfg.MaxForkCount != 4 || cfg.Pools[0].Warm != 3 {
		t.Errorf("explicit values overridden: %+v", cfg)
	}
	if cfg.Pools[0].Net != types.NetEgress {
		t.Errorf("pool net %q", cfg.Pools[0].Net)
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
