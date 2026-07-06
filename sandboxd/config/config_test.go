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
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.body))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Load: %v, want error containing %q", err, tt.want)
			}
		})
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
