package config

import (
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestReloadDiffSortsReloadableOwnedAndRestartFields(t *testing.T) {
	key := types.PoolKey{Template: "desktop:v12", Net: types.NetNone, Size: types.SizeSmall}
	v13 := types.PoolKey{Template: "desktop:v13", Net: types.NetNone, Size: types.SizeSmall}
	cur := &Config{
		Listen: "127.0.0.1:7777",
		Pools:  []PoolSpec{{PoolKey: key, Warm: 2}},
	}
	next := *cur
	next.EgressInternalAllow = []string{"10.8.0.0/16"}
	next.Pools = []PoolSpec{
		{PoolKey: key, Warm: 5, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "pypi.org"}}}},
		{PoolKey: v13, Warmup: []string{"node", "-e", "0"}, Storage: "40G", Reader: true},
	}
	next.EgressClasses = []EgressClass{{Name: "desk", Egress: &egress.Policy{}}}
	next.EgressConnRate = &EgressConnRate{PerSecond: 30, Burst: 600}
	next.DiskReserve = &DiskReserve{Free: "100G"}
	next.MemoryLimit = &MemoryLimit{Anon: "300G"}
	next.Balloon = &Balloon{IdleSeconds: 300}
	next.MaxClaims, next.MaxRunning, next.MaxForkCount, next.MoveConcurrency = 200, 12, 4, 3
	changed, ignored, err := cur.ReloadDiff(&next)
	if err != nil {
		t.Fatalf("ReloadDiff: %v", err)
	}
	wantChanged := []string{
		"egress_classes",
		"egress_internal_allow",
		"egress_conn_rate",
		"max_claims",
		"max_running",
		"move_concurrency",
		"max_fork_count",
		"disk_reserve",
		"memory_limit",
		"balloon",
		"pools[desktop:v12 none small].egress",
		"pools[desktop:v13 none small].warmup",
		"pools[desktop:v13 none small].storage",
		"pools[desktop:v13 none small].reader",
	}
	if !slices.Equal(changed, wantChanged) {
		t.Errorf("changed %v, want %v", changed, wantChanged)
	}
	wantIgnored := []string{
		"pools[desktop:v12 none small] targets (PUT /v1/pools owns them)",
		"pools[desktop:v13 none small] targets (PUT /v1/pools owns them)",
	}
	if !slices.Equal(ignored, wantIgnored) {
		t.Errorf("ignored %v, want %v", ignored, wantIgnored)
	}

	replicating := *cur
	replicating.Pools = []PoolSpec{{PoolKey: key, Warm: 2, ReplicateSeconds: 60}}
	if _, _, err := cur.ReloadDiff(&replicating); err == nil || err.Error() != "pools[desktop:v12 none small].replicate_seconds change only at a restart" {
		t.Errorf("ReloadDiff over replicate_seconds: %v, want a restart refusal", err)
	}

	restart := *cur
	restart.Listen, restart.Bridges = "127.0.0.1:8888", []string{"br1"}
	if _, _, err := cur.ReloadDiff(&restart); err == nil || err.Error() != "listen, bridges change only at a restart" {
		t.Errorf("ReloadDiff over restart fields: %v", err)
	}
	if changed, ignored, err := cur.ReloadDiff(cur); err != nil || changed != nil || ignored != nil {
		t.Errorf("an identical config: %v %v %v, want nothing", changed, ignored, err)
	}
}

func TestReloadDiffReloadsTheEgressUsageInterval(t *testing.T) {
	cur := &Config{EgressUsageBytes: true}
	for _, tt := range []struct {
		name     string
		interval int
		want     []string
	}{
		{"interval set", 30, []string{"egress_usage_interval_seconds"}},
		{"interval unchanged", 0, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			next := *cur
			next.EgressUsageIntervalSeconds = tt.interval
			changed, ignored, err := cur.ReloadDiff(&next)
			if err != nil || ignored != nil || !slices.Equal(changed, tt.want) {
				t.Errorf("ReloadDiff: changed %v ignored %v err %v, want changed %v", changed, ignored, err, tt.want)
			}
		})
	}
}

func TestReloadDiffReloadsTheExitsAndRefusesASubnetChange(t *testing.T) {
	key := types.PoolKey{Template: "desktop:v12", Net: types.NetNone, Size: types.SizeSmall}
	tun := TunConfig{Subnet: "240.0.0.0/4", Listen: "10.0.0.5:4789", VNI: 1, Port: 4789, MTU: 1450, Senders: 4, Exits: map[string]TunExit{"default": {VTEP: "10.0.0.9", MAC: "02:aa:00:00:00:01"}}}
	cur := &Config{Tun: &tun, Pools: []PoolSpec{{PoolKey: key}}}

	exits := tun
	exits.MTU = 1410
	exits.Exits = map[string]TunExit{"default": {VTEP: "10.0.0.10", MAC: "02:aa:00:00:00:01"}}
	next := *cur
	next.Tun, next.Pools = &exits, []PoolSpec{{PoolKey: key, Tun: true}}
	changed, _, err := cur.ReloadDiff(&next)
	if want := []string{"tun.mtu", "tun.exits", "pools[desktop:v12 none small].tun"}; err != nil || !slices.Equal(changed, want) {
		t.Errorf("ReloadDiff over exits and the pool switch: %v %v, want %v", changed, err, want)
	}

	for _, tt := range []struct {
		edit func(*TunConfig)
		want string
	}{
		{func(c *TunConfig) { c.Subnet = "10.0.0.0/8" }, "tun.subnet change only at a restart"},
		{func(c *TunConfig) { c.Listen = "10.0.0.6:4789" }, "tun.listen change only at a restart"},
		{func(c *TunConfig) { c.VNI = 2 }, "tun.vni change only at a restart"},
	} {
		moved := tun
		tt.edit(&moved)
		edited := *cur
		edited.Tun = &moved
		if _, _, err := cur.ReloadDiff(&edited); err == nil || err.Error() != tt.want {
			t.Errorf("ReloadDiff: %v, want %q", err, tt.want)
		}
	}
	dropped := *cur
	dropped.Tun = nil
	if _, _, err := cur.ReloadDiff(&dropped); err == nil || err.Error() != "tun change only at a restart" {
		t.Errorf("ReloadDiff dropping the block: %v, want a restart refusal", err)
	}
}
