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
		{PoolKey: v13, Warmup: []string{"node", "-e", "0"}, Storage: "40G"},
	}
	next.EgressClasses = []EgressClass{{Name: "desk", Egress: &egress.Policy{}}}
	changed, ignored, err := cur.ReloadDiff(&next)
	if err != nil {
		t.Fatalf("ReloadDiff: %v", err)
	}
	wantChanged := []string{
		"egress_classes",
		"egress_internal_allow",
		"pools[desktop:v12 none small].egress",
		"pools[desktop:v13 none small].warmup",
		"pools[desktop:v13 none small].storage",
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

	restart := *cur
	restart.Listen, restart.Bridges, restart.EgressUsageBytes = "127.0.0.1:8888", []string{"br1"}, true
	if _, _, err := cur.ReloadDiff(&restart); err == nil || err.Error() != "listen, bridges change only at a restart" {
		t.Errorf("ReloadDiff over restart fields: %v", err)
	}
	if changed, ignored, err := cur.ReloadDiff(cur); err != nil || changed != nil || ignored != nil {
		t.Errorf("an identical config: %v %v %v, want nothing", changed, ignored, err)
	}
}
