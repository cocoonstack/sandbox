package engine

import (
	"slices"
	"strconv"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestCloneArgsRestoreMode(t *testing.T) {
	noneKey := types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeMedium}
	egressKey := types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeMedium}
	cases := []struct {
		name string
		mode types.RestoreMode
		key  types.PoolKey
		want bool
	}{
		{"none lane carries the flag", types.RestoreMmap, noneKey, true},
		{"egress lane carries the flag", types.RestoreMmap, egressKey, true},
		{"unset mode adds nothing", "", noneKey, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := New("cocoon", []string{"br0"}, nil, false, tc.mode)
			for _, args := range [][]string{
				e.cloneArgs("/goldens/g1", "sbx-1", tc.key),
				e.cloneSnapArgs("ck_1", "sbx-1", tc.key),
				e.restoreCmdArgs("sbx-1", "sbx-hib-1"),
			} {
				i := slices.Index(args, "--restore-mode")
				if got := i >= 0; got != tc.want {
					t.Fatalf("args %v: restore-mode present=%v, want %v", args, got, tc.want)
				}
				if tc.want && args[i+1] != string(tc.mode) {
					t.Fatalf("args %v: mode %q, want %q", args, args[i+1], tc.mode)
				}
			}
		})
	}
}

func TestLifecycleArgsApplyDirectIOPolicy(t *testing.T) {
	key := types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeMedium}
	for _, noDirectIO := range []bool{false, true} {
		t.Run(strconv.FormatBool(noDirectIO), func(t *testing.T) {
			e := New("cocoon", nil, nil, noDirectIO, "")
			want := "--no-direct-io=" + strconv.FormatBool(noDirectIO)
			cold := e.runColdArgs("sbx-1", key)
			for _, args := range [][]string{
				cold,
				e.cloneArgs("/goldens/g1", "sbx-1", key),
				e.cloneSnapArgs("ck_1", "sbx-1", key),
			} {
				if !slices.Contains(args, want) {
					t.Errorf("args %v missing %q", args, want)
				}
			}
			if !slices.Contains(cold, "--nics") || !slices.Contains(cold, "0") {
				t.Errorf("none-lane cold args %v missing --nics 0", cold)
			}
		})
	}
}

func TestEgressVMsSpreadOverEveryConfiguredShard(t *testing.T) {
	shards := []string{"sbx0", "sbx1", "sbx2", "sbx3"}
	key := types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeMedium}
	for name, tc := range map[string]struct {
		e    *Engine
		flag string
	}{
		"networks": {New("cocoon", nil, shards, false, ""), "--network"},
		"bridges":  {New("cocoon", shards, nil, false, ""), "--bridge"},
	} {
		t.Run(name, func(t *testing.T) {
			counts := map[string]int{}
			for i := range 4000 {
				args := tc.e.cloneArgs("/goldens/g1", "sbx-pool1-"+strconv.Itoa(i), key)
				j := slices.Index(args, tc.flag)
				if j < 0 {
					t.Fatalf("args %v carry no %s", args, tc.flag)
				}
				counts[args[j+1]]++
			}
			for _, s := range shards {
				if counts[s] < 4000/len(shards)/2 {
					t.Errorf("shard %s got %d of 4000, want a fair share: %v", s, counts[s], counts)
				}
			}
		})
	}
}

func TestNetworkChoiceIsStableForAName(t *testing.T) {
	shards := []string{"a", "b", "c"}
	first := shardOf(shards, "sbx-pool1-42")
	for range 100 {
		if got := shardOf(shards, "sbx-pool1-42"); got != first {
			t.Fatalf("shardOf returned %q then %q for one name", first, got)
		}
	}
}

func TestNetArgsHonorsTheLaneAndTheAttachment(t *testing.T) {
	none := types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeMedium}
	egress := types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeMedium}

	if args := New("cocoon", nil, []string{"cni"}, false, "").netArgs("sbx-1", none, false); len(args) != 0 {
		t.Errorf("none lane took an attachment: %v", args)
	}
	if args := New("cocoon", []string{"br0"}, nil, false, "").netArgs("sbx-1", egress, false); !slices.Equal(args, []string{"--bridge", "br0"}) {
		t.Errorf("bridge lane args = %v", args)
	}
	if args := New("cocoon", nil, []string{"cocoon-dhcp"}, false, "").netArgs("sbx-1", egress, false); !slices.Equal(args, []string{"--network", "cocoon-dhcp"}) {
		t.Errorf("single-network args = %v", args)
	}
}
