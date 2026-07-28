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
			e := New("cocoon", "br0", nil, false, tc.mode)
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
			e := New("cocoon", "", nil, noDirectIO, "")
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

// TestEgressVMsSpreadOverEveryConfiguredNetwork pins the point of the list:
// a shard the hash never picks is bridge capacity that does not exist.
func TestEgressVMsSpreadOverEveryConfiguredNetwork(t *testing.T) {
	nets := []string{"cocoon-sbx0", "cocoon-sbx1", "cocoon-sbx2", "cocoon-sbx3"}
	e := New("cocoon", "", nets, false, "")
	key := types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeMedium}

	counts := map[string]int{}
	for i := range 4000 {
		args := e.cloneArgs("/goldens/g1", "sbx-pool1-"+strconv.Itoa(i), key)
		j := slices.Index(args, "--network")
		if j < 0 {
			t.Fatalf("args %v carry no --network", args)
		}
		counts[args[j+1]]++
	}
	for _, n := range nets {
		// The bound catches a starved shard, not non-uniformity.
		if counts[n] < 4000/len(nets)/2 {
			t.Errorf("shard %s got %d of 4000, want a fair share: %v", n, counts[n], counts)
		}
	}
}

// TestNetworkChoiceIsStableForAName guards restore and teardown: the record
// persists the network a VM was built on, so the choice must not move.
func TestNetworkChoiceIsStableForAName(t *testing.T) {
	e := New("cocoon", "", []string{"a", "b", "c"}, false, "")
	first := e.networkFor("sbx-pool1-42")
	for range 100 {
		if got := e.networkFor("sbx-pool1-42"); got != first {
			t.Fatalf("networkFor returned %q then %q for one name", first, got)
		}
	}
}

// TestNetArgsHonorsTheLaneAndTheAttachment keeps the three attachment shapes
// distinct: no NIC on the none lane, a device on the bridge lane, and a
// single-entry list byte-for-byte the old scalar.
func TestNetArgsHonorsTheLaneAndTheAttachment(t *testing.T) {
	none := types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeMedium}
	egress := types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeMedium}

	if args := New("cocoon", "", []string{"cni"}, false, "").netArgs("sbx-1", none, false); len(args) != 0 {
		t.Errorf("none lane took an attachment: %v", args)
	}
	if args := New("cocoon", "br0", nil, false, "").netArgs("sbx-1", egress, false); !slices.Equal(args, []string{"--bridge", "br0"}) {
		t.Errorf("bridge lane args = %v", args)
	}
	if args := New("cocoon", "", []string{"cocoon-dhcp"}, false, "").netArgs("sbx-1", egress, false); !slices.Equal(args, []string{"--network", "cocoon-dhcp"}) {
		t.Errorf("single-network args = %v", args)
	}
}
