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
			e := New("cocoon", "br0", "", false, tc.mode)
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
			e := New("cocoon", "", "", noDirectIO, "")
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
