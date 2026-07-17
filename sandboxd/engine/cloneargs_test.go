package engine

import (
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestCloneArgsRestoreMode(t *testing.T) {
	chKey := types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeMedium}
	fcKey := types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeMedium}
	cases := []struct {
		name string
		mode types.RestoreMode
		key  types.PoolKey
		want bool
	}{
		{"ch lane carries the flag", types.RestoreMmap, chKey, true},
		{"fc lane never carries it", types.RestoreMmap, fcKey, false},
		{"unset mode adds nothing", "", chKey, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := New("cocoon", "br0", "", tc.mode)
			for _, args := range [][]string{
				e.cloneArgs("/goldens/g1", "sbx-1", tc.key),
				e.cloneSnapArgs("ck_1", "sbx-1", tc.key),
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
