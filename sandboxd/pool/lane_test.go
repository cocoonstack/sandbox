package pool

import (
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestGoldenBuildMarksLockedNICOnBridgeLane(t *testing.T) {
	tests := []struct {
		name string
		key  types.PoolKey
		want int
	}{
		{"egress lane", egKey, 1},
		{"none lane", testKey, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := egressManager(t, eng, config.PoolSpec{PoolKey: tt.key, Warm: 1, Egress: egPolicy})
			final := filepath.Join(m.goldensDir(), tt.key.Hash())
			if err := m.buildGoldenSteps(t.Context(), tt.key, "sbx-gb", "snap", final); err != nil {
				t.Fatalf("buildGoldenSteps: %v", err)
			}
			if got := len(eng.nicMarks); got != tt.want {
				t.Errorf("MarkNICLocked calls = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestColdProvisionMarksLockedNICOnBridgeLane(t *testing.T) {
	tests := []struct {
		name string
		key  types.PoolKey
		want int
	}{
		{"egress lane", egKey, 1},
		{"none lane", testKey, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := egressManager(t, eng, config.PoolSpec{PoolKey: tt.key, Warm: 1, Egress: egPolicy})
			sb, err := m.provision(t.Context(), tt.key, "")
			if err != nil {
				t.Fatalf("cold provision: %v", err)
			}
			m.destroy(t.Context(), sb.VMName)
			if got := len(eng.nicMarks); got != tt.want {
				t.Errorf("MarkNICLocked calls = %d, want %d", got, tt.want)
			}
		})
	}
}
