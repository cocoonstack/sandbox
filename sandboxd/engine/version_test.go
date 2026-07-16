package engine

import "testing"

func TestBelowFloor(t *testing.T) {
	tests := []struct {
		v          string
		below      bool
		comparable bool
	}{
		{"v0.5.2", false, true},
		{"v0.5.3", false, true},
		{"v0.6.0", false, true},
		{"v0.5.1", true, true},
		{"v0.5.0", true, true},
		{"v0.4.9", true, true},
		{"v1.0.0", false, true},
		{"master-82e5902", false, false},
		{"unknown", false, false},
		{"v0.5", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.v, func(t *testing.T) {
			below, comparable := belowFloor(tt.v)
			if below != tt.below || comparable != tt.comparable {
				t.Errorf("belowFloor(%q) = (%v, %v), want (%v, %v)", tt.v, below, comparable, tt.below, tt.comparable)
			}
		})
	}
}
