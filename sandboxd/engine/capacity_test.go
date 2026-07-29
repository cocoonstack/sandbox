package engine

import (
	"errors"
	"testing"
)

// TestOnlyNodeWideFailuresCarryACapacitySignature guards the classifier.
// Parking on an ordinary clone failure would stall a healthy node over one bad VM.
func TestOnlyNodeWideFailuresCarryACapacitySignature(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"bridge port exhaustion", errors.New(`failed to connect "veth1" to bridge cni0: exchange full`), true},
		{"disk exhaustion", errors.New("write overlay: no space left on device"), true},
		{"one VM failed to boot", errors.New("cocoon vm clone: exit status 1: Error: guest did not respond"), false},
		{"missing golden", errors.New("cocoon vm clone: exit status 1: Error: snapshot not found"), false},
		{"no error", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := CapacitySignature(tc.err) != ""; got != tc.want {
				t.Errorf("CapacitySignature(%v) carries a signature: %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
