package engine

import (
	"errors"
	"fmt"
	"testing"
)

func TestOnlyNodeWideFailuresCarryACapacitySignature(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want bool
	}{
		{"bridge port exhaustion", errors.New(`failed to connect "veth1" to bridge cni0: exchange full`), true},
		{"disk exhaustion", errors.New("write overlay: no space left on device"), true},
		{"one VM failed to boot", errors.New("cocoon vm clone: exit status 1: Error: guest did not respond"), false},
		{"missing golden", errors.New("cocoon vm clone: exit status 1: Error: snapshot not found"), false},
		{"guest command out of disk", fmt.Errorf("warmup: %w", &guestExitError{code: 1, output: "dd: no space left on device"}), false},
		{"no error", nil, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := CapacitySignature(tt.err) != ""; got != tt.want {
				t.Errorf("CapacitySignature(%v) carries a signature: %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
