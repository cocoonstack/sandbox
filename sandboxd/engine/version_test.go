package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestBelowFloor(t *testing.T) {
	tests := []struct {
		v          string
		below      bool
		comparable bool
	}{
		{"v0.6.9", true, true},
		{"v0.6.10", true, true},
		{"v0.7.0", true, true},
		{"v0.6.8", true, true},
		{"v0.5.2", true, true},
		{"v0.4.9", true, true},
		{"v1.0.0", false, true},
		{"v0.10.0", false, true},
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

func TestCheckLiveVolumeSupport(t *testing.T) {
	for _, tt := range []struct {
		version       string
		fail, allowed bool
	}{
		{"v0.7.1", false, false},
		{"v0.7.2", false, true},
		{"v0.8.0", false, true},
		{"v1.0.0", false, true},
		{"v0.7.2-rc.1", false, false},
		{"master-abcdef", false, false},
		{"unknown", false, false},
		{"", false, false},
		{"v0.7.2", true, false},
	} {
		t.Run(fmt.Sprintf("%s/fail=%v", tt.version, tt.fail), func(t *testing.T) {
			t.Setenv("COCOON_TEST_VERSION", tt.version)
			if tt.fail {
				t.Setenv("COCOON_TEST_VERSION_FAIL", "1")
			} else {
				t.Setenv("COCOON_TEST_VERSION_FAIL", "")
			}
			fakeCocoon(t, `#!/bin/sh
if [ "$#" != 1 ] || [ "$1" != version ]; then exit 2; fi
if [ -n "$COCOON_TEST_VERSION_FAIL" ]; then echo /private/cocoon >&2; exit 1; fi
printf 'Version: %s\n' "$COCOON_TEST_VERSION"
`)
			err := New("cocoon", nil, nil, false, false, "").CheckLiveVolumeSupport(t.Context())
			if tt.allowed {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var unsupported *LiveVolumeUnsupported
			if !errors.As(err, &unsupported) || !strings.Contains(err.Error(), RequiredCocoon) || strings.Contains(err.Error(), "/private") {
				t.Fatalf("error %v", err)
			}
		})
	}
}
