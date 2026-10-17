package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestBelowFloor(t *testing.T) {
	tests := []struct {
		v     string
		below bool
		known bool
	}{
		{"v0.6.9", false, true},
		{"v0.6.10", false, true},
		{"v0.7.0", false, true},
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
			below, known := belowFloor(tt.v, RequiredCocoon)
			if below != tt.below || known != tt.known {
				t.Errorf("belowFloor(%q) = (%v, %v), want (%v, %v)", tt.v, below, known, tt.below, tt.known)
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
		{"v0.7.2-rc.1", false, false},
		{"v0.7.2", false, true},
		{"v0.8.0", false, true},
		{"master-abcdef", false, true},
		{"v0.7.2", true, false},
	} {
		t.Run(fmt.Sprintf("%s/fail=%v", tt.version, tt.fail), func(t *testing.T) {
			t.Setenv("COCOON_TEST_VERSION", tt.version)
			t.Setenv("COCOON_TEST_VERSION_FAIL", "")
			if tt.fail {
				t.Setenv("COCOON_TEST_VERSION_FAIL", "1")
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
			if _, ok := errors.AsType[*LiveVolumeUnsupported](err); !ok || !strings.Contains(err.Error(), LiveVolumeCocoon) || strings.Contains(err.Error(), "/private") {
				t.Fatalf("error %v", err)
			}
		})
	}
}

func TestVersionQueriesCocoonOnce(t *testing.T) {
	t.Setenv("COCOON_TEST_VERSION_FAIL", "")
	fakeCocoon(t, `#!/bin/sh
if [ -n "$COCOON_TEST_VERSION_FAIL" ]; then exit 1; fi
printf 'Version: v0.7.2\n'
`)
	eng := New("cocoon", nil, nil, false, false, "")
	if _, err := eng.Version(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COCOON_TEST_VERSION_FAIL", "1")
	if v, err := eng.Version(t.Context()); err != nil || v != "v0.7.2" {
		t.Fatalf("cached version %q, %v", v, err)
	}
}
