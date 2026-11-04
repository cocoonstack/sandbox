package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestBelowFloorComparesReleasesAndLeavesOthersUnknown(t *testing.T) {
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

func TestFeatureSupportFollowsTheCocoonVersion(t *testing.T) {
	liveVolume, migrate := (*Engine).CheckLiveVolumeSupport, (*Engine).CheckMigrateSupport
	for _, tt := range []struct {
		check         func(*Engine, context.Context) error
		floor         string
		version       string
		fail, allowed bool
	}{
		{liveVolume, LiveVolumeCocoon, "v0.7.1", false, false},
		{liveVolume, LiveVolumeCocoon, "v0.7.2-rc.1", false, false},
		{liveVolume, LiveVolumeCocoon, "v0.7.2", false, true},
		{liveVolume, LiveVolumeCocoon, "v0.8.0", false, true},
		{liveVolume, LiveVolumeCocoon, "master-abcdef", false, true},
		{liveVolume, LiveVolumeCocoon, "v0.7.2", true, false},
		{migrate, MigrateCocoon, "v0.7.6", false, false},
		{migrate, MigrateCocoon, "v0.7.7", false, true},
		{migrate, MigrateCocoon, "master-ba2020c", false, true},
		{migrate, MigrateCocoon, "v0.7.7", true, false},
	} {
		t.Run(fmt.Sprintf("floor=%s/%s/fail=%v", tt.floor, tt.version, tt.fail), func(t *testing.T) {
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
			err := tt.check(newTestEngine(), t.Context())
			if tt.allowed {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			_, isVolume := errors.AsType[*LiveVolumeUnsupported](err)
			_, isMigrate := errors.AsType[*MigrateUnsupported](err)
			if !isVolume && !isMigrate || !strings.Contains(err.Error(), tt.floor) || strings.Contains(err.Error(), "/private") {
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
	eng := newTestEngine()
	if _, err := eng.Version(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COCOON_TEST_VERSION_FAIL", "1")
	if v, err := eng.Version(t.Context()); err != nil || v != "v0.7.2" {
		t.Fatalf("cached version %q, %v", v, err)
	}
}
