package types

import (
	"slices"
	"testing"
)

func TestValidVolumeName(t *testing.T) {
	for _, tt := range []struct {
		name  string
		valid bool
	}{
		{"dataset", true},
		{"a_b-0", true},
		{"abcdefghijklmnopqrst", true},
		{"", false},
		{"Dataset", false},
		{"1dataset", false},
		{"data/set", false},
		{"abcdefghijklmnopqrstu", false},
		{"cocoon-data", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidVolumeName(tt.name); got != tt.valid {
				t.Errorf("ValidVolumeName(%q) = %v, want %v", tt.name, got, tt.valid)
			}
		})
	}
}

func TestValidateVolumes(t *testing.T) {
	volumes := []Volume{{Name: "dataset"}, {Name: "weights-1", Mount: "/models"}}
	wantInput := slices.Clone(volumes)
	got, err := ValidateVolumes(volumes)
	if err != nil {
		t.Fatalf("ValidateVolumes: %v", err)
	}
	want := []Volume{
		{Name: "dataset", Mount: "/volumes/dataset"},
		{Name: "weights-1", Mount: "/models"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("volumes %v, want %v", got, want)
	}
	if !slices.Equal(volumes, wantInput) {
		t.Errorf("input mutated to %v, want %v", volumes, wantInput)
	}
	if got, err := ValidateVolumes([]Volume{
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
		{Name: "d"},
		{Name: "e"},
		{Name: "f"},
		{Name: "g"},
		{Name: "h"},
	}); err != nil || len(got) != MaxClaimVolumes {
		t.Errorf("ValidateVolumes at limit: got=%v err=%v", got, err)
	}

	for _, tt := range []struct {
		name    string
		volumes []Volume
	}{
		{"too-many", []Volume{{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "d"}, {Name: "e"}, {Name: "f"}, {Name: "g"}, {Name: "h"}, {Name: "i"}}},
		{"invalid-name", []Volume{{Name: "bad/name"}}},
		{"duplicate-name", []Volume{{Name: "dataset"}, {Name: "dataset", Mount: "/other"}}},
		{"relative-mount", []Volume{{Name: "dataset", Mount: "data"}}},
		{"unclean-mount", []Volume{{Name: "dataset", Mount: "/data/../dataset"}}},
		{"doubled-separator", []Volume{{Name: "dataset", Mount: "/data//dataset"}}},
		{"duplicate-mount", []Volume{{Name: "a", Mount: "/data"}, {Name: "b", Mount: "/data"}}},
		{"nested-mount", []Volume{{Name: "a", Mount: "/data"}, {Name: "b", Mount: "/data/child"}}},
		{"parent-after-child", []Volume{{Name: "a", Mount: "/data/child"}, {Name: "b", Mount: "/data"}}},
		{"unknown-mode", []Volume{{Name: "dataset", Mode: "readwrite"}}},
		{"uppercase-mode", []Volume{{Name: "dataset", Mode: "RW"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ValidateVolumes(tt.volumes); err == nil {
				t.Fatal("ValidateVolumes succeeded")
			}
		})
	}
}

func TestValidateVolumesNormalizesMode(t *testing.T) {
	got, err := ValidateVolumes([]Volume{
		{Name: "shared"},
		{Name: "explicit", Mode: VolumeModeRO},
		{Name: "writable", Mode: VolumeModeRW},
	})
	if err != nil {
		t.Fatalf("ValidateVolumes: %v", err)
	}
	want := []Volume{
		{Name: "shared", Mount: "/volumes/shared"},
		{Name: "explicit", Mount: "/volumes/explicit"},
		{Name: "writable", Mount: "/volumes/writable", Mode: VolumeModeRW},
	}
	if !slices.Equal(got, want) {
		t.Errorf("volumes %v, want %v", got, want)
	}
	if got[0].RW() || got[1].RW() || !got[2].RW() {
		t.Errorf("RW predicate disagrees with modes %v", got)
	}
	if names := VolumeRWNames(got); !slices.Equal(names, []string{"writable"}) {
		t.Errorf("rw names %v, want [writable]", names)
	}
	if names := VolumeRWNames(got[:2]); names != nil {
		t.Errorf("rw names %v, want nil", names)
	}
}

func TestValidateVolumesAttachOnly(t *testing.T) {
	got, err := ValidateVolumes([]Volume{
		{Name: "dataset", AttachOnly: true},
		{Name: "scratch", Mode: VolumeModeRW, AttachOnly: true},
		{Name: "explicit", Mode: VolumeModeRO, AttachOnly: true},
	})
	if err != nil {
		t.Fatalf("ValidateVolumes: %v", err)
	}
	want := []Volume{
		{Name: "dataset", AttachOnly: true},
		{Name: "scratch", Mode: VolumeModeRW, AttachOnly: true},
		{Name: "explicit", AttachOnly: true},
	}
	if !slices.Equal(got, want) {
		t.Errorf("volumes %v, want %v", got, want)
	}

	for _, tt := range []struct {
		name    string
		volumes []Volume
	}{
		{"default mount spelled out", []Volume{{Name: "dataset", Mount: "/volumes/dataset"}}},
		{"custom mount", []Volume{{Name: "dataset", Mount: "/datasets"}}},
		{"one entry of many", []Volume{{Name: "dataset"}, {Name: "scratch", Mount: "/scratch"}}},
		{"duplicate name", []Volume{{Name: "dataset"}, {Name: "dataset"}}},
		{"invalid name", []Volume{{Name: "bad/name"}}},
		{"unknown mode", []Volume{{Name: "dataset", Mode: "readwrite"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for i := range tt.volumes {
				tt.volumes[i].AttachOnly = true
			}
			if _, err := ValidateVolumes(tt.volumes); err == nil {
				t.Fatal("ValidateVolumes succeeded")
			}
		})
	}
}

func TestValidateVolumesRejectsGuestOSMounts(t *testing.T) {
	for _, root := range append([]string{"/"}, guestOSMountRoots...) {
		t.Run(root, func(t *testing.T) {
			if _, err := ValidateVolumes([]Volume{{Name: "dataset", Mount: root}}); err == nil {
				t.Fatalf("accepted OS mount %q", root)
			}
			if root != "/" {
				if _, err := ValidateVolumes([]Volume{{Name: "dataset", Mount: root + "/child"}}); err == nil {
					t.Fatalf("accepted mount under OS root %q", root)
				}
			}
		})
	}
	for _, mount := range []string{"/data", "/home/dataset", "/opt-data", "/usrdata"} {
		if _, err := ValidateVolumes([]Volume{{Name: "dataset", Mount: mount}}); err != nil {
			t.Errorf("rejected allowed mount %q: %v", mount, err)
		}
	}
}

func TestValidateMixedVolumesPreservesPerEntryIntent(t *testing.T) {
	request := []Volume{{Name: "mounted"}, {Name: "writer", Mode: VolumeModeRW}, {Name: "raw", AttachOnly: true}, {Name: "raw-writer", Mode: VolumeModeRW, AttachOnly: true}}
	got, err := ValidateVolumes(request)
	if err != nil {
		t.Fatal(err)
	}
	want := []Volume{{Name: "mounted", Mount: "/volumes/mounted"}, {Name: "writer", Mount: "/volumes/writer", Mode: VolumeModeRW}, {Name: "raw", AttachOnly: true}, {Name: "raw-writer", Mode: VolumeModeRW, AttachOnly: true}}
	if !slices.Equal(got, want) {
		t.Fatalf("mixed intent changed: got %+v, want %+v", got, want)
	}
	again, err := ValidateVolumes(got)
	if err != nil || !slices.Equal(again, want) {
		t.Fatalf("second validation changed mixed intent: %+v, %v", again, err)
	}
	if _, err = ValidateVolumes([]Volume{{Name: "raw", AttachOnly: true, Mount: "/data"}}); err == nil {
		t.Fatal("accepted attach-only with a mount")
	}
}
