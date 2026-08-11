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
	} {
		t.Run(tt.name, func(t *testing.T) {
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
