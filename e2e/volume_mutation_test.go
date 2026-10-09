package e2e

import (
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func TestLiveVolumesEndToEnd(t *testing.T) {
	image := writeVolumeImage(t, "dataset.img", "dataset-bytes")
	stack := startTenantStack(t, "node-token", nil, []config.VolumeSpec{{Name: "dataset", Path: image}})
	sb, err := stack.client.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()
	for range 2 {
		got, err := sb.AttachVolumes(t.Context(), sandbox.Volume{Name: "dataset"})
		if err != nil || !slices.Equal(got, []sandbox.Volume{{Name: "dataset", Mount: "/volumes/dataset"}}) {
			t.Fatalf("attach: %v %v", got, err)
		}
	}
	if err := sb.Hibernate(t.Context()); err == nil {
		t.Fatal("captured an attached volume")
	}
	for range 2 {
		got, err := sb.DetachVolumes(t.Context(), "dataset")
		if err != nil || len(got) != 0 {
			t.Fatalf("detach: %v %v", got, err)
		}
	}
	want := []string{"attach:dataset:ro", "mount:dataset:/volumes/dataset:ro", "umount:/volumes/dataset", "detach:dataset"}
	if ops := stack.eng.volumeOpsLog(); !slices.Equal(ops, want) {
		t.Fatalf("device operations=%v want=%v", ops, want)
	}
}
