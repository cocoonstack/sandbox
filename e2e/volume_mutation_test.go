package e2e

import (
	"errors"
	"net/http"
	"slices"
	"strings"
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
		got, err := sb.SetVolumes(t.Context(), []sandbox.Volume{{Name: "dataset"}})
		if err != nil || !slices.Equal(got.Volumes, []sandbox.Volume{{Name: "dataset", Mount: "/volumes/dataset"}}) {
			t.Fatalf("attach: %v %v", got, err)
		}
	}
	if err := sb.Hibernate(t.Context()); err == nil {
		t.Fatal("captured an attached volume")
	}
	for range 2 {
		got, err := sb.SetVolumes(t.Context(), []sandbox.Volume{})
		if err != nil || len(got.Volumes) != 0 {
			t.Fatalf("detach: %v %v", got, err)
		}
	}
	want := []string{"attach:dataset:ro", "mount:dataset:/volumes/dataset:ro", "umount:/volumes/dataset", "detach:dataset"}
	if ops := stack.eng.volumeOpsLog(); !slices.Equal(ops, want) {
		t.Fatalf("device operations=%v want=%v", ops, want)
	}
}

func TestRejectedVolumeTargetExplainsRefusalWithoutChangingState(t *testing.T) {
	image := writeVolumeImage(t, "dataset.img", "dataset-bytes")
	stack := startTenantStack(t, "node-token", nil, []config.VolumeSpec{{Name: "dataset", Path: image}})
	sb, err := stack.client.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()
	if _, checkErr := sb.SetVolumes(t.Context(), []sandbox.Volume{{Name: "dataset"}}); checkErr != nil {
		t.Fatal(checkErr)
	}
	_, err = sb.SetVolumes(t.Context(), []sandbox.Volume{{Name: "dataset"}, {Name: "dataset"}})
	if refusal, ok := errors.AsType[*sandbox.VolumeError](err); !ok || refusal.Status != http.StatusBadRequest || !strings.Contains(refusal.Message, "duplicates name") {
		t.Errorf("invalid target explanation: %v", err)
	}
	view, err := sb.GetVolumes(t.Context())
	if err != nil || view.Error != "" || view.Converging || len(view.States) != 1 || view.States[0].Error != "" || view.States[0].State != sandbox.VolumeAttached {
		t.Fatalf("rejected request changed settled state: %+v err=%v", view, err)
	}
}

func TestEmptyVolumePatchChangesNothing(t *testing.T) {
	image := writeVolumeImage(t, "dataset.img", "dataset-bytes")
	stack := startTenantStack(t, "node-token", nil, []config.VolumeSpec{{Name: "dataset", Path: image}})
	sb, err := stack.client.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()
	if _, err = sb.SetVolumes(t.Context(), []sandbox.Volume{{Name: "dataset"}}); err != nil {
		t.Fatal(err)
	}
	got, err := sb.PatchVolumes(t.Context(), sandbox.VolumePatch{Add: []sandbox.Volume{}, Remove: []string{}})
	if err != nil || len(got.Volumes) != 1 || got.Volumes[0].Name != "dataset" || got.Converging {
		t.Fatalf("empty patch: %+v err=%v", got, err)
	}
}
