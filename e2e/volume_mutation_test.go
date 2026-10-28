package e2e

import (
	"errors"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func TestLiveWritableRemovalClearsMarkerThroughSDK(t *testing.T) {
	for _, tt := range []struct {
		name          string
		beforeDetach  func(*stack) error
		markerErr     error
		readerBlocked bool
	}{
		{name: "daemon unmount", beforeDetach: func(*stack) error { return nil }, markerErr: os.ErrNotExist},
		{name: "external unmount", beforeDetach: func(s *stack) error {
			return s.eng.unmountNamedVolume("data", "/volumes/data")
		}, readerBlocked: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			image := writeVolumeImage(t, "data.img", "original-data")
			stack := startTenantStack(t, "node-token", nil, []config.VolumeSpec{{Name: "data", Path: image, Writable: true}})
			writer, err := stack.client.New(t.Context(), "rt:24.04")
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			reader, err := stack.client.New(t.Context(), "rt:24.04")
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if _, err = writer.SetVolumes(t.Context(), []sandbox.Volume{{Name: "data", Mode: "rw"}}); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(image + ".dirty"); err != nil {
				t.Fatalf("writer has no dirty marker: %v", err)
			}
			if _, err = reader.SetVolumes(t.Context(), []sandbox.Volume{{Name: "data"}}); err == nil {
				t.Fatal("reader admitted while writer was attached")
			}
			if err = tt.beforeDetach(stack); err != nil {
				t.Fatal(err)
			}
			if _, err = writer.PatchVolumes(t.Context(), sandbox.VolumePatch{Remove: []string{"data"}}); err != nil {
				t.Fatal(err)
			}
			_, markerErr := os.Stat(image + ".dirty")
			if !errors.Is(markerErr, tt.markerErr) {
				t.Fatalf("dirty marker: got %v, want %v", markerErr, tt.markerErr)
			}
			_, err = reader.SetVolumes(t.Context(), []sandbox.Volume{{Name: "data"}})
			if (err != nil) != tt.readerBlocked {
				t.Fatalf("reader admission after detach: blocked=%t err=%v", tt.readerBlocked, err)
			}
			if data, readErr := os.ReadFile(image); readErr != nil || string(data) != "original-data" {
				t.Fatalf("detach changed backing contents: %q, %v", data, readErr)
			}
		})
	}
}

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

func TestMixedLiveVolumeModesThroughSDK(t *testing.T) {
	volumes := []sandbox.Volume{
		{Name: "mounted-ro"},
		{Name: "mounted-rw", Mode: "rw"},
		{Name: "device-ro", AttachOnly: true},
		{Name: "device-rw", Mode: "rw", AttachOnly: true},
	}
	var catalog []config.VolumeSpec
	for _, volume := range volumes {
		catalog = append(catalog, config.VolumeSpec{
			Name: volume.Name, Path: writeVolumeImage(t, volume.Name+".img", "seed"), Writable: volume.Mode == "rw",
		})
	}
	stack := startTenantStack(t, "node-token", nil, catalog)
	sb, err := stack.client.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()
	view, err := sb.PatchVolumes(t.Context(), sandbox.VolumePatch{Add: volumes})
	if err != nil {
		t.Fatal(err)
	}
	want := slices.Clone(volumes)
	want[0].Mount, want[1].Mount = "/volumes/mounted-ro", "/volumes/mounted-rw"
	if !slices.Equal(view.Volumes, want) || !slices.Equal(sb.Volumes, want) {
		t.Fatalf("mixed SDK attachment = %+v, handle=%+v, want %+v", view, sb.Volumes, want)
	}
	status, body := rawJSON(t, stack, http.MethodGet, "/v1/sandboxes/"+sb.ID+"/volumes", "")
	if status != http.StatusOK || strings.Count(string(body), `"attach_only":true`) != 4 {
		t.Fatalf("mixed HTTP view lost explicit attach_only in desired or applied entries: status=%d body=%s", status, body)
	}
	view, err = sb.GetVolumes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	view, err = sb.SetVolumes(t.Context(), view.VolumesWant)
	if err != nil || !slices.Equal(view.Volumes, want) || view.Converging {
		t.Fatalf("mixed GET-to-PUT = %+v, err=%v", view, err)
	}
	view, err = sb.SetVolumes(t.Context(), []sandbox.Volume{})
	if err != nil || len(view.Volumes) != 0 || len(view.VolumesWant) != 0 || view.Converging {
		t.Fatalf("mixed removal = %+v, err=%v", view, err)
	}
	initial, err := stack.client.New(t.Context(), "rt:24.04", sandbox.WithVolumes(volumes...))
	if err != nil {
		t.Fatalf("mixed initial claim: %v", err)
	}
	defer initial.Close()
	if !slices.Equal(initial.Volumes, want) {
		t.Fatalf("mixed initial claim = %+v, want %+v", initial.Volumes, want)
	}
}
