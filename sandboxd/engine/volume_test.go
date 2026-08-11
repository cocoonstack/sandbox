package engine

import (
	"slices"
	"strings"
	"testing"
)

func TestDiskAttachArgsForceReadOnlyAndDirectIO(t *testing.T) {
	for _, tt := range []struct {
		name     string
		directIO string
		want     string
	}{
		{"default is buffered", "", "off"},
		{"explicit buffered", "off", "off"},
		{"direct io", "on", "on"},
		{"cocoon auto", "auto", "auto"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := New("cocoon", nil, nil, false, "")
			args, err := e.diskAttachArgs("sbx-1", VolumeSpec{Name: "imagenet", Path: "/srv/datasets/imagenet.img", DirectIO: tt.directIO})
			if err != nil {
				t.Fatalf("diskAttachArgs: %v", err)
			}
			want := []string{
				"vm", "disk", "attach", "sbx-1",
				"--path", "/srv/datasets/imagenet.img",
				"--name", "imagenet",
				"--readonly",
				"--directio", tt.want,
			}
			if !slices.Equal(args, want) {
				t.Errorf("args = %v, want %v", args, want)
			}
		})
	}
}

func TestDiskAttachArgsRejectBadDirectIO(t *testing.T) {
	_, err := New("cocoon", nil, nil, false, "").diskAttachArgs("sbx-1", VolumeSpec{DirectIO: "maybe"})
	if err == nil || !strings.Contains(err.Error(), "on, off, or auto") {
		t.Errorf("got %v, want directio validation error", err)
	}
}

func TestMountVolumeUsesBoundedArgvSetup(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	if err := New("cocoon", nil, nil, false, "").MountVolume(t.Context(), path, "imagenet", "/datasets/training"); err != nil {
		t.Fatalf("MountVolume: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	want := [][]string{
		{"udevadm", "trigger", "--action=add", "--subsystem-match=block"},
		{"udevadm", "settle", "--timeout=5", "--exit-if-exists=/dev/disk/by-id/virtio-imagenet"},
		{"test", "-e", "/dev/disk/by-id/virtio-imagenet"},
		{"mkdir", "-p", "--", "/datasets/training"},
		{"mount", "-o", "ro", "--", "/dev/disk/by-id/virtio-imagenet", "/datasets/training"},
	}
	if !slices.EqualFunc(fake.execCalls, want, slices.Equal) {
		t.Errorf("exec calls = %v, want %v", fake.execCalls, want)
	}
	if fake.execEnv["PATH"] == "" {
		t.Error("guest exec PATH is empty")
	}
}

func TestMountVolumeWaitsForDelayedDevice(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	fake.execCode = 3
	fake.execFailAt = 3
	if err := New("cocoon", nil, nil, false, "").MountVolume(t.Context(), path, "imagenet", "/datasets/training"); err != nil {
		t.Fatalf("MountVolume: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	want := [][]string{
		{"udevadm", "trigger", "--action=add", "--subsystem-match=block"},
		{"udevadm", "settle", "--timeout=5", "--exit-if-exists=/dev/disk/by-id/virtio-imagenet"},
		{"test", "-e", "/dev/disk/by-id/virtio-imagenet"},
		{"test", "-e", "/dev/disk/by-id/virtio-imagenet"},
		{"mkdir", "-p", "--", "/datasets/training"},
		{"mount", "-o", "ro", "--", "/dev/disk/by-id/virtio-imagenet", "/datasets/training"},
	}
	if !slices.EqualFunc(fake.execCalls, want, slices.Equal) {
		t.Errorf("exec calls = %v, want delayed retry %v", fake.execCalls, want)
	}
}

func TestMountVolumeStopsAfterGuestFailure(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	fake.execCode = 3
	err := New("cocoon", nil, nil, false, "").MountVolume(t.Context(), path, "imagenet", "/datasets/training")
	if err == nil || !strings.Contains(err.Error(), "trigger volume device imagenet") {
		t.Errorf("got %v, want trigger failure", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.execCalls) != 1 {
		t.Errorf("exec calls = %v, want one failed trigger", fake.execCalls)
	}
}

func TestMountVolumeStopsAtFailedSetupStage(t *testing.T) {
	wantCalls := [][]string{
		{"udevadm", "trigger", "--action=add", "--subsystem-match=block"},
		{"udevadm", "settle", "--timeout=5", "--exit-if-exists=/dev/disk/by-id/virtio-imagenet"},
		{"test", "-e", "/dev/disk/by-id/virtio-imagenet"},
		{"mkdir", "-p", "--", "/datasets/training"},
		{"mount", "-o", "ro", "--", "/dev/disk/by-id/virtio-imagenet", "/datasets/training"},
	}
	for _, tt := range []struct {
		name    string
		failAt  int
		wantErr string
	}{
		{"settle", 2, "settle volume device imagenet"},
		{"mkdir", 4, "create volume mount point /datasets/training"},
		{"mount", 5, "mount volume imagenet"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := sockPath(t)
			fake := serveFakeSilkd(t, path)
			fake.execCode = 3
			fake.execFailAt = tt.failAt
			err := New("cocoon", nil, nil, false, "").MountVolume(t.Context(), path, "imagenet", "/datasets/training")
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("got %v, want %q failure", err, tt.wantErr)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if want := wantCalls[:tt.failAt]; !slices.EqualFunc(fake.execCalls, want, slices.Equal) {
				t.Errorf("exec calls = %v, want %v", fake.execCalls, want)
			}
		})
	}
}
