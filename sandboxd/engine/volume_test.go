package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

func TestDiskAttachArgsReadOnlyAndDirectIO(t *testing.T) {
	for _, tt := range []struct {
		name     string
		directIO string
		wantIO   string
	}{
		{"default buffered", "", "off"},
		{"direct", "on", "on"},
		{"auto", "auto", "auto"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := New("cocoon", nil, nil, false, "")
			args, err := e.diskAttachArgs("sbx-1", VolumeSpec{
				Name: "imagenet", Path: "/srv/datasets/imagenet.img", DirectIO: tt.directIO,
			})
			if err != nil {
				t.Fatalf("diskAttachArgs: %v", err)
			}
			want := []string{
				"vm", "disk", "attach", "sbx-1",
				"--path", "/srv/datasets/imagenet.img",
				"--name", "imagenet",
				"--readonly",
				"--directio", tt.wantIO,
			}
			if !slices.Equal(args, want) {
				t.Errorf("args = %v, want %v", args, want)
			}
		})
	}
}

func TestDiskAttachArgsRejectBadOptions(t *testing.T) {
	_, err := New("cocoon", nil, nil, false, "").diskAttachArgs("sbx-1", VolumeSpec{DirectIO: "maybe"})
	if err == nil || !strings.Contains(err.Error(), "on, off, or auto") {
		t.Errorf("got %v, want directio validation error", err)
	}
}

func TestMountVolumeUsesSysfsAndReadOnlyMount(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	configureVolumeDevices(fake)
	if err := New("cocoon", nil, nil, false, "").MountVolume(
		t.Context(), path, "imagenet", "/datasets/training",
	); err != nil {
		t.Fatalf("MountVolume: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	wantExec := [][]string{
		{"mkdir", "-p", "--", "/datasets/training"},
		{"mount", "-o", "ro", "--", "/dev/vdc", "/datasets/training"},
	}
	if !slices.EqualFunc(fake.execCalls, wantExec, slices.Equal) {
		t.Errorf("exec calls = %v, want %v", fake.execCalls, wantExec)
	}
	wantReads := []string{
		"/sys/block/vda/serial",
		"/sys/block/vdb/serial",
		"/sys/block/vdc/serial",
	}
	if fake.listCalls != 1 || !slices.Equal(fake.readCalls, wantReads) {
		t.Errorf("sysfs calls = list:%d read:%v, want list:1 read:%v", fake.listCalls, fake.readCalls, wantReads)
	}
	if fake.execEnv["PATH"] == "" {
		t.Error("guest exec PATH is empty")
	}
}

func TestMountVolumeWaitsForDelayedSysfsSerial(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	configureVolumeDevices(fake)
	fake.mu.Lock()
	fake.readMisses["/sys/block/vdc/serial"] = 1
	fake.mu.Unlock()
	if err := New("cocoon", nil, nil, false, "").MountVolume(
		t.Context(), path, "imagenet", "/datasets/training",
	); err != nil {
		t.Fatalf("MountVolume: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.listCalls != 2 {
		t.Errorf("fs_list calls = %d, want 2", fake.listCalls)
	}
	if got := fake.readCalls[len(fake.readCalls)-1]; got != "/sys/block/vdc/serial" {
		t.Errorf("last fs_read = %q, want target serial", got)
	}
}

func TestMountVolumeStopsAtFailedStage(t *testing.T) {
	for _, tt := range []struct {
		name     string
		prepare  func(*fakeSilkd)
		wantErr  string
		wantExec int
	}{
		{"list", func(f *fakeSilkd) { f.listErr = "list failed" }, "wait for volume device imagenet", 0},
		{"read", func(f *fakeSilkd) { f.readErr["/sys/block/vda/serial"] = "read failed" }, "wait for volume device imagenet", 0},
		{"mkdir", func(f *fakeSilkd) { f.execCode, f.execFailAt = 3, 1 }, "create volume mount point", 1},
		{"mount", func(f *fakeSilkd) { f.execCode, f.execFailAt = 3, 2 }, "mount volume imagenet", 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := sockPath(t)
			fake := serveFakeSilkd(t, path)
			configureVolumeDevices(fake)
			fake.mu.Lock()
			tt.prepare(fake)
			fake.mu.Unlock()
			err := New("cocoon", nil, nil, false, "").MountVolume(
				t.Context(), path, "imagenet", "/datasets/training",
			)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("got %v, want %q failure", err, tt.wantErr)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.execCalls) != tt.wantExec {
				t.Errorf("exec calls = %v, want %d", fake.execCalls, tt.wantExec)
			}
		})
	}
}

func TestMountVolumeDeviceProbeIsBoundedAndCancelable(t *testing.T) {
	if volumeProbeTimeout != 2*time.Second {
		t.Fatalf("volume probe timeout = %s, want 2s", volumeProbeTimeout)
	}
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	configureVolumeDevices(fake)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := New("cocoon", nil, nil, false, "").MountVolume(
		ctx, path, "missing", "/datasets/training",
	)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context cancellation", err)
	}
}

func configureVolumeDevices(f *fakeSilkd) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.block = []wire.DirEntry{
		{Name: "vda", Kind: wire.FileKindSymlink},
		{Name: "vdb", Kind: wire.FileKindSymlink},
		{Name: "vdc", Kind: wire.FileKindSymlink},
	}
	f.serial["/sys/block/vda/serial"] = []byte("root\n")
	f.serial["/sys/block/vdc/serial"] = []byte("imagenet\n")
}
