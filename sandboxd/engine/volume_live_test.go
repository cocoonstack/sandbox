package engine

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const rootMountInfo = "1 0 8:0 / / rw - ext4 /dev/vda rw\n"

func TestLiveVolumeConvergesMountAndDetach(t *testing.T) {
	for _, tt := range []struct {
		name, mount, kernelPath string
		realpath                map[string]string
		noRealpath              bool
		lookups                 int
	}{
		{name: "canonical path", mount: "/data", kernelPath: "/data", lookups: 1},
		{name: "symlinked parent", mount: "/workspace/data", kernelPath: "/home/u/ws/data", realpath: map[string]string{"/workspace/data": "/home/u/ws/data"}, lookups: 3},
		{name: "trailing space behind a symlink", mount: "/ws/data ", kernelPath: `/home/u/ws/data\040`, realpath: map[string]string{"/ws/data ": "/home/u/ws/data "}, lookups: 3},
		{name: "unresolvable path", mount: "/data", kernelPath: "/data", noRealpath: true, lookups: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sock := sockPath(t)
			fake := serveFakeSilkd(t, sock)
			configureVolumeDevices(fake)
			spec := liveDiskCLI(t, "")
			if err := os.Remove(os.Getenv("COCOON_TEST_ATTACHED")); err != nil {
				t.Fatal(err)
			}
			fake.mu.Lock()
			fake.realpath, fake.noRealpath = tt.realpath, tt.noRealpath
			fake.serial["/sys/block/vdc/dev"] = []byte("8:32\n")
			fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo)
			fake.execHook = func(argv []string) {
				switch argv[0] {
				case "mount":
					fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo + "2 1 8:32 / " + tt.kernelPath + " ro - ext4 /dev/vdc ro\n")
				case "umount":
					fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo)
				}
			}
			fake.mu.Unlock()
			e := New("cocoon", nil, nil, false, false, "")
			for range 2 {
				if err := e.DiskAttach(t.Context(), "sbx-1", spec); err != nil {
					t.Fatal(err)
				}
				if err := e.MountLiveVolume(t.Context(), sock, spec.Name, tt.mount, false); err != nil {
					t.Fatal(err)
				}
			}
			if err := e.DetachVolume(t.Context(), "sbx-1", sock, spec.Name, tt.mount); err != nil {
				t.Fatal(err)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			changes := guestChanges(fake.execCalls)
			if len(changes) != 3 || changes[2][0] != "umount" {
				t.Fatalf("expected one mkdir/mount/umount, got %v", fake.execCalls)
			}
			if lookups := len(fake.execCalls) - len(changes); lookups != tt.lookups {
				t.Fatalf("realpath lookups = %d, want %d: %v", lookups, tt.lookups, fake.execCalls)
			}
			args, err := os.ReadFile(os.Getenv("COCOON_TEST_ATTACH_ARGS"))
			if err != nil || !strings.Contains(string(args), "--readonly --directio off") {
				t.Fatalf("attach args=%s error=%v", args, err)
			}
		})
	}
}

func TestLiveMountRefusesAnOccupiedPathBehindASymlink(t *testing.T) {
	sock := sockPath(t)
	fake := serveFakeSilkd(t, sock)
	configureVolumeDevices(fake)
	spec := liveDiskCLI(t, "")
	fake.mu.Lock()
	fake.serial["/sys/block/vdc/dev"] = []byte("8:32\n")
	fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo + "2 1 8:48 / /home/u/ws/data ro - ext4 /dev/vdd ro\n")
	fake.realpath = map[string]string{"/workspace/data": "/home/u/ws/data"}
	fake.mu.Unlock()
	e := New("cocoon", nil, nil, false, false, "")
	if err := e.DiskAttach(t.Context(), "sbx-1", spec); err != nil {
		t.Fatal(err)
	}
	err := e.MountLiveVolume(t.Context(), sock, spec.Name, "/workspace/data", false)
	if _, ok := errors.AsType[*VolumeMountConflict](err); !ok || !strings.Contains(err.Error(), `"/home/u/ws/data" has a different device`) {
		t.Fatalf("mounted over another device behind a symlink or hid its real path: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if slices.ContainsFunc(fake.execCalls, func(argv []string) bool { return argv[0] == "mount" }) {
		t.Fatalf("mount ran over an occupied path: %v", fake.execCalls)
	}
}

func TestLiveMountReadsMountinfoBeyondTheSysfsCap(t *testing.T) {
	sock := sockPath(t)
	fake := serveFakeSilkd(t, sock)
	configureVolumeDevices(fake)
	spec := liveDiskCLI(t, "")
	overlays := strings.Repeat("9 1 0:99 / /var/lib/docker/overlay2/layer/merged rw - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/ABCDEFGHIJKLMNOPQRSTUVWXYZ\n", 2000)
	fake.mu.Lock()
	fake.serial["/sys/block/vdc/dev"] = []byte("8:32\n")
	fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo + overlays)
	fake.mu.Unlock()
	if len(rootMountInfo+overlays) <= silkdReadMax {
		t.Fatalf("fixture of %d bytes does not exceed the sysfs cap", len(rootMountInfo+overlays))
	}
	e := New("cocoon", nil, nil, false, false, "")
	if err := e.DiskAttach(t.Context(), "sbx-1", spec); err != nil {
		t.Fatal(err)
	}
	if err := e.MountLiveVolume(t.Context(), sock, spec.Name, "/data", false); err != nil {
		t.Fatal(err)
	}
}

func TestLiveAttachRejectsForeignMounts(t *testing.T) {
	for _, row := range []string{
		"2 1 8:99 / /data ro - ext4 /dev/vdz ro\n",
		"2 1 8:32 / /data rw - ext4 /dev/vdc rw\n",
		"2 1 8:32 /subdir /data ro - ext4 /dev/vdc ro\n",
		"malformed\n",
	} {
		t.Run(strings.TrimSpace(row), func(t *testing.T) {
			sock := sockPath(t)
			fake := serveFakeSilkd(t, sock)
			configureVolumeDevices(fake)
			spec := liveDiskCLI(t, "")
			fake.mu.Lock()
			fake.serial["/sys/block/vdc/dev"] = []byte("8:32\n")
			fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo + row)
			fake.mu.Unlock()
			err := New("cocoon", nil, nil, false, false, "").MountLiveVolume(t.Context(), sock, spec.Name, "/data", false)
			if err == nil {
				t.Fatal("accepted a foreign mount")
			}
			conflict, isConflict := errors.AsType[*VolumeMountConflict](err)
			if isConflict != (row != "malformed\n") {
				t.Fatalf("conflict type: %v", err)
			}
			if isConflict && !strings.Contains(conflict.Error(), `"/data" has a different device`) {
				t.Fatalf("conflict: %v", conflict)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if changes := guestChanges(fake.execCalls); len(changes) != 0 {
				t.Fatalf("modified foreign mount: %v", changes)
			}
		})
	}
}

func TestLiveDetachKeepsBusyAndUnobservableDevices(t *testing.T) {
	for _, mode := range []string{"busy", "second mount", "probe error", "guest error", "transport disconnect", "host error", "stacked mount"} {
		t.Run(mode, func(t *testing.T) {
			sock := sockPath(t)
			fake := serveFakeSilkd(t, sock)
			configureVolumeDevices(fake)
			hostErr := ""
			if mode == "host error" {
				hostErr = "device removal initiated but guest has not ejected it"
			}
			spec := liveDiskCLI(t, hostErr)
			fake.mu.Lock()
			fake.serial["/sys/block/vdc/dev"] = []byte("8:32\n")
			fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo)
			switch mode {
			case "busy":
				fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo + "2 1 8:32 / /data ro - ext4 /dev/vdc ro\n")
				fake.execCode = 32
			case "second mount":
				fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo + "2 1 8:32 / /other ro - ext4 /dev/vdc ro\n")
			case "transport disconnect":
				fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo + "2 1 8:32 / /data ro - ext4 /dev/vdc ro\n")
				fake.execDrop = true
			case "guest error":
				fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo + "2 1 8:32 / /data ro - ext4 /dev/vdc ro\n")
				fake.execErr = "guest connection failed /private/runtime"
			case "probe error":
				fake.readErr["/proc/self/mountinfo"] = "read failed"
			case "stacked mount":
				fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo + "2 1 8:32 / /data ro - ext4 /dev/vdc ro\n3 2 0:39 / /data rw - tmpfs guesttmp rw\n")
			}
			fake.mu.Unlock()
			err := New("cocoon", nil, nil, false, false, "").DetachVolume(t.Context(), "sbx-1", sock, spec.Name, "/data")
			if err == nil {
				t.Fatal("detach must fail closed")
			}
			conflict, isConflict := errors.AsType[*VolumeMountConflict](err)
			wantPath := map[string]string{"busy": "/data", "second mount": "/other", "stacked mount": "/data"}[mode]
			conflictExpected := wantPath != ""
			if isConflict != conflictExpected {
				t.Fatalf("conflict=%v error=%v", conflictExpected, err)
			}
			if conflictExpected && (!strings.Contains(conflict.Error(), strconv.Quote(wantPath)) || strings.Contains(conflict.Error(), "/private")) {
				t.Fatalf("unsafe or missing guest path %s: %v", wantPath, conflict)
			}
			if mode == "busy" && !strings.HasPrefix(conflict.Error(), "cannot unmount") {
				t.Fatalf("busy conflict: %v", conflict)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if mode == "stacked mount" && len(guestChanges(fake.execCalls)) != 0 {
				t.Fatalf("unmounted the guest's mount stacked on the volume: %v", fake.execCalls)
			}
		})
	}
}

func TestLiveDetachMissingGuestStillChecksHost(t *testing.T) {
	for _, hostErr := range []string{"", `disk "imagenet" not attached`, "vm not running", "backend unavailable"} {
		t.Run(hostErr, func(t *testing.T) {
			sock := sockPath(t)
			serveFakeSilkd(t, sock)
			spec := liveDiskCLI(t, hostErr)
			err := New("cocoon", nil, nil, false, false, "").DetachVolume(t.Context(), "sbx-1", sock, spec.Name, "/data")
			wantErr := hostErr != ""
			if (err != nil) != wantErr {
				t.Fatalf("got %v, want error=%v", err, wantErr)
			}
		})
	}
}

func TestLiveDetachIgnoresAnotherDeviceAtThePath(t *testing.T) {
	sock := sockPath(t)
	fake := serveFakeSilkd(t, sock)
	configureVolumeDevices(fake)
	spec := liveDiskCLI(t, "")
	fake.mu.Lock()
	fake.serial["/sys/block/vdc/dev"] = []byte("8:32\n")
	fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo + "2 1 8:48 / /data ro - ext4 /dev/vdd ro\n")
	fake.mu.Unlock()
	if err := New("cocoon", nil, nil, false, false, "").DetachVolume(t.Context(), "sbx-1", sock, spec.Name, "/data"); err != nil {
		t.Fatalf("detach of an unmounted device blocked by another device's mount: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if changes := guestChanges(fake.execCalls); len(changes) != 0 {
		t.Fatalf("touched another device's mount: %v", changes)
	}
}

func liveDiskCLI(t *testing.T, detachErr string) VolumeSpec {
	t.Helper()
	dir := t.TempDir()
	image := filepath.Join(dir, "volume.img")
	if err := os.WriteFile(image, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COCOON_TEST_DETACH_ERROR", detachErr)
	attached := filepath.Join(dir, "attached")
	if err := os.WriteFile(attached, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COCOON_TEST_ATTACHED", attached)
	t.Setenv("COCOON_TEST_ATTACH_ARGS", filepath.Join(dir, "args"))
	fakeCocoon(t, `#!/bin/sh
if [ "$1 $2 $3" = "vm disk attach" ]; then
  printf '%s\n' "$*" >> "$COCOON_TEST_ATTACH_ARGS"
  touch "$COCOON_TEST_ATTACHED"
  exit 0
fi
if [ "$1 $2 $3" = "vm disk detach" ]; then
  if [ -n "$COCOON_TEST_DETACH_ERROR" ]; then echo "$COCOON_TEST_DETACH_ERROR" >&2; exit 1; fi
  exit 0
fi
exit 2
`)
	return VolumeSpec{Name: "imagenet", Path: image}
}

func guestChanges(calls [][]string) [][]string {
	return slices.DeleteFunc(slices.Clone(calls), func(argv []string) bool { return argv[0] == "realpath" })
}
