package engine

import (
	"encoding/json/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const rootMountInfo = "1 0 8:0 / / rw - ext4 /dev/vda rw\n"

func TestLiveVolumeConvergesMountAndDetach(t *testing.T) {
	sock := sockPath(t)
	fake := serveFakeSilkd(t, sock)
	configureVolumeDevices(fake)
	spec := liveDiskCLI(t, "")
	// Start without a host attachment, then prove the second call reuses it.
	if err := os.Remove(os.Getenv("COCOON_TEST_ATTACHED")); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.serial["/sys/block/vdc/dev"] = []byte("8:32\n")
	fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo)
	fake.execHook = func(argv []string) {
		switch argv[0] {
		case "mount":
			fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo + "2 1 8:32 / /data ro - ext4 /dev/vdc ro\n")
		case "umount":
			fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo)
		}
	}
	fake.mu.Unlock()
	e := New("cocoon", nil, nil, false, false, "")
	for range 2 {
		if err := e.AttachVolume(t.Context(), "sbx-1", sock, spec, "/data"); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.DetachVolume(t.Context(), "sbx-1", sock, spec.Name, "/data"); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.execCalls) != 3 || fake.execCalls[2][0] != "umount" {
		t.Fatalf("expected one mkdir/mount/umount, got %v", fake.execCalls)
	}
	args, err := os.ReadFile(os.Getenv("COCOON_TEST_ATTACH_ARGS"))
	if err != nil || !strings.Contains(string(args), "--readonly --directio off") {
		t.Fatalf("attach args=%s error=%v", args, err)
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
			if err := New("cocoon", nil, nil, false, false, "").AttachVolume(t.Context(), "sbx-1", sock, spec, "/data"); err == nil {
				t.Fatal("accepted a foreign mount")
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.execCalls) != 0 {
				t.Fatalf("modified foreign mount: %v", fake.execCalls)
			}
		})
	}
}

func TestLiveDetachKeepsBusyAndUnobservableDevices(t *testing.T) {
	for _, mode := range []string{"busy", "second mount", "probe error", "host error"} {
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
			case "probe error":
				fake.readErr["/proc/self/mountinfo"] = "read failed"
			}
			fake.mu.Unlock()
			if err := New("cocoon", nil, nil, false, false, "").DetachVolume(t.Context(), "sbx-1", sock, spec.Name, "/data"); err == nil {
				t.Fatal("detach must fail closed")
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
			wantErr := hostErr != "" && !strings.Contains(hostErr, "not attached")
			if (err != nil) != wantErr {
				t.Fatalf("got %v, want error=%v", err, wantErr)
			}
		})
	}
}

func TestLiveAttachChecksHostImageAndMode(t *testing.T) {
	spec := liveDiskCLI(t, "")
	e := New("cocoon", nil, nil, false, false, "")
	spec.Path = t.TempDir()
	if _, err := e.attachedVolume(t.Context(), "sbx-1", spec); err == nil {
		t.Fatal("accepted mismatched host image")
	}
}

func liveDiskCLI(t *testing.T, detachErr string) VolumeSpec {
	t.Helper()
	dir := t.TempDir()
	image := filepath.Join(dir, "volume.img")
	if err := os.WriteFile(image, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	path, err := filepath.EvalSymlinks(image)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"attached_devices": map[string]any{"disks": []any{map[string]any{"name": "imagenet", "path": path, "readonly": true}}}})
	if err != nil {
		t.Fatal(err)
	}
	inspect := filepath.Join(dir, "inspect.json")
	if err := os.WriteFile(inspect, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COCOON_TEST_INSPECT", inspect)
	t.Setenv("COCOON_TEST_DETACH_ERROR", detachErr)
	attached := filepath.Join(dir, "attached")
	if err := os.WriteFile(attached, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COCOON_TEST_ATTACHED", attached)
	t.Setenv("COCOON_TEST_ATTACH_ARGS", filepath.Join(dir, "args"))
	fakeCocoon(t, `#!/bin/sh
if [ "$1 $2" = "vm inspect" ]; then
  if [ -f "$COCOON_TEST_ATTACHED" ]; then cat "$COCOON_TEST_INSPECT"; else echo '{}'; fi
  exit 0
fi
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
