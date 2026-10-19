package engine

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func TestManagedHookUsesBoundedSerializedArgv(t *testing.T) {
	sock := sockPath(t)
	fake := serveFakeSilkd(t, sock)
	e := New("cocoon", nil, nil, false, false, "")
	call := VolumeHookCall{Argv: []string{"/opt/hook", "$(touch /tmp/escape); literal"}, Action: "prepare", Operation: "op", VolumeID: "vol", VolumeName: "data", Mount: "/data with spaces", Timeout: 10 * time.Second}
	if err := e.RunVolumeHook(t.Context(), sock, call); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	got := slices.Clone(fake.execCalls[0])
	fake.execCode = 137
	fake.mu.Unlock()
	want := []string{"/usr/bin/flock", "--timeout", "10", "/run/sandboxd-volume-hook.lock", "/usr/bin/timeout", "--signal=KILL", "--", "10s", "/opt/hook", "$(touch /tmp/escape); literal", "prepare", "op", "vol", "data", "/data with spaces"}
	if !slices.Equal(got, want) {
		t.Fatalf("guest argv %q", got)
	}
	if err := e.RunVolumeHook(t.Context(), sock, call); err == nil {
		t.Fatal("hook termination reported success")
	}
}

func TestManagedMountRequiresLiteralWritableRoot(t *testing.T) {
	for _, tc := range []struct {
		name, mountinfo string
		valid           bool
	}{
		{"exact", "2 1 8:32 / /data rw - ext4 /dev/vdc rw\n", true},
		{"absent", "", false},
		{"read only", "2 1 8:32 / /data ro - ext4 /dev/vdc ro\n", false},
		{"wrong device", "2 1 8:48 / /data rw - ext4 /dev/vdd rw\n", false},
		{"subdirectory bind", "2 1 8:32 /sub /data rw - ext4 /dev/vdc rw\n", false},
		{"alias", "2 1 8:32 / /elsewhere rw - ext4 /dev/vdc rw\n", false},
		{"extra mount", "2 1 8:32 / /data rw - ext4 /dev/vdc rw\n3 1 8:32 / /alias rw - ext4 /dev/vdc rw\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sock := sockPath(t)
			fake := serveFakeSilkd(t, sock)
			configureVolumeDevices(fake)
			fake.mu.Lock()
			fake.serial["/sys/block/vdc/dev"] = []byte("8:32\n")
			fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo + tc.mountinfo)
			fake.mu.Unlock()
			err := New("cocoon", nil, nil, false, false, "").VerifyMountedVolume(t.Context(), sock, "imagenet", "/data")
			if tc.valid && err != nil {
				t.Fatal(err)
			}
			if !tc.valid {
				if _, ok := errors.AsType[*VolumeMountConflict](err); !ok {
					t.Fatalf("mount accepted or error wrong: %v", err)
				}
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.execCalls) != 0 {
				t.Fatal("mount preflight changed guest")
			}
		})
	}
}

func TestHibernateSourceRequiresExactInspectMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, listed, metadata string
		found, fail            bool
	}{
		{"present", `[{"name":"sleep"}]`, `{"name":"sleep","id":"immutable-source"}`, true, false},
		{"missing", `[]`, `{}`, false, false},
		{"wrong name", `[{"name":"sleep"}]`, `{"name":"other","id":"immutable-source"}`, false, true},
		{"missing id", `[{"name":"sleep"}]`, `{"name":"sleep"}`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeCocoon(t, "#!/bin/sh\ncase \"$1 $2\" in\n'snapshot list') echo '"+tc.listed+"';;\n'snapshot inspect') echo '"+tc.metadata+"';;\n*) exit 1;;\nesac\n")
			source, found, err := New("cocoon", nil, nil, false, false, "").InspectHibernateSource(t.Context(), "sleep")
			if found != tc.found || (err != nil) != tc.fail {
				t.Fatalf("source=%+v found=%v err=%v", source, found, err)
			}
		})
	}
}
