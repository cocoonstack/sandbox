package pool

import (
	"os"
	"path/filepath"
	"testing"
)

func TestManagedDirectoryRequiresTrustedAncestorOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("fixture needs root to create a root-owned child below a different owner")
	}
	for _, tc := range []struct {
		name   string
		owner  int
		sticky bool
		allow  bool
	}{
		{name: "untrusted-0755", owner: 65534},
		{name: "untrusted-sticky", owner: 65534, sticky: true},
		{name: "root-0755", owner: 0, allow: true},
		{name: "root-sticky", owner: 0, sticky: true, allow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := filepath.Join(t.TempDir(), "parent")
			if err := os.Mkdir(parent, 0o700); err != nil {
				t.Fatal(err)
			}
			mode := os.FileMode(0o755)
			if tc.sticky {
				mode = 0o777 | os.ModeSticky
			}
			if err := os.Chmod(parent, mode); err != nil {
				t.Fatal(err)
			}
			managed := filepath.Join(parent, "managed")
			if err := os.Mkdir(managed, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(parent, tc.owner, tc.owner); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chown(parent, 0, 0) })
			err := privateVolumeDir(managed)
			if (err == nil) != tc.allow {
				t.Fatalf("trusted=%v, validation error=%v", tc.allow, err)
			}
		})
	}
}
