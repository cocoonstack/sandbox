package pool

import (
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestRuntimeCatalogFailedDeleteRetriesAbsence(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete retry", true: "changed-file put"}[replace], func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, nil)
			spec := types.VolumeRegistration{Path: writeVolumeImage(t, "old", "bytes")}
			if err := m.RegisterVolume(t.Context(), "data", spec); err != nil {
				t.Fatal(err)
			}
			restore := blockCatalogRename(t, m)
			if err := m.UnregisterVolume(t.Context(), "data"); !errors.Is(err, ErrVolumeCatalogUnavailable) {
				t.Fatal(err)
			}
			if got := m.VolumeNames(); len(got) != 0 {
				t.Fatalf("failed delete remained published: %v", got)
			}
			if err := m.UnregisterVolume(t.Context(), "data"); !errors.Is(err, ErrVolumeCatalogUnavailable) {
				t.Fatalf("missing delete skipped persistence: %v", err)
			}
			restore()
			if got := newVolumeManagerAt(t, eng, m.dataDir, nil).VolumeNames(); !slices.Equal(got, []string{"data"}) {
				t.Fatalf("restart lost old file: %v", got)
			}
			if !replace {
				if err := m.UnregisterVolume(t.Context(), "data"); err != nil {
					t.Fatal(err)
				}
				if got := newVolumeManagerAt(t, eng, m.dataDir, nil).VolumeNames(); len(got) != 0 {
					t.Fatalf("retry did not persist absence: %v", got)
				}
				return
			}
			spec.Path = writeVolumeImage(t, "new", "replacement")
			if err := m.RegisterVolume(t.Context(), "data", spec); err != nil {
				t.Fatal(err)
			}
			restarted := newVolumeManagerAt(t, eng, m.dataDir, nil)
			resolved, err := restarted.resolveVolumes(t.Context(), "", []types.Volume{{Name: "data"}})
			if err != nil || resolved[0].disk.Path != spec.Path {
				t.Fatalf("replacement after failed delete: %v, %v", resolved, err)
			}
		})
	}
}

func TestRuntimeCatalogFailedPutIsNotReplayed(t *testing.T) {
	m := newVolumeManager(t, newFakeEngine(), nil)
	spec := types.VolumeRegistration{Path: writeVolumeImage(t, "data", "bytes")}
	restore := blockCatalogRename(t, m)
	if err := m.RegisterVolume(t.Context(), "data", spec); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatal(err)
	}
	restore()
	if err := m.RegisterVolume(t.Context(), "replacement", spec); err != nil {
		t.Fatal(err)
	}
	if got := newVolumeManagerAt(t, newFakeEngine(), m.dataDir, nil).VolumeNames(); !slices.Equal(got, []string{"replacement"}) {
		t.Fatalf("failed put was replayed: %v", got)
	}
}

func TestRuntimeCatalogRenameSucceededBeforeDirectoryOpenFailed(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "register", true: "unregister"}[remove], func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, nil)
			spec := types.VolumeRegistration{Path: writeVolumeImage(t, "data", "bytes")}
			if remove {
				if err := m.RegisterVolume(t.Context(), "data", spec); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Chmod(m.dataDir, 0o300); err != nil {
				t.Fatal(err)
			}
			restore := func() {
				if err := os.Chmod(m.dataDir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(restore)
			if dir, err := os.Open(m.dataDir); err == nil {
				dir.Close()
				t.Skip("requires directory read permission enforcement; drop CAP_DAC_OVERRIDE and CAP_DAC_READ_SEARCH when running as root")
			}
			var err error
			if remove {
				err = m.UnregisterVolume(t.Context(), "data")
			} else {
				err = m.RegisterVolume(t.Context(), "data", spec)
			}
			if !errors.Is(err, ErrVolumeCatalogUnavailable) || !strings.Contains(err.Error(), "open dir") {
				t.Fatalf("expected failure after rename: %v", err)
			}
			if got := m.VolumeNames(); len(got) != 0 {
				t.Fatalf("failed operation published data: %v", got)
			}
			restore()
			restarted := newVolumeManagerAt(t, eng, m.dataDir, nil)
			if got := slices.Contains(restarted.VolumeNames(), "data"); got == remove {
				t.Fatalf("restart did not load renamed file: %v", restarted.VolumeNames())
			}
			if remove {
				err = m.UnregisterVolume(t.Context(), "data")
			} else {
				err = m.RegisterVolume(t.Context(), "data", spec)
			}
			if err != nil {
				t.Fatalf("retry: %v", err)
			}
			if got := slices.Contains(m.VolumeNames(), "data"); got == remove {
				t.Fatalf("retry publication: %v", m.VolumeNames())
			}
		})
	}
}
