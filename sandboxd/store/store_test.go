package store

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestWalkExportVisitsRegularFilesBySlashPath(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "sub", "deep"), 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	for _, rel := range []string{"a.bin", "sub/b.bin", "sub/deep/c.bin"} {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(rel)), []byte(rel), 0o600); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	var got []string
	err := WalkExport(root, func(rel, path string, info fs.FileInfo) error {
		if path != filepath.Join(root, filepath.FromSlash(rel)) || info.Size() != int64(len(rel)) {
			t.Errorf("entry %q: path %q size %d", rel, path, info.Size())
		}
		got = append(got, rel)
		return nil
	})
	if want := []string{"a.bin", "sub/b.bin", "sub/deep/c.bin"}; err != nil || !slices.Equal(got, want) {
		t.Errorf("WalkExport = %v, %v, want %v", got, err, want)
	}
}

func TestWalkExportRejectsNonRegularEntries(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink("/etc/passwd", filepath.Join(root, "link")); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := WalkExport(root, func(string, string, fs.FileInfo) error { return nil }); err == nil {
		t.Error("WalkExport accepted a symlink entry")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := WalkExport(file, func(string, string, fs.FileInfo) error { return nil }); err == nil {
		t.Error("WalkExport accepted a file as the export root")
	}
}
