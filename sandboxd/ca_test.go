package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteCAFilesRefusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	if err := writeCAFiles(dir, "root", []byte("cert"), []byte("key"), false); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := writeCAFiles(dir, "root", []byte("cert2"), []byte("key2"), false); err == nil {
		t.Error("second write without -force overwrote an existing key")
	}
	if err := writeCAFiles(dir, "root", []byte("cert2"), []byte("key2"), true); err != nil {
		t.Errorf("-force write: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, "root.key"))
	if err != nil {
		t.Fatalf("stat key: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("key mode = %o, want 600", info.Mode().Perm())
	}
}
