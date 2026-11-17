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
	if got, err := os.ReadFile(filepath.Join(dir, "root.key")); err != nil || string(got) != "key" {
		t.Errorf("root.key after the refused write = %q, %v; want the original key", got, err)
	}
	keyPath := filepath.Join(dir, "root.key")
	if err := os.Chmod(keyPath, 0o644); err != nil {
		t.Fatalf("loosen key mode: %v", err)
	}
	if err := writeCAFiles(dir, "root", []byte("cert2"), []byte("key2"), true); err != nil {
		t.Errorf("-force write: %v", err)
	}
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("stat key: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("key mode = %o, want 600", info.Mode().Perm())
	}
	if got, err := os.ReadFile(keyPath); err != nil || string(got) != "key2" {
		t.Errorf("root.key after the -force write = %q, %v; want the new key", got, err)
	}
}
