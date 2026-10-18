package utils

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestCloneFileCopiesContentAndSize(t *testing.T) {
	src, dst := writeSparse(t)
	if err := CloneFile(src, dst); err != nil {
		t.Fatalf("CloneFile: %v", err)
	}
	assertSameFile(t, src, dst)
}

func TestCloneFileRefusesExistingDestination(t *testing.T) {
	src, dst := writeSparse(t)
	if err := os.WriteFile(dst, []byte("keep"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := CloneFile(src, dst); err == nil {
		t.Fatal("CloneFile overwrote an existing destination")
	}
	if got, _ := os.ReadFile(dst); string(got) != "keep" {
		t.Errorf("destination holds %q after the refusal, want it untouched", got)
	}
}

func TestCloneFileRemovesFailedDestination(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "dst")
	if err := CloneFile(t.TempDir(), dst); err == nil {
		t.Fatal("CloneFile copied a directory")
	}
	if _, err := os.Stat(dst); err == nil {
		t.Error("a failed clone left its destination behind")
	}
}

func TestCopyDataCopiesDataRangesOnly(t *testing.T) {
	srcPath, dstPath := writeSparse(t)
	src, err := os.Open(srcPath)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer func() { _ = src.Close() }()
	dst, err := os.Create(dstPath)
	if err != nil {
		t.Fatalf("create dst: %v", err)
	}
	defer func() { _ = dst.Close() }()
	if err = copyData(dst, src); err != nil {
		t.Fatalf("copyData: %v", err)
	}
	assertSameFile(t, srcPath, dstPath)
}

func writeSparse(t *testing.T) (src, dst string) {
	t.Helper()
	dir := t.TempDir()
	src = filepath.Join(dir, "src")
	f, err := os.Create(src)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err = f.WriteAt([]byte("head"), 0); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err = f.WriteAt([]byte("tail"), 8<<20); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err = f.Truncate(16 << 20); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err = f.Close(); err != nil {
		t.Fatalf("setup: %v", err)
	}
	return src, filepath.Join(dir, "dst")
}

func assertSameFile(t *testing.T, a, b string) {
	t.Helper()
	got, err := os.ReadFile(b)
	if err != nil {
		t.Fatalf("read copy: %v", err)
	}
	want, err := os.ReadFile(a)
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("copy differs from the source (%d vs %d bytes)", len(got), len(want))
	}
}
