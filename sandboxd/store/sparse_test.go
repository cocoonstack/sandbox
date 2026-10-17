package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestScanFileEmitsEachNonZeroChunkOnce(t *testing.T) {
	size := 4*DigestChunkSize + 5
	path := filepath.Join(t.TempDir(), "disk.img")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err = f.Truncate(size); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	for off, data := range map[int64]string{DigestChunkSize - 2: "abcd", DigestChunkSize + 10: "zz", 4*DigestChunkSize + 1: "t"} {
		if _, err = f.WriteAt([]byte(data), off); err != nil {
			t.Fatalf("write at %d: %v", off, err)
		}
	}

	var emitted []int64
	digest, err := ScanFile(f, "disk.img", size, func() []byte { return make([]byte, DigestChunkSize) }, func(c Chunk) error {
		emitted = append(emitted, c.Index)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanFile: %v", err)
	}
	if want := []int64{0, 1, 4}; !slices.Equal(emitted, want) {
		t.Errorf("emitted chunks %v, want %v", emitted, want)
	}
	want := make([][sha256.Size]byte, 0, 5)
	buf := make([]byte, DigestChunkSize)
	for off := int64(0); off < size; off += DigestChunkSize {
		n, readErr := f.ReadAt(buf, off)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			t.Fatalf("read at %d: %v", off, readErr)
		}
		want = append(want, sha256.Sum256(buf[:n]))
	}
	if !slices.Equal(digest.Chunks, want) {
		t.Errorf("chunk sums differ from a dense read")
	}
}

func TestWriteChunkRejectsWrongLengthAndChecksum(t *testing.T) {
	data := "payload"
	sum := sha256.Sum256([]byte(data))
	badSum := "0000000000000000000000000000000000000000000000000000000000000000"
	for _, tc := range []struct {
		name string
		size int64
		sha  string
		body string
		want string
	}{
		{"short body", int64(len(data)) + 1, hex.EncodeToString(sum[:]), data, "want 8"},
		{"long body", int64(len(data)) - 1, hex.EncodeToString(sum[:]), data, "want 6"},
		{"wrong sum", int64(len(data)), badSum, data, "checksum"},
		{"index outside file", int64(len(data)), hex.EncodeToString(sum[:]), data, "outside"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := os.Create(filepath.Join(t.TempDir(), "chunk"))
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			defer func() { _ = f.Close() }()
			index := int64(0)
			if tc.name == "index outside file" {
				index = 1
			}
			err = WriteChunk(f, tc.size, index, tc.sha, strings.NewReader(tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("WriteChunk = %v, want an error containing %q", err, tc.want)
			}
		})
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "ok"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err = WriteChunk(f, int64(len(data)), 0, hex.EncodeToString(sum[:]), strings.NewReader(data)); err != nil {
		t.Errorf("WriteChunk of a matching chunk: %v", err)
	}
}
