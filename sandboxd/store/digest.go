package store

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
)

const (
	// DigestChunkSize is the fixed byte width of every non-final digest chunk.
	DigestChunkSize int64 = 16 << 20

	digestDomain = "sandbox-template-export-v2\x00"
)

// DigestFile is one regular-file entry in the canonical export manifest.
type DigestFile struct {
	Path   string // slash-relative path; length and sorting use its raw bytes
	Size   int64
	Chunks [][sha256.Size]byte // indexed by ascending file offset
}

// AssembleDigest returns the canonical v2 digest of indexed file chunks.
func AssembleDigest(files []DigestFile) (string, error) {
	files = slices.Clone(files)
	slices.SortFunc(files, func(a, b DigestFile) int { return cmp.Compare(a.Path, b.Path) })

	h := sha256.New()
	_, _ = h.Write([]byte(digestDomain))
	for i, file := range files {
		if file.Path == "" {
			return "", fmt.Errorf("digest file path is empty")
		}
		if i > 0 && files[i-1].Path == file.Path {
			return "", fmt.Errorf("duplicate digest path %q", file.Path)
		}
		if file.Size < 0 {
			return "", fmt.Errorf("digest file %q has negative size %d", file.Path, file.Size)
		}
		var wantChunks int64
		if file.Size > 0 {
			wantChunks = (file.Size-1)/DigestChunkSize + 1
		}
		if int64(len(file.Chunks)) != wantChunks {
			return "", fmt.Errorf("digest file %q has %d chunks, want %d for size %d", file.Path, len(file.Chunks), wantChunks, file.Size)
		}

		var frame [8]byte
		_, _ = h.Write([]byte{'f'})
		binary.BigEndian.PutUint64(frame[:], uint64(len(file.Path)))
		_, _ = h.Write(frame[:])
		_, _ = h.Write([]byte(file.Path))
		binary.BigEndian.PutUint64(frame[:], uint64(file.Size))
		_, _ = h.Write(frame[:])
		binary.BigEndian.PutUint64(frame[:], uint64(len(file.Chunks)))
		_, _ = h.Write(frame[:])
		for _, chunk := range file.Chunks {
			_, _ = h.Write(chunk[:])
		}
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}
