package store

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestAssembleDigestVectors(t *testing.T) {
	a := sha256.Sum256([]byte("a"))
	z := sha256.Sum256([]byte("z"))
	tests := []struct {
		name  string
		files []DigestFile
		want  string
	}{
		{
			name: "sorted tree",
			files: []DigestFile{
				{Path: "z.bin", Size: 1, Chunks: [][sha256.Size]byte{z}},
				{Path: "nested/a.bin", Size: 1, Chunks: [][sha256.Size]byte{a}},
			},
			want: "sha256:ad65b315de70e494c767969e65fc5de65c73846c4ebcdc7051abe08b056637ad",
		},
		{
			name:  "empty file",
			files: []DigestFile{{Path: "empty.bin"}},
			want:  "sha256:e3a1004a091d5fbaf95fb995f6999355a060a04949e1c3c9d8e886abcd933251",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AssembleDigest(tt.files)
			if err != nil {
				t.Fatalf("AssembleDigest: %v", err)
			}
			if got != tt.want {
				t.Errorf("digest %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAssembleDigestUsesChunkOffsetOrder(t *testing.T) {
	first := sha256.Sum256(bytes.Repeat([]byte("a"), int(DigestChunkSize)))
	second := sha256.Sum256([]byte("b"))
	completed := []struct {
		index int
		sum   [sha256.Size]byte
	}{
		{index: 1, sum: second},
		{index: 0, sum: first},
	}
	chunks := make([][sha256.Size]byte, len(completed))
	for _, result := range completed {
		chunks[result.index] = result.sum
	}

	got, err := AssembleDigest([]DigestFile{{Path: "big.bin", Size: DigestChunkSize + 1, Chunks: chunks}})
	if err != nil {
		t.Fatalf("AssembleDigest: %v", err)
	}
	const want = "sha256:d6aba4c9ea1dfbace830a37704633339e4e485654542569c878e70b7cf088bff"
	if got != want {
		t.Errorf("digest %q, want %q", got, want)
	}
}

func TestAssembleDigestRejectsInvalidEntries(t *testing.T) {
	chunk := sha256.Sum256([]byte("x"))
	tests := []struct {
		name  string
		files []DigestFile
	}{
		{name: "empty path", files: []DigestFile{{}}},
		{name: "duplicate path", files: []DigestFile{{Path: "x"}, {Path: "x"}}},
		{name: "negative size", files: []DigestFile{{Path: "x", Size: -1}}},
		{name: "missing chunk", files: []DigestFile{{Path: "x", Size: 1}}},
		{name: "extra chunk", files: []DigestFile{{Path: "x", Chunks: [][sha256.Size]byte{chunk}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := AssembleDigest(tt.files); err == nil {
				t.Fatal("AssembleDigest accepted an invalid entry")
			}
		})
	}
}
