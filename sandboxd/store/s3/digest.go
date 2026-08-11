package s3

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

type digestReader struct {
	body       io.Reader
	chunkHash  hash.Hash
	path       string
	size       int64
	read       int64
	chunkBytes int64
	chunks     [][sha256.Size]byte
	finished   bool
}

func newDigestReader(body io.Reader, path string, size int64) *digestReader {
	return &digestReader{body: body, chunkHash: sha256.New(), path: path, size: size}
}

func (r *digestReader) Read(p []byte) (int, error) {
	n, err := r.body.Read(p)
	r.read += int64(n)
	for offset := 0; offset < n; {
		remaining := int(store.DigestChunkSize - r.chunkBytes)
		written := min(n-offset, remaining)
		_, _ = r.chunkHash.Write(p[offset : offset+written])
		r.chunkBytes += int64(written)
		offset += written
		if r.chunkBytes == store.DigestChunkSize {
			r.finishChunk()
		}
	}
	return n, err
}

func (r *digestReader) Digest() (store.DigestFile, error) {
	if !r.finished {
		if r.chunkBytes > 0 {
			r.finishChunk()
		}
		r.finished = true
	}
	if r.read != r.size {
		return store.DigestFile{}, fmt.Errorf("read %s: got %d bytes, want %d", r.path, r.read, r.size)
	}
	return store.DigestFile{Path: r.path, Size: r.size, Chunks: r.chunks}, nil
}

func (r *digestReader) finishChunk() {
	var sum [sha256.Size]byte
	r.chunkHash.Sum(sum[:0])
	r.chunks = append(r.chunks, sum)
	r.chunkHash.Reset()
	r.chunkBytes = 0
}
