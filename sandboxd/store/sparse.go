package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

const zeroBlockSize = 1 << 20

var zeroChunkSum = sync.OnceValue(func() [sha256.Size]byte { return zeroSum(DigestChunkSize) })

// Chunk is one non-zero DigestChunkSize slice of a file; Data is owned by the emit callee.
type Chunk struct {
	Index  int64
	SHA256 string // hex of the chunk bytes
	Data   []byte
}

// ScanFile emits every non-zero chunk of f, reading only its data segments, and returns the file's digest entry; newBuf must return a DigestChunkSize buffer.
func ScanFile(f *os.File, rel string, size int64, newBuf func() []byte, emit func(Chunk) error) (DigestFile, error) {
	digest := DigestFile{Path: rel, Size: size, Chunks: make([][sha256.Size]byte, ChunkCount(size))}
	visited := make([]bool, len(digest.Chunks))
	var buf []byte
	var next int64
	for pos := int64(0); pos < size; {
		start, end, err := dataExtent(f, pos)
		if err != nil {
			return DigestFile{}, fmt.Errorf("scan %s: %w", rel, err)
		}
		if end <= start {
			break
		}
		for index := max(next, start/DigestChunkSize); index*DigestChunkSize < min(end, size); index++ {
			if buf == nil {
				buf = newBuf()
			}
			data := buf[:chunkLen(size, index)]
			if _, err = f.ReadAt(data, index*DigestChunkSize); err != nil {
				return DigestFile{}, fmt.Errorf("read %s chunk %d: %w", rel, index, err)
			}
			next = index + 1
			visited[index] = true
			if isZero(data) {
				digest.Chunks[index] = chunkZeroSum(int64(len(data)))
				continue
			}
			sum := sha256.Sum256(data)
			digest.Chunks[index] = sum
			if err = emit(Chunk{Index: index, SHA256: hex.EncodeToString(sum[:]), Data: data}); err != nil {
				return DigestFile{}, err
			}
			buf = nil
		}
		pos = end
	}
	for i, seen := range visited {
		if !seen {
			digest.Chunks[i] = chunkZeroSum(chunkLen(size, int64(i)))
		}
	}
	return digest, nil
}

// WriteChunk streams one chunk from r into f at its offset and fails unless it is chunkLen bytes hashing to wantSHA256.
func WriteChunk(f *os.File, size, index int64, wantSHA256 string, r io.Reader) error {
	if index < 0 || index >= ChunkCount(size) {
		return fmt.Errorf("chunk %d outside a %d-byte file", index, size)
	}
	want := chunkLen(size, index)
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(h, io.NewOffsetWriter(f, index*DigestChunkSize)), io.LimitReader(r, want+1))
	if err != nil {
		return fmt.Errorf("write chunk %d: %w", index, err)
	}
	if n != want {
		return fmt.Errorf("chunk %d is %d bytes, want %d", index, n, want)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != wantSHA256 {
		return fmt.Errorf("chunk %d checksum %s, want %s", index, got, wantSHA256)
	}
	return nil
}

func dataExtent(f *os.File, pos int64) (start, end int64, err error) {
	start, err = f.Seek(pos, unix.SEEK_DATA)
	if errors.Is(err, unix.ENXIO) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	end, err = f.Seek(start, unix.SEEK_HOLE)
	return start, end, err
}

func chunkLen(size, index int64) int64 {
	return min(DigestChunkSize, size-index*DigestChunkSize)
}

func chunkZeroSum(n int64) [sha256.Size]byte {
	if n == DigestChunkSize {
		return zeroChunkSum()
	}
	return zeroSum(n)
}

func zeroSum(n int64) [sha256.Size]byte {
	h := sha256.New()
	block := make([]byte, min(n, zeroBlockSize))
	for ; n > 0; n -= int64(len(block)) {
		_, _ = h.Write(block[:min(n, int64(len(block)))])
	}
	var sum [sha256.Size]byte
	h.Sum(sum[:0])
	return sum
}

// isZero compares data with itself shifted by one byte, which holds only for a constant run.
func isZero(data []byte) bool {
	return data[0] == 0 && bytes.Equal(data[1:], data[:len(data)-1])
}
