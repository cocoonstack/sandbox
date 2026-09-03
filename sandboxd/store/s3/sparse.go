package s3

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	sparseManifestVersion  = 1
	sparseChunkSize        = 64 << 20
	sparseObjectPrefix     = ".sandboxd-sparse-v1"
	sparseManifestObject   = sparseObjectPrefix + "/manifest.json"
	sparseManifestMaxBytes = 16 << 20
)

type sparseManifest struct {
	Version int          `json:"version"`
	Files   []sparseFile `json:"files"`
}

type sparseFile struct {
	Path   string        `json:"path"`
	Size   int64         `json:"size"`
	Mode   uint32        `json:"mode"`
	Chunks []sparseChunk `json:"chunks"`
}

type sparseChunk struct {
	Size    int64          `json:"size"`
	Object  string         `json:"object"`
	Extents []sparseExtent `json:"extents"`
}

type sparseExtent struct {
	Offset int64 `json:"offset"`
	Size   int64 `json:"size"`
}

type extentWriterAt struct {
	file        *os.File
	extents     []sparseExtent
	dataOffsets []int64
	size        int64
}

func newExtentWriterAt(file *os.File, extents []sparseExtent) *extentWriterAt {
	w := &extentWriterAt{file: file, extents: extents, dataOffsets: make([]int64, len(extents))}
	for i, extent := range extents {
		w.dataOffsets[i] = w.size
		w.size += extent.Size
	}
	return w
}

func (w *extentWriterAt) WriteAt(p []byte, offset int64) (int, error) {
	if offset < 0 || offset > w.size || int64(len(p)) > w.size-offset {
		return 0, io.ErrShortWrite
	}
	written := 0
	for len(p) > 0 {
		i := sort.Search(len(w.extents), func(i int) bool {
			return w.dataOffsets[i]+w.extents[i].Size > offset
		})
		if i == len(w.extents) {
			return written, io.ErrShortWrite
		}
		within := offset - w.dataOffsets[i]
		n := min(int64(len(p)), w.extents[i].Size-within)
		count, err := w.file.WriteAt(p[:n], w.extents[i].Offset+within)
		written += count
		offset += int64(count)
		p = p[count:]
		if err != nil {
			return written, err
		}
		if int64(count) != n {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func planSparseFile(path, exportPath string) (sparseFile, []sparseChunk, bool, error) {
	f, err := os.Open(path) //nolint:gosec // path walked from our own staging dir
	if err != nil {
		return sparseFile{}, nil, false, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return sparseFile{}, nil, false, err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return sparseFile{}, nil, false, nil
	}
	extents, supported, err := sparseExtents(f, info.Size())
	if err != nil {
		return sparseFile{}, nil, false, fmt.Errorf("scan sparse file %s: %w", exportPath, err)
	}
	if !supported {
		return sparseFile{}, nil, false, nil
	}
	var dataSize int64
	for _, extent := range extents {
		dataSize += extent.size
	}
	if dataSize >= info.Size() {
		return sparseFile{}, nil, false, nil
	}
	fileID := fmt.Sprintf("%x", sha256.Sum256([]byte(exportPath)))
	planned := sparseFile{Path: exportPath, Size: info.Size(), Mode: uint32(info.Mode().Perm())}
	planned.Chunks = packExtents(fileID, extents)
	return planned, planned.Chunks, true, nil
}

func packExtents(fileID string, extents []fileExtent) []sparseChunk {
	var chunks []sparseChunk
	for _, extent := range extents {
		for offset := extent.offset; offset < extent.offset+extent.size; {
			if len(chunks) == 0 || chunks[len(chunks)-1].Size == sparseChunkSize {
				chunks = append(chunks, sparseChunk{
					Object: fmt.Sprintf("%s/chunks/%s/%08x", sparseObjectPrefix, fileID, len(chunks)),
				})
			}
			chunk := &chunks[len(chunks)-1]
			size := min(sparseChunkSize-chunk.Size, extent.offset+extent.size-offset)
			chunk.Extents = append(chunk.Extents, sparseExtent{Offset: offset, Size: size})
			chunk.Size += size
			offset += size
		}
	}
	return chunks
}

type fileExtent struct {
	offset int64
	size   int64
}

func sparseExtents(file *os.File, size int64) ([]fileExtent, bool, error) {
	var extents []fileExtent

scan:
	for offset := int64(0); offset < size; {
		data, err := unix.Seek(int(file.Fd()), offset, unix.SEEK_DATA)
		switch {
		case errors.Is(err, unix.ENXIO):
			break scan
		case sparseSeekUnsupported(err):
			return nil, false, nil
		case err != nil:
			return nil, true, err
		}
		if data < offset {
			return nil, false, nil
		}
		if data >= size {
			break
		}
		hole, err := unix.Seek(int(file.Fd()), data, unix.SEEK_HOLE)
		switch {
		case errors.Is(err, unix.ENXIO):
			hole = size
		case sparseSeekUnsupported(err):
			return nil, false, nil
		case err != nil:
			return nil, true, err
		}
		hole = min(hole, size)
		if hole <= data {
			return nil, false, nil
		}
		extents = append(extents, fileExtent{offset: data, size: hole - data})
		offset = hole
	}
	return extents, true, nil
}

func sparseSeekUnsupported(err error) bool {
	return errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.ENOSYS)
}

func validateSparseManifest(manifest sparseManifest, exportPrefix string, keys []string) error {
	if manifest.Version != sparseManifestVersion {
		return fmt.Errorf("unsupported sparse manifest version %d", manifest.Version)
	}
	keySet := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		keySet[key] = struct{}{}
	}
	paths := make(map[string]struct{}, len(manifest.Files))
	objects := make(map[string]struct{})
	for _, file := range manifest.Files {
		clean := pathpkg.Clean(file.Path)
		if clean != file.Path || clean == "." || pathpkg.IsAbs(clean) || strings.HasPrefix(clean, "../") {
			return fmt.Errorf("invalid sparse file path %q", file.Path)
		}
		if _, ok := paths[file.Path]; ok {
			return fmt.Errorf("duplicate sparse file path %q", file.Path)
		}
		paths[file.Path] = struct{}{}
		if file.Size < 0 {
			return fmt.Errorf("negative sparse file size for %q", file.Path)
		}
		if _, ok := keySet[exportPrefix+file.Path]; ok {
			return fmt.Errorf("sparse file %q also has a dense object", file.Path)
		}
		var previousEnd int64
		for _, chunk := range file.Chunks {
			cleanObject := pathpkg.Clean(chunk.Object)
			if cleanObject != chunk.Object || !strings.HasPrefix(cleanObject, sparseObjectPrefix+"/chunks/") {
				return fmt.Errorf("invalid sparse chunk object %q", chunk.Object)
			}
			if _, ok := objects[chunk.Object]; ok {
				return fmt.Errorf("duplicate sparse chunk object %q", chunk.Object)
			}
			objects[chunk.Object] = struct{}{}
			if _, ok := keySet[exportPrefix+chunk.Object]; !ok {
				return fmt.Errorf("missing sparse chunk object %q", chunk.Object)
			}
			var chunkSize int64
			for _, extent := range chunk.Extents {
				if extent.Offset < previousEnd || extent.Size <= 0 || extent.Offset > file.Size || extent.Size > file.Size-extent.Offset {
					return fmt.Errorf("invalid sparse extent for %q at %d", file.Path, extent.Offset)
				}
				chunkSize += extent.Size
				previousEnd = extent.Offset + extent.Size
			}
			if chunk.Size <= 0 || chunk.Size != chunkSize || chunk.Size > sparseChunkSize {
				return fmt.Errorf("invalid sparse chunk size for %q: %d", file.Path, chunk.Size)
			}
		}
	}
	return nil
}

func materializeSparseFiles(root string, files []sparseFile) error {
	for _, file := range files {
		path, err := exportPath(root, file.Path)
		if err != nil {
			return err
		}
		if mkdirErr := os.MkdirAll(filepath.Dir(path), 0o750); mkdirErr != nil {
			return mkdirErr
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // validated path
		if err != nil {
			return err
		}
		truncateErr := f.Truncate(file.Size)
		closeErr := f.Close()
		if truncateErr != nil {
			return truncateErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func chmodSparseFiles(root string, files []sparseFile) error {
	for _, file := range files {
		path, err := exportPath(root, file.Path)
		if err != nil {
			return err
		}
		if err := os.Chmod(path, os.FileMode(file.Mode)&os.ModePerm); err != nil {
			return err
		}
	}
	return nil
}
