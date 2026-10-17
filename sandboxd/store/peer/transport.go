// Package peer adds cross-node reach to a node-local record store: a record
// missing locally is pulled from a node that gossiped it, published locally,
// and served from there.
package peer

import (
	"archive/tar"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

const (
	// generous for a guest memory image, but a wedged peer must fail so the next owner is tried.
	pullTimeout = 30 * time.Minute

	maxRecordBytes = 1 << 40 // 1 TiB

	exportPrefix = store.ExportDir + "/"

	// recordTrailer is written last, so EOF without it means the transfer stopped early.
	recordTrailer = ".record-complete"

	// paxFileSize marks the entry that declares a sparse file; paxChunk marks one present chunk as "<index>:<sha256>".
	paxFileSize = "SANDBOX.size"
	paxChunk    = "SANDBOX.chunk"
)

// ErrNotFound reports that a peer does not hold the requested record.
var ErrNotFound = errors.New("peer does not hold record")

// Puller fetches a record from a peer into a local directory.
type Puller interface {
	Pull(ctx context.Context, addr, id, dst string) error
}

// HTTPPuller pulls records over sandboxd's control-plane HTTP port.
type HTTPPuller struct {
	// a nil Client uses a default with no overall timeout; the deadline comes from the context.
	Client *http.Client
	Token  string
}

func (p *HTTPPuller) Pull(ctx context.Context, addr, id, dst string) error {
	ctx, cancel := context.WithTimeout(ctx, pullTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, checkpointURL(addr, id, "/blob"), nil)
	if err != nil {
		return err
	}
	if p.Token != "" {
		req.Header.Set("Authorization", "Bearer "+p.Token)
	}

	client := cmp.Or(p.Client, http.DefaultClient)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("pull %s from %s: %w", id, addr, err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return ErrNotFound
	default:
		return fmt.Errorf("pull %s from %s: unexpected status %d", id, addr, resp.StatusCode)
	}
	return Untar(resp.Body, dst)
}

// TarRecord streams a whole store record: export/ plus meta.json, both required by the claim path.
func TarRecord(exportDir string, meta []byte, w io.Writer) error {
	tw := tar.NewWriter(w)
	defer func() { _ = tw.Close() }()

	if err := tw.WriteHeader(&tar.Header{
		Name: store.MetaFile, Mode: 0o600, Size: int64(len(meta)), Typeflag: tar.TypeReg,
	}); err != nil {
		return fmt.Errorf("tar record meta: %w", err)
	}
	if _, err := tw.Write(meta); err != nil {
		return fmt.Errorf("tar record meta: %w", err)
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: exportPrefix, Mode: 0o700, Typeflag: tar.TypeDir,
	}); err != nil {
		return fmt.Errorf("tar record export dir: %w", err)
	}
	if err := tarInto(exportDir, exportPrefix, tw); err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: recordTrailer, Mode: 0o600, Size: 0, Typeflag: tar.TypeReg,
	}); err != nil {
		return fmt.Errorf("tar record trailer: %w", err)
	}
	return tw.Close()
}

// Untar writes a tar stream into dst, validating every entry name against traversal.
func Untar(r io.Reader, dst string) error {
	tr := tar.NewReader(r)
	sizes := map[string]int64{}
	var written int64
	var complete bool
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			if !complete {
				return fmt.Errorf("untar: record incomplete, no completion marker (transfer cut short)")
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("untar: %w", err)
		}
		if hdr.Name == recordTrailer {
			complete = true // a protocol marker, never written to disk
			continue
		}
		target, err := safeJoin(dst, hdr.Name)
		if err != nil {
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return fmt.Errorf("untar mkdir %s: %w", hdr.Name, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return fmt.Errorf("untar mkdir %s: %w", filepath.Dir(hdr.Name), err)
			}
			written += hdr.Size
			if written > maxRecordBytes {
				return fmt.Errorf("untar: record exceeds %d bytes", int64(maxRecordBytes))
			}
			if err := untarRegular(tr, hdr, target, sizes); err != nil {
				return err
			}
		}
	}
}

func untarRegular(r io.Reader, hdr *tar.Header, target string, sizes map[string]int64) error {
	mode := os.FileMode(hdr.Mode).Perm() //nolint:gosec // Perm masks to 0777
	if raw, ok := hdr.PAXRecords[paxFileSize]; ok {
		return createSparse(target, raw, mode, sizes)
	}
	if raw, ok := hdr.PAXRecords[paxChunk]; ok {
		return writeChunk(target, raw, r, sizes)
	}
	return writeFile(target, r, mode)
}

func createSparse(target, rawSize string, mode os.FileMode, sizes map[string]int64) error {
	size, err := strconv.ParseInt(rawSize, 10, 64)
	if err != nil || size < 0 || size > maxRecordBytes {
		return fmt.Errorf("untar: bad size %q for %s", rawSize, target)
	}
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, cmp.Or(mode, os.FileMode(0o600))) //nolint:gosec // target resolved by safeJoin
	if err != nil {
		return fmt.Errorf("untar create %s: %w", target, err)
	}
	if err = f.Truncate(size); err != nil {
		_ = f.Close()
		return fmt.Errorf("untar size %s: %w", target, err)
	}
	sizes[target] = size
	return f.Close()
}

func writeChunk(target, raw string, r io.Reader, sizes map[string]int64) (err error) {
	size, ok := sizes[target]
	rawIndex, sum, found := strings.Cut(raw, ":")
	index, parseErr := strconv.ParseInt(rawIndex, 10, 64)
	if !ok || !found || parseErr != nil {
		return fmt.Errorf("untar: bad chunk entry %q for %s", raw, target)
	}
	f, err := os.OpenFile(target, os.O_WRONLY, 0) //nolint:gosec // target resolved by safeJoin
	if err != nil {
		return fmt.Errorf("untar open %s: %w", target, err)
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if err = store.WriteChunk(f, size, index, sum, r); err != nil {
		return fmt.Errorf("untar %s: %w", target, err)
	}
	return nil
}

func writeFile(target string, r io.Reader, mode os.FileMode) error {
	mode = cmp.Or(mode, os.FileMode(0o600))
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode) //nolint:gosec // target resolved by safeJoin
	if err != nil {
		return fmt.Errorf("untar create %s: %w", target, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("untar write %s: %w", target, err)
	}
	return f.Close()
}

func checkpointURL(addr, id, suffix string) string {
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	return addr + "/v1/checkpoints/" + url.PathEscape(id) + suffix
}

// safeJoin resolves name under root, rejecting absolute paths and names that escape root.
func safeJoin(root, name string) (string, error) {
	if name == "" || filepath.IsAbs(name) || strings.Contains(name, `\`) {
		return "", fmt.Errorf("untar: refusing entry %q", name)
	}
	// clean as-is, never "/"+name: a leading slash absorbs a leading ".." instead of rejecting it.
	clean := filepath.Clean(name)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("untar: entry %q escapes destination", name)
	}
	target := filepath.Join(root, clean)
	if target != root && !strings.HasPrefix(target, root+string(os.PathSeparator)) {
		return "", fmt.Errorf("untar: entry %q escapes destination", name)
	}
	return target, nil
}

// tarInto emits only regular files and directories (a symlink would steer the reader outside dst); a file is a size entry plus its non-zero chunks.
func tarInto(src, prefix string, tw *tar.Writer) error {
	scratch := sync.OnceValue(func() []byte { return make([]byte, store.DigestChunkSize) })
	err := filepath.WalkDir(src, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if !entry.IsDir() && !entry.Type().IsRegular() {
			return nil
		}
		fi, err := entry.Info()
		if err != nil {
			return err
		}
		name := prefix + rel
		if entry.IsDir() {
			return tw.WriteHeader(&tar.Header{
				Name:     name + "/",
				Mode:     int64(fi.Mode().Perm()),
				Typeflag: tar.TypeDir,
			})
		}
		if err = tw.WriteHeader(&tar.Header{
			Name:       name,
			Mode:       int64(fi.Mode().Perm()),
			Typeflag:   tar.TypeReg,
			PAXRecords: map[string]string{paxFileSize: strconv.FormatInt(fi.Size(), 10)},
		}); err != nil {
			return err
		}
		return tarChunks(path, name, fi.Size(), tw, scratch)
	})
	if err != nil {
		return fmt.Errorf("tar %s: %w", src, err)
	}
	return nil
}

func tarChunks(path, name string, size int64, tw *tar.Writer, scratch func() []byte) error {
	f, openErr := os.Open(path) //nolint:gosec // path comes from WalkDir over src
	if openErr != nil {
		return openErr
	}
	defer func() { _ = f.Close() }()
	_, err := store.ScanFile(f, name, size, scratch, func(c store.Chunk) error {
		if err := tw.WriteHeader(&tar.Header{
			Name:       name,
			Mode:       0o600,
			Size:       int64(len(c.Data)),
			Typeflag:   tar.TypeReg,
			PAXRecords: map[string]string{paxChunk: strconv.FormatInt(c.Index, 10) + ":" + c.SHA256},
		}); err != nil {
			return err
		}
		_, err := tw.Write(c.Data)
		return err
	})
	return err
}
