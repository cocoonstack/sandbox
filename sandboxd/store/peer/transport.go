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
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// A checkpoint carries a guest memory image, so this is generous — but a
	// wedged peer must fail the pull so the next owner is tried, not hang it.
	pullTimeout = 30 * time.Minute

	maxRecordBytes = 1 << 40 // 1 TiB

	metaFile     = "meta.json"
	exportPrefix = "export/"

	// recordTrailer is written last, only after the whole export streamed, so a
	// receiver that reaches EOF without it knows the transfer stopped early — a
	// source-side walk or read error leaves a valid but short tar (Close still
	// writes the footer), and that must not be published as a complete record.
	recordTrailer = ".record-complete"
)

// ErrNotFound reports that a peer does not hold the requested record.
var ErrNotFound = errors.New("peer does not hold record")

// Puller fetches a record from a peer into a local directory.
type Puller interface {
	Pull(ctx context.Context, addr, id, dst string) error
}

// HTTPPuller pulls records over sandboxd's control-plane HTTP port.
type HTTPPuller struct {
	// A nil Client uses a default with no overall timeout: the per-pull
	// deadline comes from the context, so a large record is not cut off.
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

	client := p.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req) //nolint:gosec // addr comes from the mesh's own member view
	if err != nil {
		return fmt.Errorf("pull %s from %s: %w", id, addr, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return ErrNotFound
	default:
		return fmt.Errorf("pull %s from %s: unexpected status %d", id, addr, resp.StatusCode)
	}
	return Untar(resp.Body, dst)
}

// TarRecord streams a whole store record: export/ plus meta.json. Both are
// required — the claim path reads the meta's pool key and lineage before it
// ever touches the export, so an export-only copy fails every read.
func TarRecord(exportDir string, meta []byte, w io.Writer) error {
	tw := tar.NewWriter(w)
	defer func() { _ = tw.Close() }()

	if err := tw.WriteHeader(&tar.Header{
		Name: metaFile, Mode: 0o600, Size: int64(len(meta)), Typeflag: tar.TypeReg,
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

// Untar writes a tar stream into dst. An authenticated peer is still not
// allowed to write outside the destination this node chose, so every entry
// name is validated against traversal.
func Untar(r io.Reader, dst string) error {
	tr := tar.NewReader(r)
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
			if err := writeFile(target, tr, os.FileMode(hdr.Mode).Perm()); err != nil { //nolint:gosec // Perm masks to 0777
				return err
			}
		default:
			continue
		}
	}
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

// checkpointURL builds the control-plane URL for id's record on addr — the
// one home for the scheme default a mesh member view omits.
func checkpointURL(addr, id, suffix string) string {
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	return addr + "/v1/checkpoints/" + url.PathEscape(id) + suffix
}

// safeJoin resolves name under root, rejecting absolute paths and any name
// that would escape root.
func safeJoin(root, name string) (string, error) {
	if name == "" || filepath.IsAbs(name) || strings.Contains(name, `\`) {
		return "", fmt.Errorf("untar: refusing entry %q", name)
	}
	// Clean as-is, never as "/"+name: a leading slash absorbs a leading "..",
	// rewriting ../escape to /escape instead of rejecting it.
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

// tarInto walks src into tw under prefix, emitting only regular files and
// directories: a symlink or device node would let the reader be steered outside
// its destination.
func tarInto(src, prefix string, tw *tar.Writer) error {
	err := filepath.Walk(src, func(path string, fi os.FileInfo, err error) error {
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
		name := prefix + rel
		switch {
		case fi.IsDir():
			return tw.WriteHeader(&tar.Header{
				Name:     name + "/",
				Mode:     int64(fi.Mode().Perm()),
				Typeflag: tar.TypeDir,
			})
		case fi.Mode().IsRegular():
			if err := tw.WriteHeader(&tar.Header{
				Name:     name,
				Mode:     int64(fi.Mode().Perm()),
				Size:     fi.Size(),
				Typeflag: tar.TypeReg,
			}); err != nil {
				return err
			}
			f, err := os.Open(path) //nolint:gosec // path comes from Walk over src
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()
			_, err = io.Copy(tw, f)
			return err
		default:
			return nil
		}
	})
	if err != nil {
		return fmt.Errorf("tar %s: %w", src, err)
	}
	return nil
}
