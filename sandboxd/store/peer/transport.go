// Package peer adds cross-node reach to a node-local record store: when a
// record is missing locally, it is pulled from a node that gossiped it, cached
// locally, and served from there.
//
// The transport is sandboxd's own — deliberately NOT cocoon's image/snapshot
// transfer. cocoon's mover is bound to its VM store layout and its own
// addressing, and reusing it would tie a checkpoint's mobility to the engine's
// release cycle. This one moves exactly one thing (a store record: an export
// directory plus its meta.json) between two sandboxd nodes that already
// authenticate to each other with the fleet api_token, over the control-plane
// port they already share. It is a tar stream, so a pull costs one round trip
// and never materializes the record twice.
package peer

import (
	"archive/tar"
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

// pullTimeout bounds a single record transfer. A checkpoint carries a guest's
// memory image, so this is generous — but it is never unbounded: a wedged peer
// must fail the pull and let the next owner be tried, not hang the branch.
const pullTimeout = 30 * time.Minute

// maxRecordBytes caps a pulled record. It is a decompression-bomb guard, not a
// business limit: a peer is authenticated, but a compromised or buggy one must
// not be able to fill this node's disk.
const maxRecordBytes = 1 << 40 // 1 TiB

// The record layout a pull must reproduce, mirroring the store's own names.
const (
	metaFile     = "meta.json"
	exportPrefix = "export/"
)

// Puller fetches a record from a peer into a local directory.
type Puller interface {
	// Pull writes the record's contents (export/ and meta.json) into dst,
	// which the caller has created. It returns ErrNotFound when the peer does
	// not hold the record, so the caller can try the next owner.
	Pull(ctx context.Context, addr, id, dst string) error
}

// ErrNotFound reports that a peer does not hold the requested record. It
// mirrors store.ErrNotFound but is declared here so the transport does not
// import the store package it is a backend for.
var ErrNotFound = errors.New("peer does not hold record")

// HTTPPuller pulls records over sandboxd's control-plane HTTP port.
type HTTPPuller struct {
	// Client is the HTTP client. A nil Client uses a default with no overall
	// timeout — the per-pull deadline comes from the context instead, so a
	// large record is not cut off mid-stream.
	Client *http.Client
	// Token is the fleet api_token presented to the serving peer.
	Token string
}

// Pull implements Puller.
func (p *HTTPPuller) Pull(ctx context.Context, addr, id, dst string) error {
	ctx, cancel := context.WithTimeout(ctx, pullTimeout)
	defer cancel()

	base := addr
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	u := base + "/v1/checkpoints/" + url.PathEscape(id) + "/blob"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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
	resp, err := client.Do(req)
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

// TarRecord streams a whole store record: the export directory under
// "export/" plus its "meta.json". A record is only branchable with both — the
// meta carries the pool key and lineage the claim path reads before it ever
// touches the export, so shipping the export alone produces a directory that
// publishes cleanly and then fails every read as "unknown checkpoint".
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
	return tw.Close()
}

// Tar streams src's contents as a tar archive. Only regular files and
// directories are emitted: a record is data, and a symlink or device node in
// the stream would be a way to write outside the reader's destination.
func Tar(src string, w io.Writer) error {
	tw := tar.NewWriter(w)
	defer func() { _ = tw.Close() }()
	if err := tarInto(src, "", tw); err != nil {
		return err
	}
	return tw.Close()
}

// tarInto walks src into tw, prefixing every entry name with prefix.
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
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()
			_, err = io.Copy(tw, f)
			return err
		default:
			// Skip anything that is not data.
			return nil
		}
	})
	if err != nil {
		return fmt.Errorf("tar %s: %w", src, err)
	}
	return nil
}

// Untar writes a tar stream into dst. Entry names are validated against path
// traversal — a peer is authenticated, but an authenticated peer is still not
// allowed to write outside the destination this node chose.
func Untar(r io.Reader, dst string) error {
	tr := tar.NewReader(r)
	var written int64
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("untar: %w", err)
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
			if err := writeFile(target, tr, os.FileMode(hdr.Mode).Perm()); err != nil {
				return err
			}
		default:
			// Ignore non-data entries rather than trusting them.
			continue
		}
	}
}

// writeFile copies one entry to disk.
func writeFile(target string, r io.Reader, mode os.FileMode) error {
	if mode == 0 {
		mode = 0o600
	}
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("untar create %s: %w", target, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(f, io.LimitReader(r, maxRecordBytes)); err != nil {
		return fmt.Errorf("untar write %s: %w", target, err)
	}
	return f.Close()
}

// safeJoin resolves name under root, rejecting absolute paths and any name
// that would escape root.
func safeJoin(root, name string) (string, error) {
	if name == "" || filepath.IsAbs(name) || strings.Contains(name, `\`) {
		return "", fmt.Errorf("untar: refusing entry %q", name)
	}
	// Clean the name as-is, NOT as "/"+name: a leading slash would absorb any
	// leading "..", silently rewriting ../escape into /escape and landing the
	// entry inside root under a surprising name. A record this node produced
	// never contains "..", so its presence means corruption or an attack —
	// reject it loudly instead of neutralizing it.
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
