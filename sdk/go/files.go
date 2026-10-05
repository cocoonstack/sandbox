package sandbox

import (
	"bytes"
	"context"
	"io"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

// WriteFile atomically writes data to path in the sandbox; a non-nil mode sets the file's permission bits.
func (s *Sandbox) WriteFile(ctx context.Context, path string, data []byte, mode *uint32) error {
	return s.uploadRPC(ctx, &wire.FsWrite{Path: path, Mode: mode}, bytes.NewReader(data))
}

// ReadFile returns the contents of path.
func (s *Sandbox) ReadFile(ctx context.Context, path string) ([]byte, error) {
	var buf bytes.Buffer
	if err := s.ReadFileTo(ctx, path, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ReadFileTo streams path into w; an error from w ends the read there.
func (s *Sandbox) ReadFileTo(ctx context.Context, path string, w io.Writer) error {
	return s.downloadRPC(ctx, &wire.FsRead{Path: path}, func(b []byte) error {
		_, err := w.Write(b)
		return err
	})
}

// ListDir returns the entries of a directory (batched frames are concatenated).
func (s *Sandbox) ListDir(ctx context.Context, path string) ([]wire.DirEntry, error) {
	frames, err := s.collectRPC[wire.Entries](ctx, &wire.FsList{Path: path})
	if err != nil {
		return nil, err
	}
	var entries []wire.DirEntry
	for _, f := range frames {
		entries = append(entries, f.Entries...)
	}
	return entries, nil
}

// Stat returns metadata for path.
func (s *Sandbox) Stat(ctx context.Context, path string) (wire.FileInfo, error) {
	st, err := s.oneShotRPC[wire.Stat](ctx, &wire.FsStat{Path: path})
	if err != nil {
		return wire.FileInfo{}, err
	}
	return st.Info, nil
}

// Mkdir creates a directory, with parents when set.
func (s *Sandbox) Mkdir(ctx context.Context, path string, parents bool) error {
	return s.doneRPC(ctx, &wire.FsMkdir{Path: path, Parents: parents})
}

// Remove deletes a file or directory (recursively when set).
func (s *Sandbox) Remove(ctx context.Context, path string, recursive bool) error {
	return s.doneRPC(ctx, &wire.FsRm{Path: path, Recursive: recursive})
}

// Rename moves a file within the sandbox.
func (s *Sandbox) Rename(ctx context.Context, from, to string) error {
	return s.doneRPC(ctx, &wire.FsRename{From: from, To: to})
}
