package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/cocoonstack/sandbox/sdk/silkd"
)

const fsChunk = 32 * 1024

// WriteFile writes data to path in the sandbox, atomically (silkd renames a
// temp file into place). mode, when non-nil, sets the file's permission bits.
func (s *Sandbox) WriteFile(ctx context.Context, path string, data []byte, mode *uint32) error {
	conn, done, err := s.dial(ctx)
	if err != nil {
		return err
	}
	defer done()
	if err := conn.Send(&silkd.FsWrite{Path: path, Mode: mode}); err != nil {
		return err
	}
	if err := uploadStream(conn, bytes.NewReader(data)); err != nil {
		return err
	}
	return terminalErr(ctx, conn)
}

// ReadFile returns the contents of path.
func (s *Sandbox) ReadFile(ctx context.Context, path string) ([]byte, error) {
	conn, done, err := s.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err = conn.Send(&silkd.FsRead{Path: path}); err != nil {
		return nil, err
	}
	var out []byte
	err = drainData(ctx, conn, func(b []byte) error { out = append(out, b...); return nil })
	return out, err
}

// ListDir returns the entries of a directory (batched frames are concatenated).
func (s *Sandbox) ListDir(ctx context.Context, path string) ([]silkd.DirEntry, error) {
	conn, done, err := s.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := conn.Send(&silkd.FsList{Path: path}); err != nil {
		return nil, err
	}
	var entries []silkd.DirEntry
	for {
		resp, err := recv(ctx, conn)
		if err != nil {
			return nil, err
		}
		switch r := resp.(type) {
		case *silkd.Entries:
			entries = append(entries, r.Entries...)
		case *silkd.Done:
			return entries, nil
		case *silkd.ErrorResp:
			return nil, r
		default:
			return nil, unexpected(resp)
		}
	}
}

// Stat returns metadata for path.
func (s *Sandbox) Stat(ctx context.Context, path string) (silkd.FileInfo, error) {
	resp, err := s.oneShot(ctx, &silkd.FsStat{Path: path})
	if err != nil {
		return silkd.FileInfo{}, err
	}
	st, ok := resp.(*silkd.Stat)
	if !ok {
		return silkd.FileInfo{}, unexpected(resp)
	}
	return st.Info, nil
}

// Mkdir creates a directory, with parents when set.
func (s *Sandbox) Mkdir(ctx context.Context, path string, parents bool) error {
	return s.doneRPC(ctx, &silkd.FsMkdir{Path: path, Parents: parents})
}

// Remove deletes a file or directory (recursively when set).
func (s *Sandbox) Remove(ctx context.Context, path string, recursive bool) error {
	return s.doneRPC(ctx, &silkd.FsRm{Path: path, Recursive: recursive})
}

// Rename moves a file within the sandbox.
func (s *Sandbox) Rename(ctx context.Context, from, to string) error {
	return s.doneRPC(ctx, &silkd.FsRename{From: from, To: to})
}

// oneShot sends a request and returns its single non-terminal reply frame,
// mapping an error frame to a Go error.
func (s *Sandbox) oneShot(ctx context.Context, req silkd.Request) (silkd.Response, error) {
	conn, done, err := s.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err = conn.Send(req); err != nil {
		return nil, err
	}
	resp, err := recv(ctx, conn)
	if err != nil {
		return nil, err
	}
	if e, ok := resp.(*silkd.ErrorResp); ok {
		return nil, e
	}
	return resp, nil
}

// doneRPC sends a request that answers with Done or an error frame.
func (s *Sandbox) doneRPC(ctx context.Context, req silkd.Request) error {
	conn, done, err := s.dial(ctx)
	if err != nil {
		return err
	}
	defer done()
	if err := conn.Send(req); err != nil {
		return err
	}
	return terminalErr(ctx, conn)
}

// uploadStream chunks r into Data frames terminated by DataEnd; shared by the
// FsWrite payload and the FsPush tar stream.
func uploadStream(conn *silkd.Conn, r io.Reader) error {
	buf := make([]byte, fsChunk)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			if err := conn.Send(&silkd.Data{Data: buf[:n]}); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return conn.Send(silkd.DataEnd{})
		}
		if readErr != nil {
			return readErr
		}
	}
}

// drainData consumes Data frames into sink until Done; an error frame or an
// unexpected frame is a Go error.
func drainData(ctx context.Context, conn *silkd.Conn, sink func([]byte) error) error {
	for {
		resp, err := recv(ctx, conn)
		if err != nil {
			return err
		}
		switch r := resp.(type) {
		case *silkd.DataResp:
			if err := sink(r.Data); err != nil {
				return err
			}
		case *silkd.Done:
			return nil
		case *silkd.ErrorResp:
			return r
		default:
			return unexpected(resp)
		}
	}
}

// terminalErr reads one frame and requires it to be Done (else the error).
func terminalErr(ctx context.Context, conn *silkd.Conn) error {
	resp, err := recv(ctx, conn)
	if err != nil {
		return err
	}
	switch r := resp.(type) {
	case *silkd.Done:
		return nil
	case *silkd.ErrorResp:
		return r
	default:
		return unexpected(resp)
	}
}

// recv reads one frame, translating a canceled ctx and an early EOF.
func recv(ctx context.Context, conn *silkd.Conn) (silkd.Response, error) {
	resp, err := conn.Recv()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if err == io.EOF {
			return nil, fmt.Errorf("connection closed before a terminal frame")
		}
		return nil, err
	}
	return resp, nil
}

func unexpected(resp silkd.Response) error {
	return fmt.Errorf("unexpected frame %q", resp.RespType())
}
