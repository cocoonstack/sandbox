package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

const fsChunk = 32 * 1024

// WriteFile writes data to path in the sandbox, atomically (silkd renames a
// temp file into place). mode, when non-nil, sets the file's permission bits.
func (s *Sandbox) WriteFile(ctx context.Context, path string, data []byte, mode *uint32) error {
	return s.uploadRPC(ctx, &silkd.FsWrite{Path: path, Mode: mode}, bytes.NewReader(data))
}

// ReadFile returns the contents of path.
func (s *Sandbox) ReadFile(ctx context.Context, path string) ([]byte, error) {
	var out []byte
	err := s.downloadRPC(ctx, &silkd.FsRead{Path: path}, func(b []byte) error {
		out = append(out, b...)
		return nil
	})
	return out, err
}

// ListDir returns the entries of a directory (batched frames are concatenated).
func (s *Sandbox) ListDir(ctx context.Context, path string) ([]silkd.DirEntry, error) {
	frames, err := collectRPC[silkd.Entries](ctx, s, &silkd.FsList{Path: path})
	if err != nil {
		return nil, err
	}
	var entries []silkd.DirEntry
	for _, f := range frames {
		entries = append(entries, f.Entries...)
	}
	return entries, nil
}

// Stat returns metadata for path.
func (s *Sandbox) Stat(ctx context.Context, path string) (silkd.FileInfo, error) {
	st, err := oneShotRPC[silkd.Stat](ctx, s, &silkd.FsStat{Path: path})
	if err != nil {
		return silkd.FileInfo{}, err
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

// doneRPC sends a request that answers with Done or an error frame.
func (s *Sandbox) doneRPC(ctx context.Context, req silkd.Request) error {
	conn, done, err := s.call(ctx, req)
	if err != nil {
		return err
	}
	defer done()
	return terminalErr(ctx, conn)
}

// uploadRPC sends req, streams r as Data frames, and expects a terminal Done.
func (s *Sandbox) uploadRPC(ctx context.Context, req silkd.Request, r io.Reader) error {
	conn, done, err := s.call(ctx, req)
	if err != nil {
		return err
	}
	defer done()
	if err := uploadStream(conn, r); err != nil {
		return err
	}
	return terminalErr(ctx, conn)
}

// downloadRPC sends req and drains its Data stream into sink until Done.
func (s *Sandbox) downloadRPC(ctx context.Context, req silkd.Request, sink func([]byte) error) error {
	conn, done, err := s.call(ctx, req)
	if err != nil {
		return err
	}
	defer done()
	return drainData(ctx, conn, sink)
}

// oneShotRPC sends req and returns its single typed reply frame.
func oneShotRPC[T any](ctx context.Context, s *Sandbox, req silkd.Request) (*T, error) {
	conn, done, err := s.call(ctx, req)
	if err != nil {
		return nil, err
	}
	defer done()
	return expect[T](ctx, conn)
}

// collectRPC sends req and gathers every streamed frame of type T until Done.
func collectRPC[T any](ctx context.Context, s *Sandbox, req silkd.Request) ([]T, error) {
	conn, done, err := s.call(ctx, req)
	if err != nil {
		return nil, err
	}
	defer done()
	var out []T
	for {
		resp, err := recv(ctx, conn)
		if err != nil {
			return nil, err
		}
		// Via any: a direct resp.(*T) assertion is rejected at compile time
		// (*T is not known to implement Response).
		if v, ok := any(resp).(*T); ok {
			out = append(out, *v)
			continue
		}
		switch r := resp.(type) {
		case *silkd.Done:
			return out, nil
		case *silkd.ErrorResp:
			return nil, r
		default:
			return nil, unexpected(resp)
		}
	}
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
	_, err := expect[silkd.Done](ctx, conn)
	return err
}

// expect reads one frame and requires it to be a *T, mapping an error frame
// to a Go error.
func expect[T any](ctx context.Context, conn *silkd.Conn) (*T, error) {
	resp, err := recv(ctx, conn)
	if err != nil {
		return nil, err
	}
	if v, ok := any(resp).(*T); ok {
		return v, nil
	}
	if e, ok := resp.(*silkd.ErrorResp); ok {
		return nil, e
	}
	return nil, unexpected(resp)
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
