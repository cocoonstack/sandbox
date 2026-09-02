package sandbox

import (
	"context"
	"fmt"
	"io"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

// fsChunk matches silkd's BULK_CHUNK.
const fsChunk = 256 * 1024

// respPtr is a frame type's pointer form, so a non-Response T fails to compile.
type respPtr[T any] interface {
	*T
	wire.Response
}

// doneRPC sends a request that answers with Done or an error frame.
func (s *Sandbox) doneRPC(ctx context.Context, req wire.Request) error {
	conn, done, err := s.call(ctx, req)
	if err != nil {
		return err
	}
	defer done()
	return terminalErr(ctx, conn)
}

// uploadRPC sends req, streams r as Data frames, and expects a terminal Done.
func (s *Sandbox) uploadRPC(ctx context.Context, req wire.Request, r io.Reader) error {
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
func (s *Sandbox) downloadRPC(ctx context.Context, req wire.Request, sink func([]byte) error) error {
	conn, done, err := s.call(ctx, req)
	if err != nil {
		return err
	}
	defer done()
	return drainData(ctx, conn, sink)
}

// oneShotRPC sends req and returns its single typed reply frame.
func oneShotRPC[T any, PT respPtr[T]](ctx context.Context, s *Sandbox, req wire.Request) (*T, error) {
	conn, done, err := s.call(ctx, req)
	if err != nil {
		return nil, err
	}
	defer done()
	return expect[T, PT](ctx, conn)
}

// collectRPC sends req and gathers every streamed frame of type T until Done.
func collectRPC[T any, PT respPtr[T]](ctx context.Context, s *Sandbox, req wire.Request) ([]T, error) {
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
		if v, ok := resp.(PT); ok {
			out = append(out, *v)
			continue
		}
		switch r := resp.(type) {
		case *wire.Done:
			return out, nil
		case *wire.ErrorResp:
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
			if err := conn.Send(&wire.Data{Data: buf[:n]}); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return conn.Send(wire.DataEnd{})
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
		case *wire.DataResp:
			if err := sink(r.Data); err != nil {
				return err
			}
		case *wire.Done:
			return nil
		case *wire.ErrorResp:
			return r
		default:
			return unexpected(resp)
		}
	}
}

// terminalErr reads one frame and requires it to be Done (else the error).
func terminalErr(ctx context.Context, conn *silkd.Conn) error {
	_, err := expect[wire.Done](ctx, conn)
	return err
}

// expect reads one frame and requires it to be a *T, mapping an error frame
// to a Go error.
func expect[T any, PT respPtr[T]](ctx context.Context, conn *silkd.Conn) (*T, error) {
	resp, err := recv(ctx, conn)
	if err != nil {
		return nil, err
	}
	if v, ok := resp.(PT); ok {
		return v, nil
	}
	if e, ok := resp.(*wire.ErrorResp); ok {
		return nil, e
	}
	return nil, unexpected(resp)
}

// recv reads one frame, translating a canceled ctx and an early EOF.
func recv(ctx context.Context, conn *silkd.Conn) (wire.Response, error) {
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

func unexpected(resp wire.Response) error {
	return fmt.Errorf("unexpected frame %q", resp.RespType())
}
