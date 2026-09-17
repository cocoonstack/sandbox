package sandbox

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

// respPtr is a frame type's pointer form, so a non-Response T fails to compile.
type respPtr[T any] interface {
	*T
	wire.Response
}

func (s *Sandbox) doneRPC(ctx context.Context, req wire.Request) error {
	conn, l, err := s.call(ctx, req)
	if err != nil {
		return err
	}
	defer l.close()
	return l.done(terminalErr(ctx, conn))
}

// uploadRPC streams r as Data frames after req and expects Done; an early terminal frame is the guest rejecting the upload and stops the stream.
func (s *Sandbox) uploadRPC(ctx context.Context, req wire.Request, r io.Reader) error {
	conn, l, err := s.call(ctx, req)
	if err != nil {
		return err
	}
	defer l.close()
	terminal := make(chan error, 1)
	go func() { terminal <- terminalErr(ctx, conn) }()
	buf := make([]byte, wire.BulkChunk)
	for {
		select {
		case err := <-terminal:
			return l.done(err)
		default:
		}
		n, readErr := r.Read(buf)
		if n > 0 {
			if err := conn.Send(&wire.Data{Data: buf[:n]}); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if err := conn.Send(wire.DataEnd{}); err != nil {
		return err
	}
	return l.done(<-terminal)
}

func (s *Sandbox) downloadRPC(ctx context.Context, req wire.Request, sink func([]byte) error) error {
	conn, l, err := s.call(ctx, req)
	if err != nil {
		return err
	}
	defer l.close()
	return l.done(drainData(ctx, conn, sink))
}

// pumpStdio copies stdout/stderr frames to the writers until the terminal frame: exit carries the code, done means the stream ended without one.
func pumpStdio(ctx context.Context, conn *silkd.Conn, stdout, stderr io.Writer) (code int32, exited bool, err error) {
	stdout = cmp.Or(stdout, io.Discard)
	stderr = cmp.Or(stderr, io.Discard)
	for {
		resp, err := recv(ctx, conn)
		if err != nil {
			return 0, false, err
		}
		switch resp := resp.(type) {
		case *wire.Started:
		case *wire.Stdout:
			if _, err := stdout.Write(resp.Data); err != nil {
				return 0, false, err
			}
		case *wire.Stderr:
			if _, err := stderr.Write(resp.Data); err != nil {
				return 0, false, err
			}
		case *wire.Exit:
			return resp.Code, true, nil
		case *wire.Done:
			return 0, false, nil
		case *wire.ErrorResp:
			return 0, false, resp
		default:
			return 0, false, unexpected(resp)
		}
	}
}

func oneShotRPC[T any, PT respPtr[T]](ctx context.Context, s *Sandbox, req wire.Request) (*T, error) {
	conn, l, err := s.call(ctx, req)
	if err != nil {
		return nil, err
	}
	defer l.close()
	v, err := expect[T, PT](ctx, conn)
	return v, l.done(err)
}

func collectRPC[T any, PT respPtr[T]](ctx context.Context, s *Sandbox, req wire.Request) ([]T, error) {
	var out []T
	for v, err := range streamRPC[T, PT](ctx, s, req) {
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// streamRPC sends req and yields each streamed frame of type T until Done; breaking out closes the connection, which ends the guest-side producer.
func streamRPC[T any, PT respPtr[T]](ctx context.Context, s *Sandbox, req wire.Request) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		conn, l, err := s.call(ctx, req)
		if err != nil {
			yield(zero, err)
			return
		}
		defer l.close()
		for {
			resp, err := recv(ctx, conn)
			if err != nil {
				yield(zero, err)
				return
			}
			if v, ok := resp.(PT); ok {
				if !yield(*v, nil) {
					return
				}
				continue
			}
			switch r := resp.(type) {
			case *wire.Done:
				l.reuse()
			case *wire.ErrorResp:
				l.reuse()
				yield(zero, r)
			default:
				yield(zero, unexpected(resp))
			}
			return
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
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("connection closed before a terminal frame")
		}
		return nil, err
	}
	return resp, nil
}

func unexpected(resp wire.Response) error {
	return fmt.Errorf("unexpected frame %q", resp.RespType())
}

// sendChunks feeds b to conn in frames of at most size bytes, so no frame outgrows the protocol cap.
func sendChunks(conn *silkd.Conn, size int, frame func([]byte) wire.Request, b []byte) (int, error) {
	sent := 0
	for len(b) > 0 {
		chunk := b[:min(len(b), size)]
		if err := conn.Send(frame(chunk)); err != nil {
			return sent, err
		}
		sent += len(chunk)
		b = b[len(chunk):]
	}
	return sent, nil
}
