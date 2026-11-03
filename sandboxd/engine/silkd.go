package engine

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

const (
	silkdListMax   = 4096
	silkdReadMax   = 64 << 10
	mountInfoMax   = 1 << 20
	guestOutputMax = 4096
	// guestExecPATH is set because silkd starts the guest command with an empty environment.
	guestExecPATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)

type guestExitError struct {
	code   int32
	output string
}

func (e *guestExitError) Error() string {
	return fmt.Sprintf("exit code %d: %s", e.code, e.output)
}

// silkdSession is a dialed silkd conn bound to a ctx, with wire-typed request/reply helpers.
type silkdSession struct {
	conn net.Conn
	sc   *bufio.Scanner
	stop func() bool
}

func (s *silkdSession) send(req wire.Request) error {
	buf, err := wire.EncodeRequest(req)
	if err != nil {
		return err
	}
	if _, err := s.conn.Write(append(buf, '\n')); err != nil {
		return fmt.Errorf("write silkd frame: %w", err)
	}
	return nil
}

func (s *silkdSession) recv() (wire.Response, error) {
	if !s.sc.Scan() {
		if err := s.sc.Err(); err != nil {
			return nil, fmt.Errorf("read silkd frame: %w", err)
		}
		return nil, io.ErrUnexpectedEOF
	}
	return wire.DecodeResponse(s.sc.Bytes())
}

func (s *silkdSession) close() {
	s.stop()
	_ = s.conn.Close()
}

func (e *Engine) dialSilkdSession(ctx context.Context, vsockSocket string) (*silkdSession, error) {
	conn, err := e.DialSilkd(ctx, vsockSocket)
	if err != nil {
		return nil, err
	}
	return &silkdSession{
		conn: conn,
		sc:   wire.NewFrameScanner(conn),
		stop: context.AfterFunc(ctx, func() { _ = conn.Close() }),
	}, nil
}

// silkdStream dials per request; the node's own calls are too rare to keep a connection.
func (e *Engine) silkdStream(ctx context.Context, vsockSocket string, req wire.Request, onFrame func(wire.Response) error) error {
	s, err := e.dialSilkdSession(ctx, vsockSocket)
	if err != nil {
		return err
	}
	defer s.close()
	if err := s.send(req); err != nil {
		return err
	}
	for {
		frame, err := s.recv()
		if err != nil {
			return err
		}
		switch resp := frame.(type) {
		case *wire.Done:
			return nil
		case *wire.ErrorResp:
			return fmt.Errorf("silkd %w", resp)
		default:
			if err := onFrame(resp); err != nil {
				return err
			}
		}
	}
}

func (e *Engine) silkdList(ctx context.Context, vsockSocket, path string) ([]wire.DirEntry, error) {
	var entries []wire.DirEntry
	err := e.silkdStream(ctx, vsockSocket, wire.FsList{Path: path}, func(frame wire.Response) error {
		resp, ok := frame.(*wire.Entries)
		if !ok {
			return fmt.Errorf("unexpected silkd frame %q", frame.RespType())
		}
		entries = append(entries, resp.Entries...)
		if len(entries) > silkdListMax {
			return fmt.Errorf("silkd list of %s exceeds %d entries", path, silkdListMax)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (e *Engine) silkdReadFile(ctx context.Context, vsockSocket, path string, limit int) ([]byte, error) {
	var data []byte
	err := e.silkdStream(ctx, vsockSocket, wire.FsRead{Path: path}, func(frame wire.Response) error {
		resp, ok := frame.(*wire.DataResp)
		if !ok {
			return fmt.Errorf("unexpected silkd frame %q", frame.RespType())
		}
		data = append(data, resp.Data...)
		if len(data) > limit {
			return fmt.Errorf("silkd read of %s exceeds %d bytes", path, limit)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

// silkdOnce sends a request answered by a single T frame and no Done, so it cannot ride silkdStream.
func (e *Engine) silkdOnce[T wire.Response](ctx context.Context, vsockSocket string, req wire.Request) error {
	s, err := e.dialSilkdSession(ctx, vsockSocket)
	if err != nil {
		return err
	}
	defer s.close()
	if err = s.send(req); err != nil {
		return err
	}
	frame, err := s.recv()
	if err != nil {
		return err
	}
	if _, ok := frame.(T); ok {
		return nil
	}
	if resp, ok := frame.(*wire.ErrorResp); ok {
		return fmt.Errorf("silkd %w", resp)
	}
	return fmt.Errorf("unexpected silkd frame %q", frame.RespType())
}

func (e *Engine) silkdWriteFile(ctx context.Context, vsockSocket, path string, mode uint32, data []byte) error {
	s, err := e.dialSilkdSession(ctx, vsockSocket)
	if err != nil {
		return err
	}
	defer s.close()
	// A send racing the guest's early answer+close hits EPIPE; prefer the buffered verdict.
	sendErr := func() error {
		if serr := s.send(wire.FsWrite{Path: path, Mode: &mode}); serr != nil {
			return serr
		}
		for chunk := range slices.Chunk(data, wire.BulkChunk) {
			if serr := s.send(wire.Data{Data: chunk}); serr != nil {
				return serr
			}
		}
		return s.send(wire.DataEnd{})
	}()
	frame, err := s.recv()
	if err != nil {
		return errors.Join(sendErr, err)
	}
	switch resp := frame.(type) {
	case *wire.Done:
		return nil
	case *wire.ErrorResp:
		return fmt.Errorf("silkd %w", resp)
	default:
		return fmt.Errorf("unexpected silkd frame %q", resp.RespType())
	}
}

func (e *Engine) silkdExec(ctx context.Context, vsockSocket string, argv ...string) error {
	_, err := e.silkdOutput(ctx, vsockSocket, argv...)
	return err
}

func (e *Engine) silkdOutput(ctx context.Context, vsockSocket string, argv ...string) ([]byte, error) {
	s, err := e.dialSilkdSession(ctx, vsockSocket)
	if err != nil {
		return nil, err
	}
	defer s.close()
	sendErr := s.send(wire.Exec{Argv: argv, Env: map[string]string{"PATH": guestExecPATH}})
	if sendErr == nil {
		// A child exiting without reading stdin races this close; prefer the buffered exit frame.
		sendErr = s.send(wire.StdinClose{})
	}
	var out, stdout []byte
	for {
		frame, err := s.recv()
		if err != nil {
			return nil, errors.Join(sendErr, err)
		}
		switch resp := frame.(type) {
		case *wire.Started:
		case *wire.Stdout:
			out, stdout = appendCapped(out, resp.Data), appendCapped(stdout, resp.Data)
		case *wire.Stderr:
			out = appendCapped(out, resp.Data)
		case *wire.Exit:
			if resp.Code != 0 {
				return nil, &guestExitError{code: resp.Code, output: string(bytes.TrimSpace(out))}
			}
			return stdout, nil
		case *wire.ErrorResp:
			return nil, fmt.Errorf("silkd %w", resp)
		default:
			return nil, fmt.Errorf("unexpected silkd frame %q", resp.RespType())
		}
	}
}

func appendCapped(out, data []byte) []byte {
	if room := guestOutputMax - len(out); room > 0 {
		out = append(out, data[:min(len(data), room)]...)
	}
	return out
}
