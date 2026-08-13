package engine

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

const silkdChunk = 256 * 1024

// silkdSession is a dialed silkd conn bound to a ctx, with wire-typed
// request/reply helpers. The hot port-forward relay (portconn.go) does not use it.
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

// silkdStream serves one request per dial: silkd handles a single request
// per connection.
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
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (e *Engine) silkdReadFile(ctx context.Context, vsockSocket, path string) ([]byte, error) {
	var data []byte
	err := e.silkdStream(ctx, vsockSocket, wire.FsRead{Path: path}, func(frame wire.Response) error {
		resp, ok := frame.(*wire.DataResp)
		if !ok {
			return fmt.Errorf("unexpected silkd frame %q", frame.RespType())
		}
		data = append(data, resp.Data...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

// silkdStat answers with a single Stat frame and no terminal Done, unlike the
// streaming ops above, so it dials and reads one frame directly.
func (e *Engine) silkdStat(ctx context.Context, vsockSocket, path string) (wire.FileInfo, error) {
	s, err := e.dialSilkdSession(ctx, vsockSocket)
	if err != nil {
		return wire.FileInfo{}, err
	}
	defer s.close()
	if err = s.send(wire.FsStat{Path: path}); err != nil {
		return wire.FileInfo{}, err
	}
	frame, err := s.recv()
	if err != nil {
		return wire.FileInfo{}, err
	}
	switch resp := frame.(type) {
	case *wire.Stat:
		return resp.Info, nil
	case *wire.ErrorResp:
		return wire.FileInfo{}, fmt.Errorf("silkd %w", resp)
	default:
		return wire.FileInfo{}, fmt.Errorf("unexpected silkd frame %q", frame.RespType())
	}
}
