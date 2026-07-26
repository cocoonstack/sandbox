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
