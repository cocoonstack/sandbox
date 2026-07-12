package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
)

const (
	silkdChunk    = 256 * 1024
	maxSilkdFrame = 8 << 20
)

// silkdFrame is one newline-JSON frame silkd replies with; a given reply
// populates only the fields its type carries.
type silkdFrame struct {
	Type    string `json:"type"`
	Code    int32  `json:"code"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
	Data    []byte `json:"data"`
}

// silkdSession is a dialed silkd conn whose lifetime is bound to a ctx: the
// request/reply helpers frame newline-JSON, and close releases both the
// ctx hook and the conn. The hot port-forward relay (portconn.go) frames its
// own data path and does not use this.
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
		sc:   silkdScanner(conn),
		stop: context.AfterFunc(ctx, func() { _ = conn.Close() }),
	}, nil
}

func (s *silkdSession) send(frame map[string]any) error {
	buf, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	if _, err := s.conn.Write(append(buf, '\n')); err != nil {
		return fmt.Errorf("write silkd frame: %w", err)
	}
	return nil
}

func (s *silkdSession) recv() (silkdFrame, error) {
	if !s.sc.Scan() {
		if err := s.sc.Err(); err != nil {
			return silkdFrame{}, fmt.Errorf("read silkd frame: %w", err)
		}
		return silkdFrame{}, io.ErrUnexpectedEOF
	}
	var frame silkdFrame
	if err := json.Unmarshal(s.sc.Bytes(), &frame); err != nil {
		return silkdFrame{}, fmt.Errorf("parse silkd frame: %w", err)
	}
	return frame, nil
}

func (s *silkdSession) close() {
	s.stop()
	_ = s.conn.Close()
}

func silkdScanner(conn net.Conn) *bufio.Scanner {
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 64*1024), maxSilkdFrame)
	return sc
}
