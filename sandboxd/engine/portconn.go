package engine

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

const (
	// portWriteChunk keeps each data frame well under silkd's 8MiB frame cap.
	portWriteChunk = 1 << 20

	// portReadBuf fits silkd's data frames in one buffered read.
	portReadBuf = 64 << 10
)

var portDataHead = []byte(`{"type":"data","data":"`)

// guestPortConn adapts silkd's port_forward data frames to a plain net.Conn.
type guestPortConn struct {
	net.Conn
	r       *bufio.Reader
	wbuf    []byte
	rbuf    []byte
	pending []byte
}

func newGuestPortConn(conn net.Conn, r *bufio.Reader) *guestPortConn {
	return &guestPortConn{Conn: conn, r: r}
}

func (g *guestPortConn) Read(p []byte) (int, error) {
	for len(g.pending) == 0 {
		line, err := g.r.ReadBytes('\n')
		if err != nil {
			return 0, err
		}
		if data, ok := g.fastPortData(line); ok {
			g.pending = data
			continue
		}
		var frame struct {
			Type string `json:"type"`
			Data []byte `json:"data"`
		}
		if err := json.Unmarshal(line, &frame); err != nil {
			return 0, fmt.Errorf("port frame: %w", err)
		}
		switch frame.Type {
		case "data": //nolint:goconst // wire tag, kept literal by design
			g.pending = frame.Data
		case "done", "":
			return 0, io.EOF // the guest closed the forwarded port
		default:
			return 0, fmt.Errorf("port stream ended: %s", frame.Type)
		}
	}
	n := copy(p, g.pending)
	g.pending = g.pending[n:]
	return n, nil
}

func (g *guestPortConn) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n := min(len(p), portWriteChunk)
		// the hot relay path reuses one buffer instead of allocating per chunk.
		g.wbuf = wire.AppendBulkRequest(g.wbuf, "data", p[:n])
		if _, err := g.Conn.Write(g.wbuf); err != nil {
			return written, err
		}
		p = p[n:]
		written += n
	}
	return written, nil
}

// fastPortData slices the canonical data frame's base64 out without a JSON parse.
func (g *guestPortConn) fastPortData(line []byte) ([]byte, bool) {
	after, ok := bytes.CutPrefix(line, portDataHead)
	if !ok {
		return nil, false
	}
	b64, rest, ok := bytes.Cut(after, []byte(`"`))
	if !ok || !bytes.Equal(rest, []byte("}\n")) {
		return nil, false
	}
	if size := base64.StdEncoding.DecodedLen(len(b64)); cap(g.rbuf) < size {
		g.rbuf = make([]byte, size)
	} else {
		g.rbuf = g.rbuf[:size]
	}
	n, err := base64.StdEncoding.Decode(g.rbuf, b64)
	if err != nil || base64.StdEncoding.EncodedLen(n) != len(b64) {
		return nil, false
	}
	return g.rbuf[:n], true
}
