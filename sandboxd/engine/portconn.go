package engine

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
)

const (
	// portWriteChunk keeps each data frame (base64 ×4/3 + envelope) well
	// under silkd's 8MiB frame cap, so a large HTTP body is split across
	// frames.
	portWriteChunk = 1 << 20

	// portReadBuf fits silkd's data frames (32KiB chunks, base64-expanded)
	// in one buffered read.
	portReadBuf = 64 << 10
)

// guestPortConn adapts silkd's newline-JSON data-frame port_forward channel to
// a plain net.Conn (base64 payloads), the server-side twin of the SDK's
// PortConn, so a reverse proxy can splice HTTP straight through it.
type guestPortConn struct {
	net.Conn
	r       *bufio.Reader
	wbuf    []byte
	pending []byte
}

func newGuestPortConn(conn net.Conn) *guestPortConn {
	return &guestPortConn{Conn: conn, r: bufio.NewReaderSize(conn, portReadBuf)}
}

func (g *guestPortConn) Read(p []byte) (int, error) {
	for len(g.pending) == 0 {
		line, err := g.r.ReadBytes('\n')
		if err != nil {
			return 0, err
		}
		var frame struct {
			Type string `json:"type"`
			Data []byte `json:"data"`
		}
		if err := json.Unmarshal(line, &frame); err != nil {
			return 0, fmt.Errorf("port frame: %w", err)
		}
		switch frame.Type {
		case "data":
			g.pending = frame.Data
		case "done", "":
			return 0, io.EOF // the guest closed the forwarded port
		default: // error frame or anything terminal
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
		// Hand-built envelope: the hot relay path reuses one buffer instead
		// of allocating two frame-sized slices per chunk (base64 needs no
		// JSON escaping).
		g.wbuf = append(g.wbuf[:0], `{"v":1,"op":"data","data":"`...)
		g.wbuf = base64.StdEncoding.AppendEncode(g.wbuf, p[:n])
		g.wbuf = append(g.wbuf, '"', '}', '\n')
		if _, err := g.Conn.Write(g.wbuf); err != nil {
			return written, err
		}
		p = p[n:]
		written += n
	}
	return written, nil
}
