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
	// portWriteChunk keeps each data frame (base64 ×4/3 + envelope) well
	// under silkd's 8MiB frame cap, so a large HTTP body is split across
	// frames.
	portWriteChunk = 1 << 20

	// portReadBuf fits silkd's data frames (32KiB chunks, base64-expanded)
	// in one buffered read.
	portReadBuf = 64 << 10
)

var portDataHead = []byte(`{"type":"data","data":"`)

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
		if data, ok := fastPortData(line); ok {
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

// fastPortData slices the canonical data frame's base64 out without a JSON
// parse — json.Unmarshal otherwise dominates the download relay, and the
// SDK's fastBulk sets the contract: only the exact canonical shape takes the
// slice, anything else falls back to the full parse.
func fastPortData(line []byte) ([]byte, bool) {
	after, ok := bytes.CutPrefix(line, portDataHead)
	if !ok {
		return nil, false
	}
	b64, rest, ok := bytes.Cut(after, []byte(`"`))
	if !ok || !bytes.Equal(rest, []byte("}\n")) {
		return nil, false
	}
	out := make([]byte, base64.StdEncoding.DecodedLen(len(b64)))
	n, err := base64.StdEncoding.Decode(out, b64)
	if err != nil || base64.StdEncoding.EncodedLen(n) != len(b64) {
		return nil, false
	}
	return out[:n], true
}

func (g *guestPortConn) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n := min(len(p), portWriteChunk)
		// The hot relay path renders into one reused buffer instead of
		// allocating two frame-sized slices per chunk.
		g.wbuf = wire.AppendBulkRequest(g.wbuf, "data", p[:n])
		if _, err := g.Conn.Write(g.wbuf); err != nil {
			return written, err
		}
		p = p[n:]
		written += n
	}
	return written, nil
}
