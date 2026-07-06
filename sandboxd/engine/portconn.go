package engine

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// guestPortConn adapts silkd's framed port_forward channel to a plain
// net.Conn: the guest's TCP bytes ride inside newline-JSON `data` frames
// (base64 payloads), so writes wrap into `{"op":"data",...}` frames and
// reads unwrap `{"type":"data",...}` frames back to raw bytes. This is the
// server-side twin of the SDK's PortConn, so a reverse proxy can splice HTTP
// straight through it.
type guestPortConn struct {
	net.Conn
	r       *bufio.Reader
	pending []byte
}

func newGuestPortConn(conn net.Conn) *guestPortConn {
	return &guestPortConn{Conn: conn, r: bufio.NewReader(conn)}
}

func (g *guestPortConn) Read(p []byte) (int, error) {
	for len(g.pending) == 0 {
		line, err := g.r.ReadBytes('\n')
		if err != nil {
			return 0, err
		}
		var frame struct {
			Type string `json:"type"`
			Data string `json:"data"`
		}
		if err := json.Unmarshal(line, &frame); err != nil {
			return 0, fmt.Errorf("port frame: %w", err)
		}
		switch frame.Type {
		case "data":
			decoded, err := base64.StdEncoding.DecodeString(frame.Data)
			if err != nil {
				return 0, fmt.Errorf("port data: %w", err)
			}
			g.pending = decoded
		case "done", "":
			return 0, errPortClosed
		default: // error frame or anything terminal
			return 0, fmt.Errorf("port stream ended: %s", frame.Type)
		}
	}
	n := copy(p, g.pending)
	g.pending = g.pending[n:]
	return n, nil
}

func (g *guestPortConn) Write(p []byte) (int, error) {
	frame := struct {
		V    int    `json:"v"`
		Op   string `json:"op"`
		Data []byte `json:"data"`
	}{V: 1, Op: "data", Data: p}
	line, err := json.Marshal(frame)
	if err != nil {
		return 0, err
	}
	if _, err := g.Conn.Write(append(line, '\n')); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (g *guestPortConn) SetDeadline(t time.Time) error      { return g.Conn.SetDeadline(t) }
func (g *guestPortConn) SetReadDeadline(t time.Time) error  { return g.Conn.SetReadDeadline(t) }
func (g *guestPortConn) SetWriteDeadline(t time.Time) error { return g.Conn.SetWriteDeadline(t) }
