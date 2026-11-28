package engine

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

// portReadBuf fits silkd's data frames in one buffered read.
const portReadBuf = wire.BulkChunk*4/3 + 64

// guestPortConn adapts silkd's port_forward data frames to a plain net.Conn.
type guestPortConn struct {
	net.Conn
	r       *bufio.Reader
	wbuf    []byte
	rbuf    []byte
	pending []byte
}

func (g *guestPortConn) Read(p []byte) (int, error) {
	for len(g.pending) == 0 {
		line, err := g.readFrame()
		if errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}
		if err != nil {
			return 0, err
		}
		if data, ok := wire.DecodeData(bytes.TrimSuffix(line, []byte("\n")), &g.rbuf); ok {
			g.pending = data
			continue
		}
		resp, err := wire.DecodeResponse(line)
		if err != nil {
			return 0, fmt.Errorf("port frame: %w", err)
		}
		switch resp := resp.(type) {
		case *wire.DataResp:
			g.pending = resp.Data
		case *wire.Done:
			return 0, io.EOF
		default:
			return 0, fmt.Errorf("port stream ended: %w", frameErr(resp))
		}
	}
	n := copy(p, g.pending)
	g.pending = g.pending[n:]
	return n, nil
}

// CloseWrite half-closes the guest socket; silkd turns data_end into a TCP shutdown.
func (g *guestPortConn) CloseWrite() error {
	req, err := wire.EncodeRequest(wire.DataEnd{})
	if err != nil {
		return err
	}
	_, err = g.Conn.Write(append(req, '\n'))
	return err
}

func (g *guestPortConn) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n := min(len(p), wire.PortWriteChunk)
		g.wbuf = wire.AppendBulkRequest(g.wbuf, "data", p[:n])
		if _, err := g.Conn.Write(g.wbuf); err != nil {
			return written, err
		}
		p = p[n:]
		written += n
	}
	return written, nil
}

func (g *guestPortConn) readFrame() ([]byte, error) {
	line, err := g.r.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) && g.r.Size() < portReadBuf {
		g.r = bufio.NewReaderSize(io.MultiReader(bytes.NewReader(line), g.Conn), portReadBuf)
		line, err = g.r.ReadSlice('\n')
	}
	if errors.Is(err, bufio.ErrBufferFull) {
		return nil, fmt.Errorf("port frame exceeds %d bytes", portReadBuf)
	}
	return line, err
}
