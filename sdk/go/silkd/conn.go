// Package silkd frames a relay connection into silkd wire RPCs.
package silkd

import (
	"bufio"
	"cmp"
	"fmt"
	"io"
	"sync"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

// Conn frames a byte stream (typically the upgraded relay connection) into
// silkd RPCs. Reads are single-consumer; Send is safe for concurrent use so
// a stdin pump can interleave with the caller.
type Conn struct {
	wmu  sync.Mutex
	wbuf []byte
	rbuf []byte
	rwc  io.ReadWriteCloser
	sc   *bufio.Scanner
}

// NewConn wraps rwc; the caller keeps ownership of closing via Close.
func NewConn(rwc io.ReadWriteCloser) *Conn {
	return &Conn{rwc: rwc, sc: wire.NewFrameScanner(rwc)}
}

// Send writes one request frame.
func (c *Conn) Send(r wire.Request) error {
	switch v := r.(type) {
	case *wire.Data:
		return c.sendBulk(v.Op(), v.Data)
	case *wire.Stdin:
		return c.sendBulk(v.Op(), v.Data)
	}
	frame, err := wire.EncodeRequest(r)
	if err != nil {
		return fmt.Errorf("encode %s: %w", r.Op(), err)
	}
	frame = append(frame, '\n') // in-place: wire.EncodeRequest reserves the byte
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err = c.rwc.Write(frame)
	return err
}

// Recv reads the next response frame; io.EOF signals a clean close.
func (c *Conn) Recv() (wire.Response, error) {
	if err := c.scan(); err != nil {
		return nil, err
	}
	return wire.DecodeResponse(c.sc.Bytes())
}

// RecvTransient is Recv for a streaming consumer: a bulk frame's Data is valid only until the next RecvTransient.
func (c *Conn) RecvTransient() (wire.Response, error) {
	if err := c.scan(); err != nil {
		return nil, err
	}
	return wire.DecodeResponseInto(c.sc.Bytes(), &c.rbuf)
}

func (c *Conn) Close() error {
	return c.rwc.Close()
}

func (c *Conn) scan() error {
	if c.sc.Scan() {
		return nil
	}
	return cmp.Or(c.sc.Err(), io.EOF)
}

// sendBulk renders a data/stdin frame into the reused buffer.
func (c *Conn) sendBulk(op string, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.wbuf = wire.AppendBulkRequest(c.wbuf, op, payload)
	_, err := c.rwc.Write(c.wbuf)
	return err
}
