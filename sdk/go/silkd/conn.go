package silkd

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"sync"
)

// Conn frames a byte stream (typically the upgraded relay connection) into
// silkd RPCs. Reads are single-consumer; Send is safe for concurrent use so
// a stdin pump can interleave with the caller.
type Conn struct {
	wmu  sync.Mutex
	wbuf []byte
	rwc  io.ReadWriteCloser
	sc   *bufio.Scanner
}

// NewConn wraps rwc; the caller keeps ownership of closing via Close.
func NewConn(rwc io.ReadWriteCloser) *Conn {
	sc := bufio.NewScanner(rwc)
	sc.Buffer(make([]byte, 64*1024), MaxFrame)
	return &Conn{rwc: rwc, sc: sc}
}

// Send writes one request frame. Bulk payload frames (data, stdin) take a
// hand-built envelope instead of json.Marshal — the upload twin of the
// server relay's portWriteChunk.
func (c *Conn) Send(r Request) error {
	switch v := r.(type) {
	case *Data:
		return c.sendBulk(v.Op(), v.Data)
	case *Stdin:
		return c.sendBulk(v.Op(), v.Data)
	}
	frame, err := EncodeRequest(r)
	if err != nil {
		return fmt.Errorf("encode %s: %w", r.Op(), err)
	}
	frame = append(frame, '\n') // in-place: EncodeRequest reserves the byte
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err = c.rwc.Write(frame)
	return err
}

// Recv reads the next response frame; io.EOF signals a clean close.
func (c *Conn) Recv() (Response, error) {
	if !c.sc.Scan() {
		if err := c.sc.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	return DecodeResponse(c.sc.Bytes())
}

func (c *Conn) Close() error {
	return c.rwc.Close()
}

// sendBulk writes a payload frame from a reused buffer, skipping the two
// frame-sized allocations and the JSON escape rescan json.Marshal costs (the
// base64 alphabet needs no escaping).
func (c *Conn) sendBulk(op string, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.wbuf = append(c.wbuf[:0], requestHead...)
	c.wbuf = append(c.wbuf, op...)
	c.wbuf = append(c.wbuf, `","data":"`...)
	c.wbuf = base64.StdEncoding.AppendEncode(c.wbuf, payload)
	c.wbuf = append(c.wbuf, '"', '}', '\n')
	_, err := c.rwc.Write(c.wbuf)
	return err
}
