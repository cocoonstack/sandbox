package sandbox

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

type previewRequest struct {
	Token      string `json:"token"`
	Port       uint16 `json:"port"`
	TTLSeconds int    `json:"ttl_seconds,omitzero"`
}

type previewResponse struct {
	URL string `json:"url"`
}

var _ net.Conn = (*PortConn)(nil)

// PortConn is a net.Conn to a guest TCP port over the silkd relay; Read returns io.EOF when the guest server closes.
type PortConn struct {
	conn *silkd.Conn
	stop func()
	out  *io.PipeReader

	closeOnce sync.Once
}

func (p *PortConn) Read(b []byte) (int, error) { return p.out.Read(b) }

// Write chunks b because one frame over the protocol cap kills the relay.
func (p *PortConn) Write(b []byte) (int, error) {
	return sendChunks(p.conn, wire.PortWriteChunk, func(chunk []byte) wire.Request { return &wire.Data{Data: chunk} }, b)
}

// CloseWrite half-closes the guest socket while reads keep draining its response.
func (p *PortConn) CloseWrite() error {
	return p.conn.Send(wire.DataEnd{})
}

func (p *PortConn) Close() error {
	p.closeOnce.Do(func() {
		p.stop()
		// A drain parked in a pipe write on an unread tail unblocks only from the reader side.
		_ = p.out.CloseWithError(net.ErrClosed)
	})
	return nil
}

func (p *PortConn) LocalAddr() net.Addr { return portAddr("sandbox-relay") }

func (p *PortConn) RemoteAddr() net.Addr { return portAddr("sandbox-guest") }

// SetDeadline implements net.Conn; deadlines are unsupported, so bound the DialPort ctx instead.
func (p *PortConn) SetDeadline(time.Time) error { return errors.ErrUnsupported }

func (p *PortConn) SetReadDeadline(time.Time) error { return errors.ErrUnsupported }

func (p *PortConn) SetWriteDeadline(time.Time) error { return errors.ErrUnsupported }

// drain relays guest bytes into the pipe; a relay dropped before its terminal frame ends in drainData's truncation error, not EOF.
func (p *PortConn) drain(ctx context.Context, pw *io.PipeWriter) {
	_ = pw.CloseWithError(drainData(ctx, p.conn, func(b []byte) error {
		_, err := pw.Write(b)
		return err
	}))
}

// DialPort connects to 127.0.0.1:port inside the sandbox for the lifetime of ctx; a closed port fails with not_found.
func (s *Sandbox) DialPort(ctx context.Context, port uint16) (*PortConn, error) {
	return s.openStream(ctx, &wire.PortForward{Port: port})
}

// PreviewURL mints a browser URL for a guest HTTP port, valid for ttl clamped to the lease.
func (s *Sandbox) PreviewURL(ctx context.Context, port uint16, ttl time.Duration) (string, error) {
	body, err := encodeBody("preview", previewRequest{Token: s.token, Port: port, TTLSeconds: ttlSeconds(ttl)})
	if err != nil {
		return "", err
	}
	pr, _, err := s.ownerJSON[previewResponse](ctx, http.MethodPost, "/preview", body, s.c.apiToken, "preview")
	if err != nil {
		return "", err
	}
	return pr.URL, nil
}

// ProxyPort pipes every connection accepted on localAddr to the sandbox port; canceling ctx tears down the live ones.
func (s *Sandbox) ProxyPort(ctx context.Context, localAddr string, port uint16) (net.Listener, error) {
	l, err := net.Listen("tcp", localAddr)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			local, err := l.Accept()
			if err != nil {
				return
			}
			go s.proxyConn(ctx, local, port)
		}
	}()
	return l, nil
}

// openStream turns a call into a raw byte stream.
func (s *Sandbox) openStream(ctx context.Context, req wire.Request) (p *PortConn, err error) {
	err = s.retryMoved(ctx, func() error {
		conn, l, err := s.call(ctx, req)
		if err != nil {
			return err
		}
		if _, err = expect[wire.Ready](ctx, conn); err != nil {
			return l.done(err)
		}
		pr, pw := io.Pipe()
		p = &PortConn{conn: conn, stop: l.close, out: pr}
		go p.drain(ctx, pw)
		return nil
	})
	return p, err
}

func (s *Sandbox) proxyConn(ctx context.Context, local net.Conn, port uint16) {
	defer func() { _ = local.Close() }()
	// The copy can sit in local.Read forever; only closing the conn unblocks it, so ctx teardown reaches local too
	unarm := context.AfterFunc(ctx, func() { _ = local.Close() })
	defer unarm()
	guest, err := s.DialPort(ctx, port)
	if err != nil {
		return
	}
	defer func() { _ = guest.Close() }()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(guest, local)
		_ = guest.CloseWrite()
	}()
	_, _ = io.Copy(local, guest)
	closeWrite(local)
	<-done
}

type portAddr string

func (a portAddr) Network() string { return "silkd" }
func (a portAddr) String() string  { return string(a) }

// closeWrite half-closes so the peer sees EOF while the tail still drains.
func closeWrite(conn net.Conn) {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
}
