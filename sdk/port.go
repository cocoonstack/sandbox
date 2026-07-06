package sandbox

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cocoonstack/sandbox/sdk/silkd"
)

var _ net.Conn = (*PortConn)(nil)

// PortConn is a net.Conn to a TCP port inside the sandbox, relayed over the
// silkd protocol (works on the no-network lane — the vsock relay is its only
// transport). Read returns io.EOF when the guest server closes; CloseWrite
// half-closes the guest socket. Deadlines are not supported (bound the
// DialPort ctx instead).
type PortConn struct {
	conn *silkd.Conn
	stop func()
	out  *io.PipeReader

	closeOnce sync.Once
}

// DialPort connects to 127.0.0.1:port inside the sandbox. The ctx governs
// the connection's lifetime: canceling it (or Close) tears the relay down.
// A port nobody listens on fails here with silkd's not_found error.
func (s *Sandbox) DialPort(ctx context.Context, port uint16) (*PortConn, error) {
	conn, done, err := s.dial(ctx)
	if err != nil {
		return nil, err
	}
	if err = conn.Send(&silkd.PortForward{Port: port}); err != nil {
		done()
		return nil, err
	}
	if _, err = expect[silkd.Ready](ctx, conn); err != nil {
		done()
		return nil, err
	}
	pr, pw := io.Pipe()
	p := &PortConn{conn: conn, stop: done, out: pr}
	go p.drain(pw)
	return p, nil
}

// ProxyPort listens on localAddr (e.g. "127.0.0.1:0") and pipes every
// accepted connection to the sandbox port, so unmodified local tools reach
// the guest server. Closing the listener stops new connections; canceling
// ctx tears down the live ones.
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

func (s *Sandbox) proxyConn(ctx context.Context, local net.Conn, port uint16) {
	defer func() { _ = local.Close() }()
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

// closeWrite half-closes conn's write side when it supports it, so the peer
// sees EOF while the tail still drains.
func closeWrite(conn net.Conn) {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
}

func (p *PortConn) Read(b []byte) (int, error) { return p.out.Read(b) }

func (p *PortConn) Write(b []byte) (int, error) {
	if err := p.conn.Send(&silkd.Data{Data: b}); err != nil {
		return 0, err
	}
	return len(b), nil
}

// CloseWrite half-closes the guest socket (the server sees EOF) while reads
// keep draining its response.
func (p *PortConn) CloseWrite() error {
	return p.conn.Send(silkd.DataEnd{})
}

func (p *PortConn) Close() error {
	p.closeOnce.Do(p.stop)
	return nil
}

// LocalAddr implements net.Conn; the relay has no meaningful socket address.
func (p *PortConn) LocalAddr() net.Addr { return portAddr("sandbox-relay") }

// RemoteAddr implements net.Conn.
func (p *PortConn) RemoteAddr() net.Addr { return portAddr("sandbox-guest") }

// SetDeadline implements net.Conn; deadlines are unsupported — bound the
// DialPort ctx instead.
func (p *PortConn) SetDeadline(time.Time) error { return errors.ErrUnsupported }

// SetReadDeadline implements net.Conn; unsupported, see SetDeadline.
func (p *PortConn) SetReadDeadline(time.Time) error { return errors.ErrUnsupported }

// SetWriteDeadline implements net.Conn; unsupported, see SetDeadline.
func (p *PortConn) SetWriteDeadline(time.Time) error { return errors.ErrUnsupported }

// drain relays guest bytes into the pipe until Done (clean server close →
// EOF), an error frame, or teardown.
func (p *PortConn) drain(pw *io.PipeWriter) {
	for {
		resp, err := p.conn.Recv()
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		switch r := resp.(type) {
		case *silkd.DataResp:
			if _, err := pw.Write(r.Data); err != nil {
				return
			}
		case *silkd.Done:
			_ = pw.Close()
			return
		case *silkd.ErrorResp:
			_ = pw.CloseWithError(r)
			return
		default:
			_ = pw.CloseWithError(unexpected(resp))
			return
		}
	}
}

type portAddr string

func (a portAddr) Network() string { return "silkd" }
func (a portAddr) String() string  { return string(a) }
