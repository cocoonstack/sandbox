package sandbox

import (
	"bytes"
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

// portWriteChunk keeps a data frame (payload ×4/3 base64 + envelope) well
// under wire.MaxFrame.
const portWriteChunk = 1 << 20

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

func (p *PortConn) Read(b []byte) (int, error) { return p.out.Read(b) }

// Write chunks b so no frame (with its base64 and envelope overhead) can
// exceed the protocol cap — one oversized frame would kill the relay.
func (p *PortConn) Write(b []byte) (int, error) {
	sent := 0
	for len(b) > 0 {
		chunk := b[:min(len(b), portWriteChunk)]
		if err := p.conn.Send(&wire.Data{Data: chunk}); err != nil {
			return sent, err
		}
		sent += len(chunk)
		b = b[len(chunk):]
	}
	return sent, nil
}

// CloseWrite half-closes the guest socket (the server sees EOF) while reads
// keep draining its response.
func (p *PortConn) CloseWrite() error {
	return p.conn.Send(wire.DataEnd{})
}

func (p *PortConn) Close() error {
	p.closeOnce.Do(func() {
		p.stop()
		// drain can be parked in a pipe write on an unread tail; only the
		// reader side unblocks it, so teardown must close the pipe too.
		_ = p.out.CloseWithError(net.ErrClosed)
	})
	return nil
}

// LocalAddr implements net.Conn; the relay has no meaningful socket address.
func (p *PortConn) LocalAddr() net.Addr { return portAddr("sandbox-relay") }

func (p *PortConn) RemoteAddr() net.Addr { return portAddr("sandbox-guest") }

// SetDeadline implements net.Conn; deadlines are unsupported — bound the
// DialPort ctx instead.
func (p *PortConn) SetDeadline(time.Time) error { return errors.ErrUnsupported }

// SetReadDeadline implements net.Conn; unsupported, see SetDeadline.
func (p *PortConn) SetReadDeadline(time.Time) error { return errors.ErrUnsupported }

// SetWriteDeadline implements net.Conn; unsupported, see SetDeadline.
func (p *PortConn) SetWriteDeadline(time.Time) error { return errors.ErrUnsupported }

// drain relays guest bytes into the pipe until Done (clean server close →
// EOF), an error frame, or teardown; a relay dropped before its terminal
// frame surfaces as drainData's truncation error, not a clean EOF.
func (p *PortConn) drain(ctx context.Context, pw *io.PipeWriter) {
	_ = pw.CloseWithError(drainData(ctx, p.conn, func(b []byte) error {
		_, err := pw.Write(b)
		return err
	}))
}

// DialPort connects to 127.0.0.1:port inside the sandbox. The ctx governs
// the connection's lifetime: canceling it (or Close) tears the relay down.
// A port nobody listens on fails here with silkd's not_found error.
func (s *Sandbox) DialPort(ctx context.Context, port uint16) (*PortConn, error) {
	return s.openStream(ctx, &wire.PortForward{Port: port})
}

// PreviewURL mints a shareable URL that serves the sandbox's guest HTTP
// port from a plain browser, valid for ttl (the node clamps it to the
// claim's lease). Requires the node to have preview configured.
func (s *Sandbox) PreviewURL(ctx context.Context, port uint16, ttl time.Duration) (string, error) {
	body, err := encodeBody("preview", previewRequest{Token: s.token, Port: port, TTLSeconds: ttlSeconds(ttl)})
	if err != nil {
		return "", err
	}
	pr, err := doJSON[previewResponse](ctx, s.c, http.MethodPost, s.owner, "/v1/sandboxes/"+s.ID+"/preview", bytes.NewReader(body), s.c.apiToken, "preview")
	if err != nil {
		return "", err
	}
	return pr.URL, nil
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

// openStream drives the call → Ready → attach dance shared by every verb
// that turns the connection into a raw byte stream (DialPort, Lsp.Request).
func (s *Sandbox) openStream(ctx context.Context, req wire.Request) (*PortConn, error) {
	conn, done, err := s.call(ctx, req)
	if err != nil {
		return nil, err
	}
	if _, err = expect[wire.Ready](ctx, conn); err != nil {
		done()
		return nil, err
	}
	pr, pw := io.Pipe()
	p := &PortConn{conn: conn, stop: done, out: pr}
	go p.drain(ctx, pw)
	return p, nil
}

func (s *Sandbox) proxyConn(ctx context.Context, local net.Conn, port uint16) {
	defer func() { _ = local.Close() }()
	// The copy below can sit in local.Read forever; only closing the conn
	// unblocks it, so ctx teardown must reach the local side too.
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

// Half-close so the peer sees EOF while the tail still drains.
func closeWrite(conn net.Conn) {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
}

type previewRequest struct {
	Token      string `json:"token"`
	Port       uint16 `json:"port"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

type previewResponse struct {
	URL string `json:"url"`
}

type portAddr string

func (a portAddr) Network() string { return "silkd" }
func (a portAddr) String() string  { return string(a) }
