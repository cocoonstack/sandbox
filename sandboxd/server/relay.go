package server

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const (
	// drainGrace bounds how long a finished silkd relay waits for the client to close.
	drainGrace = 30 * time.Second

	// the keepAlive trio fails a relay whose client vanished without a FIN in about two minutes.
	keepAliveIdle     = 60 * time.Second
	keepAliveInterval = 15 * time.Second
	keepAliveCount    = 4
)

var (
	switchingProtocols = []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: silkd\r\nConnection: Upgrade\r\n\r\n")

	// silkd delimits input with terminal frames, so a TCP half-close carries no meaning here.
	silkdEnds = relayEnds{guest: func(c net.Conn) { _ = c.Close() }, client: drainClient}
)

// CloseRelays force-closes in-flight relays; http.Server.Shutdown does not track hijacked conns.
func (s *Server) CloseRelays() {
	s.relayMu.Lock()
	s.relayClosed = true
	for client, guest := range s.relays {
		_ = client.Close()
		_ = guest.Close()
	}
	s.relayMu.Unlock()
	s.relayWG.Wait()
}

func (s *Server) handleAgent(w http.ResponseWriter, r *http.Request) {
	token, ok := upgradeGate(w, r, upgradeProto)
	if !ok {
		return
	}
	guest, done, ok := s.wakeGuest(r.Context(), w, r.PathValue("id"), token)
	if !ok {
		return
	}
	defer done()
	client, clientBuf, ok := hijackClient(r.Context(), w, guest)
	if !ok {
		return
	}
	s.relay(r.Context(), r.PathValue("id"), client, clientBuf, guest)
}

// wakeGuest dials the sandbox's silkd, waking a hibernated VM first; on failure it has already answered the request.
func (s *Server) wakeGuest(ctx context.Context, w http.ResponseWriter, id, token string) (guest net.Conn, done func(), ok bool) {
	sock, done, err := s.mgr.WakeAgentSocket(ctx, id, token)
	switch {
	case writePoolErr(w, err):
		return nil, nil, false
	case err != nil:
		log.WithFunc("server.wakeGuest").Errorf(ctx, err, "agent socket for %s", id)
		writeErr(w, http.StatusInternalServerError, "sandbox lookup failed")
		return nil, nil, false
	}
	guest, err = s.dialer.DialSilkd(ctx, sock)
	if err != nil {
		done()
		log.WithFunc("server.wakeGuest").Errorf(ctx, err, "dial silkd for %s", id)
		writeErr(w, http.StatusBadGateway, "guest agent unreachable")
		return nil, nil, false
	}
	return guest, done, true
}

func (s *Server) relay(ctx context.Context, id string, client net.Conn, clientBuf *bufio.Reader, guest net.Conn) {
	clientR := clientReader(client, clientBuf)
	if s.mgr.AuditEnabled() {
		clientR = &auditTee{r: clientR, record: func(line []byte) {
			s.mgr.Audit(ctx, id, line)
		}}
	}
	s.splice(client, clientR, guest, switchingProtocols, silkdEnds)
}

// relayEnds is what a finished direction means to the peer that did not end it.
type relayEnds struct {
	guest  func(net.Conn)
	client func(net.Conn)
}

func (s *Server) splice(client net.Conn, clientR io.Reader, guest net.Conn, hello []byte, ends relayEnds) {
	release, ok := s.trackRelay(client, guest)
	if !ok {
		return
	}
	defer release()

	// the http server may have armed a header-read deadline on this conn
	_ = client.SetDeadline(time.Time{})
	keepAlive(client)
	if _, err := client.Write(hello); err != nil {
		return
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// the wrappers hide ReadFrom/WriteTo so the copy keeps silkd's frame size instead of io.Copy's 32 KiB
		_, err := io.CopyBuffer(struct{ io.Writer }{guest}, struct{ io.Reader }{clientR}, make([]byte, wire.BulkChunk))
		if err != nil {
			// the client died rather than half-closing, so nothing is left to answer it
			_ = guest.Close()
			return
		}
		ends.guest(guest)
	}()

	_, _ = io.Copy(client, guest)
	ends.client(client)
	<-done
}

// trackRelay registers a hijacked pair for CloseRelays; !ok means the server is already draining.
func (s *Server) trackRelay(client, guest net.Conn) (func(), bool) {
	s.relayMu.Lock()
	if s.relayClosed {
		s.relayMu.Unlock()
		_ = client.Close()
		_ = guest.Close()
		return nil, false
	}
	s.relays[client] = guest
	s.relayWG.Add(1)
	s.relayMu.Unlock()
	return func() {
		_ = client.Close()
		_ = guest.Close()
		s.relayMu.Lock()
		delete(s.relays, client)
		s.relayMu.Unlock()
		s.relayWG.Done()
	}, true
}

// keepAlive bounds a client that dies without a FIN; without it the splice goroutine blocks forever.
func keepAlive(client net.Conn) {
	tcp, ok := client.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcp.SetKeepAliveConfig(net.KeepAliveConfig{
		Enable:   true,
		Idle:     keepAliveIdle,
		Interval: keepAliveInterval,
		Count:    keepAliveCount,
	})
}

// drainClient half-closes; the read deadline is what unblocks the splice goroutine.
func drainClient(client net.Conn) {
	utils.CloseWrite(client)
	_ = client.SetReadDeadline(time.Now().Add(drainGrace))
}

// clientReader replays what the http server already buffered; reading the bufio past Buffered() would race the direct conn reads.
func clientReader(client net.Conn, clientBuf *bufio.Reader) io.Reader {
	if n := clientBuf.Buffered(); n > 0 {
		return io.MultiReader(io.LimitReader(clientBuf, int64(n)), client)
	}
	return client
}

// auditTee records each request line it relays; input frames and the bytes past the cap pass unrecorded.
type auditTee struct {
	r      io.Reader
	record func([]byte)
	buf    []byte
	skip   bool
}

func (t *auditTee) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	rest := p[:n]
	for len(rest) > 0 {
		i := bytes.IndexByte(rest, '\n')
		if i < 0 {
			t.take(rest)
			break
		}
		t.take(rest[:i])
		t.endLine()
		rest = rest[i+1:]
	}
	return n, err
}

// take keeps a line's head up to the cap; a request frame that outgrows it is recorded once, oversized.
func (t *auditTee) take(b []byte) {
	if t.skip {
		return
	}
	if room := pool.AuditLineCap + 1 - len(t.buf); len(b) > room {
		b = b[:room]
	}
	t.buf = append(t.buf, b...)
	if len(t.buf) <= pool.AuditLineCap {
		return
	}
	t.skip = true
	if !wire.IsContinuation(t.buf) {
		t.record(t.buf)
	}
	t.buf = t.buf[:0]
}

func (t *auditTee) endLine() {
	if !t.skip && len(t.buf) > 0 && !wire.IsContinuation(t.buf) {
		t.record(t.buf)
	}
	t.buf = t.buf[:0]
	t.skip = false
}
