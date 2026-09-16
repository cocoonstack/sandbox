package server

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

// drainGrace bounds how long a finished relay waits for the client to close.
const drainGrace = 30 * time.Second

var switchingProtocols = []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: silkd\r\nConnection: Upgrade\r\n\r\n")

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
	token, ok := sandboxToken(w, r)
	if !ok {
		return
	}
	// waking consumes the hibernate snapshot, so a non-upgrade GET must not trigger it
	if !strings.EqualFold(r.Header.Get("Upgrade"), upgradeProto) {
		w.Header().Set("Upgrade", upgradeProto)
		writeErr(w, http.StatusUpgradeRequired, "upgrade to "+upgradeProto+" required")
		return
	}
	guest, done, ok := s.wakeGuest(r.Context(), w, r.PathValue("id"), token)
	if !ok {
		return
	}
	defer done()
	client, bufrw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		_ = guest.Close()
		log.WithFunc("server.handleAgent").Error(r.Context(), err, "hijack")
		writeErr(w, http.StatusInternalServerError, "connection cannot be hijacked")
		return
	}
	s.relay(r.Context(), r.PathValue("id"), client, bufrw.Reader, guest)
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

// relay writes the 101 and splices the conns until silkd closes or the client vanishes.
func (s *Server) relay(ctx context.Context, id string, client net.Conn, clientBuf *bufio.Reader, guest net.Conn) {
	s.relayMu.Lock()
	if s.relayClosed {
		s.relayMu.Unlock()
		_ = client.Close()
		_ = guest.Close()
		return
	}
	s.relays[client] = guest
	s.relayWG.Add(1)
	s.relayMu.Unlock()
	defer func() {
		_ = client.Close()
		_ = guest.Close()
		s.relayMu.Lock()
		delete(s.relays, client)
		s.relayMu.Unlock()
		s.relayWG.Done()
	}()

	// the http server may have armed a header-read deadline on this conn
	_ = client.SetDeadline(time.Time{})
	if _, err := client.Write(switchingProtocols); err != nil {
		return
	}

	// reading the bufio past Buffered() would race the direct conn reads below
	clientR := io.Reader(client)
	if n := clientBuf.Buffered(); n > 0 {
		clientR = io.MultiReader(io.LimitReader(clientBuf, int64(n)), client)
	}
	if s.mgr.AuditEnabled() {
		clientR = &auditTee{r: clientR, record: func(line []byte) {
			s.mgr.Audit(ctx, id, line)
		}}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(guest, clientR)
		// silkd delimits input with terminal frames, so a TCP half-close carries no meaning
		_ = guest.Close()
	}()

	_, _ = io.Copy(client, guest)
	utils.CloseWrite(client)
	// the read deadline unblocks the splice goroutine if the client never closes
	_ = client.SetReadDeadline(time.Now().Add(drainGrace))
	<-done
}

// auditTee records each request line it relays; input frames and the bytes past the cap pass unrecorded.
type auditTee struct {
	r      io.Reader
	record func([]byte)
	buf    []byte
	skip   bool
}

// WriteTo keeps an audited upload at bulk-sized reads; io.Copy's own buffer would split each frame into eight.
func (t *auditTee) WriteTo(w io.Writer) (int64, error) {
	var total int64
	buf := make([]byte, wire.BulkChunk)
	for {
		n, err := t.Read(buf)
		if n > 0 {
			wn, werr := w.Write(buf[:n])
			total += int64(wn)
			if werr != nil {
				return total, werr
			}
		}
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
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
