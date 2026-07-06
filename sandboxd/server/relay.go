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
)

// drainGrace bounds how long a finished relay waits for the client to
// consume the tail and close; past it the client conn is force-closed.
const (
	drainGrace = 30 * time.Second
	// auditTeeCap bounds first-line capture: real request frames are small,
	// anything larger is a payload stream not worth recording.
	auditTeeCap = 4096
)

var switchingProtocols = []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: silkd\r\nConnection: Upgrade\r\n\r\n")

// CloseRelays force-closes every in-flight relay, refuses new ones, and
// waits for the drain; http.Server.Shutdown does not track hijacked
// connections, and a relay hijacked inside the shutdown window would
// otherwise register after this snapshot and hang the drain forever.
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
	logger := log.WithFunc("server.handleAgent")
	token, ok := bearerToken(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	// The cheap header check runs first: waking is expensive and consumes
	// the hibernate snapshot, so a non-upgrade GET must not trigger it.
	if !strings.EqualFold(r.Header.Get("Upgrade"), upgradeProto) {
		w.Header().Set("Upgrade", upgradeProto)
		writeErr(w, http.StatusUpgradeRequired, "upgrade to "+upgradeProto+" required")
		return
	}
	// WakeAgentSocket restores a hibernated VM first, so the relay is the
	// transparent wake path: the client's next call just works.
	sock, err := s.mgr.WakeAgentSocket(r.Context(), r.PathValue("id"), token)
	switch {
	case writePoolErr(w, err):
		return
	case err != nil:
		logger.Errorf(r.Context(), err, "agent socket for %s", r.PathValue("id"))
		writeErr(w, http.StatusInternalServerError, "sandbox lookup failed")
		return
	}
	guest, err := s.dialer.DialSilkd(r.Context(), sock)
	if err != nil {
		logger.Errorf(r.Context(), err, "dial silkd for %s", r.PathValue("id"))
		writeErr(w, http.StatusBadGateway, "guest agent unreachable")
		return
	}
	client, bufrw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		_ = guest.Close()
		logger.Error(r.Context(), err, "hijack")
		writeErr(w, http.StatusInternalServerError, "connection cannot be hijacked")
		return
	}
	s.relay(r.PathValue("id"), client, bufrw.Reader, guest)
}

// relay writes the 101 and splices the two connections until the guest side
// finishes (silkd closes after the terminal frame) or the client vanishes.
func (s *Server) relay(id string, client net.Conn, clientBuf *bufio.Reader, guest net.Conn) {
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

	// The http server may have armed a header-read deadline on this conn.
	_ = client.SetDeadline(time.Time{})
	if _, err := client.Write(switchingProtocols); err != nil {
		return
	}

	// Bytes the client pipelined behind the request head already sit in the
	// server's bufio reader; drain exactly those, never more — reading the
	// bufio past Buffered() would race the direct conn reads below.
	clientR := io.Reader(client)
	if n := clientBuf.Buffered(); n > 0 {
		clientR = io.MultiReader(io.LimitReader(clientBuf, int64(n)), client)
	}
	if s.mgr.AuditEnabled() {
		// One connection is one RPC: the first line client→guest is the
		// request frame. The tee records it as it flows, then passes through.
		clientR = &auditTee{r: clientR, record: func(line []byte) {
			s.mgr.Audit(context.WithoutCancel(context.Background()), id, line)
		}}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(guest, clientR)
		// Client done or vanished — either way hard-close: silkd treats
		// disconnect as abort, and the protocol delimits input with explicit
		// terminal frames (stdin_close, data_end), so a TCP half-close
		// carries no meaning worth relaying through the vsock muxer.
		_ = guest.Close()
	}()

	_, _ = io.Copy(client, guest)
	closeWrite(client)
	// Wait for the client to consume the tail and close its side; the
	// deadline unblocks the splice goroutine if it never does.
	_ = client.SetReadDeadline(time.Now().Add(drainGrace))
	<-done
}

// auditTee captures the first newline-terminated line (capped) flowing
// through it, hands it to record once, then degrades to a pass-through.
type auditTee struct {
	r      io.Reader
	record func([]byte)
	buf    []byte
	done   bool
}

func (t *auditTee) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if t.done || n == 0 {
		return n, err
	}
	chunk := p[:n]
	if i := bytes.IndexByte(chunk, '\n'); i >= 0 {
		t.buf = append(t.buf, chunk[:i+1]...)
		t.done = true
		t.record(t.buf)
		t.buf = nil
		return n, err
	}
	t.buf = append(t.buf, chunk...)
	if len(t.buf) > auditTeeCap {
		t.done = true // a frame this large is payload, not addressing
		t.buf = nil
	}
	return n, err
}

// closeWrite signals EOF to the client without tearing down its read
// direction, so the tail written above survives until the client drains it.
func closeWrite(conn net.Conn) {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
}
