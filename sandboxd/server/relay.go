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
	// WakeAgentSocket restores a hibernated VM first, making the relay the transparent wake path
	sock, err := s.mgr.WakeAgentSocket(r.Context(), r.PathValue("id"), token)
	switch {
	case writePoolErr(w, err):
		return
	case err != nil:
		log.WithFunc("server.handleAgent").Errorf(r.Context(), err, "agent socket for %s", r.PathValue("id"))
		writeErr(w, http.StatusInternalServerError, "sandbox lookup failed")
		return
	}
	guest, err := s.dialer.DialSilkd(r.Context(), sock)
	if err != nil {
		log.WithFunc("server.handleAgent").Errorf(r.Context(), err, "dial silkd for %s", r.PathValue("id"))
		writeErr(w, http.StatusBadGateway, "guest agent unreachable")
		return
	}
	client, bufrw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		_ = guest.Close()
		log.WithFunc("server.handleAgent").Error(r.Context(), err, "hijack")
		writeErr(w, http.StatusInternalServerError, "connection cannot be hijacked")
		return
	}
	s.relay(r.Context(), r.PathValue("id"), client, bufrw.Reader, guest)
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
		// one connection is one RPC, so the first line client→guest is the request frame
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

// auditTee captures the first line flowing through it, then degrades to a pass-through.
type auditTee struct {
	r      io.Reader
	record func([]byte)
	buf    []byte
	done   bool
}

func (t *auditTee) WriteTo(w io.Writer) (int64, error) {
	var total int64
	buf := make([]byte, 4096)
	for !t.done {
		n, err := t.Read(buf)
		if n > 0 {
			wn, werr := w.Write(buf[:n])
			total += int64(wn)
			if werr != nil {
				return total, werr
			}
		}
		if err != nil {
			return total, err
		}
	}
	n, err := io.Copy(w, t.r)
	return total + n, err
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
	if len(t.buf) > pool.AuditLineCap {
		t.done = true // a frame this large is payload, not addressing
		t.buf = nil
	}
	return n, err
}
