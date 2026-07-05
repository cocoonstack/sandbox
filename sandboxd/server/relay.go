package server

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/projecteru2/core/log"
)

// drainGrace bounds how long a finished relay waits for the client to
// consume the tail and close; past it the client conn is force-closed.
const drainGrace = 30 * time.Second

var switchingProtocols = []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: silkd\r\nConnection: Upgrade\r\n\r\n")

// CloseRelays force-closes every in-flight relay and waits for them to
// drain; http.Server.Shutdown does not track hijacked connections.
func (s *Server) CloseRelays() {
	s.relayMu.Lock()
	for client, guest := range s.relays {
		_ = client.Close()
		_ = guest.Close()
	}
	s.relayMu.Unlock()
	s.relayWG.Wait()
}

func (s *Server) handleAgent(w http.ResponseWriter, r *http.Request) {
	token, ok := bearerToken(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	sock, err := s.mgr.AgentSocket(r.PathValue("id"), token)
	if err != nil {
		writeErr(w, http.StatusNotFound, "unknown sandbox")
		return
	}
	if !strings.EqualFold(r.Header.Get("Upgrade"), upgradeProto) {
		w.Header().Set("Upgrade", upgradeProto)
		writeErr(w, http.StatusUpgradeRequired, "upgrade to "+upgradeProto+" required")
		return
	}
	logger := log.WithFunc("server.handleAgent")
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
	s.relay(client, bufrw.Reader, guest)
}

// relay writes the 101 and splices the two connections until the guest side
// finishes (silkd closes after the terminal frame) or the client vanishes.
func (s *Server) relay(client net.Conn, clientBuf *bufio.Reader, guest net.Conn) {
	s.relayMu.Lock()
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

// closeWrite signals EOF to the client without tearing down its read
// direction, so the tail written above survives until the client drains it.
func closeWrite(conn net.Conn) {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
}
