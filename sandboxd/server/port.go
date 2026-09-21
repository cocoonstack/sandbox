package server

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

var switchingProtocolsTCP = []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: " + upgradeProtoTCP + "\r\nConnection: Upgrade\r\n\r\n")

// handlePort splices the caller onto 127.0.0.1:port inside the guest, so an edge proxy reaches a guest listener without a NIC.
func (s *Server) handlePort(w http.ResponseWriter, r *http.Request) {
	token, ok := sandboxToken(w, r)
	if !ok {
		return
	}
	// waking consumes the hibernate snapshot, so a non-upgrade GET must not trigger it
	if !strings.EqualFold(r.Header.Get("Upgrade"), upgradeProtoTCP) {
		w.Header().Set("Upgrade", upgradeProtoTCP)
		writeErr(w, http.StatusUpgradeRequired, "upgrade to "+upgradeProtoTCP+" required")
		return
	}
	port, err := strconv.ParseUint(r.PathValue("port"), 10, 16)
	if err != nil || port == 0 {
		writeErr(w, http.StatusBadRequest, "port must be 1-65535")
		return
	}
	id := r.PathValue("id")
	guest, err := s.mgr.DialPort(r.Context(), id, pool.Cred{Token: token}, uint16(port))
	switch {
	case writePoolErr(w, err):
		return
	case err != nil:
		log.WithFunc("server.handlePort").Errorf(r.Context(), err, "dial port %d of %s", port, id)
		writeErr(w, http.StatusBadGateway, "guest port unreachable")
		return
	}
	client, bufrw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		_ = guest.Close()
		log.WithFunc("server.handlePort").Error(r.Context(), err, "hijack")
		writeErr(w, http.StatusInternalServerError, "connection cannot be hijacked")
		return
	}
	s.relayPort(client, bufrw.Reader, guest)
}

// relayPort splices a hijacked client onto a guest port, carrying each direction's half-close through.
func (s *Server) relayPort(client net.Conn, clientBuf *bufio.Reader, guest net.Conn) {
	release, ok := s.trackRelay(client, guest)
	if !ok {
		return
	}
	defer release()

	// the http server may have armed a header-read deadline on this conn
	_ = client.SetDeadline(time.Time{})
	if _, err := client.Write(switchingProtocolsTCP); err != nil {
		return
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(guest, clientReader(client, clientBuf))
		utils.CloseWrite(guest)
	}()

	_, _ = io.Copy(client, guest)
	utils.CloseWrite(client)
	// the read deadline unblocks the splice goroutine if the client never closes
	_ = client.SetReadDeadline(time.Now().Add(drainGrace))
	<-done
}
