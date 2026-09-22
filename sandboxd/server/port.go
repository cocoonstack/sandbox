package server

import (
	"net"
	"net/http"
	"strconv"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

var switchingProtocolsTCP = []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: " + upgradeProtoTCP + "\r\nConnection: Upgrade\r\n\r\n")

// handlePort splices the caller onto 127.0.0.1:port inside the guest, so an edge proxy reaches a guest listener without a NIC.
func (s *Server) handlePort(w http.ResponseWriter, r *http.Request) {
	token, ok := upgradeGate(w, r, upgradeProtoTCP)
	if !ok {
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
	client, clientBuf, ok := hijackClient(r.Context(), w, guest)
	if !ok {
		return
	}
	// silkd stops feeding the guest socket once the guest reports EOF, so half-closing
	// toward the client would advertise a write direction that discards what it accepts
	s.splice(client, clientReader(client, clientBuf), guest, switchingProtocolsTCP,
		utils.CloseWrite, func(client net.Conn) { _ = client.Close() })
}
