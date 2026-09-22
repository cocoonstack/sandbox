package server

import (
	"net/http"
	"strconv"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

var (
	switchingProtocolsTCP = []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: " + upgradeProtoTCP + "\r\nConnection: Upgrade\r\n\r\n")

	tunnelEnds = relayEnds{guest: utils.CloseWrite, client: utils.CloseWrite}
)

// handlePort is how an edge proxy reaches a guest listener on a sandbox with no NIC.
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
	s.splice(client, clientReader(client, clientBuf), guest, switchingProtocolsTCP, tunnelEnds)
}
