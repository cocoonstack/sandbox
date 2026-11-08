package peer

import (
	"cmp"
	"context"
	"net/http"
	"net/url"

	"github.com/projecteru2/core/log"
)

// Adopter asks peers to take over a claim their owner released.
type Adopter struct {
	Peers func() []string
	Token string
}

// Adopt tries the peers until one holds the claim and reports whether one does; with room, only a peer that could wake it adopts.
func (a *Adopter) Adopt(ctx context.Context, id, token string, room bool) bool {
	logger := log.WithFunc("peer.Adopter.Adopt")
	path := "/v1/sandboxes/" + url.PathEscape(id) + "/owner"
	if room {
		path += "?room=1"
	}
	for _, addr := range a.Peers() {
		err := peerCall(ctx, http.MethodGet, URL(addr, path), cmp.Or(token, a.Token), nil, peerCallTimeout, http.StatusOK)
		if err == nil {
			return true
		}
		logger.Warnf(ctx, "adopt %s on %s: %v", id, addr, err)
	}
	return false
}
