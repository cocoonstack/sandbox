package peer

import (
	"context"
	"net/http"
	"net/url"

	"github.com/projecteru2/core/log"
)

// Adopter asks peers to take over a claim their owner released.
type Adopter struct {
	Peers func() []string
}

// Adopt tries the peers until one holds the claim and reports whether one does.
func (a *Adopter) Adopt(ctx context.Context, id, token string) bool {
	logger := log.WithFunc("peer.Adopter.Adopt")
	for _, addr := range a.Peers() {
		err := peerCall(ctx, http.MethodGet, peerURL(addr, "/v1/sandboxes/"+url.PathEscape(id)+"/owner"), token, http.StatusOK)
		if err == nil {
			return true
		}
		logger.Warnf(ctx, "adopt %s on %s: %v", id, addr, err)
	}
	return false
}
