package peer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/projecteru2/core/log"
)

const peerCallTimeout = 5 * time.Second

// Broadcaster fans a checkpoint delete out to every peer so a healed copy does not outlive it.
type Broadcaster struct {
	Peers func() []string
	Token string
}

// Delete sends DELETE ?no_forward=1 to every peer in parallel; failures are logged, not returned.
func (b *Broadcaster) Delete(ctx context.Context, id string) {
	addrs := slices.Compact(slices.Sorted(slices.Values(b.Peers())))
	logger := log.WithFunc("peer.Broadcaster.Delete")
	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Go(func() {
			// A 404 means the peer never healed a copy.
			if err := peerCall(ctx, http.MethodDelete, checkpointURL(addr, id, "?no_forward=1"), b.Token, nil, peerCallTimeout, http.StatusNoContent, http.StatusNotFound); err != nil {
				logger.Warnf(ctx, "broadcast delete %s to %s: %v", id, addr, err)
			}
		})
	}
	wg.Wait()
}

// peerCall bounds one request to a peer unless timeout is zero; any status outside ok is an error.
func peerCall(ctx context.Context, method, target, token string, body io.Reader, timeout time.Duration, ok ...int) error {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if !slices.Contains(ok, resp.StatusCode) {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}
