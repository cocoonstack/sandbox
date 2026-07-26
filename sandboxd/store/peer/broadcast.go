package peer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/projecteru2/core/log"
)

const (
	deleteTimeout              = 5 * time.Second
	defaultDeleteClientTimeout = 10 * time.Second
)

// Broadcaster fans a checkpoint delete out to every peer after a local
// delete, so a record healed onto another node does not outlive the source.
type Broadcaster struct {
	// A nil Client uses a default with a short timeout: one wedged peer must
	// not hold up the fan-out.
	Client *http.Client
	Peers  func() []string
	Token  string
}

// Delete sends DELETE ?no_forward=1 to every peer in parallel. Failures are
// logged and never returned: a broadcast is best-effort and must never fail
// the local delete it follows.
func (b *Broadcaster) Delete(ctx context.Context, id string) {
	addrs := dedupAddrs(b.Peers())
	if len(addrs) == 0 {
		return
	}
	client := b.Client
	if client == nil {
		client = &http.Client{Timeout: defaultDeleteClientTimeout}
	}
	logger := log.WithFunc("peer.Broadcaster.Delete")
	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Go(func() {
			if err := deleteOn(ctx, client, addr, id, b.Token); err != nil {
				logger.Warnf(ctx, "broadcast delete %s to %s: %v", id, addr, err)
			}
		})
	}
	wg.Wait()
}

func deleteOn(ctx context.Context, client *http.Client, addr, id, token string) error {
	ctx, cancel := context.WithTimeout(ctx, deleteTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, checkpointURL(addr, id, "?no_forward=1"), nil)
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req) //nolint:gosec // addr comes from the mesh's own member view
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	// 404 means the peer never healed a copy — not a failure worth a retry.
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}
