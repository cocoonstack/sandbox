package peer

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

// Owners resolves a record id to the peer addresses that hold it. It is the
// mesh's gossiped view (Mesh.CheckpointOwners), injected so the store stays
// testable without a live cluster.
type Owners func(id string) []string

// Store is a node-local record store that can heal a miss from a peer.
//
// It is the third tier of the snapshot placement design, and deliberately the
// last resort. The first two tiers never move data: a branch normally runs on
// the node that already holds the record, on its local reflink fast path, and
// a request that lands elsewhere is redirected there. This tier exists for the
// case redirect cannot answer — the owning node is gone, draining, or full —
// where the choice is between paying one transfer and failing the request.
//
// A pull publishes the record into the local store, so the cost is paid once:
// afterwards this node is itself an owner, gossips the record, and serves
// branches of it locally like any other.
type Store struct {
	// local is the node's own store; every operation but a Fetch/ReadMeta miss
	// is served entirely by it.
	local store.Store
	// owners resolves who to pull from. Nil disables healing (the store then
	// behaves exactly like its local backend).
	owners Owners
	// puller moves the bytes.
	puller Puller
}

var _ store.Store = (*Store)(nil)

// New wraps local with peer healing. A nil owners or puller leaves the wrapper
// inert — it degrades to the local backend rather than failing, so a node with
// no mesh keeps working.
func New(local store.Store, owners Owners, puller Puller) *Store {
	return &Store{local: local, owners: owners, puller: puller}
}

func (s *Store) Stage(id string) (string, error) { return s.local.Stage(id) }

func (s *Store) Publish(ctx context.Context, staging, id string) error {
	return s.local.Publish(ctx, staging, id)
}

func (s *Store) Delete(ctx context.Context, id string) error { return s.local.Delete(ctx, id) }

func (s *Store) SweepStaging() error { return s.local.SweepStaging() }

// Metas lists local records only. A cluster-wide listing is the control
// plane's job (it already scatter-gathers every node); having each node
// answer for its peers would return the same record N times.
func (s *Store) Metas(ctx context.Context) ([][]byte, error) { return s.local.Metas(ctx) }

// Fetch serves the record locally, healing from a peer on a miss.
func (s *Store) Fetch(ctx context.Context, id string) (string, []byte, func(), error) {
	dir, meta, release, err := s.local.Fetch(ctx, id)
	if !errors.Is(err, store.ErrNotFound) {
		return dir, meta, release, err
	}
	if err := s.heal(ctx, id); err != nil {
		return "", nil, nil, err
	}
	return s.local.Fetch(ctx, id)
}

// ReadMeta reads the record's metadata, healing from a peer on a miss.
func (s *Store) ReadMeta(ctx context.Context, id string) ([]byte, error) {
	meta, err := s.local.ReadMeta(ctx, id)
	if !errors.Is(err, store.ErrNotFound) {
		return meta, err
	}
	if err := s.heal(ctx, id); err != nil {
		return nil, err
	}
	return s.local.ReadMeta(ctx, id)
}

// heal pulls id from the first peer that serves it and publishes it locally.
// Returns store.ErrNotFound when no peer has it, so a miss stays a miss and
// the caller's existing not-found handling is unchanged.
func (s *Store) heal(ctx context.Context, id string) error {
	if s.owners == nil || s.puller == nil {
		return store.ErrNotFound
	}
	addrs := s.owners(id)
	if len(addrs) == 0 {
		return store.ErrNotFound
	}
	// A started pull must finish even if the requesting client hangs up:
	// abandoning it halfway would leave staging to the sweeper and make the
	// next branch pay the whole transfer again.
	ctx = context.WithoutCancel(ctx)

	var errs []error
	for _, addr := range addrs {
		if err := s.pullFrom(ctx, addr, id); err != nil {
			if !errors.Is(err, ErrNotFound) {
				errs = append(errs, fmt.Errorf("peer %s: %w", addr, err))
			}
			continue
		}
		return nil
	}
	if len(errs) > 0 {
		return fmt.Errorf("heal %s from %d peer(s): %w", id, len(addrs), errors.Join(errs...))
	}
	// Every owner answered "not found": the gossiped view was stale.
	return store.ErrNotFound
}

// pullFrom stages a peer's copy and publishes it, so the record becomes local
// through the same atomic path a locally-created one takes.
func (s *Store) pullFrom(ctx context.Context, addr, id string) error {
	staging, err := s.local.Stage(id)
	if err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()

	if err := s.puller.Pull(ctx, addr, id, staging); err != nil {
		return err
	}
	if err := s.local.Publish(ctx, staging, id); err != nil {
		return fmt.Errorf("publish pulled record: %w", err)
	}
	return nil
}
