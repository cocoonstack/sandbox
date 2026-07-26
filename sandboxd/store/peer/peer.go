package peer

import (
	"context"
	"errors"
	"fmt"
	"os"

	"golang.org/x/sync/singleflight"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

// Owners resolves a record id to the peer addresses that hold it — the mesh's
// gossiped view, injected so the store stays testable without a live cluster.
type Owners func(id string) []string

var _ store.Store = (*Store)(nil)

// Store is a node-local record store that heals a Fetch/ReadMeta miss by
// pulling the record from a peer and publishing it locally, so the transfer is
// paid once: this node then owns the record and serves later reads locally.
type Store struct {
	store.Store
	owners  Owners
	puller  Puller
	flights singleflight.Group
}

// New wraps local with peer healing. A nil owners or puller leaves the wrapper
// inert, so a node with no mesh degrades to the local backend rather than
// failing.
func New(local store.Store, owners Owners, puller Puller) *Store {
	return &Store{Store: local, owners: owners, puller: puller}
}

// Fetch serves the record locally, healing from a peer on a miss.
func (s *Store) Fetch(ctx context.Context, id string) (string, []byte, func(), error) {
	dir, meta, release, err := s.Store.Fetch(ctx, id)
	if !errors.Is(err, store.ErrNotFound) {
		return dir, meta, release, err
	}
	if err := s.heal(ctx, id); err != nil {
		return "", nil, nil, err
	}
	return s.Store.Fetch(ctx, id)
}

// ReadMeta reads the record's metadata, healing from a peer on a miss.
func (s *Store) ReadMeta(ctx context.Context, id string) ([]byte, error) {
	meta, err := s.Store.ReadMeta(ctx, id)
	if !errors.Is(err, store.ErrNotFound) {
		return meta, err
	}
	if err := s.heal(ctx, id); err != nil {
		return nil, err
	}
	return s.Store.ReadMeta(ctx, id)
}

// heal pulls id from the first peer that serves it and publishes it locally,
// returning store.ErrNotFound when no peer has it so a miss stays a miss.
// Concurrent misses of the same id share one pull.
func (s *Store) heal(ctx context.Context, id string) error {
	if s.owners == nil || s.puller == nil {
		return store.ErrNotFound
	}
	addrs := s.owners(id)
	if len(addrs) == 0 {
		return store.ErrNotFound
	}
	// A client hanging up must not abandon a started pull: the next branch
	// would pay the whole transfer again.
	ctx = context.WithoutCancel(ctx)
	_, err, _ := s.flights.Do(id, func() (any, error) {
		return nil, s.healFrom(ctx, id, addrs)
	})
	return err
}

// healFrom tries each owner in order until one serves id; callers hold the
// id's singleflight slot.
func (s *Store) healFrom(ctx context.Context, id string, addrs []string) error {
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
	return store.ErrNotFound // every owner answered not-found: stale gossip
}

// pullFrom stages a peer's copy and publishes it, so the record becomes local
// through the same atomic path a locally-created one takes.
func (s *Store) pullFrom(ctx context.Context, addr, id string) error {
	staging, err := s.Stage(id)
	if err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()

	if err := s.puller.Pull(ctx, addr, id, staging); err != nil {
		return err
	}
	if err := s.Publish(ctx, staging, id); err != nil {
		return fmt.Errorf("publish pulled record: %w", err)
	}
	return nil
}
