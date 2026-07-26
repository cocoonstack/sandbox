package peer

import (
	"context"
	"errors"
	"fmt"
	"os"

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
	local  store.Store
	owners Owners
	puller Puller
}

// New wraps local with peer healing. A nil owners or puller leaves the wrapper
// inert, so a node with no mesh degrades to the local backend rather than
// failing.
func New(local store.Store, owners Owners, puller Puller) *Store {
	return &Store{local: local, owners: owners, puller: puller}
}

func (s *Store) Stage(id string) (string, error) { return s.local.Stage(id) }

func (s *Store) Publish(ctx context.Context, staging, id string) error {
	return s.local.Publish(ctx, staging, id)
}

func (s *Store) Delete(ctx context.Context, id string) error { return s.local.Delete(ctx, id) }

func (s *Store) SweepStaging() error { return s.local.SweepStaging() }

// Metas lists local records only: answering for peers would return the same
// record once per node to a control plane that already scatter-gathers.
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

// heal pulls id from the first peer that serves it and publishes it locally,
// returning store.ErrNotFound when no peer has it so a miss stays a miss.
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
