package peer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

// healBudget bounds one Pull across every owner tried, replacing the old
// per-owner-only bound: a heal that tried N owners at 30 minutes each could
// run for N*30 minutes with nothing else limiting it.
const healBudget = 30 * time.Minute

// Owners resolves a record id to the peer addresses that hold it — the mesh's
// gossiped view, injected so the healer stays testable without a live cluster.
type Owners func(id string) []string

// Validate checks a staged pull before Pull trusts the owner that sent it; a
// non-nil error tries the next owner instead of failing the whole heal. The
// healer does not know the record's shape, so the caller supplies this.
type Validate func(staging string) error

// Healer pulls a record this node does not hold from whichever peer gossiped
// it, staging the transfer into a caller-provided directory; the caller
// validates and publishes it. A nil owners or puller leaves Pull always
// answering store.ErrNotFound, so a node with no mesh degrades to unable to
// heal rather than failing.
type Healer struct {
	owners Owners
	puller Puller

	// budget overrides healBudget when set; a test seam, since 30 minutes is
	// not something a test should wait out.
	budget time.Duration

	flights singleflight.Group
}

// NewHealer builds a Healer wired to owners and puller.
func NewHealer(owners Owners, puller Puller) *Healer {
	return &Healer{owners: owners, puller: puller}
}

// Pull fetches id into staging from the first owner whose transfer validates,
// bounding the whole attempt (every owner) to one budget so a wedged or
// endlessly-invalid peer cannot starve the rest. Concurrent pulls of the same
// id share one flight.
func (h *Healer) Pull(ctx context.Context, id, staging string, validate Validate) error {
	if h.owners == nil || h.puller == nil {
		return store.ErrNotFound
	}
	addrs := h.owners(id)
	if len(addrs) == 0 {
		return store.ErrNotFound
	}
	budget := cmp.Or(h.budget, healBudget)
	// A client hanging up must not abandon a started pull: the budget below
	// bounds it instead of the caller's ctx.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
	defer cancel()
	_, err, _ := h.flights.Do(id, func() (any, error) {
		return nil, h.pullFrom(ctx, id, staging, addrs, budget, validate)
	})
	return err
}

// pullFrom tries each owner in turn, giving each an even slice of budget;
// callers hold id's singleflight slot.
func (h *Healer) pullFrom(ctx context.Context, id, staging string, addrs []string, budget time.Duration, validate Validate) error {
	perOwner := budget / time.Duration(len(addrs))
	var errs []error
	for _, addr := range addrs {
		if err := clearDir(staging); err != nil {
			return fmt.Errorf("reset staging: %w", err)
		}
		attemptCtx, cancel := context.WithTimeout(ctx, perOwner)
		err := h.puller.Pull(attemptCtx, addr, id, staging)
		cancel()
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				errs = append(errs, fmt.Errorf("peer %s: %w", addr, err))
			}
			continue
		}
		if validate == nil {
			return nil
		}
		if err := validate(staging); err != nil {
			errs = append(errs, fmt.Errorf("peer %s: %w", addr, err))
			continue
		}
		return nil
	}
	if len(errs) > 0 {
		return fmt.Errorf("heal %s from %d peer(s): %w", id, len(addrs), errors.Join(errs...))
	}
	return store.ErrNotFound // every owner answered not-found: stale gossip
}

// clearDir empties dir's contents (keeping dir itself) between owner
// attempts, so a rejected or partial transfer from one peer cannot linger
// into the next's.
func clearDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}
