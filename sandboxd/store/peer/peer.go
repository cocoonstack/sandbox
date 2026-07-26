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

// healBudget bounds one Pull across every owner tried, not each.
const healBudget = 30 * time.Minute

// Owners resolves a record id to the peer addresses that hold it.
type Owners func(id string) []string

// Validate checks a staged pull before Pull trusts the owner that sent it; an
// error tries the next owner. The healer does not know the record's shape.
type Validate func(staging string) error

// Healer pulls a record this node does not hold into a caller-provided staging
// directory; the caller validates and publishes it. A nil owners or puller
// makes every Pull a miss, so a node with no mesh simply cannot heal.
type Healer struct {
	owners Owners
	puller Puller

	// budget overrides healBudget; a test seam, 30 minutes is unwaitable.
	budget time.Duration

	flights singleflight.Group
}

// NewHealer builds a Healer wired to owners and puller.
func NewHealer(owners Owners, puller Puller) *Healer {
	return &Healer{owners: owners, puller: puller}
}

// Pull fetches id into staging from the first owner whose transfer validates,
// bounding every owner tried to one budget. Concurrent pulls share a flight.
func (h *Healer) Pull(ctx context.Context, id, staging string, validate Validate) error {
	if h.owners == nil || h.puller == nil {
		return store.ErrNotFound
	}
	addrs := h.owners(id)
	if len(addrs) == 0 {
		return store.ErrNotFound
	}
	budget := cmp.Or(h.budget, healBudget)
	// A client hanging up must not abandon a started pull; the budget bounds it.
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

// clearDir empties dir between owner attempts, so one peer's rejected or
// partial transfer cannot linger into the next's.
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
