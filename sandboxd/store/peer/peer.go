package peer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

// healBudget bounds one Pull across every owner tried, not each.
const healBudget = 30 * time.Minute

// Owners resolves a record id to the peer addresses that hold it.
type Owners func(id string) []string

// Validate checks a staged pull before Pull trusts the owner that sent it; an
// error tries the next owner.
type Validate func(staging string) error

// Healer pulls a record this node does not hold into a caller-provided
// staging directory; the caller validates and publishes it. A nil owners or
// puller makes every Pull a miss (no mesh, no heal). Pull never dedups
// concurrent calls: each caller owns its destination dir, so dedup belongs to
// whoever owns the staging — pool.Manager's per-id heal flight.
type Healer struct {
	owners Owners
	puller Puller

	// budget overrides healBudget; a test seam, 30 minutes is unwaitable.
	budget time.Duration
}

// NewHealer builds a Healer wired to owners and puller.
func NewHealer(owners Owners, puller Puller) *Healer {
	return &Healer{owners: owners, puller: puller}
}

// Pull fetches id into staging from the first owner whose transfer validates,
// bounding every owner tried to one budget.
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
	return h.pullFrom(ctx, id, staging, addrs, budget, validate)
}

func (h *Healer) pullFrom(ctx context.Context, id, staging string, addrs []string, budget time.Duration, validate Validate) error {
	perOwner := budget / time.Duration(len(addrs))
	var errs []error
	for _, addr := range addrs {
		// One peer's rejected or partial transfer must not linger into the next's.
		if err := utils.RemoveDirEntries(staging, nil); err != nil {
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
