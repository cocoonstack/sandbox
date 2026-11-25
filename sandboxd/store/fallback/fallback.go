// Package fallback reads the records a primary store lacks from a second store, so a cell can move its records to a new bucket without copying them.
package fallback

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/projecteru2/core/log"
	"golang.org/x/sync/errgroup"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

type op int

const (
	opFetch op = iota
	opFetchFile
	opReadMeta
	opReadDigest
	opSetLabels
	opDelete
)

const (
	hit = iota
	miss
	failed
)

var (
	ops     = [...]string{"fetch", "fetch_file", "read_meta", "read_digest", "set_labels", "delete"}
	results = [...]string{"hit", "miss", "error"}
)

// Stat counts how often the fallback answered Op with Result: hit it held the record, miss it answered not found, error it failed.
type Stat struct {
	Store, Op, Result string
	Count             uint64
}

var _ store.Store = (*Store)(nil)

// Store writes only to its primary and asks its fallback only once the primary answers store.ErrNotFound for a record's meta; any other primary failure is returned, so an outage never reads as an absent record.
type Store struct {
	name              string
	primary, fallback store.Store
	counts            [len(ops)][len(results)]atomic.Uint64
}

// New wraps primary and fallback; name labels the wrapper's logs and counters.
func New(name string, primary, fallback store.Store) *Store {
	return &Store{name: name, primary: primary, fallback: fallback}
}

func (s *Store) Stage(id string) (string, error) {
	return s.primary.Stage(id)
}

func (s *Store) Publish(ctx context.Context, staging, id, lineage string) error {
	return s.primary.Publish(ctx, staging, id, lineage)
}

func (s *Store) PrepareDigested(ctx context.Context, staging, id, lineage string) (string, store.CommitFunc, error) {
	return s.primary.PrepareDigested(ctx, staging, id, lineage)
}

func (s *Store) Fetch(ctx context.Context, id string) (string, []byte, string, error) {
	dir, meta, digest, err := s.primary.Fetch(ctx, id)
	if !errors.Is(err, store.ErrNotFound) {
		return dir, meta, digest, err
	}
	if err = s.primaryLacks(ctx, id); err != nil {
		return "", nil, "", err
	}
	dir, meta, digest, err = s.fallback.Fetch(ctx, id)
	if err = s.note(ctx, opFetch, id, err); err != nil {
		return "", nil, "", err
	}
	return dir, meta, digest, nil
}

func (s *Store) FetchFile(ctx context.Context, id, rel, dst string) error {
	err := s.primary.FetchFile(ctx, id, rel, dst)
	if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	if err = s.primaryLacks(ctx, id); err != nil {
		return err
	}
	return s.note(ctx, opFetchFile, id, s.fallback.FetchFile(ctx, id, rel, dst))
}

func (s *Store) ReadMeta(ctx context.Context, id string) ([]byte, error) {
	meta, err := s.primary.ReadMeta(ctx, id)
	if !errors.Is(err, store.ErrNotFound) {
		return meta, err
	}
	meta, err = s.fallback.ReadMeta(ctx, id)
	if err = s.note(ctx, opReadMeta, id, err); err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *Store) ReadDigest(ctx context.Context, id string) (string, error) {
	digest, err := s.primary.ReadDigest(ctx, id)
	if !errors.Is(err, store.ErrNotFound) {
		return digest, err
	}
	digest, err = s.fallback.ReadDigest(ctx, id)
	if err = s.note(ctx, opReadDigest, id, err); err != nil {
		return "", err
	}
	return digest, nil
}

// Metas lists both stores and keeps the primary's record of an id both hold; a listing missing either store fails rather than come back short.
func (s *Store) Metas(ctx context.Context, prefix string) ([]store.Record, error) {
	var primary, fallback []store.Record
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		primary, err = s.primary.Metas(gctx, prefix)
		return err
	})
	g.Go(func() (err error) {
		if fallback, err = s.fallback.Metas(gctx, prefix); err != nil {
			return fmt.Errorf("fallback store: %w", err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	held := make(map[string]struct{}, len(primary))
	for _, r := range primary {
		held[r.ID] = struct{}{}
	}
	for _, r := range fallback {
		if _, ok := held[r.ID]; !ok {
			primary = append(primary, r)
		}
	}
	return primary, nil
}

// SetLabels writes beside the meta the labels describe: in the fallback only when the primary holds no such record and the fallback does.
func (s *Store) SetLabels(ctx context.Context, id string, labels []byte) error {
	_, err := s.primary.ReadMeta(ctx, id)
	if !errors.Is(err, store.ErrNotFound) {
		if err != nil {
			return err
		}
		return s.primary.SetLabels(ctx, id, labels)
	}
	_, err = s.fallback.ReadMeta(ctx, id)
	switch err = s.note(ctx, opSetLabels, id, err); {
	case err == nil:
		if err = s.fallback.SetLabels(ctx, id, labels); err != nil {
			return fmt.Errorf("fallback store: %w", err)
		}
		return nil
	case errors.Is(err, store.ErrNotFound):
		return s.primary.SetLabels(ctx, id, labels)
	default:
		return err
	}
}

// Delete removes the fallback's copy before the primary's, so a failure between them never leaves an older copy readable over a newer one; a retry finishes either half.
func (s *Store) Delete(ctx context.Context, id string) error {
	_, err := s.fallback.ReadMeta(ctx, id)
	switch err = s.note(ctx, opDelete, id, err); {
	case err == nil:
		if err = s.fallback.Delete(ctx, id); err != nil {
			return fmt.Errorf("fallback store: %w", err)
		}
	case !errors.Is(err, store.ErrNotFound):
		return err
	}
	return s.primary.Delete(ctx, id)
}

func (s *Store) SweepStaging() error {
	return errors.Join(s.primary.SweepStaging(), s.fallback.SweepStaging())
}

func (s *Store) SweepGenerations(ctx context.Context) error {
	return errors.Join(s.primary.SweepGenerations(ctx), s.fallback.SweepGenerations(ctx))
}

// Stats lists every counter, zeros included, so each series exists from the start.
func (s *Store) Stats() []Stat {
	out := make([]Stat, 0, len(ops)*len(results))
	for o := range ops {
		for r := range results {
			out = append(out, Stat{Store: s.name, Op: ops[o], Result: results[r], Count: s.counts[o][r].Load()})
		}
	}
	return out
}

// primaryLacks is nil only when the primary holds no meta for id: a primary record missing its export or a file answers not found itself.
func (s *Store) primaryLacks(ctx context.Context, id string) error {
	_, err := s.primary.ReadMeta(ctx, id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return nil
	case err == nil:
		return store.ErrNotFound
	default:
		return err
	}
}

// note counts the fallback's answer to o and names the fallback in a failure; not found stays store.ErrNotFound.
func (s *Store) note(ctx context.Context, o op, id string, err error) error {
	logger := log.WithFunc("fallback.Store")
	switch {
	case err == nil:
		s.counts[o][hit].Add(1)
		logger.Infof(ctx, "%s %s %s: the fallback store holds it", s.name, ops[o], id)
		return nil
	case errors.Is(err, store.ErrNotFound):
		s.counts[o][miss].Add(1)
		return err
	default:
		s.counts[o][failed].Add(1)
		err = fmt.Errorf("fallback store: %w", err)
		logger.Warnf(ctx, "%s %s %s: %v", s.name, ops[o], id, err)
		return err
	}
}
