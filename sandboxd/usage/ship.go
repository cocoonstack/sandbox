package usage

import (
	"bufio"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"math"
	"os"
	"time"

	"github.com/projecteru2/core/log"
)

const (
	shipEvery  = 5 * time.Second
	shipBatch  = 1000
	pruneEvery = time.Hour
)

// Ship lands the journal at path in store as node's events until ctx ends, reading it only; a nonzero retention prunes node's older rows hourly.
func Ship(ctx context.Context, path, node string, store Store, retention time.Duration) {
	s := &shipper{path: path, node: node, store: store}
	tick := time.Tick(shipEvery)
	var prune <-chan time.Time
	if retention > 0 {
		prune = time.Tick(pruneEvery)
	}
	for {
		s.drain(ctx)
		select {
		case <-ctx.Done():
			final, cancel := context.WithTimeout(context.WithoutCancel(ctx), shipEvery)
			s.drain(final)
			cancel()
			return
		case <-prune:
			s.prune(ctx, retention)
		case <-tick:
		}
	}
}

// shipper resumes after the line the store's mark names, else after its floor, node's oldest row.
type shipper struct {
	path    string
	node    string
	store   Store
	mark    [32]byte
	floor   [32]byte
	synced  bool
	at      anchor
	failing bool
}

func (s *shipper) drain(ctx context.Context) {
	logger := log.WithFunc("usage.shipper.drain")
	for {
		n, err := s.shipOnce(ctx)
		switch {
		case err != nil:
			s.synced = false
			if !s.failing {
				logger.Warnf(ctx, "usage events stop landing (%v); the journal keeps them, retrying every %s", err, shipEvery)
			}
			s.failing = true
			return
		case s.failing:
			logger.Info(ctx, "usage events land again")
			s.failing = false
		}
		if n < shipBatch {
			return
		}
	}
}

func (s *shipper) shipOnce(ctx context.Context) (int, error) {
	if !s.synced {
		mark, err := s.store.Last(ctx, s.node)
		if err != nil {
			return 0, fmt.Errorf("read mark: %w", err)
		}
		floor, err := s.store.First(ctx, s.node)
		if err != nil {
			return 0, fmt.Errorf("read floor: %w", err)
		}
		if mark != s.mark {
			s.at = anchor{}
		}
		s.mark, s.floor, s.synced = mark, floor, true
	}
	live, err := os.Open(s.path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = live.Close() }()
	backup, err := os.Open(s.path + ".1")
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return 0, err
	}
	if backup != nil {
		defer func() { _ = backup.Close() }()
		if sameFile(live, backup) {
			return 0, nil // rotated between the two opens
		}
	}
	f, off := s.locate(ctx, live, backup)
	files := []*os.File{f}
	if f == backup {
		files = append(files, live)
	}
	recs, last, n := s.read(ctx, files, off)
	if len(recs) > 0 {
		if err = s.store.Land(ctx, recs); err != nil {
			return 0, fmt.Errorf("land %d events: %w", len(recs), err)
		}
		s.mark = recs[len(recs)-1].Digest
	}
	if n > 0 {
		s.at = last
	}
	return n, nil
}

func (s *shipper) locate(ctx context.Context, live, backup *os.File) (*os.File, int64) {
	for _, f := range []*os.File{live, backup} {
		if f != nil && s.at.in(f) {
			return f, s.at.end
		}
	}
	oldest := cmp.Or(backup, live)
	if s.mark == ([32]byte{}) {
		return oldest, 0
	}
	var found, floor *os.File
	var floorAt anchor
	for _, f := range []*os.File{backup, live} {
		if f == nil {
			continue
		}
		for line, end := range lines(f, 0) {
			switch a := newAnchor(line, end); a.digest {
			case s.mark:
				found, s.at = f, a
			case s.floor:
				floor, floorAt = f, a
			}
		}
	}
	if found != nil {
		return found, s.at.end
	}
	logger := log.WithFunc("usage.shipper.locate")
	if floor != nil {
		logger.Warn(ctx, "the last landed usage line is gone from the journal; shipping resumes past the oldest landed line it still holds")
		s.at = floorAt
		return floor, floorAt.end
	}
	logger.Warn(ctx, "the last landed usage line is gone from the journal; shipping resumes at the oldest kept line, and lines rotated out unlanded are lost")
	return oldest, 0
}

func (s *shipper) prune(ctx context.Context, retention time.Duration) {
	if s.failing {
		return
	}
	logger := log.WithFunc("usage.shipper.prune")
	if err := s.syncJournal(); err != nil {
		logger.Warnf(ctx, "skip pruning usage rows this hour: sync the journal: %v", err)
		return
	}
	n, err := s.store.Prune(ctx, s.node, time.Now().Add(-retention))
	if err != nil {
		logger.Warnf(ctx, "prune usage rows older than %s (%d deleted): %v; the next sweep retries", retention, n, err)
		return
	}
	if n > 0 {
		logger.Infof(ctx, "pruned %d usage rows older than %s", n, retention)
	}
}

func (s *shipper) syncJournal() error {
	return errors.Join(syncOpened(os.Open(s.path)), syncOpened(os.Open(s.path+".1")))
}

func (s *shipper) read(ctx context.Context, files []*os.File, off int64) ([]Record, anchor, int) {
	var (
		recs []Record
		last anchor
		n    int
	)
	for _, f := range files {
		for line, end := range lines(f, off) {
			n++
			last = newAnchor(line, end)
			ev, err := decode(line)
			if err != nil {
				log.WithFunc("usage.shipper.read").Warnf(ctx, "skip undecodable line ending at %s:%d: %v", f.Name(), end, err)
			} else {
				recs = append(recs, Record{Node: s.node, Digest: last.digest, Event: ev})
			}
			if n == shipBatch {
				return recs, last, n
			}
		}
		off = 0
	}
	return recs, last, n
}

// anchor is one journal line by content and by the offset just past it.
type anchor struct {
	digest [32]byte
	end    int64
	size   int64
}

func newAnchor(line []byte, end int64) anchor {
	return anchor{digest: sha256.Sum256(line[:len(line)-1]), end: end, size: int64(len(line))}
}

// in reports whether f still holds the anchor's line at its offset.
func (a anchor) in(f *os.File) bool {
	if a.size == 0 {
		return false
	}
	buf := make([]byte, a.size)
	if _, err := f.ReadAt(buf, a.end-a.size); err != nil {
		return false
	}
	return buf[a.size-1] == '\n' && sha256.Sum256(buf[:a.size-1]) == a.digest
}

// lines yields each newline-terminated line of f from off, newline included, with the offset just past it.
func lines(f *os.File, off int64) iter.Seq2[[]byte, int64] {
	return func(yield func([]byte, int64) bool) {
		r := bufio.NewReader(io.NewSectionReader(f, off, math.MaxInt64-off))
		for {
			line, err := r.ReadBytes('\n')
			if err != nil {
				return
			}
			off += int64(len(line))
			if !yield(line, off) {
				return
			}
		}
	}
}

func decode(line []byte) (Event, error) {
	var ev Event
	err := json.Unmarshal(line[:len(line)-1], &ev, jsontext.AllowInvalidUTF8(true))
	return ev, err
}

func syncOpened(f *os.File, err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func sameFile(a, b *os.File) bool {
	sa, errA := a.Stat()
	sb, errB := b.Stat()
	return errA == nil && errB == nil && os.SameFile(sa, sb)
}
