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
	shipEvery = 5 * time.Second
	shipBatch = 1000
)

var errNoEvent = errors.New("no ev")

// Ship lands the journal at path in store as node's events until ctx ends; it only reads the journal.
func Ship(ctx context.Context, path, node string, store Store) {
	s := &shipper{path: path, node: node, store: store}
	tick := time.Tick(shipEvery)
	for {
		s.drain(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick:
		}
	}
}

// shipper resumes after the line whose digest the store's mark names; at is the last line read, a hint that survives a rename.
type shipper struct {
	path    string
	node    string
	store   Store
	mark    Mark
	synced  bool
	at      anchor
	failing bool
}

func (s *shipper) drain(ctx context.Context) {
	logger := log.WithFunc("usage.Ship")
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

// shipOnce lands the next batch of lines and returns how many it read.
func (s *shipper) shipOnce(ctx context.Context) (int, error) {
	if !s.synced {
		mark, err := s.store.Last(ctx, s.node)
		if err != nil {
			return 0, fmt.Errorf("read mark: %w", err)
		}
		if mark != s.mark {
			s.at = anchor{}
		}
		s.mark, s.synced = mark, true
	}
	live, err := os.Open(s.path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = live.Close() }()
	backup, err := openBackup(s.path + ".1")
	if err != nil {
		return 0, err
	}
	if backup != nil {
		defer func() { _ = backup.Close() }()
		if sameFile(live, backup) {
			return 0, nil // rotated between the two opens
		}
	}
	f, off := s.locate(live, backup)
	files := []*os.File{f}
	if f == backup {
		files = append(files, live)
	}
	recs, last, n := s.read(ctx, files, off)
	if len(recs) > 0 {
		if err = s.store.Land(ctx, recs); err != nil {
			return 0, fmt.Errorf("land %d events: %w", len(recs), err)
		}
		s.mark = recs[len(recs)-1].Mark
	}
	if n > 0 {
		s.at = last
	}
	return n, nil
}

// locate returns where the first unlanded line starts: past the anchor when a file still holds it, else past the marked line, else at the oldest line.
func (s *shipper) locate(live, backup *os.File) (*os.File, int64) {
	for _, f := range []*os.File{live, backup} {
		if f != nil && s.at.in(f) {
			return f, s.at.end
		}
	}
	oldest := cmp.Or(backup, live)
	if s.mark.Seq == 0 {
		return oldest, 0
	}
	var found *os.File
	for _, f := range []*os.File{backup, live} {
		if f == nil {
			continue
		}
		for line, end := range lines(f, 0) {
			if a := newAnchor(line, end); a.digest == s.mark.Digest {
				found, s.at = f, a
			}
		}
	}
	if found == nil {
		return oldest, 0
	}
	return found, s.at.end
}

// read decodes up to shipBatch lines of files, the first from off, into s.node's next records.
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
				log.WithFunc("usage.Ship").Warnf(ctx, "skip undecodable line ending at %s:%d: %v", f.Name(), end, err)
			} else {
				recs = append(recs, Record{Node: s.node, Seq: s.mark.Seq + int64(len(recs)) + 1, Digest: last.digest, Event: ev})
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
	if err := json.Unmarshal(line[:len(line)-1], &ev, jsontext.AllowInvalidUTF8(true)); err != nil {
		return Event{}, err
	}
	if ev.Event == "" {
		return Event{}, errNoEvent
	}
	return ev, nil
}

func openBackup(path string) (*os.File, error) {
	f, err := os.Open(path) //nolint:gosec // under data_dir
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return f, err
}

func sameFile(a, b *os.File) bool {
	sa, errA := a.Stat()
	sb, errB := b.Stat()
	return errA == nil && errB == nil && os.SameFile(sa, sb)
}
