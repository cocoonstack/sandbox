package pool

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const archiveDeleteDir = "archive-deletes"

type ckMarkers struct {
	dir string
}

// mark records durable delete intent before the journal drops its reference.
func (c ckMarkers) mark(ckID string) error {
	return c.markAll([]string{ckID})[ckID]
}

// markAll writes a delete marker per checkpoint and syncs the directory once; failed holds each id whose marker is not durable.
func (c ckMarkers) markAll(ckIDs []string) (failed map[string]error) {
	if len(ckIDs) == 0 {
		return nil
	}
	failed = map[string]error{}
	if err := os.MkdirAll(c.dir, 0o750); err != nil {
		for _, id := range ckIDs {
			failed[id] = err
		}
		return failed
	}
	written := make([]string, 0, len(ckIDs))
	for _, id := range ckIDs {
		if !store.CheckpointIDRe.MatchString(id) {
			failed[id] = fmt.Errorf("invalid checkpoint id %q", id)
			continue
		}
		if err := os.WriteFile(filepath.Join(c.dir, id), nil, 0o600); err != nil {
			failed[id] = err
			continue
		}
		written = append(written, id)
	}
	if len(written) == 0 {
		return failed
	}
	if err := utils.SyncDir(c.dir); err != nil {
		for _, id := range written {
			failed[id] = err
		}
	}
	return failed
}

func (c ckMarkers) list() ([]string, error) {
	entries, err := os.ReadDir(c.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Type().IsRegular() && store.CheckpointIDRe.MatchString(entry.Name()) {
			ids = append(ids, entry.Name())
		}
	}
	return ids, nil
}

func (c ckMarkers) clear(ckID string) error {
	err := os.Remove(filepath.Join(c.dir, ckID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (c ckMarkers) marked(ckID string) bool {
	_, err := os.Stat(filepath.Join(c.dir, ckID))
	return err == nil
}

func (m *Manager) deleteArchiveCk(ctx context.Context, ckID string) (bool, error) {
	if err := m.ckMarks.mark(ckID); err != nil {
		return false, fmt.Errorf("track: %w", err)
	}
	return m.finishArchiveCkDelete(ctx, ckID)
}

func (m *Manager) finishArchiveCkDelete(ctx context.Context, ckID string) (bool, error) {
	unlock := m.recs.lockRec(ckID)
	if m.store.referencesCheckpoint(ckID) {
		unlock()
		return false, nil
	}
	err := m.ckpts.Delete(ctx, ckID)
	unlock()
	if err != nil {
		return false, err
	}
	if err := m.ckMarks.clear(ckID); err != nil {
		return false, fmt.Errorf("clear: %w", err)
	}
	return true, nil
}

func (m *Manager) retryArchiveDeletes(ctx context.Context) {
	if !m.sweeps.archiveDelete.CompareAndSwap(false, true) {
		return
	}
	defer m.sweeps.archiveDelete.Store(false)
	ids, err := m.ckMarks.list()
	if err != nil {
		log.WithFunc("pool.retryArchiveDeletes").Warnf(ctx, "list: %v", err)
		return
	}
	if len(ids) == 0 {
		return
	}
	pinned := m.pinnedArchiveCks()
	ids = slices.DeleteFunc(ids, func(id string) bool { _, ok := pinned[id]; return ok })
	m.runBounded(ctx, len(ids), func(ctx context.Context, i int) {
		m.retryArchiveDelete(ctx, ids[i])
	}).Wait()
}

func (m *Manager) retryArchiveDelete(ctx context.Context, ckID string) {
	unlock := m.recs.lockRec(ckID)
	if m.archiveCkPinned(ckID) || !m.ckMarks.marked(ckID) {
		unlock()
		return
	}
	err := m.ckpts.Delete(ctx, ckID)
	unlock()
	if err != nil {
		log.WithFunc("pool.retryArchiveDelete").Warnf(ctx, "delete %s: %v", ckID, err)
		return
	}
	if err := m.ckMarks.clear(ckID); err != nil {
		log.WithFunc("pool.retryArchiveDelete").Warnf(ctx, "clear %s: %v", ckID, err)
	}
}

func (m *Manager) archiveCkPinned(ckID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, pending := m.pendingCks[ckID]
	return pending || m.store.referencesCheckpoint(ckID)
}
