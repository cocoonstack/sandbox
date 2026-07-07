package pool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var (
	ErrBadName           = errors.New("invalid checkpoint name")
	ErrUnknownCheckpoint = errors.New("unknown checkpoint")
)

// Checkpoint captures a claimed sandbox's full state under a fresh id and
// returns its record; the source keeps running (a hibernated source is
// captured from its wake image and stays hibernated). Claims born from the
// checkpoint clone that exact state — branching — and the source can be
// checkpointed again later, so successive checkpoints form a tree.
func (m *Manager) Checkpoint(ctx context.Context, id, token, name string) (types.Checkpoint, error) {
	if name != "" && !templateNameRe.MatchString(name) {
		return types.Checkpoint{}, fmt.Errorf("%w: %q must match %s", ErrBadName, name, templateNameRe)
	}
	sb, ok := m.claim(id, token)
	if !ok {
		return types.Checkpoint{}, ErrUnknownSandbox
	}
	// See Hibernate: a started capture must finish even if the caller hangs up.
	ctx = context.WithoutCancel(ctx)

	ckpt := types.Checkpoint{
		ID:        "ck_" + randHex(8),
		Name:      name,
		SandboxID: sb.ID,
		Key:       sb.Key,
		CreatedAt: time.Now(),
	}
	staging, err := m.ckpts.Stage(ckpt.ID)
	if err != nil {
		return types.Checkpoint{}, fmt.Errorf("stage checkpoint: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	if err = m.exportSource(ctx, sb, filepath.Join(staging, store.ExportDir)); err != nil {
		return types.Checkpoint{}, fmt.Errorf("checkpoint %s: %w", id, err)
	}
	meta, err := json.Marshal(ckpt)
	if err != nil {
		return types.Checkpoint{}, fmt.Errorf("encode checkpoint meta: %w", err)
	}
	if err := os.WriteFile(filepath.Join(staging, store.MetaFile), meta, 0o600); err != nil {
		return types.Checkpoint{}, fmt.Errorf("write checkpoint meta: %w", err)
	}
	if err := m.ckpts.Publish(ctx, staging, ckpt.ID); err != nil {
		return types.Checkpoint{}, fmt.Errorf("commit checkpoint: %w", err)
	}
	m.counters.checkpoints.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "checkpoint", ID: sb.ID, VMName: sb.VMName, Reference: ckpt.ID})
	return ckpt, nil
}

// ClaimCheckpoint provisions a fresh claim cloned from a checkpoint — a
// branch. The checkpoint's recorded key applies (snapshots pin size and
// lane); the checkpoint itself is read-only and reusable.
func (m *Manager) ClaimCheckpoint(ctx context.Context, ckptID string, ttl time.Duration) (*types.Sandbox, error) {
	ckpt, err := m.loadCheckpoint(ctx, ckptID)
	if err != nil {
		return nil, err
	}
	dir, release, err := m.ckpts.Fetch(ctx, ckpt.ID)
	if err != nil {
		return nil, fmt.Errorf("fetch checkpoint: %w", err)
	}
	defer release()
	sb, err := m.provision(ctx, ckpt.Key, dir)
	if err != nil {
		return nil, err
	}
	sb.FromCheckpoint = ckpt.ID
	out, err := m.finalize(ctx, sb, ttl)
	if err == nil {
		m.counters.claimsClone.Add(1)
	}
	return out, err
}

// Checkpoints lists the store's checkpoints, newest first — on a shared
// checkpoint_dir (a FUSE mount), that is the cluster's set, not one node's.
func (m *Manager) Checkpoints(ctx context.Context) ([]types.Checkpoint, error) {
	metas, err := m.ckpts.Metas(ctx)
	if err != nil {
		return nil, fmt.Errorf("list checkpoints: %w", err)
	}
	ckpts := make([]types.Checkpoint, 0, len(metas))
	for _, raw := range metas {
		var ckpt types.Checkpoint
		if err := json.Unmarshal(raw, &ckpt); err == nil {
			ckpts = append(ckpts, ckpt)
		}
	}
	slices.SortFunc(ckpts, func(a, b types.Checkpoint) int { return b.CreatedAt.Compare(a.CreatedAt) })
	return ckpts, nil
}

// DeleteCheckpoint removes a checkpoint's snapshot and record.
func (m *Manager) DeleteCheckpoint(ctx context.Context, ckptID string) error {
	if _, err := m.loadCheckpoint(ctx, ckptID); err != nil {
		return err
	}
	if err := m.ckpts.Delete(ctx, ckptID); err != nil {
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	return nil
}

// sweepExpiredCheckpoints ages out checkpoints older than the configured
// TTL; explicit deletes never wait for it.
func (m *Manager) sweepExpiredCheckpoints(ctx context.Context) {
	logger := log.WithFunc("pool.sweepExpiredCheckpoints")
	ckpts, err := m.Checkpoints(ctx)
	if err != nil {
		logger.Error(ctx, err, "list for retention")
		return
	}
	cutoff := time.Now().Add(-m.ckptTTL)
	for _, ckpt := range ckpts {
		if ckpt.CreatedAt.After(cutoff) {
			continue
		}
		if err := m.ckpts.Delete(ctx, ckpt.ID); err != nil {
			logger.Errorf(ctx, err, "expire %s", ckpt.ID)
		}
	}
}

func (m *Manager) loadCheckpoint(ctx context.Context, ckptID string) (types.Checkpoint, error) {
	if !store.IDRe.MatchString(ckptID) {
		return types.Checkpoint{}, ErrUnknownCheckpoint
	}
	raw, err := m.ckpts.ReadMeta(ctx, ckptID)
	if err != nil {
		return types.Checkpoint{}, ErrUnknownCheckpoint
	}
	var ckpt types.Checkpoint
	if err := json.Unmarshal(raw, &ckpt); err != nil {
		return types.Checkpoint{}, ErrUnknownCheckpoint
	}
	return ckpt, nil
}
