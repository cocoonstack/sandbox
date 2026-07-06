package pool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const checkpointExport = "export"

var (
	ErrBadName           = errors.New("invalid checkpoint name")
	ErrUnknownCheckpoint = errors.New("unknown checkpoint")

	// checkpointIDRe pins the id shape wherever an id reaches the filesystem,
	// so a crafted id can never escape the checkpoints dir.
	checkpointIDRe = regexp.MustCompile(`^ck_[0-9a-f]{16}$`)
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
	staging, err := os.MkdirTemp(m.checkpointsDir(), ckpt.ID+"-*.tmp")
	if err != nil {
		return types.Checkpoint{}, fmt.Errorf("stage checkpoint: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	if err = m.exportSource(ctx, sb, filepath.Join(staging, checkpointExport)); err != nil {
		return types.Checkpoint{}, fmt.Errorf("checkpoint %s: %w", id, err)
	}
	meta, err := json.Marshal(ckpt)
	if err != nil {
		return types.Checkpoint{}, fmt.Errorf("encode checkpoint meta: %w", err)
	}
	if err := os.WriteFile(filepath.Join(staging, "meta.json"), meta, 0o600); err != nil {
		return types.Checkpoint{}, fmt.Errorf("write checkpoint meta: %w", err)
	}
	if err := os.Rename(staging, filepath.Join(m.checkpointsDir(), ckpt.ID)); err != nil {
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
	ckpt, err := m.loadCheckpoint(ckptID)
	if err != nil {
		return nil, err
	}
	sb, err := m.provision(ctx, ckpt.Key, filepath.Join(m.checkpointsDir(), ckpt.ID, checkpointExport))
	if err != nil {
		return nil, err
	}
	sb.FromCheckpoint = ckpt.ID
	return m.finalize(ctx, sb, ttl)
}

// Checkpoints lists this node's checkpoints, newest first.
func (m *Manager) Checkpoints() ([]types.Checkpoint, error) {
	entries, err := os.ReadDir(m.checkpointsDir())
	if err != nil {
		return nil, fmt.Errorf("list checkpoints: %w", err)
	}
	var ckpts []types.Checkpoint
	for _, e := range entries {
		if !e.IsDir() || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		if ckpt, err := m.loadCheckpoint(e.Name()); err == nil {
			ckpts = append(ckpts, ckpt)
		}
	}
	slices.SortFunc(ckpts, func(a, b types.Checkpoint) int { return b.CreatedAt.Compare(a.CreatedAt) })
	return ckpts, nil
}

// DeleteCheckpoint removes a checkpoint's snapshot and record.
func (m *Manager) DeleteCheckpoint(ckptID string) error {
	if _, err := m.loadCheckpoint(ckptID); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(m.checkpointsDir(), ckptID)); err != nil {
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	return nil
}

func (m *Manager) loadCheckpoint(ckptID string) (types.Checkpoint, error) {
	if !checkpointIDRe.MatchString(ckptID) {
		return types.Checkpoint{}, ErrUnknownCheckpoint
	}
	raw, err := os.ReadFile(filepath.Join(m.checkpointsDir(), ckptID, "meta.json")) //nolint:gosec // ckptID pinned by checkpointIDRe above
	if err != nil {
		return types.Checkpoint{}, ErrUnknownCheckpoint
	}
	var ckpt types.Checkpoint
	if err := json.Unmarshal(raw, &ckpt); err != nil {
		return types.Checkpoint{}, ErrUnknownCheckpoint
	}
	return ckpt, nil
}

func (m *Manager) checkpointsDir() string {
	return filepath.Join(m.dataDir, "checkpoints")
}
