package pool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// Fork clones a claimed sandbox into count children, each a fresh claim with
// its own lease: memory, disk, and guest state (sessions, processes, tmpfs)
// duplicate at the snapshot point, and cocoon's clone reseed gives every
// child a distinct machine identity. Children inherit the parent's tenant
// and count against its quota. All-or-nothing: any child failing destroys
// the ones already built, so an error means no child survived.
func (m *Manager) Fork(ctx context.Context, id, token string, count int, ttl time.Duration) ([]*types.Sandbox, error) {
	if count < 1 || count > m.maxFork {
		return nil, fmt.Errorf("%w: %d not in 1..%d", ErrBadCount, count, m.maxFork)
	}
	sb, ok := m.claim(id, token)
	if !ok {
		return nil, ErrUnknownSandbox
	}
	if sb.Key.Net == types.NetEgress {
		return nil, ErrNoEgressFork
	}
	if err := m.overQuota(count, sb.Tenant); err != nil {
		return nil, err
	}
	// See Hibernate: a started fork must finish even if the caller hangs up.
	ctx = context.WithoutCancel(ctx)

	children, err := m.forkClones(ctx, sb, count)
	if err != nil {
		return nil, fmt.Errorf("fork %s: %w", id, err)
	}
	for _, c := range children {
		c.Tenant = sb.Tenant
	}
	if err := m.finalizeBatch(ctx, children, ttl); err != nil {
		return nil, fmt.Errorf("fork %s: %w", id, err)
	}
	m.counters.forks.Add(1)
	m.counters.claimsClone.Add(uint64(len(children))) //nolint:gosec // count is bounded by maxFork
	ids := make([]string, len(children))
	for i, c := range children {
		ids[i] = c.ID
	}
	m.recordUsage(ctx, usageEvent{Event: "fork", ID: sb.ID, VMName: sb.VMName, Children: ids})
	return children, nil
}

// forkClones builds count children from a claimed source: a running source is
// cloned straight from a fresh store snapshot (no export copy); a hibernated
// source stays on the export path (its shared wake image needs a private copy).
func (m *Manager) forkClones(ctx context.Context, sb *types.Sandbox, count int) ([]*types.Sandbox, error) {
	if sb.HibernateSnap != "" {
		dir, err := os.MkdirTemp(m.dataDir, "fork-")
		if err != nil {
			return nil, err
		}
		defer func() { _ = os.RemoveAll(dir) }()
		exportDir := filepath.Join(dir, "export") // cocoon wants the target absent
		if _, err := m.exportSource(ctx, sb, exportDir); err != nil {
			return nil, err
		}
		return m.cloneBatch(ctx, count, func() (*types.Sandbox, error) {
			return m.provision(ctx, sb.Key, exportDir)
		})
	}
	snap, drop, err := m.captureRunning(ctx, sb)
	if err != nil {
		return nil, err
	}
	defer drop()
	return m.cloneBatch(ctx, count, func() (*types.Sandbox, error) {
		return m.provisionSnap(ctx, sb.Key, snap)
	})
}

// captureRunning snapshots a running sandbox into the local store under the
// transition lock (pinning it against a concurrent wake); the fan-out then runs
// unlocked since the fresh snapshot is private to this fork. Cleanup drops it.
func (m *Manager) captureRunning(ctx context.Context, sb *types.Sandbox) (string, func(), error) {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	snap := forkPrefix + strings.TrimPrefix(sb.VMName, vmPrefix) + "-" + randHex(3)
	if err := m.eng.SnapshotSave(ctx, sb.VMName, snap); err != nil {
		return "", nil, err
	}
	return snap, func() { m.dropSnap(ctx, snap) }, nil
}
