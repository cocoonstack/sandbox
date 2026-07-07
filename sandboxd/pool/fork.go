// Fork: clone a running sandbox into children at its current state.
package pool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	if err := m.overQuota(count, sb.Tenant); err != nil {
		return nil, err
	}
	// See Hibernate: a started fork must finish even if the caller hangs up.
	ctx = context.WithoutCancel(ctx)

	dir, err := os.MkdirTemp(m.dataDir, "fork-")
	if err != nil {
		return nil, fmt.Errorf("fork %s: %w", id, err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	exportDir := filepath.Join(dir, "export") // cocoon wants the target absent
	if err = m.exportSource(ctx, sb, exportDir); err != nil {
		return nil, fmt.Errorf("fork %s: %w", id, err)
	}

	children, err := m.cloneBatch(ctx, sb.Key, exportDir, count)
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
