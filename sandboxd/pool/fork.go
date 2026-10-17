package pool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// Fork clones a claimed sandbox into count children, each a fresh claim; all-or-nothing.
func (m *Manager) Fork(ctx context.Context, id string, cred Cred, count int, ttl time.Duration, onExpire types.ExpireAction, claimRefPrefix string) ([]*types.Sandbox, error) {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return nil, ErrUnknownSandbox
	}
	if !cred.Operator {
		if err := m.tenantRemoved(ctx, sb.Tenant); err != nil {
			return nil, err
		}
	}
	if count < 1 || count > m.maxFork {
		return nil, fmt.Errorf("%w: %d not in 1..%d", ErrBadCount, count, m.maxFork)
	}
	if !sb.Key.Capturable() {
		return nil, ErrNoEgressFork
	}
	if err := m.overQuota(count, sb.Tenant); err != nil {
		return nil, err
	}
	// See Hibernate: a started fork must finish even if the caller hangs up.
	ctx = context.WithoutCancel(ctx)

	children, inherited, err := m.forkClones(ctx, sb, count)
	if err != nil {
		return nil, fmt.Errorf("fork %s: %w", sb.ID, err)
	}
	m.mu.Lock()
	parentExpire := sb.OnExpire
	m.mu.Unlock()
	for _, c := range children {
		c.Tenant, c.EgressClass = sb.Tenant, sb.EgressClass
		c.PolicySource = sb.PolicySource
		c.NoEgress = sb.NoEgress
		c.Metadata = sb.Metadata
		c.OnExpire = onExpire.Or(parentExpire)
	}
	if err := m.finalizeBatch(ctx, children, ttl, claimRefPrefix, inherited); err != nil {
		return nil, fmt.Errorf("fork %s: %w", sb.ID, err)
	}
	m.counters.forks.Add(1)
	m.counters.claimsClone.Add(uint64(len(children)))
	ids := make([]string, len(children))
	for i, c := range children {
		ids[i] = c.ID
	}
	m.recordUsage(ctx, usageEvent{Event: "fork", ID: sb.ID, VMName: lockedVMName(sb), Children: ids})
	return children, nil
}

// forkClones clones a running source from a fresh snapshot, a hibernated one from an export; inherited reports a captured guest env file.
func (m *Manager) forkClones(ctx context.Context, sb *types.Sandbox, count int) (children []*types.Sandbox, inherited bool, err error) {
	create, cleanup, inherited, err := m.forkSource(ctx, sb)
	if err != nil {
		return nil, false, err
	}
	defer cleanup()
	children, err = m.cloneBatch(ctx, count, sb.Key, create)
	return children, inherited, err
}

// forkSource captures the fork source under the transition lock, against a racing hibernate.
func (m *Manager) forkSource(ctx context.Context, sb *types.Sandbox) (create vmProvisioner, cleanup func(), inherited bool, err error) {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if err = m.captureAllowed(sb); err != nil {
		return nil, nil, false, err
	}
	inherited = m.heldGuestEnv(sb)
	if sb.HibernateSnap == "" {
		snap, drop, snapErr := m.sourceSnap(ctx, sb)
		if snapErr != nil {
			return nil, nil, false, snapErr
		}
		return func(name string) (types.VMRecord, error) { return m.eng.CloneSnap(ctx, snap, name, sb.Key) },
			drop, inherited, nil
	}
	dir, err := os.MkdirTemp(m.dataDir, "fork-")
	if err != nil {
		return nil, nil, false, err
	}
	exportDir := filepath.Join(dir, "export") // cocoon wants the target absent
	if err = m.eng.SnapshotExport(ctx, sb.HibernateSnap, exportDir); err != nil {
		_ = os.RemoveAll(dir)
		return nil, nil, false, err
	}
	return func(name string) (types.VMRecord, error) { return m.eng.Clone(ctx, exportDir, name, sb.Key) },
		func() { _ = os.RemoveAll(dir) }, inherited, nil
}
