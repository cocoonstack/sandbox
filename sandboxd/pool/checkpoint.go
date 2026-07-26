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

// Checkpoint captures a claimed sandbox's state under a fresh id; the source
// keeps running (a hibernated one is captured from its wake image). Branches
// clone that exact state, and a source can be checkpointed again — a tree.
// tenant attributes the record; empty means the operator (root).
func (m *Manager) Checkpoint(ctx context.Context, id, token, name, tenant string) (types.Checkpoint, error) {
	sb, ok := m.claim(id, token)
	if !ok {
		return types.Checkpoint{}, ErrUnknownSandbox
	}
	return m.checkpointResolved(ctx, sb, name, tenant)
}

// CheckpointOperator checkpoints a sandbox by id without a per-sandbox token.
// It is the operator (root) path, authorized by the node's root api_token
// before the call — mirroring ReleaseOperator.
func (m *Manager) CheckpointOperator(ctx context.Context, id, name, tenant string) (types.Checkpoint, error) {
	sb, ok := m.byID(id)
	if !ok {
		return types.Checkpoint{}, ErrUnknownSandbox
	}
	return m.checkpointResolved(ctx, sb, name, tenant)
}

// checkpointResolved is Checkpoint's body once the source claim is resolved.
func (m *Manager) checkpointResolved(ctx context.Context, sb *types.Sandbox, name, tenant string) (types.Checkpoint, error) {
	if name != "" && !types.NameRe.MatchString(name) {
		return types.Checkpoint{}, fmt.Errorf("%w: %q must match %s", ErrBadName, name, types.NameRe)
	}
	if !sb.Key.Capturable() {
		return types.Checkpoint{}, ErrNoEgressFork
	}
	// See Hibernate: a started capture must finish even if the caller hangs up.
	ctx = context.WithoutCancel(ctx)
	ckpt, _, err := m.publishCheckpoint(ctx, sb, store.CheckpointID(randHex(8)), name, tenant, false)
	if err != nil {
		return types.Checkpoint{}, err
	}
	m.counters.checkpoints.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "checkpoint", ID: sb.ID, VMName: sb.VMName, Reference: ckpt.ID})
	return ckpt, nil
}

// publishCheckpoint stages the sandbox's exported state, writes the meta, and
// publishes it to the store, returning the record and the source snapshot the
// export captured. Shared by Checkpoint and archive; a hibernated source
// exports its wake image directly (no VM start — refill.sourceSnap).
func (m *Manager) publishCheckpoint(ctx context.Context, sb *types.Sandbox, ckID, name, tenant string, archive bool) (types.Checkpoint, string, error) {
	ckpt := types.Checkpoint{
		ID:        ckID,
		Name:      name,
		SandboxID: sb.ID,
		Key:       sb.Key,
		Tenant:    tenant,
		CreatedAt: time.Now(),
		Archive:   archive,
	}
	staging, err := m.ckpts.Stage(ckpt.ID)
	if err != nil {
		return types.Checkpoint{}, "", fmt.Errorf("stage checkpoint: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	srcSnap, err := m.exportSource(ctx, sb, filepath.Join(staging, store.ExportDir))
	if err != nil {
		return types.Checkpoint{}, "", fmt.Errorf("checkpoint %s: %w", sb.ID, err)
	}
	meta, err := json.Marshal(ckpt)
	if err != nil {
		return types.Checkpoint{}, "", fmt.Errorf("encode checkpoint meta: %w", err)
	}
	if err := os.WriteFile(filepath.Join(staging, store.MetaFile), meta, 0o600); err != nil {
		return types.Checkpoint{}, "", fmt.Errorf("write checkpoint meta: %w", err)
	}
	if err := m.ckpts.Publish(ctx, staging, ckpt.ID); err != nil {
		return types.Checkpoint{}, "", fmt.Errorf("commit checkpoint: %w", err)
	}
	return ckpt, srcSnap, nil
}

// ClaimCheckpoint provisions a fresh claim cloned from a checkpoint — a
// branch. The checkpoint's recorded key applies (snapshots pin size and
// lane); the checkpoint itself is read-only and reusable. The claim is
// attributed to tenant, not the checkpoint's recorder — the unguessable id
// is the capability to branch.
func (m *Manager) ClaimCheckpoint(ctx context.Context, ckptID string, ttl time.Duration, tenant string) (*types.Sandbox, error) {
	if err := m.overQuota(1, tenant); err != nil {
		return nil, err
	}
	// Reject a bad or unknown id before recLock: a rejected id must not leave
	// a lock-map entry (only a delete evicts one). Checkpoints are immutable,
	// so this parse stands in for the fetched meta below.
	ckpt, err := m.loadCheckpoint(ctx, ckptID)
	if err != nil {
		return nil, err
	}
	l := m.recLock(ckptID)
	l.RLock()
	defer l.RUnlock()
	dir, _, release, err := m.ckpts.Fetch(ctx, ckptID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrUnknownCheckpoint // deleted between the pre-check and the lock
	}
	if err != nil {
		return nil, fmt.Errorf("fetch checkpoint: %w", err)
	}
	defer release()
	if ckpt.Archive {
		return nil, ErrUnknownCheckpoint // a wake image, not a branchable checkpoint
	}
	if !ckpt.Key.Capturable() {
		return nil, ErrNoEgressFork
	}
	sb, err := m.provision(ctx, ckpt.Key, dir)
	if err != nil {
		return nil, err
	}
	sb.FromCheckpoint = ckpt.ID
	sb.Tenant = tenant
	out, err := m.finalize(ctx, sb, ttl)
	if err == nil {
		m.counters.claimsClone.Add(1)
	}
	return out, err
}

// Checkpoints lists the store's checkpoints, newest first — on a shared
// backend (a FUSE mount, a bucket), that is the cluster's set, not one
// node's. A non-empty tenant filters to that tenant's records; empty (root)
// lists everything. Checkpoints backing a live archived claim are hidden:
// they are lifecycle-internal wake images, not user checkpoints.
func (m *Manager) Checkpoints(ctx context.Context, tenant string) ([]types.Checkpoint, error) {
	metas, err := m.ckpts.Metas(ctx)
	if err != nil {
		return nil, fmt.Errorf("list checkpoints: %w", err)
	}
	pinned := m.pinnedArchiveCks()
	ckpts := make([]types.Checkpoint, 0, len(metas))
	for _, raw := range metas {
		var ckpt types.Checkpoint
		if err := json.Unmarshal(raw, &ckpt); err != nil || !tenantOwns(tenant, ckpt.Tenant) {
			continue
		}
		if _, archived := pinned[ckpt.ID]; archived || ckpt.Archive {
			continue
		}
		ckpts = append(ckpts, ckpt)
	}
	slices.SortFunc(ckpts, func(a, b types.Checkpoint) int { return b.CreatedAt.Compare(a.CreatedAt) })
	return ckpts, nil
}

// pinnedArchiveCks is the set of checkpoint ids backing a live archived claim
// or an archive publish in flight (pendingCks): wake images the listing hides
// and delete/TTL must spare (deleting one would strand its sandbox).
func (m *Manager) pinnedArchiveCks() map[string]struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	pinned := make(map[string]struct{}, len(m.pendingCks))
	for id := range m.pendingCks {
		pinned[id] = struct{}{}
	}
	for _, sb := range m.claimed {
		if sb.ArchiveCk != "" {
			pinned[sb.ArchiveCk] = struct{}{}
		}
	}
	return pinned
}

// DeleteCheckpoint removes a checkpoint's snapshot and record. A tenant may
// delete only its own records — anything else answers ErrUnknownCheckpoint,
// never a hint that the id exists; root (empty tenant) deletes anything.
func (m *Manager) DeleteCheckpoint(ctx context.Context, ckptID, tenant string) error {
	// Validate off the lock — a rejected id must not leave a lock-map entry;
	// loadCheckpoint is safe unlocked (checkpoints are single-publish, immutable).
	ckpt, err := m.loadCheckpoint(ctx, ckptID)
	if err != nil {
		return err
	}
	if !tenantOwns(tenant, ckpt.Tenant) {
		return ErrUnknownCheckpoint
	}
	// ckpt.Archive guards wake images across every node sharing the store;
	// the pin set guards this node's not-yet-committed ones.
	if _, pinned := m.pinnedArchiveCks()[ckptID]; pinned || ckpt.Archive {
		return ErrUnknownCheckpoint // backs an archived sandbox, not a deletable checkpoint
	}
	if err := m.deleteCkLocked(ctx, ckptID); err != nil {
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	return nil
}

// sweepExpiredCheckpoints ages out checkpoints older than the configured
// TTL; explicit deletes never wait for it. It runs detached from the Run
// loop, so the guard keeps a slow backend from stacking sweeps.
func (m *Manager) sweepExpiredCheckpoints(ctx context.Context) {
	if !m.ckptSweeping.CompareAndSwap(false, true) {
		return
	}
	defer m.ckptSweeping.Store(false)
	logger := log.WithFunc("pool.sweepExpiredCheckpoints")
	// Checkpoints already hides archive images backing a live claim, so the
	// TTL never reaches one; their retention is the claim's own Deadline
	// (reapPurge). An orphaned archive ck carries no live reference and ages out.
	ckpts, err := m.Checkpoints(ctx, "")
	if err != nil {
		logger.Error(ctx, err, "list for retention")
		return
	}
	cutoff := time.Now().Add(-m.ckptTTL)
	for _, ckpt := range ckpts {
		if ckpt.CreatedAt.After(cutoff) {
			continue
		}
		if err := m.deleteCkLocked(ctx, ckpt.ID); err != nil {
			logger.Errorf(ctx, err, "expire %s", ckpt.ID)
		}
	}
}

// deleteCkLocked removes a checkpoint under its record lock, so the delete
// never runs beneath an in-flight branch clone or archived wake.
func (m *Manager) deleteCkLocked(ctx context.Context, ckID string) error {
	l := m.recLock(ckID)
	l.Lock()
	defer l.Unlock()
	if err := m.ckpts.Delete(ctx, ckID); err != nil {
		return err
	}
	m.dropRecLock(ckID)
	return nil
}

func (m *Manager) loadCheckpoint(ctx context.Context, ckptID string) (types.Checkpoint, error) {
	if !store.CheckpointIDRe.MatchString(ckptID) {
		return types.Checkpoint{}, ErrUnknownCheckpoint
	}
	raw, err := m.ckpts.ReadMeta(ctx, ckptID)
	if errors.Is(err, store.ErrNotFound) {
		return types.Checkpoint{}, ErrUnknownCheckpoint
	}
	if err != nil {
		return types.Checkpoint{}, fmt.Errorf("read checkpoint: %w", err)
	}
	return parseCheckpoint(raw)
}

func parseCheckpoint(raw []byte) (types.Checkpoint, error) {
	var ckpt types.Checkpoint
	if err := json.Unmarshal(raw, &ckpt); err != nil {
		return types.Checkpoint{}, ErrUnknownCheckpoint
	}
	return ckpt, nil
}

// CheckpointIDs lists this node's checkpoint ids for the mesh to gossip, so a
// peer can resolve a branch of a checkpoint it does not hold to the node that
// does. Archive records are excluded: they are lifecycle-internal wake images,
// never a branch target. Errors are swallowed to nil — gossip is best-effort
// and a transient store read must not take the tick down.
func (m *Manager) CheckpointIDs() []string {
	ckpts, err := m.Checkpoints(context.Background(), "")
	if err != nil {
		return nil
	}
	ids := make([]string, 0, len(ckpts))
	for _, c := range ckpts {
		if !c.Archive {
			ids = append(ids, c.ID)
		}
	}
	slices.Sort(ids)
	return ids
}

// FetchCheckpoint materializes a checkpoint's export for a peer transfer,
// returning the local directory, its meta, and the release to call when the
// copy is done. It is the read half of the peer-heal path; the id is validated
// against the store's namespace before any disk is touched.
func (m *Manager) FetchCheckpoint(ctx context.Context, ckptID string) (string, []byte, func(), error) {
	if _, err := m.loadCheckpoint(ctx, ckptID); err != nil {
		return "", nil, nil, err
	}
	l := m.recLock(ckptID)
	l.RLock()
	dir, meta, release, err := m.ckpts.Fetch(ctx, ckptID)
	if err != nil {
		l.RUnlock()
		if errors.Is(err, store.ErrNotFound) {
			return "", nil, nil, ErrUnknownCheckpoint
		}
		return "", nil, nil, fmt.Errorf("fetch checkpoint: %w", err)
	}
	// The read lock spans the transfer: a delete must not pull the export out
	// from under a stream already writing it to a peer.
	return dir, meta, func() { release(); l.RUnlock() }, nil
}
