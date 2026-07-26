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

// DeleteScope selects how far a checkpoint delete reaches. The fleet-wide
// value is the zero one: a caller that does not think about scope gets the
// safe behavior, and the narrower local-only delete must be asked for.
type DeleteScope int

const (
	DeleteFleet DeleteScope = iota
	DeleteLocal
)

var (
	ErrBadName           = errors.New("invalid checkpoint name")
	ErrUnknownCheckpoint = errors.New("unknown checkpoint")
	ErrHealBusy          = errors.New("too many checkpoint heals in flight")
)

// Checkpoint captures a claimed sandbox's state under a fresh id; the source
// keeps running (a hibernated one is captured from its wake image). Branches
// clone that exact state, and a source can be checkpointed again — a tree.
// tenant attributes the record; empty means the operator (root).
func (m *Manager) Checkpoint(ctx context.Context, id string, cred Cred, name, tenant string) (types.Checkpoint, error) {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return types.Checkpoint{}, ErrUnknownSandbox
	}
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
	// Reject a bad or unknown id before recLock: a rejected id must not leave
	// a lock-map entry (only a delete evicts one). Checkpoints are immutable,
	// so this parse stands in for the fetched meta below. The local read is
	// cheap, so it goes before quota: a full node must still answer "not
	// here" on a miss, or the handler's redirect/heal tiers never run.
	ckpt, err := m.loadCheckpoint(ctx, ckptID)
	if err != nil {
		return nil, err
	}
	if err := m.overQuota(1, tenant); err != nil {
		return nil, err
	}
	return m.claimLoaded(ctx, ckpt, ttl, tenant)
}

// ClaimCheckpointHeal claims a checkpoint this node does not hold locally,
// pulling it from a peer first. The server calls it only after the local
// claim missed and a redirect could not answer.
func (m *Manager) ClaimCheckpointHeal(ctx context.Context, ckptID string, ttl time.Duration, tenant string) (*types.Sandbox, error) {
	if m.healer == nil || !store.CheckpointIDRe.MatchString(ckptID) {
		return nil, ErrUnknownCheckpoint
	}
	// Unlike ClaimCheckpoint, resolving the record here means a peer
	// transfer of a whole guest memory image: quota must reject a full node
	// before paying that cost, not after.
	if err := m.overQuota(1, tenant); err != nil {
		return nil, err
	}
	ckpt, err := m.healCheckpoint(ctx, ckptID)
	if err != nil {
		return nil, err
	}
	return m.claimLoaded(ctx, ckpt, ttl, tenant)
}

// claimLoaded is ClaimCheckpoint's and ClaimCheckpointHeal's shared body once
// ckpt's meta is resolved: it re-fetches under the record lock (immune to a
// delete racing the pre-check), provisions the branch, and finalizes the claim.
func (m *Manager) claimLoaded(ctx context.Context, ckpt types.Checkpoint, ttl time.Duration, tenant string) (*types.Sandbox, error) {
	l := m.recLock(ckpt.ID)
	l.RLock()
	defer l.RUnlock()
	dir, _, release, err := m.ckpts.Fetch(ctx, ckpt.ID)
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

// healCheckpoint pulls ckptID from a peer under its record lock, so the
// transfer, validation, and publish race no concurrent heal, delete, or TTL
// sweep of the same id. A record already local when the lock is acquired
// (another heal won the race) is used as-is. claimLoaded takes its own
// RLock on the same recLock, so this releases before returning instead of
// holding it into the claim; a delete racing in between is handled there
// (Fetch answers store.ErrNotFound).
func (m *Manager) healCheckpoint(ctx context.Context, ckptID string) (types.Checkpoint, error) {
	l := m.recLock(ckptID)
	l.Lock()
	defer l.Unlock()
	if ckpt, err := m.loadCheckpoint(ctx, ckptID); err == nil {
		return ckpt, nil
	}
	select {
	case m.healSem <- struct{}{}:
		defer func() { <-m.healSem }()
	default:
		return types.Checkpoint{}, ErrHealBusy
	}
	staging, err := m.ckpts.Stage(ckptID)
	if err != nil {
		return types.Checkpoint{}, fmt.Errorf("stage healed checkpoint: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	validate := func(dir string) error { return validateHealedCheckpoint(dir, ckptID) }
	if err := m.healer.Pull(ctx, ckptID, staging, validate); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return types.Checkpoint{}, ErrUnknownCheckpoint
		}
		return types.Checkpoint{}, fmt.Errorf("heal checkpoint: %w", err)
	}
	// A client hanging up must not abort a transfer that already landed.
	if err := m.ckpts.Publish(context.WithoutCancel(ctx), staging, ckptID); err != nil {
		return types.Checkpoint{}, fmt.Errorf("publish healed checkpoint: %w", err)
	}
	return m.loadCheckpoint(ctx, ckptID)
}

// validateHealedCheckpoint checks a staged pull's shape before it is trusted
// enough to publish: a hostile or buggy peer must not be able to install an
// unreadable or misattributed record that then suppresses future healing.
func validateHealedCheckpoint(staging, wantID string) error {
	raw, err := os.ReadFile(filepath.Join(staging, store.MetaFile)) //nolint:gosec // staging dir is this manager's own
	if err != nil {
		return fmt.Errorf("read healed meta: %w", err)
	}
	ckpt, err := parseCheckpoint(raw)
	if err != nil {
		return fmt.Errorf("parse healed meta: %w", err)
	}
	if ckpt.ID != wantID {
		return fmt.Errorf("healed record id %q does not match requested %q", ckpt.ID, wantID)
	}
	if ckpt.Archive {
		return fmt.Errorf("healed record %s is a wake image, not a checkpoint", wantID)
	}
	if !dirExists(filepath.Join(staging, store.ExportDir)) {
		return fmt.Errorf("healed record %s missing export dir", wantID)
	}
	entries, err := os.ReadDir(staging)
	if err != nil {
		return fmt.Errorf("read staging: %w", err)
	}
	for _, e := range entries {
		if e.Name() != store.MetaFile && e.Name() != store.ExportDir {
			return fmt.Errorf("healed record %s has unexpected entry %q", wantID, e.Name())
		}
	}
	return nil
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

// DeleteCheckpoint removes a checkpoint's snapshot and record. A fleet-scoped
// delete then best-effort broadcasts to peers, so a copy healed onto one does
// not outlive it; a forwarded delete is DeleteLocal, since re-forwarding would
// loop the fleet forever. A tenant may delete only its own records — anything
// else answers ErrUnknownCheckpoint, never a hint that the id exists; root
// (empty tenant) deletes anything.
func (m *Manager) DeleteCheckpoint(ctx context.Context, ckptID, tenant string, scope DeleteScope) error {
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
	// A shared backend (s3) has no per-node replicas to chase: the delete
	// above already removed the one copy every node sees.
	if scope == DeleteFleet && m.peerDelete != nil && !m.ckptsShared {
		m.peerDelete(context.WithoutCancel(ctx), ckptID)
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

// loadCheckpoint reads and parses a checkpoint's meta from the local store.
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

// HasCheckpoint reports whether this node's local store holds a branchable
// (non-archive) record for id — the probe answer, never a fetch.
func (m *Manager) HasCheckpoint(ctx context.Context, ckptID string) bool {
	ckpt, err := m.loadCheckpoint(ctx, ckptID)
	return err == nil && !ckpt.Archive
}

// FetchCheckpoint materializes a checkpoint's export for a peer transfer,
// returning the local directory, its meta, and the release to call when done.
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
