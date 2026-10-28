package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const (
	recommitBackoff    = 20 * time.Millisecond
	recommitMaxBackoff = 5 * time.Second
)

// claimSnapshot sequences one persist request; commit skips it once a newer write has landed.
type claimSnapshot struct {
	seq uint64
}

// claimDTO is the persisted projection of a Sandbox, copied so commit marshals off m.mu.
type claimDTO struct {
	ID             string             `json:"id"`
	VMName         string             `json:"vm_name"`
	Key            types.PoolKey      `json:"key"`
	Token          string             `json:"token,omitempty"`
	Deadline       time.Time          `json:"deadline,omitzero"`
	ClaimedAt      time.Time          `json:"claimed_at,omitzero"`
	LeaseSeconds   int                `json:"lease_seconds,omitzero"`
	Layer          types.PolicyLayer  `json:"policy_layer,omitempty"`
	PolicySource   types.PoolKey      `json:"policy_source,omitzero"`
	NoEgress       bool               `json:"no_egress,omitzero"`
	Tenant         string             `json:"tenant,omitempty"`
	EgressClass    string             `json:"egress_class,omitempty"`
	ClaimRef       string             `json:"claim_ref,omitempty"`
	Metadata       types.Metadata     `json:"metadata,omitempty"`
	OnExpire       types.ExpireAction `json:"on_expire,omitempty"`
	Volumes        []types.Volume     `json:"volumes,omitempty"`
	VolumesWant    []types.Volume     `json:"volumes_want,omitzero"`
	Env            types.Env          `json:"env,omitempty"`
	VsockSocket    string             `json:"vsock_socket,omitempty"`
	TAP            string             `json:"tap,omitempty"`
	HibernateSnap  string             `json:"hibernate_snap,omitempty"`
	PendingSnap    string             `json:"pending_snap,omitempty"`
	ReaderSafe     bool               `json:"reader_safe,omitzero"`
	ArchiveCk      string             `json:"archive_ck,omitempty"`
	ArchiveEpoch   int64              `json:"archive_epoch,omitzero"`
	ArchivePending string             `json:"archive_pending,omitempty"`
	ReplicaCk      string             `json:"replica_ck,omitempty"`
	ReplicaPending string             `json:"replica_pending,omitempty"`
	FromCheckpoint string             `json:"from_checkpoint,omitempty"`
	Restarts       int                `json:"restarts,omitzero"`
	RestartedAt    time.Time          `json:"restarted_at,omitzero"`
	Failed         string             `json:"failed,omitempty"`
}

// claimRow keeps its encoded member until set replaces the row; set never edits one.
type claimRow struct {
	dto claimDTO
	raw []byte
}

// claimStore persists claimed sandboxes across daemon restarts; warm VMs are not persisted.
type claimStore struct {
	path string
	sync bool

	// writeMu orders commits and guards every row's raw and buf.
	writeMu sync.Mutex
	buf     []byte

	mu          sync.Mutex
	rows        map[string]*claimRow
	seq         uint64
	written     uint64
	refs        map[string][4]string
	retained    map[string][]string
	changed     map[string]struct{}
	checkpoints map[string]int

	recommitting atomic.Bool
}

func newClaimStore(dataDir string, sync bool) *claimStore {
	return &claimStore{
		path: filepath.Join(dataDir, "claims.json"), sync: sync,
		rows: map[string]*claimRow{}, refs: map[string][4]string{}, retained: map[string][]string{},
		changed: map[string]struct{}{}, checkpoints: map[string]int{},
	}
}

func (s *claimStore) load() (map[string]*types.Sandbox, error) {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]*types.Sandbox{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read claims: %w", err)
	}
	claims := map[string]*types.Sandbox{}
	if err := json.Unmarshal(raw, &claims); err != nil {
		return nil, fmt.Errorf("parse claims: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sb := range claims {
		for _, volumes := range [][]types.Volume{sb.Volumes, sb.VolumesWant} {
			for i := range volumes {
				volumes[i].AttachOnly = volumes[i].AttachOnly || volumes[i].Mount == ""
			}
		}
		if sb.VolumesWant == nil {
			sb.VolumesWant = sb.Volumes
		}
		if refs := checkpointRefs(sb); refs != [4]string{} {
			s.changeRetainedRefs(sb.ID, appendRefs(nil, refs))
			s.changed[sb.ID] = struct{}{}
		}
	}
	return claims, nil
}

// set records each sandbox's projection and sequences the write; callers may hold m.mu.
func (s *claimStore) set(sbs ...*types.Sandbox) claimSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sb := range sbs {
		s.put(sb.ID, sb)
	}
	s.seq++
	return claimSnapshot{seq: s.seq}
}

// del drops the projections of ids and sequences the write; callers may hold m.mu.
func (s *claimStore) del(ids ...string) claimSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		s.put(id, nil)
	}
	s.seq++
	return claimSnapshot{seq: s.seq}
}

func (s *claimStore) put(id string, sb *types.Sandbox) {
	var refs [4]string
	if sb == nil {
		delete(s.rows, id)
	} else {
		refs = checkpointRefs(sb)
		s.rows[id] = &claimRow{dto: dtoOf(sb)}
	}
	before := s.refs[id]
	if before == refs {
		return
	}
	s.changeCheckpointCounts(before[:], -1)
	s.changeCheckpointCounts(refs[:], 1)
	if refs == [4]string{} {
		delete(s.refs, id)
	} else {
		s.refs[id] = refs
	}
	s.changed[id] = struct{}{}
}

// mark sequences a persist of the projection as it already stands.
func (s *claimStore) mark() claimSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return claimSnapshot{seq: s.seq}
}

func (s *claimStore) pending(snap claimSnapshot) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return snap.seq > s.written
}

// reset rebuilds the whole projection; the startup path, before contention exists.
func (s *claimStore) reset(claims map[string]*types.Sandbox) claimSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.rows {
		if _, ok := claims[id]; !ok {
			s.put(id, nil)
		}
	}
	for id, sb := range claims {
		s.put(id, sb)
	}
	s.seq++
	return claimSnapshot{seq: s.seq}
}

// commit atomically replaces claims.json, encoding only the rows changed since the last commit; fsync is opt-in (sync_claims).
func (s *claimStore) commit(snap claimSnapshot) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if snap.seq <= s.written {
		s.mu.Unlock()
		return nil
	}
	rows, seq := slices.AppendSeq(make([]*claimRow, 0, len(s.rows)), maps.Values(s.rows)), s.seq
	captured := make(map[string][4]string, len(s.changed))
	for id := range s.changed {
		captured[id] = s.refs[id]
		s.changeRetainedRefs(id, appendRefs(s.retained[id], s.refs[id]))
	}
	s.mu.Unlock()
	s.buf = append(s.buf[:0], '{')
	for i, row := range rows {
		if row.raw == nil {
			raw, err := encodeRow(&row.dto)
			if err != nil {
				return err
			}
			row.raw, row.dto = raw, claimDTO{}
		}
		if i > 0 {
			s.buf = append(s.buf, ',')
		}
		s.buf = append(s.buf, row.raw...)
	}
	s.buf = append(s.buf, '}')
	if err := s.replace(s.buf); err != nil {
		return err
	}
	s.mu.Lock()
	for id, refs := range captured {
		s.changeRetainedRefs(id, appendRefs(nil, refs))
		if s.refs[id] == refs {
			delete(s.changed, id)
		}
	}
	s.written = seq
	s.mu.Unlock()
	return nil
}

// recommit keeps claims.json catching up with the store: one retrier at a time writes the current state until it is durable and nothing newer is pending.
func (s *claimStore) recommit(ctx context.Context, snap claimSnapshot) {
	if !s.recommitting.CompareAndSwap(false, true) {
		return
	}
	go func() {
		backoff := recommitBackoff
		for {
			err := s.commit(snap)
			if err == nil {
				s.recommitting.Store(false)
				if snap = s.mark(); !s.pending(snap) || !s.recommitting.CompareAndSwap(false, true) {
					return
				}
				continue
			}
			log.WithFunc("pool.recommit").Error(ctx, err, "persist claims")
			time.Sleep(backoff)
			backoff = min(backoff*2, recommitMaxBackoff)
		}
	}()
}

// replace swaps claims.json for raw through the fixed temp name, durably when sync is set.
func (s *claimStore) replace(raw []byte) error {
	tmp := s.path + ".tmp"
	if !s.sync {
		if err := os.WriteFile(tmp, raw, 0o600); err != nil {
			return fmt.Errorf("write claims: %w", err)
		}
		if err := os.Rename(tmp, s.path); err != nil {
			return fmt.Errorf("commit claims: %w", err)
		}
		return nil
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec // our own data-dir path
	if err != nil {
		return fmt.Errorf("write claims: %w", err)
	}
	if err := utils.ReplaceFileSync(f, s.path, raw); err != nil {
		return fmt.Errorf("commit claims: %w", err)
	}
	return nil
}

// save is the combined form for the startup Reconcile pass, before contention exists.
func (s *claimStore) save(claims map[string]*types.Sandbox) error {
	return s.commit(s.reset(claims))
}

func (s *claimStore) synced() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.written == s.seq
}

func (s *claimStore) changeCheckpointCounts(refs []string, delta int) {
	for _, ck := range refs {
		if ck == "" {
			continue
		}
		s.checkpoints[ck] += delta
		if s.checkpoints[ck] == 0 {
			delete(s.checkpoints, ck)
		}
	}
}

func (s *claimStore) changeRetainedRefs(id string, after []string) {
	s.changeCheckpointCounts(s.retained[id], -1)
	s.changeCheckpointCounts(after, 1)
	if len(after) == 0 {
		delete(s.retained, id)
	} else {
		s.retained[id] = after
	}
}

func (s *claimStore) referencesCheckpoint(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkpoints[id] > 0
}

func (s *claimStore) checkpointReferences() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	refs := make(map[string]struct{}, len(s.checkpoints))
	for ck := range s.checkpoints {
		refs[ck] = struct{}{}
	}
	return refs
}

func (s *claimStore) hasCheckpointReferences(owner string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refs[owner] != [4]string{} || len(s.retained[owner]) != 0
}

func encodeRow(dto *claimDTO) ([]byte, error) {
	raw, err := json.Marshal(map[string]*claimDTO{dto.ID: dto})
	if err != nil {
		return nil, fmt.Errorf("encode claim %s: %w", dto.ID, err)
	}
	return raw[1 : len(raw)-1], nil
}

func dtoOf(sb *types.Sandbox) claimDTO {
	return claimDTO{
		ID: sb.ID, VMName: sb.VMName, Key: sb.Key, Token: sb.Token,
		Deadline: sb.Deadline, ClaimedAt: sb.ClaimedAt, LeaseSeconds: sb.LeaseSeconds, Layer: sb.Layer,
		PolicySource: sb.PolicySource, NoEgress: sb.NoEgress,
		Tenant: sb.Tenant, EgressClass: sb.EgressClass, ClaimRef: sb.ClaimRef, Metadata: sb.Metadata, OnExpire: sb.OnExpire,
		Volumes: slices.Clone(sb.Volumes), VolumesWant: volumesWantDTO(sb), Env: sb.Env, VsockSocket: sb.VsockSocket,
		TAP: sb.TAP, HibernateSnap: sb.HibernateSnap, PendingSnap: sb.PendingSnap, ReaderSafe: sb.ReaderSafe,
		ArchiveCk: sb.ArchiveCk, ArchiveEpoch: sb.ArchiveEpoch, ArchivePending: sb.ArchivePending, ReplicaCk: sb.ReplicaCk, ReplicaPending: sb.ReplicaPending, FromCheckpoint: sb.FromCheckpoint,
		Restarts: sb.Restarts, RestartedAt: sb.RestartedAt, Failed: sb.Failed,
	}
}

// volumesWantDTO persists the desired set only when it differs; absent reads back as equal to Volumes.
func volumesWantDTO(sb *types.Sandbox) []types.Volume {
	if slices.Equal(sb.VolumesWant, sb.Volumes) {
		return nil
	}
	return append([]types.Volume{}, sb.VolumesWant...)
}

func checkpointRefs(sb *types.Sandbox) [4]string {
	return [4]string{sb.ArchiveCk, sb.ArchivePending, sb.ReplicaCk, sb.ReplicaPending}
}

func appendRefs(list []string, refs [4]string) []string {
	for _, ck := range refs {
		if ck != "" && !slices.Contains(list, ck) {
			list = append(list, ck)
		}
	}
	return list
}
