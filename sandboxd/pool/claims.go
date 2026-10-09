package pool

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

// claimSnapshot sequences one persist request; commit skips it once a newer write has landed.
type claimSnapshot struct {
	seq uint64
}

// claimDTO is the persisted projection of a Sandbox, copied so commit marshals off m.mu.
type claimDTO struct {
	ID             string                `json:"id"`
	VMName         string                `json:"vm_name"`
	Key            types.PoolKey         `json:"key"`
	Token          string                `json:"token,omitempty"`
	Deadline       time.Time             `json:"deadline,omitzero"`
	ClaimedAt      time.Time             `json:"claimed_at,omitzero"`
	LeaseSeconds   int                   `json:"lease_seconds,omitzero"`
	Layer          types.PolicyLayer     `json:"policy_layer,omitempty"`
	PolicySource   types.PoolKey         `json:"policy_source,omitzero"`
	NoEgress       bool                  `json:"no_egress,omitzero"`
	Tenant         string                `json:"tenant,omitempty"`
	EgressClass    string                `json:"egress_class,omitempty"`
	ClaimRef       string                `json:"claim_ref,omitempty"`
	Metadata       types.Metadata        `json:"metadata,omitempty"`
	OnExpire       types.ExpireAction    `json:"on_expire,omitempty"`
	Volumes        []types.Volume        `json:"volumes,omitempty"`
	PendingVolume  *types.VolumeMutation `json:"pending_volume,omitempty"`
	Env            types.Env             `json:"env,omitempty"`
	VsockSocket    string                `json:"vsock_socket,omitempty"`
	TAP            string                `json:"tap,omitempty"`
	HibernateSnap  string                `json:"hibernate_snap,omitempty"`
	PendingSnap    string                `json:"pending_snap,omitempty"`
	ArchiveCk      string                `json:"archive_ck,omitempty"`
	FromCheckpoint string                `json:"from_checkpoint,omitempty"`
	Restarts       int                   `json:"restarts,omitzero"`
	RestartedAt    time.Time             `json:"restarted_at,omitzero"`
	Failed         string                `json:"failed,omitempty"`
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

	mu      sync.Mutex
	rows    map[string]*claimRow
	seq     uint64
	written uint64
}

func newClaimStore(dataDir string, sync bool) *claimStore {
	return &claimStore{path: filepath.Join(dataDir, "claims.json"), sync: sync, rows: map[string]*claimRow{}}
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
	return claims, nil
}

// set records each sandbox's projection and sequences the write; callers may hold m.mu.
func (s *claimStore) set(sbs ...*types.Sandbox) claimSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sb := range sbs {
		s.rows[sb.ID] = &claimRow{dto: dtoOf(sb)}
	}
	s.seq++
	return claimSnapshot{seq: s.seq}
}

// del drops the projections of ids and sequences the write; callers may hold m.mu.
func (s *claimStore) del(ids ...string) claimSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		delete(s.rows, id)
	}
	s.seq++
	return claimSnapshot{seq: s.seq}
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
	s.rows = make(map[string]*claimRow, len(claims))
	for id, sb := range claims {
		s.rows[id] = &claimRow{dto: dtoOf(sb)}
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
	s.written = seq
	s.mu.Unlock()
	return nil
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
		Volumes: slices.Clone(sb.Volumes), PendingVolume: sb.PendingVolume, Env: sb.Env, VsockSocket: sb.VsockSocket,
		TAP: sb.TAP, HibernateSnap: sb.HibernateSnap, PendingSnap: sb.PendingSnap,
		ArchiveCk: sb.ArchiveCk, FromCheckpoint: sb.FromCheckpoint,
		Restarts: sb.Restarts, RestartedAt: sb.RestartedAt, Failed: sb.Failed,
	}
}
