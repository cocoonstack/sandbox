package pool

import (
	"encoding/json"
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
)

// claimSnapshot sequences one persist request; commit skips it once a newer write has landed.
type claimSnapshot struct {
	seq uint64
}

// claimDTO is the persisted projection of a Sandbox, copied so commit marshals off m.mu.
type claimDTO struct {
	ID             string         `json:"id"`
	VMName         string         `json:"vm_name"`
	Key            types.PoolKey  `json:"key"`
	Token          string         `json:"token,omitempty"`
	Deadline       time.Time      `json:"deadline,omitzero"`
	Tenant         string         `json:"tenant,omitempty"`
	ClaimRef       string         `json:"claim_ref,omitempty"`
	Volumes        []types.Volume `json:"volumes,omitempty"`
	VsockSocket    string         `json:"vsock_socket,omitempty"`
	TAP            string         `json:"tap,omitempty"`
	HibernateSnap  string         `json:"hibernate_snap,omitempty"`
	PendingSnap    string         `json:"pending_snap,omitempty"`
	ArchiveCk      string         `json:"archive_ck,omitempty"`
	FromCheckpoint string         `json:"from_checkpoint,omitempty"`
}

// claimStore persists claimed sandboxes across daemon restarts; warm VMs are not persisted.
type claimStore struct {
	path string

	writeMu sync.Mutex

	mu      sync.Mutex
	dtos    map[string]claimDTO
	seq     uint64
	written uint64
}

func newClaimStore(dataDir string) *claimStore {
	return &claimStore{path: filepath.Join(dataDir, "claims.json"), dtos: map[string]claimDTO{}}
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
		s.dtos[sb.ID] = dtoOf(sb)
	}
	s.seq++
	return claimSnapshot{seq: s.seq}
}

// del drops the projections of ids and sequences the write; callers may hold m.mu.
func (s *claimStore) del(ids ...string) claimSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		delete(s.dtos, id)
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

// reset rebuilds the whole projection; the startup path, before contention exists.
func (s *claimStore) reset(claims map[string]*types.Sandbox) claimSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dtos = make(map[string]claimDTO, len(claims))
	for id, sb := range claims {
		s.dtos[id] = dtoOf(sb)
	}
	s.seq++
	return claimSnapshot{seq: s.seq}
}

// commit atomically replaces claims.json; unsynced by design (#21) to keep the claim path fast.
func (s *claimStore) commit(snap claimSnapshot) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if snap.seq <= s.written {
		s.mu.Unlock()
		return nil
	}
	dtos, seq := maps.Clone(s.dtos), s.seq
	s.mu.Unlock()
	raw, err := json.Marshal(dtos)
	if err != nil {
		return fmt.Errorf("encode claims: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write claims: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("commit claims: %w", err)
	}
	s.mu.Lock()
	s.written = seq
	s.mu.Unlock()
	return nil
}

// save is the combined form for the startup Reconcile pass, before contention exists.
func (s *claimStore) save(claims map[string]*types.Sandbox) error {
	return s.commit(s.reset(claims))
}

// synced reports whether every sequenced change has reached disk.
func (s *claimStore) synced() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.written == s.seq
}

func dtoOf(sb *types.Sandbox) claimDTO {
	return claimDTO{
		ID: sb.ID, VMName: sb.VMName, Key: sb.Key, Token: sb.Token,
		Deadline: sb.Deadline, Tenant: sb.Tenant, ClaimRef: sb.ClaimRef,
		Volumes: slices.Clone(sb.Volumes), VsockSocket: sb.VsockSocket,
		TAP: sb.TAP, HibernateSnap: sb.HibernateSnap, PendingSnap: sb.PendingSnap,
		ArchiveCk: sb.ArchiveCk, FromCheckpoint: sb.FromCheckpoint,
	}
}
