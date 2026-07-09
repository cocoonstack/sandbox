package pool

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// claimSnapshot is a sequenced, detached copy of the claim set awaiting a
// durable write; commit() marshals it off the manager mutex.
type claimSnapshot struct {
	claims map[string]claimDTO
	seq    uint64
}

// claimDTO is the persisted projection of a Sandbox (its json fields only),
// copied by value so commit's marshal runs off the manager mutex without
// racing the live record's Transition mutex and lastActivity.
type claimDTO struct {
	ID             string        `json:"id"`
	VMName         string        `json:"vm_name"`
	Key            types.PoolKey `json:"key"`
	Token          string        `json:"token,omitempty"`
	Deadline       time.Time     `json:"deadline,omitzero"`
	Tenant         string        `json:"tenant,omitempty"`
	VsockSocket    string        `json:"vsock_socket,omitempty"`
	HibernateSnap  string        `json:"hibernate_snap,omitempty"`
	ArchiveCk      string        `json:"archive_ck,omitempty"`
	FromCheckpoint string        `json:"from_checkpoint,omitempty"`
}

// claimStore persists claimed sandboxes across daemon restarts. Warm pool
// VMs are deliberately not persisted: they are cheap to rebuild and unsafe
// to trust after an unsupervised gap.
//
// A write is split so the manager mutex covers only the marshal (a consistent
// snapshot of the claim map), not the file syscalls: snapshot() runs under the
// manager mutex, commit() writes off it. commit is serialized and coalescing —
// a snapshot no newer than one already on disk is dropped, because sequence
// order matches the manager-mutex mutation order, so the newest write wins and
// an older one only ever describes a superseded state.
type claimStore struct {
	path string

	seq     atomic.Uint64 // sequence source; bumped in snapshot (mutation order)
	mu      sync.Mutex
	written uint64 // highest sequence on disk; guarded by mu
}

func newClaimStore(dataDir string) *claimStore {
	return &claimStore{path: filepath.Join(dataDir, "claims.json")}
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

// snapshot copies the claim set into a detached, sequenced value and stamps it
// with the next sequence. The caller holds the manager mutex, so it does only a
// cheap field copy — not the marshal — and sequences are handed out in mutation
// order; commit() marshals and writes off the mutex.
func (s *claimStore) snapshot(claims map[string]*types.Sandbox) claimSnapshot {
	seq := s.seq.Add(1)
	dtos := make(map[string]claimDTO, len(claims))
	for id, sb := range claims {
		dtos[id] = claimDTO{
			ID: sb.ID, VMName: sb.VMName, Key: sb.Key, Token: sb.Token,
			Deadline: sb.Deadline, Tenant: sb.Tenant, VsockSocket: sb.VsockSocket,
			HibernateSnap: sb.HibernateSnap, ArchiveCk: sb.ArchiveCk,
			FromCheckpoint: sb.FromCheckpoint,
		}
	}
	return claimSnapshot{claims: dtos, seq: seq}
}

// commit marshals a snapshot and durably writes it, off the manager mutex.
// Serialized by s.mu so writes never interleave; coalescing so a snapshot no
// newer than the last written is a no-op — a higher sequence already persisted
// a later state that supersedes it — which also skips its marshal.
func (s *claimStore) commit(snap claimSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.seq <= s.written {
		return nil
	}
	raw, err := json.Marshal(snap.claims)
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
	s.written = snap.seq
	return nil
}

// save marshals and commits in one call — the combined form for the startup
// Reconcile pass, which holds the manager mutex but runs before any claim or
// housekeeping tick can contend. Hot-path callers split snapshot()/commit()
// around the mutex so the write leaves it.
func (s *claimStore) save(claims map[string]*types.Sandbox) error {
	return s.commit(s.snapshot(claims))
}
