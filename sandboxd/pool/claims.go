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

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// claimSnapshot is a sequenced, pre-marshaled claim set awaiting a durable write.
type claimSnapshot struct {
	raw []byte
	seq uint64
	err error
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

// snapshot marshals the claim set and stamps it with the next sequence. The
// caller holds the manager mutex, so the map is consistent and sequences are
// handed out in mutation order; the file write happens later in commit().
func (s *claimStore) snapshot(claims map[string]*types.Sandbox) claimSnapshot {
	seq := s.seq.Add(1)
	raw, err := json.Marshal(claims)
	return claimSnapshot{raw: raw, seq: seq, err: err}
}

// commit durably writes a snapshot off the manager mutex. Serialized by s.mu so
// writes never interleave; coalescing so a snapshot no newer than the last
// written is a no-op — a higher sequence already persisted a later state that
// supersedes it, so the caller's intent is satisfied.
func (s *claimStore) commit(snap claimSnapshot) error {
	if snap.err != nil {
		return fmt.Errorf("encode claims: %w", snap.err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.seq <= s.written {
		return nil
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, snap.raw, 0o600); err != nil {
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
