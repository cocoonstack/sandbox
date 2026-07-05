package pool

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// store persists claimed sandboxes across daemon restarts. Warm pool VMs are
// deliberately not persisted: they are cheap to rebuild and unsafe to trust
// after an unsupervised gap.
type store struct {
	path string
}

func newStore(dataDir string) *store {
	return &store{path: filepath.Join(dataDir, "claims.json")}
}

func (s *store) load() (map[string]*types.Sandbox, error) {
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

func (s *store) save(claims map[string]*types.Sandbox) error {
	raw, err := json.Marshal(claims)
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
	return nil
}
