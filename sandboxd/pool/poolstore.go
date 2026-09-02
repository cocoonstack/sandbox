package pool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

// poolsFile records the last API-applied pool set so a restart rebuilds from operator intent.
type poolsFile struct {
	ConfigSeed string            `json:"config_seed"`
	Pools      []config.PoolSpec `json:"pools"`
}

type poolStore struct {
	path string

	seq     atomic.Uint64 // sequence source; bumped under the manager mutex (apply order)
	mu      sync.Mutex
	written uint64 // highest sequence on disk; guarded by mu
}

func newPoolStore(dataDir string) *poolStore {
	return &poolStore{path: filepath.Join(dataDir, "pools.json")}
}

// load returns the persisted pool file, or nil when the node has never taken a PUT /v1/pools.
func (s *poolStore) load() (*poolsFile, error) {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read pools file: %w", err)
	}
	var pf poolsFile
	if err := json.Unmarshal(raw, &pf); err != nil {
		return nil, fmt.Errorf("parse pools file: %w", err)
	}
	return &pf, nil
}

// commit durably writes the applied set; a seq no newer than the last written is a no-op.
func (s *poolStore) commit(seq uint64, pf poolsFile) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq <= s.written {
		return nil
	}
	raw, err := json.Marshal(pf)
	if err != nil {
		return fmt.Errorf("encode pools file: %w", err)
	}
	if err := utils.WriteFileSync(s.path, raw, 0o600); err != nil {
		return err
	}
	s.written = seq
	return nil
}

// adoptPersistedPools rebuilds the pools from pools.json over the config seed when present.
func (m *Manager) adoptPersistedPools(ctx context.Context) error {
	pf, err := m.poolStore.load()
	if err != nil {
		return err
	}
	if pf == nil {
		return nil
	}
	logger := log.WithFunc("pool.adoptPersistedPools")
	if pf.ConfigSeed != m.configSeedHash {
		logger.Warnf(ctx, "config.json pools differ from the API-applied set and are overridden; delete %s to return to config-owned pools", m.poolStore.path)
	}
	clear(m.pools)
	m.idleEnabled = m.idleDefault > 0
	m.archiveEnabled = m.archiveAfterDefault > 0
	for _, spec := range pf.Pools {
		spec = normalizePoolSpec(spec)
		if err := m.validate(spec.PoolKey); err != nil {
			return fmt.Errorf("restore pool %q: %w", spec.Template, err)
		}
		if err := spec.ValidateLimits(); err != nil {
			return fmt.Errorf("restore pool %q: %w", spec.Template, err)
		}
		p := newPool(spec.PoolKey)
		p.applySpec(spec)
		m.adoptGolden(p)
		m.pools[spec.PoolKey] = p
		if spec.IdleHibernateSeconds > 0 {
			m.idleEnabled = true
		}
		if spec.ArchiveAfterSeconds > 0 {
			m.archiveEnabled = true
		}
	}
	logger.Infof(ctx, "restored %d API-applied pools from pools.json", len(pf.Pools))
	return nil
}

// poolSeedHash digests a pool set's warm-target shape, order-independent and egress-excluded.
func poolSeedHash(specs []config.PoolSpec) string {
	shaped := slices.Clone(specs)
	for i := range shaped {
		shaped[i].Egress = nil
	}
	slices.SortFunc(shaped, func(a, b config.PoolSpec) int { return strings.Compare(a.Hash(), b.Hash()) })
	raw, _ := json.Marshal(shaped)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
