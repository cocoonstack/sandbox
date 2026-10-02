// Package file keeps the tenant set in <data_dir>/tenants.json, seeded by config.json until the first API write.
package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const fileName = "tenants.json"

type persisted struct {
	ConfigSeed string                `json:"config_seed"`
	Tenants    []config.TenantRecord `json:"tenants"`
}

var _ tenants.Source = (*Source)(nil)

// Source is a node-local tenant set; reads load one snapshot lock-free.
type Source struct {
	path    string
	seed    string
	rootSum string

	// mu orders writers, which persist before they publish.
	mu  sync.Mutex
	set atomic.Pointer[set]
}

// Open seeds the set from config.json's tenants, then adopts tenants.json when an API write left one.
func Open(ctx context.Context, dataDir string, seed []config.TenantRecord, rootSum string) (*Source, error) {
	initial, err := newSet(seed, rootSum)
	if err != nil {
		return nil, fmt.Errorf("config tenants: %w", err)
	}
	s := &Source{path: filepath.Join(dataDir, fileName), seed: initial.digest, rootSum: rootSum}
	s.set.Store(initial)
	raw, err := os.ReadFile(s.path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return s, nil
	case err != nil:
		return nil, fmt.Errorf("read tenants file: %w", err)
	case rootSum == "":
		return nil, fmt.Errorf("%s needs api_token: the operator surfaces are unreachable without it", s.path)
	}
	var pf persisted
	if err = json.Unmarshal(raw, &pf); err != nil {
		return nil, fmt.Errorf("parse tenants file: %w", err)
	}
	restored, err := newSet(pf.Tenants, rootSum)
	if err != nil {
		return nil, fmt.Errorf("restore tenants from %s: %w", s.path, err)
	}
	logger := log.WithFunc("file.Open")
	if pf.ConfigSeed != s.seed {
		logger.Warnf(ctx, "config.json tenants differ from the API-applied set and are overridden; delete %s to return to config-owned tenants", s.path)
	}
	s.set.Store(restored)
	logger.Infof(ctx, "restored %d API-applied tenants from %s", len(restored.names), fileName)
	return s, nil
}

func (s *Source) Resolve(_ context.Context, sum [sha256.Size]byte) (*config.TenantRecord, error) {
	if r, ok := s.set.Load().byToken[sum]; ok {
		return r, nil
	}
	return nil, tenants.ErrUnknown
}

func (s *Source) Peek(name string) (config.TenantRecord, tenants.Presence) {
	if r, ok := s.set.Load().byName[name]; ok {
		return r, tenants.Present
	}
	return config.TenantRecord{}, tenants.Absent
}

func (s *Source) Lookup(_ context.Context, name string) (config.TenantRecord, bool, error) {
	r, p := s.Peek(name)
	return r, p == tenants.Present, nil
}

func (s *Source) List(_ context.Context, after string, limit int) ([]config.TenantRecord, error) {
	cur := s.set.Load()
	i, found := slices.BinarySearch(cur.names, after)
	if found {
		i++
	}
	names := cur.names[i:]
	names = names[:min(len(names), limit)]
	out := make([]config.TenantRecord, len(names))
	for j, name := range names {
		out[j] = cur.byName[name]
		out[j].TokenSHA256 = ""
	}
	return out, nil
}

func (s *Source) Put(_ context.Context, r config.TenantRecord) (tenants.Change, error) {
	return s.apply(func(cur *set) ([]config.TenantRecord, error) {
		kept, err := cur.keepToken(r)
		if err != nil {
			return nil, err
		}
		next := maps.Clone(cur.byName)
		next[kept.Name] = kept
		return slices.Collect(maps.Values(next)), nil
	})
}

func (s *Source) Delete(_ context.Context, name string) (tenants.Change, error) {
	return s.apply(func(cur *set) ([]config.TenantRecord, error) {
		if _, ok := cur.byName[name]; !ok {
			return nil, tenants.ErrUnknown
		}
		next := maps.Clone(cur.byName)
		delete(next, name)
		return slices.Collect(maps.Values(next)), nil
	})
}

func (s *Source) Replace(_ context.Context, records []config.TenantRecord) (tenants.Change, error) {
	return s.apply(func(cur *set) ([]config.TenantRecord, error) {
		out := make([]config.TenantRecord, len(records))
		for i, r := range records {
			kept, err := cur.keepToken(r)
			if err != nil {
				return nil, err
			}
			out[i] = kept
		}
		return out, nil
	})
}

func (s *Source) Classes(context.Context) (map[string]int, error) {
	out := map[string]int{}
	for _, r := range s.set.Load().byName {
		if r.EgressClass != "" {
			out[r.EgressClass]++
		}
	}
	return out, nil
}

func (s *Source) Records() []config.TenantRecord {
	return s.set.Load().records()
}

func (s *Source) Digest() string {
	return s.set.Load().digest
}

func (s *Source) Close() error {
	return nil
}

func (s *Source) apply(next func(cur *set) ([]config.TenantRecord, error)) (tenants.Change, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.set.Load()
	records, err := next(cur)
	if err != nil {
		return tenants.Change{}, err
	}
	updated, err := newSet(records, s.rootSum)
	if err != nil {
		return tenants.Change{}, err
	}
	raw, err := json.Marshal(persisted{ConfigSeed: s.seed, Tenants: updated.records()})
	if err != nil {
		return tenants.Change{}, fmt.Errorf("encode tenants file: %w", err)
	}
	if err := utils.WriteFileSync(s.path, raw, 0o600); err != nil {
		return tenants.Change{}, fmt.Errorf("persist tenants: %w", err)
	}
	s.set.Store(updated)
	return tenants.Diff(cur.byName, updated.byName), nil
}

type set struct {
	byToken map[[sha256.Size]byte]*config.TenantRecord
	byName  map[string]config.TenantRecord
	names   []string
	digest  string
}

func newSet(records []config.TenantRecord, rootSum string) (*set, error) {
	s := &set{
		byToken: make(map[[sha256.Size]byte]*config.TenantRecord, len(records)),
		byName:  make(map[string]config.TenantRecord, len(records)),
	}
	for _, r := range records {
		if err := tenants.Validate(r, rootSum); err != nil {
			return nil, err
		}
		sum, err := hex.DecodeString(r.TokenSHA256)
		if err != nil || len(sum) != sha256.Size {
			return nil, fmt.Errorf("%w: tenant %q needs a token", tenants.ErrInvalid, r.Name)
		}
		if _, dup := s.byName[r.Name]; dup {
			return nil, fmt.Errorf("%w: duplicate tenant name %q", tenants.ErrInvalid, r.Name)
		}
		key := [sha256.Size]byte(sum)
		if other, dup := s.byToken[key]; dup {
			return nil, fmt.Errorf("%w: tenant %q token reused by tenant %q", tenants.ErrInvalid, r.Name, other.Name)
		}
		s.byToken[key] = &r
		s.byName[r.Name] = r
	}
	s.names = slices.Sorted(maps.Keys(s.byName))
	s.digest = utils.DigestHex(s.records())
	return s, nil
}

func (s *set) records() []config.TenantRecord {
	out := make([]config.TenantRecord, len(s.names))
	for i, name := range s.names {
		out[i] = s.byName[name]
	}
	return out
}

func (s *set) keepToken(r config.TenantRecord) (config.TenantRecord, error) {
	if r.TokenSHA256 != "" {
		return r, nil
	}
	kept, ok := s.byName[r.Name]
	if !ok {
		return config.TenantRecord{}, fmt.Errorf("%w: tenant %q needs a token", tenants.ErrInvalid, r.Name)
	}
	r.TokenSHA256 = kept.TokenSHA256
	return r, nil
}
