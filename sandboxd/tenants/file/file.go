// Package file keeps the tenant set the tenant API writes in <data_dir>/tenants.json.
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

	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const fileName = "tenants.json"

type persisted struct {
	Tenants []tenants.TenantRecord `json:"tenants"`
}

var _ tenants.Source = (*Source)(nil)

// Source is a node-local tenant set; reads load one snapshot lock-free.
type Source struct {
	path     string
	reserved tenants.Reserved

	// mu orders writers, which persist before they publish.
	mu  sync.Mutex
	set atomic.Pointer[set]
}

// Open loads tenants.json; a node without one starts with no tenants.
func Open(ctx context.Context, dataDir string, reserved tenants.Reserved) (*Source, error) {
	s := &Source{path: filepath.Join(dataDir, fileName), reserved: reserved}
	raw, err := os.ReadFile(s.path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		empty, _ := newSet(nil, reserved)
		s.set.Store(empty)
		return s, nil
	case err != nil:
		return nil, fmt.Errorf("read tenants file: %w", err)
	case reserved.Root == "":
		return nil, fmt.Errorf("%s needs api_token: the operator surfaces are unreachable without it", s.path)
	}
	var pf persisted
	if err = json.Unmarshal(raw, &pf); err != nil {
		return nil, fmt.Errorf("parse tenants file: %w", err)
	}
	restored, err := newSet(pf.Tenants, reserved)
	if err != nil {
		return nil, fmt.Errorf("restore tenants from %s: %w", s.path, err)
	}
	s.set.Store(restored)
	log.WithFunc("file.Open").Infof(ctx, "restored %d tenants from %s", len(restored.names), fileName)
	return s, nil
}

func (s *Source) Resolve(_ context.Context, sum [sha256.Size]byte) (*tenants.TenantRecord, error) {
	if r, ok := s.set.Load().byToken[sum]; ok {
		return r, nil
	}
	return nil, tenants.ErrUnknown
}

func (s *Source) Peek(name string) (tenants.TenantRecord, tenants.Presence) {
	if r, ok := s.set.Load().byName[name]; ok {
		return r, tenants.Present
	}
	return tenants.TenantRecord{}, tenants.Absent
}

func (s *Source) Lookup(_ context.Context, name string) (tenants.TenantRecord, bool, error) {
	r, p := s.Peek(name)
	return r, p == tenants.Present, nil
}

func (s *Source) List(_ context.Context, after string, limit int) ([]tenants.TenantRecord, error) {
	cur := s.set.Load()
	i, found := slices.BinarySearch(cur.names, after)
	if found {
		i++
	}
	names := cur.names[i:]
	names = names[:min(len(names), limit)]
	out := make([]tenants.TenantRecord, len(names))
	for j, name := range names {
		out[j] = cur.byName[name]
		out[j].TokenSHA256 = ""
	}
	return out, nil
}

func (s *Source) Put(_ context.Context, r tenants.TenantRecord) (tenants.Change, error) {
	return s.apply(func(cur *set) (map[string]tenants.TenantRecord, error) {
		kept, err := tenants.KeepTokens(cur.byName, []tenants.TenantRecord{r})
		if err != nil {
			return nil, err
		}
		next := maps.Clone(cur.byName)
		maps.Copy(next, kept)
		return next, nil
	})
}

func (s *Source) Delete(_ context.Context, name string) (tenants.Change, error) {
	return s.apply(func(cur *set) (map[string]tenants.TenantRecord, error) {
		if _, ok := cur.byName[name]; !ok {
			return nil, tenants.ErrUnknown
		}
		next := maps.Clone(cur.byName)
		delete(next, name)
		return next, nil
	})
}

func (s *Source) Replace(_ context.Context, records []tenants.TenantRecord) (tenants.Change, error) {
	return s.apply(func(cur *set) (map[string]tenants.TenantRecord, error) {
		return tenants.KeepTokens(cur.byName, records)
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

func (s *Source) Records() []tenants.TenantRecord {
	return s.set.Load().records()
}

func (s *Source) Digest() string {
	return s.set.Load().digest
}

func (s *Source) Close() error {
	return nil
}

func (s *Source) apply(next func(cur *set) (map[string]tenants.TenantRecord, error)) (tenants.Change, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.set.Load()
	records, err := next(cur)
	if err != nil {
		return tenants.Change{}, err
	}
	updated, err := newSet(slices.Collect(maps.Values(records)), s.reserved)
	if err != nil {
		return tenants.Change{}, err
	}
	raw, err := json.Marshal(persisted{Tenants: updated.records()})
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
	byToken map[[sha256.Size]byte]*tenants.TenantRecord
	byName  map[string]tenants.TenantRecord
	names   []string
	digest  string
}

func newSet(records []tenants.TenantRecord, reserved tenants.Reserved) (*set, error) {
	s := &set{
		byToken: make(map[[sha256.Size]byte]*tenants.TenantRecord, len(records)),
		byName:  make(map[string]tenants.TenantRecord, len(records)),
	}
	for _, r := range records {
		if err := tenants.Validate(r, reserved); err != nil {
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

func (s *set) records() []tenants.TenantRecord {
	out := make([]tenants.TenantRecord, len(s.names))
	for i, name := range s.names {
		out[i] = s.byName[name]
	}
	return out
}
