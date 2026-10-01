package pool

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

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const tenantsFileName = "tenants.json"

// TenantInfo is one tenant's view on this node, never its token; Removed marks a tenant gone from the set that still owns live claims.
type TenantInfo struct {
	Name      string `json:"name"`
	MaxClaims int    `json:"max_claims,omitzero"`
	Claims    int    `json:"claims"`
	Removed   bool   `json:"removed,omitzero"`
}

type tenantRecord struct {
	Name        string `json:"name"`
	TokenSHA256 string `json:"token_sha256"`
	MaxClaims   int    `json:"max_claims,omitzero"`
}

type tenantsFile struct {
	ConfigSeed string         `json:"config_seed"`
	Tenants    []tenantRecord `json:"tenants"`
}

type tenantSet struct {
	byToken map[[sha256.Size]byte]string
	byName  map[string]tenantRecord
	names   []string
	digest  string
}

func newTenantSet(records []tenantRecord, rootSum string) (*tenantSet, error) {
	s := &tenantSet{
		byToken: make(map[[sha256.Size]byte]string, len(records)),
		byName:  make(map[string]tenantRecord, len(records)),
	}
	for _, r := range records {
		switch {
		case !types.NameRe.MatchString(r.Name):
			return nil, fmt.Errorf("tenant name %q must match %s", r.Name, types.NameRe)
		case r.MaxClaims < 0:
			return nil, fmt.Errorf("tenant %q max_claims must not be negative, got %d", r.Name, r.MaxClaims)
		case r.TokenSHA256 == rootSum:
			return nil, fmt.Errorf("tenant %q token must differ from api_token", r.Name)
		}
		sum, err := hex.DecodeString(r.TokenSHA256)
		if err != nil || len(sum) != sha256.Size {
			return nil, fmt.Errorf("tenant %q token_sha256 must be %d hex bytes", r.Name, sha256.Size)
		}
		if _, dup := s.byName[r.Name]; dup {
			return nil, fmt.Errorf("duplicate tenant name %q", r.Name)
		}
		key := [sha256.Size]byte(sum)
		if other, dup := s.byToken[key]; dup {
			return nil, fmt.Errorf("tenant %q token reused by tenant %q", r.Name, other)
		}
		s.byToken[key] = r.Name
		s.byName[r.Name] = r
	}
	s.names = slices.Sorted(maps.Keys(s.byName))
	s.digest = utils.DigestHex(s.records())
	return s, nil
}

func (s *tenantSet) records() []tenantRecord {
	out := make([]tenantRecord, len(s.names))
	for i, name := range s.names {
		out[i] = s.byName[name]
	}
	return out
}

func (s *tenantSet) has(name string) bool {
	_, ok := s.byName[name]
	return ok
}

// TenantByToken resolves a tenant bearer token to its tenant's name.
func (m *Manager) TenantByToken(token string) (string, bool) {
	name, ok := m.tenants.Load().byToken[sha256.Sum256([]byte(token))]
	return name, ok
}

// TenantNames lists the current tenants, sorted; the slice is shared and read-only.
func (m *Manager) TenantNames() []string {
	return m.tenants.Load().names
}

// TenantTokenDigests maps each current tenant to its token's SHA-256, for the cluster config digest.
func (m *Manager) TenantTokenDigests() map[string]string {
	set := m.tenants.Load()
	out := make(map[string]string, len(set.names))
	for name, r := range set.byName {
		out[name] = r.TokenSHA256
	}
	return out
}

// Tenants lists the tenant set with each tenant's live claims here, the removed tenants still holding claims, and the set's digest.
func (m *Manager) Tenants() ([]TenantInfo, string) {
	set := m.tenants.Load()
	m.mu.Lock()
	live := maps.Clone(m.tenantLive)
	m.mu.Unlock()
	out := make([]TenantInfo, 0, len(set.names))
	for _, name := range set.names {
		out = append(out, TenantInfo{Name: name, MaxClaims: set.byName[name].MaxClaims, Claims: live[name]})
	}
	for _, name := range slices.Sorted(maps.Keys(live)) {
		if !set.has(name) {
			out = append(out, TenantInfo{Name: name, Claims: live[name], Removed: true})
		}
	}
	return out, set.digest
}

// OnTenants runs f after every applied tenant change; set it before serving.
func (m *Manager) OnTenants(f func()) {
	m.onTenants = f
}

// SetTenants replaces the whole tenant set; a tenant left out stops authenticating at once and its claims stay, and an entry may omit a kept tenant's token.
func (m *Manager) SetTenants(ctx context.Context, specs []config.TenantSpec) error {
	return m.applyTenants(ctx, func(cur *tenantSet) ([]tenantRecord, error) {
		records := make([]tenantRecord, 0, len(specs))
		for _, spec := range specs {
			r, err := m.tenantRecordOf(spec, cur)
			if err != nil {
				return nil, err
			}
			records = append(records, r)
		}
		return records, nil
	})
}

// PutTenant adds or changes one tenant; an empty token keeps an existing tenant's token.
func (m *Manager) PutTenant(ctx context.Context, spec config.TenantSpec) error {
	return m.applyTenants(ctx, func(cur *tenantSet) ([]tenantRecord, error) {
		r, err := m.tenantRecordOf(spec, cur)
		if err != nil {
			return nil, err
		}
		next := maps.Clone(cur.byName)
		next[spec.Name] = r
		return slices.Collect(maps.Values(next)), nil
	})
}

// DeleteTenant removes one tenant; its token stops authenticating at once and its claims stay.
func (m *Manager) DeleteTenant(ctx context.Context, name string) error {
	return m.applyTenants(ctx, func(cur *tenantSet) ([]tenantRecord, error) {
		if !cur.has(name) {
			return nil, ErrUnknownTenant
		}
		next := maps.Clone(cur.byName)
		delete(next, name)
		return slices.Collect(maps.Values(next)), nil
	})
}

func (m *Manager) tenantRecordOf(spec config.TenantSpec, cur *tenantSet) (tenantRecord, error) {
	if spec.Egress != nil || spec.EgressUpstreamEnv != "" {
		return tenantRecord{}, fmt.Errorf("%w: tenant %q: egress and egress_upstream_env are set in the config file, not via the API", ErrBadTenant, spec.Name)
	}
	r := tenantRecord{Name: spec.Name, MaxClaims: spec.MaxClaims}
	switch kept, ok := cur.byName[spec.Name]; {
	case spec.Token != "":
		r.TokenSHA256 = config.TokenSHA256(spec.Token)
	case ok:
		r.TokenSHA256 = kept.TokenSHA256
	default:
		return tenantRecord{}, fmt.Errorf("%w: tenant %q needs a token", ErrBadTenant, spec.Name)
	}
	return r, nil
}

// applyTenants persists the next set before it serves, so an acknowledged change survives a crash and a failed write changes nothing.
func (m *Manager) applyTenants(ctx context.Context, next func(cur *tenantSet) ([]tenantRecord, error)) error {
	if m.rootSum == "" {
		return fmt.Errorf("%w: tenants require api_token", ErrBadTenant)
	}
	m.tenantMu.Lock()
	defer m.tenantMu.Unlock()
	cur := m.tenants.Load()
	records, err := next(cur)
	if err != nil {
		return err
	}
	set, err := newTenantSet(records, m.rootSum)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBadTenant, err)
	}
	raw, err := json.Marshal(tenantsFile{ConfigSeed: m.tenantSeed, Tenants: set.records()})
	if err != nil {
		return fmt.Errorf("encode tenants file: %w", err)
	}
	if err := utils.WriteFileSync(m.tenantPath, raw, 0o600); err != nil {
		return fmt.Errorf("persist tenants: %w", err)
	}
	m.tenants.Store(set)
	m.auditTenants(ctx, cur, set)
	if m.onTenants != nil {
		m.onTenants()
	}
	return nil
}

func (m *Manager) auditTenants(ctx context.Context, prev, next *tenantSet) {
	var frame auditFrame
	for _, name := range next.names {
		old, ok := prev.byName[name]
		switch {
		case !ok:
			frame.Added = append(frame.Added, name)
		case old != next.byName[name]:
			frame.Changed = append(frame.Changed, name)
		}
	}
	for _, name := range prev.names {
		if !next.has(name) {
			frame.Removed = append(frame.Removed, name)
		}
	}
	if frame.Added == nil && frame.Changed == nil && frame.Removed == nil {
		return
	}
	frame.Op = "tenants"
	m.recordAudit(ctx, "", frame)
}

func (m *Manager) seedTenants(cfg *config.Config) error {
	if cfg.APIToken != "" {
		m.rootSum = config.TokenSHA256(cfg.APIToken)
	}
	seed, err := newTenantSet(configTenantRecords(cfg.Tenants), m.rootSum)
	if err != nil {
		return fmt.Errorf("config tenants: %w", err)
	}
	m.tenants.Store(seed)
	m.tenantSeed, m.tenantPath = seed.digest, filepath.Join(cfg.DataDir, tenantsFileName)
	return nil
}

func (m *Manager) adoptPersistedTenants(ctx context.Context) error {
	raw, err := os.ReadFile(m.tenantPath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read tenants file: %w", err)
	}
	if m.rootSum == "" {
		return fmt.Errorf("%s needs api_token: the operator surfaces are unreachable without it", m.tenantPath)
	}
	var tf tenantsFile
	if err = json.Unmarshal(raw, &tf); err != nil {
		return fmt.Errorf("parse tenants file: %w", err)
	}
	set, err := newTenantSet(tf.Tenants, m.rootSum)
	if err != nil {
		return fmt.Errorf("restore tenants from %s: %w", m.tenantPath, err)
	}
	logger := log.WithFunc("pool.adoptPersistedTenants")
	if tf.ConfigSeed != m.tenantSeed {
		logger.Warnf(ctx, "config.json tenants differ from the API-applied set and are overridden; delete %s to return to config-owned tenants", m.tenantPath)
	}
	m.tenants.Store(set)
	logger.Infof(ctx, "restored %d API-applied tenants from %s", len(set.names), tenantsFileName)
	return nil
}

// warnVolumeTenants only warns: the API may add the tenant later.
func (m *Manager) warnVolumeTenants(ctx context.Context, volumes []config.VolumeSpec) {
	set := m.tenants.Load()
	for _, v := range volumes {
		for _, name := range v.Tenants {
			if !set.has(name) {
				log.WithFunc("pool.warnVolumeTenants").Warnf(ctx, "volume %q names tenant %q, which the tenant set does not hold", v.Name, name)
			}
		}
	}
}

func (m *Manager) tenantGone(tenant string) bool {
	return tenant != "" && !m.tenants.Load().has(tenant)
}

func configTenantRecords(specs []config.TenantSpec) []tenantRecord {
	out := make([]tenantRecord, len(specs))
	for i, t := range specs {
		out[i] = tenantRecord{Name: t.Name, TokenSHA256: config.TokenSHA256(t.Token), MaxClaims: t.MaxClaims}
	}
	return out
}
