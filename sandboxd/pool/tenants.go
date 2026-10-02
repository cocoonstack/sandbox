package pool

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/tenants/file"
	"github.com/cocoonstack/sandbox/sandboxd/tenants/pg"
)

// TenantInfo is one tenant's view on this node, never its token; Removed marks a tenant gone from the set that still owns live claims.
type TenantInfo struct {
	Name        string `json:"name"`
	MaxClaims   int    `json:"max_claims,omitzero"`
	EgressClass string `json:"egress_class,omitempty"`
	Claims      int    `json:"claims"`
	Removed     bool   `json:"removed,omitzero"`
}

// TenantPage is one page of the tenant set by name; Next is the cursor of the following page, empty on the last, and Digest is empty on a shared set.
type TenantPage struct {
	Tenants []TenantInfo `json:"tenants"`
	Digest  string       `json:"digest,omitempty"`
	Next    string       `json:"next,omitempty"`
}

// TenantByToken resolves a tenant bearer token to its tenant's shared, read-only record.
func (m *Manager) TenantByToken(ctx context.Context, token string) (*config.TenantRecord, error) {
	r, err := m.tenants.Resolve(ctx, sha256.Sum256([]byte(token)))
	if err != nil {
		return nil, tenantErr(err)
	}
	return r, nil
}

// TenantRecords returns a node-local tenant set for the cluster config digest; a shared set returns nil.
func (m *Manager) TenantRecords() []config.TenantRecord {
	return m.tenants.Records()
}

// Tenants lists up to limit tenants after the cursor with each one's live claims here; the first page also names the removed tenants still holding claims.
func (m *Manager) Tenants(ctx context.Context, after string, limit int) (TenantPage, error) {
	records, err := m.tenants.List(ctx, after, limit+1)
	if err != nil {
		return TenantPage{}, tenantErr(err)
	}
	more := len(records) > limit
	records = records[:min(len(records), limit)]
	m.mu.Lock()
	live := maps.Clone(m.tenantLive)
	m.mu.Unlock()
	page := TenantPage{Tenants: make([]TenantInfo, 0, len(records)), Digest: m.tenants.Digest()}
	for _, r := range records {
		page.Tenants = append(page.Tenants, TenantInfo{Name: r.Name, MaxClaims: r.MaxClaims, EgressClass: r.EgressClass, Claims: live[r.Name]})
	}
	if more {
		page.Next = records[len(records)-1].Name
	}
	if after == "" {
		for _, name := range slices.Sorted(maps.Keys(live)) {
			if m.tenantGone(name) {
				page.Tenants = append(page.Tenants, TenantInfo{Name: name, Claims: live[name], Removed: true})
			}
		}
	}
	return page, nil
}

// OnTenants runs f after every applied tenant change; set it before serving.
func (m *Manager) OnTenants(f func()) {
	m.onTenants = f
}

// SetTenants replaces the whole tenant set; a tenant left out stops authenticating at once and its claims stay, and an entry may omit a kept tenant's token.
func (m *Manager) SetTenants(ctx context.Context, specs []config.TenantSpec) error {
	records := make([]config.TenantRecord, 0, len(specs))
	for _, spec := range specs {
		r, err := m.tenantRecordOf(spec)
		if err != nil {
			return err
		}
		records = append(records, r)
	}
	return m.applyTenants(ctx, func() (tenants.Change, error) { return m.tenants.Replace(ctx, records) })
}

// PutTenant adds or changes one tenant; an empty token keeps an existing tenant's token.
func (m *Manager) PutTenant(ctx context.Context, spec config.TenantSpec) error {
	r, err := m.tenantRecordOf(spec)
	if err != nil {
		return err
	}
	return m.applyTenants(ctx, func() (tenants.Change, error) { return m.tenants.Put(ctx, r) })
}

// DeleteTenant removes one tenant; its token stops authenticating at once and its claims stay.
func (m *Manager) DeleteTenant(ctx context.Context, name string) error {
	return m.applyTenants(ctx, func() (tenants.Change, error) { return m.tenants.Delete(ctx, name) })
}

func (m *Manager) tenantRecordOf(spec config.TenantSpec) (config.TenantRecord, error) {
	if _, ok := m.view.Load().classEgress[spec.EgressClass]; spec.EgressClass != "" && !ok {
		return config.TenantRecord{}, fmt.Errorf("%w: tenant %q names unknown egress class %q", ErrBadTenant, spec.Name, spec.EgressClass)
	}
	r := config.TenantRecord{Name: spec.Name, MaxClaims: spec.MaxClaims, EgressClass: spec.EgressClass}
	if spec.Token != "" {
		r.TokenSHA256 = config.TokenSHA256(spec.Token)
	}
	if err := tenants.Validate(r, m.rootSum); err != nil {
		return config.TenantRecord{}, tenantErr(err)
	}
	return r, nil
}

func (m *Manager) applyTenants(ctx context.Context, write func() (tenants.Change, error)) error {
	if m.rootSum == "" {
		return fmt.Errorf("%w: tenants require api_token", ErrBadTenant)
	}
	c, err := write()
	if err != nil {
		return tenantErr(err)
	}
	if !c.Empty() {
		m.recordAudit(ctx, "", auditFrame{Op: "tenants", Added: c.Added, Changed: c.Changed, Removed: c.Removed})
	}
	if m.onTenants != nil {
		m.onTenants()
	}
	return nil
}

func (m *Manager) openTenants(ctx context.Context, cfg *config.Config) error {
	if cfg.APIToken != "" {
		m.rootSum = config.TokenSHA256(cfg.APIToken)
	}
	var err error
	if ms := cfg.MetaStore; ms != nil {
		m.tenants, err = pg.Open(ctx, os.Getenv(ms.DSNEnv))
		return err
	}
	m.tenants, err = file.Open(ctx, cfg.DataDir, configTenantRecords(cfg.Tenants), m.rootSum)
	return err
}

// warnMissingClasses names each egress class tenants reference but the view lacks; those tenants reach nothing until it returns.
func (m *Manager) warnMissingClasses(ctx context.Context) {
	logger := log.WithFunc("pool.warnMissingClasses")
	classes, err := m.tenants.Classes(ctx)
	if err != nil {
		logger.Warnf(ctx, "count tenants per egress class: %v", err)
		return
	}
	v := m.view.Load()
	for _, class := range slices.Sorted(maps.Keys(classes)) {
		if _, ok := v.classEgress[class]; !ok {
			logger.Warnf(ctx, "egress class %q is not configured: its %d tenants reach nothing", class, classes[class])
		}
	}
}

// warnVolumeTenants only warns: the API may add the tenant later.
func (m *Manager) warnVolumeTenants(ctx context.Context, volumes []config.VolumeSpec) {
	for _, v := range volumes {
		for _, name := range v.Tenants {
			if m.tenantGone(name) {
				log.WithFunc("pool.warnVolumeTenants").Warnf(ctx, "volume %q names tenant %q, which the tenant set does not hold", v.Name, name)
			}
		}
	}
}

// tenantRemoved asks the set itself, past a cache that has not seen the tenant, so a renew, fork or wake never outlives a removal.
func (m *Manager) tenantRemoved(ctx context.Context, tenant string) error {
	if tenant == "" {
		return nil
	}
	_, ok, err := m.tenants.Lookup(ctx, tenant)
	switch {
	case err != nil:
		return tenantErr(err)
	case !ok:
		return ErrTenantRemoved
	}
	return nil
}

// tenantGone is true only once the set answers that the tenant is absent; a cached set that has not seen the name yet keeps the claim.
func (m *Manager) tenantGone(tenant string) bool {
	if tenant == "" {
		return false
	}
	_, p := m.tenants.Peek(tenant)
	return p == tenants.Absent
}

func tenantErr(err error) error {
	switch {
	case errors.Is(err, tenants.ErrInvalid):
		return fmt.Errorf("%w: %w", ErrBadTenant, err)
	case errors.Is(err, tenants.ErrUnknown):
		return fmt.Errorf("%w: %w", ErrUnknownTenant, err)
	case errors.Is(err, tenants.ErrUnavailable):
		return fmt.Errorf("%w: %w", ErrTenantStoreDown, err)
	}
	return err
}

func configTenantRecords(specs []config.TenantSpec) []config.TenantRecord {
	out := make([]config.TenantRecord, len(specs))
	for i, t := range specs {
		out[i] = config.TenantRecord{Name: t.Name, TokenSHA256: config.TokenSHA256(t.Token), MaxClaims: t.MaxClaims, EgressClass: t.EgressClass}
	}
	return out
}
