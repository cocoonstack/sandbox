package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// TemplateInfo is the ops view of one promoted template; an empty Tenant means the operator (root).
type TemplateInfo struct {
	Key           types.PoolKey  `json:"key"`
	ContentDigest string         `json:"content_digest"`
	Tenant        string         `json:"tenant,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	CPUCount      int            `json:"cpu_count,omitzero"`
	MemTotalBytes int64          `json:"mem_total_bytes,omitzero"`
	Labels        types.Metadata `json:"labels,omitempty"`
}

// templateRecord is a template's meta.json; an empty Tenant means the operator (root).
type templateRecord struct {
	ID           string        `json:"id"`
	Key          types.PoolKey `json:"key"`
	Tenant       string        `json:"tenant,omitempty"`
	CreatedAt    time.Time     `json:"created_at"`
	PolicySource types.PoolKey `json:"policy_source,omitzero"`
}

// Promote publishes a claimed sandbox as a template under (template, parent net, parent size).
func (m *Manager) Promote(ctx context.Context, id string, cred Cred, template, tenant string) (types.PoolKey, string, error) {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return types.PoolKey{}, "", ErrUnknownSandbox
	}
	if !types.NameRe.MatchString(template) {
		return types.PoolKey{}, "", fmt.Errorf("%w: template %q must match %s", ErrBadKey, template, types.NameRe)
	}
	if hasAppliedVolumes(sb) {
		return types.PoolKey{}, "", ErrVolumeCapture
	}
	if !sb.Key.Capturable() {
		return types.PoolKey{}, "", ErrNoEgressFork
	}
	key := types.PoolKey{Template: template, Net: sb.Key.Net, Size: sb.Key.Size}
	if m.pooled(key) {
		// a configured pool owns this key; promoting over it would change what refills produce
		return types.PoolKey{}, "", ErrPooledTemplate
	}
	// commitTemplate re-checks under the template lock; this only fast-fails before the export
	if err := m.checkTemplateOwner(ctx, store.TemplateID(key.Hash()), tenant); err != nil {
		return types.PoolKey{}, "", err
	}
	// the transition lock pins the source snapshot; a started promote must finish uncanceled
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	ctx = context.WithoutCancel(ctx)
	m.trimForCapture(ctx, sb)

	snap, cleanup, err := m.sourceSnap(ctx, sb)
	if err != nil {
		return types.PoolKey{}, "", fmt.Errorf("promote %s: %w", sb.ID, err)
	}
	defer cleanup()
	rec := templateRecord{Key: key, Tenant: tenant}
	if sb.Layer == types.LayerPooled {
		rec.PolicySource = sb.PolicyKey()
	}
	digest, err := m.publishTemplate(ctx, snap, rec)
	if err != nil {
		return types.PoolKey{}, "", fmt.Errorf("promote %s: %w", sb.ID, err)
	}
	if m.notifyTemplates != nil {
		m.notifyTemplates()
	}
	m.counters.promotes.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "promote", ID: sb.ID, VMName: sb.VMName, Reference: key.Template})
	return key, digest, nil
}

// DeleteTemplate removes a promoted template; a tenant may delete only what it promoted.
func (m *Manager) DeleteTemplate(ctx context.Context, key types.PoolKey, tenant string) error {
	if err := m.validate(key); err != nil {
		return err
	}
	if m.pooled(key) {
		return ErrPooledTemplate
	}
	id := store.TemplateID(key.Hash())
	l := m.recLock(id)
	l.Lock()
	defer func() { l.Unlock(); m.recDoneEvict(id) }()
	raw, err := m.tpls.ReadMeta(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrUnknownTemplate
		}
		return fmt.Errorf("read template: %w", err)
	}
	if tenant != "" {
		var rec templateRecord
		if json.Unmarshal(raw, &rec) != nil || !tenantOwns(tenant, rec.Tenant) {
			return ErrUnknownTemplate
		}
	}
	if err := m.tpls.Delete(ctx, id); err != nil {
		return fmt.Errorf("delete template: %w", err)
	}
	m.tplMu.Lock()
	delete(m.tplSet, id)
	m.tplMu.Unlock()
	if m.notifyTemplates != nil {
		m.notifyTemplates()
	}
	return nil
}

// Templates lists the promoted templates this node holds and no pool owns, in key-hash order like Info's pools; one published before keys were recorded is listed once re-promoted.
func (m *Manager) Templates() []TemplateInfo {
	held := m.unpooledTemplates()
	out := make([]TemplateInfo, 0, len(held))
	for _, id := range slices.Sorted(maps.Keys(held)) {
		if t := held[id]; t.Key.Template != "" {
			spec, _ := t.Key.Size.Spec()
			t.CPUCount, t.MemTotalBytes = spec.CPU, spec.MemoryBytes
			out = append(out, t)
		}
	}
	return out
}

// SetTemplateLabels replaces a promoted template's labels; a tenant may label only what it promoted, and empty labels clear them.
func (m *Manager) SetTemplateLabels(ctx context.Context, key types.PoolKey, labels types.Metadata, tenant string) error {
	if err := m.validate(key); err != nil {
		return err
	}
	if m.pooled(key) {
		return ErrPooledTemplate
	}
	id := store.TemplateID(key.Hash())
	l := m.recLock(id)
	l.Lock()
	defer func() { l.Unlock(); m.recDone(id) }()
	raw, err := m.tpls.ReadMeta(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return ErrUnknownTemplate
	}
	if err != nil {
		return fmt.Errorf("read template: %w", err)
	}
	var rec templateRecord
	if json.Unmarshal(raw, &rec) != nil || (tenant != "" && !tenantOwns(tenant, rec.Tenant)) {
		return ErrUnknownTemplate
	}
	var payload []byte
	if len(labels) == 0 {
		labels = nil
	} else if payload, err = json.Marshal(labels); err != nil {
		return err
	}
	if err = m.tpls.SetLabels(ctx, id, payload); err != nil {
		return fmt.Errorf("set template labels: %w", err)
	}
	m.tplMu.Lock()
	if t, ok := m.tplSet[id]; ok {
		t.Labels = labels
		m.tplSet[id] = t
	}
	m.tplMu.Unlock()
	return nil
}

// TemplateHashes lists the promoted-template key hashes for mesh gossip, sorted for its guard.
func (m *Manager) TemplateHashes() []string {
	held := m.unpooledTemplates()
	hashes := make([]string, 0, len(held))
	for id, t := range held {
		hashes = append(hashes, types.TemplateGossipHash(store.TemplateHash(id), t.Tenant))
	}
	slices.Sort(hashes)
	return hashes
}

// HasGolden reports whether this node can provision the key without a cold boot.
func (m *Manager) HasGolden(ctx context.Context, key types.PoolKey, tenant string) bool {
	return m.HasPoolGolden(key) || m.HasPromotedTemplate(ctx, key, tenant)
}

// HasPoolGolden reports whether a configured pool can serve key from its own golden.
func (m *Manager) HasPoolGolden(key types.PoolKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.pools[key]
	return p != nil && p.goldenDir != ""
}

// HasPromotedTemplate is resolveGolden's test exactly, so routing never promises a refused golden.
func (m *Manager) HasPromotedTemplate(ctx context.Context, key types.PoolKey, tenant string) bool {
	if m.pooled(key) {
		return false
	}
	id := store.TemplateID(key.Hash())
	m.tplMu.Lock()
	held, cached := m.tplSet[id]
	m.tplMu.Unlock()
	owner := held.Tenant
	if !cached {
		// only a shared-store template promoted elsewhere after startup.
		raw, err := m.tpls.ReadMeta(ctx, id)
		if err != nil {
			return false
		}
		var rec templateRecord
		if json.Unmarshal(raw, &rec) != nil {
			return false
		}
		owner = rec.Tenant
	}
	return owner == "" || tenantOwns(tenant, owner)
}

// unpooledTemplates copies the held templates by id, less any key a pool now owns.
func (m *Manager) unpooledTemplates() map[string]TemplateInfo {
	m.mu.Lock()
	pooled := make(map[string]struct{}, len(m.pools))
	for _, p := range m.pools {
		pooled[p.hash] = struct{}{}
	}
	m.mu.Unlock()
	m.tplMu.Lock()
	defer m.tplMu.Unlock()
	held := make(map[string]TemplateInfo, len(m.tplSet))
	for id, t := range m.tplSet {
		if _, ok := pooled[store.TemplateHash(id)]; !ok {
			held[id] = t
		}
	}
	return held
}

func (m *Manager) pooled(key types.PoolKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.pools[key]
	return ok
}

// checkTemplateOwner rejects publishing or deleting over another tenant's record.
func (m *Manager) checkTemplateOwner(ctx context.Context, id, tenant string) error {
	if tenant == "" {
		return nil
	}
	raw, err := m.tpls.ReadMeta(ctx, id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("read template: %w", err)
	}
	var prev templateRecord
	if json.Unmarshal(raw, &prev) != nil || !tenantOwns(tenant, prev.Tenant) {
		return ErrTemplateOwned
	}
	return nil
}

func (m *Manager) publishTemplate(ctx context.Context, snap string, rec templateRecord) (string, error) {
	id := store.TemplateID(rec.Key.Hash())
	staging, err := m.tpls.Stage(id)
	if err != nil {
		return "", fmt.Errorf("stage template: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	if err = m.eng.SnapshotExport(ctx, snap, filepath.Join(staging, store.ExportDir)); err != nil {
		return "", fmt.Errorf("export template: %w", err)
	}
	return m.commitTemplate(ctx, staging, rec)
}

func (m *Manager) commitTemplate(ctx context.Context, staging string, rec templateRecord) (string, error) {
	id := store.TemplateID(rec.Key.Hash())
	l := m.recLock(id)
	l.Lock()
	defer func() { l.Unlock(); m.recDone(id) }()
	if ownerErr := m.checkTemplateOwner(ctx, id, rec.Tenant); ownerErr != nil {
		return "", ownerErr
	}
	rec.ID, rec.CreatedAt = id, time.Now()
	meta, err := json.Marshal(rec)
	if err != nil {
		return "", err
	}
	if err = os.WriteFile(filepath.Join(staging, store.MetaFile), meta, 0o600); err != nil {
		return "", err
	}
	digest, err := m.tpls.PublishDigested(ctx, staging, id)
	if err != nil {
		return "", fmt.Errorf("publish template: %w", err)
	}
	if err = m.tpls.SetLabels(ctx, id, nil); err != nil {
		log.WithFunc("pool.commitTemplate").Warnf(ctx, "clear the previous promote's labels on %s: %v", id, err)
	}
	m.tplMu.Lock()
	m.tplSet[id] = TemplateInfo{Key: rec.Key, ContentDigest: digest, Tenant: rec.Tenant, CreatedAt: rec.CreatedAt}
	m.tplMu.Unlock()
	return digest, nil
}

type goldenResolution struct {
	dir            string
	templateDigest string
	source         types.PoolKey
	promoted       bool
	unlock         func()
}

func (g goldenResolution) release() {
	if g.unlock != nil {
		g.unlock()
	}
}

// resolveGolden resolves a key's clone source: the pool golden, else a promoted template.
func (m *Manager) resolveGolden(ctx context.Context, key types.PoolKey, tenant string) (goldenResolution, error) {
	m.mu.Lock()
	var dir string
	if p := m.pools[key]; p != nil {
		dir = p.goldenDir
	}
	m.mu.Unlock()
	if dir != "" {
		return goldenResolution{dir: dir}, nil
	}
	if key.Net == types.NetEgress {
		return goldenResolution{}, nil // never resume a live-captured template on the egress lane; cold-boot instead
	}
	id := store.TemplateID(key.Hash())
	l := m.recLock(id)
	l.RLock()
	dir, meta, digest, err := m.tpls.Fetch(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		l.RUnlock()
		m.recDoneEvict(id)
		return goldenResolution{}, nil
	}
	if err != nil {
		l.RUnlock()
		m.recDone(id)
		return goldenResolution{}, err
	}
	cleanup := func() { l.RUnlock(); m.recDone(id) }
	var rec templateRecord
	if err := json.Unmarshal(meta, &rec); err != nil {
		cleanup()
		return goldenResolution{}, fmt.Errorf("decode template metadata: %w", err)
	}
	if rec.Tenant != "" && !tenantOwns(tenant, rec.Tenant) {
		cleanup()
		return goldenResolution{}, nil
	}
	return goldenResolution{
		dir:            dir,
		templateDigest: digest,
		source:         rec.PolicySource,
		promoted:       true,
		unlock:         cleanup,
	}, nil
}
