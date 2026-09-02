package pool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// templateRecord is a template's meta.json; an empty Tenant means the operator (root).
type templateRecord struct {
	ID        string    `json:"id"`
	Tenant    string    `json:"tenant,omitempty"`
	CreatedAt time.Time `json:"created_at"`
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
	if m.pooledHash(key.Hash()) {
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

	snap, cleanup, err := m.sourceSnap(ctx, sb)
	if err != nil {
		return types.PoolKey{}, "", fmt.Errorf("promote %s: %w", sb.ID, err)
	}
	defer cleanup()
	digest, err := m.publishTemplate(ctx, snap, key, tenant)
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
	if m.pooledHash(key.Hash()) {
		return ErrPooledTemplate
	}
	id := store.TemplateID(key.Hash())
	l := m.recLock(id)
	l.Lock()
	defer func() { l.Unlock(); m.recDone(id) }()
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

// TemplateHashes lists the promoted-template key hashes for mesh gossip, sorted for its guard.
func (m *Manager) TemplateHashes() []string {
	m.mu.Lock()
	pooled := make(map[string]struct{}, len(m.pools))
	for _, p := range m.pools {
		pooled[p.hash] = struct{}{}
	}
	m.mu.Unlock()
	m.tplMu.Lock()
	hashes := make([]string, 0, len(m.tplSet))
	for id, tenant := range m.tplSet {
		hash := store.TemplateHash(id)
		if _, ok := pooled[hash]; !ok {
			hashes = append(hashes, types.TemplateGossipHash(hash, tenant))
		}
	}
	m.tplMu.Unlock()
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
	if m.pooledHash(key.Hash()) {
		return false
	}
	id := store.TemplateID(key.Hash())
	m.tplMu.Lock()
	owner, cached := m.tplSet[id]
	m.tplMu.Unlock()
	if !cached {
		// Only a shared-store template promoted elsewhere after startup.
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

// pooledHash guards on the hash, not the key: a colliding key would reach a pool's golden dir.
func (m *Manager) pooledHash(hash string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.pools {
		if p.hash == hash {
			return true
		}
	}
	return false
}

// recLock takes the per-record lock and a live reference; pair every call with recDone.
func (m *Manager) recLock(id string) *sync.RWMutex {
	m.recLocksMu.Lock()
	defer m.recLocksMu.Unlock()
	m.recRefs[id]++
	l := m.recLocks[id]
	if l == nil {
		l = &sync.RWMutex{}
		m.recLocks[id] = l
	}
	return l
}

// recDone releases a reference taken by recLock; call after Unlock/RUnlock, never before.
func (m *Manager) recDone(id string) {
	m.recLocksMu.Lock()
	defer m.recLocksMu.Unlock()
	m.recRefs[id]--
	if m.recRefs[id] <= 0 {
		delete(m.recRefs, id)
		m.evictIfPending(id)
	}
}

// recDoneEvict is recDone for a just-deleted record: the lock slot goes at zero references.
func (m *Manager) recDoneEvict(id string) {
	m.recLocksMu.Lock()
	defer m.recLocksMu.Unlock()
	m.recRefs[id]--
	if m.recRefs[id] <= 0 {
		delete(m.recRefs, id)
		delete(m.recEvict, id)
		delete(m.recLocks, id)
		return
	}
	// a holder or waiter remains; the call that drops the last reference evicts instead
	m.recEvict[id] = struct{}{}
}

// evictIfPending drops id's lock entry if a delete asked for eviction; callers hold recLocksMu.
func (m *Manager) evictIfPending(id string) {
	if _, ok := m.recEvict[id]; ok {
		delete(m.recEvict, id)
		delete(m.recLocks, id)
	}
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

type goldenResolution struct {
	dir            string
	templateDigest string
	promoted       bool
	release        func()
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
		return goldenResolution{dir: dir, release: func() {}}, nil
	}
	if key.Net == types.NetEgress {
		return goldenResolution{release: func() {}}, nil // never resume a live-captured template on the egress lane; cold-boot instead
	}
	id := store.TemplateID(key.Hash())
	l := m.recLock(id)
	l.RLock()
	dir, meta, digest, release, err := m.tpls.Fetch(ctx, id)
	if err != nil {
		l.RUnlock()
		m.recDone(id)
		if errors.Is(err, store.ErrNotFound) {
			return goldenResolution{release: func() {}}, nil
		}
		return goldenResolution{release: func() {}}, err
	}
	cleanup := func() { release(); l.RUnlock(); m.recDone(id) }
	var rec templateRecord
	if err := json.Unmarshal(meta, &rec); err != nil {
		cleanup()
		return goldenResolution{release: func() {}}, fmt.Errorf("decode template metadata: %w", err)
	}
	if rec.Tenant != "" && !tenantOwns(tenant, rec.Tenant) {
		cleanup()
		return goldenResolution{release: func() {}}, nil
	}
	return goldenResolution{
		dir:            dir,
		templateDigest: digest,
		promoted:       true,
		release:        cleanup,
	}, nil
}

func (m *Manager) publishTemplate(ctx context.Context, snap string, key types.PoolKey, tenant string) (string, error) {
	id := store.TemplateID(key.Hash())
	staging, err := m.tpls.Stage(id)
	if err != nil {
		return "", fmt.Errorf("stage template: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	if err = m.eng.SnapshotExport(ctx, snap, filepath.Join(staging, store.ExportDir)); err != nil {
		return "", fmt.Errorf("export template: %w", err)
	}
	return m.commitTemplate(ctx, staging, id, tenant)
}

func (m *Manager) commitTemplate(ctx context.Context, staging, id, tenant string) (string, error) {
	l := m.recLock(id)
	l.Lock()
	defer func() { l.Unlock(); m.recDone(id) }()
	if ownerErr := m.checkTemplateOwner(ctx, id, tenant); ownerErr != nil {
		return "", ownerErr
	}
	meta, err := json.Marshal(templateRecord{ID: id, Tenant: tenant, CreatedAt: time.Now()})
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
	m.tplMu.Lock()
	m.tplSet[id] = tenant
	m.tplMu.Unlock()
	return digest, nil
}
