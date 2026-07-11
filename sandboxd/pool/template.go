package pool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// templateRecord is a template's meta.json: the id carries the key hash
// (tp_<hash>), which is all resolution needs — claims re-derive the hash
// from the requested key, never the reverse. Tenant attributes the promote
// and scopes deletion; empty means the operator (root).
type templateRecord struct {
	ID        string    `json:"id"`
	Tenant    string    `json:"tenant,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Promote publishes a claimed sandbox as a template under (template, parent
// net, parent size); later claims for that key clone from it. Re-promoting the
// same name replaces it, and the caller owns its lifecycle (DeleteTemplate).
// tenant attributes the record; empty means the operator (root).
func (m *Manager) Promote(ctx context.Context, id, token, template, tenant string) (types.PoolKey, error) {
	if !types.NameRe.MatchString(template) {
		return types.PoolKey{}, fmt.Errorf("%w: template %q must match %s", ErrBadKey, template, types.NameRe)
	}
	sb, ok := m.claim(id, token)
	if !ok {
		return types.PoolKey{}, ErrUnknownSandbox
	}
	if sb.Key.Net == types.NetEgress {
		return types.PoolKey{}, ErrNoEgressFork
	}
	key := types.PoolKey{Template: template, Net: sb.Key.Net, Size: sb.Key.Size}
	if m.pooledHash(key.Hash()) {
		// A configured pool owns this key — promoting over it would
		// silently change what refills produce.
		return types.PoolKey{}, ErrPooledTemplate
	}
	// Fast-fail before the export; commitTemplate re-checks under the
	// template lock, closing the check-then-publish race.
	if err := m.checkTemplateOwner(ctx, store.TemplateID(key.Hash()), tenant); err != nil {
		return types.PoolKey{}, err
	}
	// See Fork: the transition lock pins the source snapshot, and a started
	// promote must finish even if the caller hangs up.
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	ctx = context.WithoutCancel(ctx)

	snap, cleanup, err := m.sourceSnap(ctx, sb)
	if err != nil {
		return types.PoolKey{}, fmt.Errorf("promote %s: %w", id, err)
	}
	defer cleanup()
	if err := m.publishTemplate(ctx, snap, key, tenant); err != nil {
		return types.PoolKey{}, fmt.Errorf("promote %s: %w", id, err)
	}
	if m.notifyTemplates != nil {
		m.notifyTemplates()
	}
	m.counters.promotes.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "promote", ID: sb.ID, VMName: sb.VMName, Reference: key.Template})
	return key, nil
}

// DeleteTemplate removes a promoted template. Configured pools are refused:
// their goldens are owned by the node config, not an API caller. A tenant
// may delete only templates it promoted — anything else answers
// ErrUnknownTemplate; root (empty tenant) deletes anything.
func (m *Manager) DeleteTemplate(ctx context.Context, key types.PoolKey, tenant string) error {
	if m.pooledHash(key.Hash()) {
		return ErrPooledTemplate
	}
	id := store.TemplateID(key.Hash())
	l := m.recLock(id)
	l.Lock()
	defer l.Unlock()
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

// TemplateHashes lists the promoted-template key hashes for the mesh's
// template gossip, from the in-memory set — the 1s gossip tick never
// touches the store backend.
func (m *Manager) TemplateHashes() []string {
	m.mu.Lock()
	pooled := make(map[string]struct{}, len(m.pools))
	for key := range m.pools {
		pooled[key.Hash()] = struct{}{}
	}
	m.mu.Unlock()
	m.tplMu.Lock()
	hashes := make([]string, 0, len(m.tplSet))
	for id := range m.tplSet {
		hash := store.TemplateHash(id)
		if _, ok := pooled[hash]; !ok {
			hashes = append(hashes, hash)
		}
	}
	m.tplMu.Unlock()
	return hashes
}

// HasGolden reports whether this node can provision the key without a cold
// boot — a configured pool golden or a promoted template in the store. The
// tplSet answers without a store read; only a shared-store template promoted
// elsewhere after startup falls through to the backend.
func (m *Manager) HasGolden(ctx context.Context, key types.PoolKey) bool {
	m.mu.Lock()
	pooled := m.pools[key] != nil && m.pools[key].goldenDir != ""
	m.mu.Unlock()
	if pooled {
		return true
	}
	id := store.TemplateID(key.Hash())
	m.tplMu.Lock()
	_, cached := m.tplSet[id]
	m.tplMu.Unlock()
	if cached {
		return true
	}
	_, err := m.tpls.ReadMeta(ctx, id)
	return err == nil
}

// pooledHash reports whether a configured pool occupies this hash — the
// guard is on the HASH, not the key: goldens are stored by hash, so a
// colliding key would reach a pool's golden dir even though the keys differ.
func (m *Manager) pooledHash(hash string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.pools {
		if key.Hash() == hash {
			return true
		}
	}
	return false
}

// recLock is the per-record mutation lock: template publish/delete serialize
// on it directly; checkpoints take it only for delete/fetch (their ids are
// fresh and unguessable pre-publish). A clone or wake holds it shared, so a
// delete or re-publish swap never runs under an in-flight read.
func (m *Manager) recLock(id string) *sync.RWMutex {
	if l, ok := m.recLocks.Load(id); ok {
		return l.(*sync.RWMutex)
	}
	l, _ := m.recLocks.LoadOrStore(id, &sync.RWMutex{})
	return l.(*sync.RWMutex)
}

// dropRecLock evicts a deleted record's lock so recLocks can't grow per
// checkpoint. Safe only for single-use ck ids and only after the store record
// is deleted: a fresh lock is obtainable only after eviction, i.e. after the
// record is gone, so no two live ops diverge onto different locks. Template ids
// (tp_, reused on re-promote) must not be evicted; they are bounded.
func (m *Manager) dropRecLock(id string) {
	m.recLocks.Delete(id)
}

// checkTemplateOwner rejects publishing or deleting over another tenant's
// record; a meta failure other than absence refuses rather than fail open.
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

// resolveGolden resolves a key's clone source: the configured pool's local
// golden (no release), else a promoted template fetched from the store;
// empty dir cold-boots. Only a true absence cold-boots — a backend failure
// propagates rather than silently booting a template name as an image ref.
func (m *Manager) resolveGolden(ctx context.Context, key types.PoolKey) (string, func(), error) {
	m.mu.Lock()
	var dir string
	if p := m.pools[key]; p != nil {
		dir = p.goldenDir
	}
	m.mu.Unlock()
	if dir != "" {
		return dir, func() {}, nil
	}
	if key.Net == types.NetEgress {
		return "", func() {}, nil // never resume a live-captured template on the egress lane; cold-boot instead
	}
	id := store.TemplateID(key.Hash())
	l := m.recLock(id)
	l.RLock()
	dir, _, release, err := m.tpls.Fetch(ctx, id)
	if err != nil {
		l.RUnlock()
		if errors.Is(err, store.ErrNotFound) {
			return "", func() {}, nil
		}
		return "", func() {}, err
	}
	return dir, func() { release(); l.RUnlock() }, nil
}

// publishTemplate exports snap into the store under the key's template id.
func (m *Manager) publishTemplate(ctx context.Context, snap string, key types.PoolKey, tenant string) error {
	id := store.TemplateID(key.Hash())
	staging, err := m.tpls.Stage(id)
	if err != nil {
		return fmt.Errorf("stage template: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	if err = m.eng.SnapshotExport(ctx, snap, filepath.Join(staging, store.ExportDir)); err != nil {
		return fmt.Errorf("export template: %w", err)
	}
	return m.commitTemplate(ctx, staging, id, tenant)
}

// commitTemplate writes the meta record, publishes the staged template, and
// registers it in the gossip set — owner re-checked and swap serialized
// under the template lock.
func (m *Manager) commitTemplate(ctx context.Context, staging, id, tenant string) error {
	meta, err := json.Marshal(templateRecord{ID: id, Tenant: tenant, CreatedAt: time.Now()})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(staging, store.MetaFile), meta, 0o600); err != nil {
		return err
	}
	l := m.recLock(id)
	l.Lock()
	defer l.Unlock()
	if err := m.checkTemplateOwner(ctx, id, tenant); err != nil {
		return err
	}
	if err := m.tpls.Publish(ctx, staging, id); err != nil {
		return fmt.Errorf("publish template: %w", err)
	}
	m.tplMu.Lock()
	m.tplSet[id] = struct{}{}
	m.tplMu.Unlock()
	return nil
}

// migrateLegacyTemplates moves pre-store promoted templates
// (<goldens>/<hash> dirs not backing a pool) into the template store —
// one-time, at reconcile.
func (m *Manager) migrateLegacyTemplates(ctx context.Context) {
	logger := log.WithFunc("pool.migrateLegacyTemplates")
	entries, err := os.ReadDir(m.goldensDir())
	if err != nil {
		return
	}
	for _, e := range entries {
		hash := e.Name()
		id := store.TemplateID(hash)
		if !e.IsDir() || m.pooledHash(hash) || !store.TemplateIDRe.MatchString(id) {
			continue
		}
		staging, err := m.tpls.Stage(id)
		if err != nil {
			logger.Errorf(ctx, err, "stage %s", id)
			continue
		}
		legacy := filepath.Join(m.goldensDir(), hash)
		if err := os.Rename(legacy, filepath.Join(staging, store.ExportDir)); err != nil {
			logger.Errorf(ctx, err, "move %s", legacy)
			_ = os.RemoveAll(staging)
			continue
		}
		if err := m.commitTemplate(ctx, staging, id, ""); err != nil {
			// The staged export is the only copy: put it back, or quarantine
			// the staging dir out of the sweep's .tmp suffix when even that
			// rename fails.
			if rbErr := os.Rename(filepath.Join(staging, store.ExportDir), legacy); rbErr != nil {
				_ = os.Rename(staging, strings.TrimSuffix(staging, ".tmp")+".stuck")
				logger.Errorf(ctx, rbErr, "restore %s", legacy)
			} else {
				_ = os.RemoveAll(staging)
			}
			logger.Errorf(ctx, err, "publish %s", id)
			continue
		}
		logger.Infof(ctx, "migrated legacy template %s", id)
	}
}
