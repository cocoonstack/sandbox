package pool

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// ReloadResult names what a config reload applied and what it left to the API that owns it.
type ReloadResult struct {
	Changed []string `json:"changed"`
	Ignored []string `json:"ignored,omitempty"`
}

// ReloadConfig applies next's reloadable settings at once or not at all: a field that needs a restart, or intercept turned on for a key this node has served, refuses the whole reload.
func (m *Manager) ReloadConfig(ctx context.Context, next *config.Config) (ReloadResult, error) {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	changed, ignored, err := m.cfg.ReloadDiff(next)
	if err != nil {
		return ReloadResult{}, fmt.Errorf("%w: %w", ErrReloadRefused, err)
	}
	res := ReloadResult{Changed: append([]string{}, changed...), Ignored: ignored}
	if len(changed) == 0 {
		m.cfg = next
		return res, nil
	}
	secrets, err := egress.NewSecretStore(next.Secrets)
	if err != nil {
		return ReloadResult{}, fmt.Errorf("%w: secrets: %w", ErrBadConfig, err)
	}
	old, view := m.view.Load(), newConfigView(next, secrets)
	if err := m.refuseInterceptOn(old, view); err != nil {
		return ReloadResult{}, err
	}
	var trim []string
	m.mu.Lock()
	m.view.Store(view)
	for _, p := range m.pools {
		if p.goldenDir != "" && m.goldenStamp(old, p.key, p.imageID) != m.goldenStamp(view, p.key, p.imageID) {
			p.goldenDir, p.imageID = "", ""
			trim = append(trim, p.trimWarm(0)...)
		}
	}
	m.mu.Unlock()
	m.destroyAll(ctx, trim).Wait()
	m.cfg = next
	m.recordAudit(ctx, "", auditFrame{Op: "config_reload", Changed: changed})
	log.WithFunc("pool.ReloadConfig").Infof(ctx, "config reloaded: %s; %d warm VMs of retired goldens destroyed", strings.Join(changed, ", "), len(trim))
	m.kickRefill()
	return res, nil
}

// refuseInterceptOn turns intercept on only for a key whose guests can trust the CA: one the node has not served, with the CA loaded at boot.
func (m *Manager) refuseInterceptOn(old, next *configView) error {
	keys := slices.Collect(maps.Keys(next.poolEgress))
	slices.SortFunc(keys, func(a, b types.PoolKey) int { return strings.Compare(a.Hash(), b.Hash()) })
	for _, key := range keys {
		if old.poolEgress[key].Intercepts() || !next.poolEgress[key].Intercepts() {
			continue
		}
		switch {
		case m.egressCA == nil:
			return fmt.Errorf("%w: pool %s turns intercept on, which needs egress_ca loaded at boot: restart", ErrReloadRefused, key.Template)
		case m.servedKey(key):
			return fmt.Errorf("%w: pool %s turns intercept on, but its guests do not trust the CA: restart or use a new pool key", ErrReloadRefused, key.Template)
		}
	}
	return nil
}

func (m *Manager) servedKey(key types.PoolKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, pooled := m.pools[key]; pooled {
		return true
	}
	for _, sb := range m.claimed {
		if sb.PolicyKey() == key {
			return true
		}
	}
	return false
}
