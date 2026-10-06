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
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
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
	if err := m.out.RefuseInterceptOn(old.out, view.out, m.servedKey); err != nil {
		return ReloadResult{}, fmt.Errorf("%w: %w", ErrReloadRefused, err)
	}
	if err := m.refusePolicyDrop(old.out, view.out); err != nil {
		return ReloadResult{}, fmt.Errorf("%w: %w", ErrReloadRefused, err)
	}
	var trim []string
	var injecting []*types.Sandbox
	m.mu.Lock()
	m.view.Store(view)
	for _, sb := range m.claimed {
		if sb.Env.HasInject() {
			injecting = append(injecting, sb)
		}
	}
	for _, p := range m.pools {
		if p.goldenDir != "" && m.goldenStamp(old, p.key, p.imageID) != m.goldenStamp(view, p.key, p.imageID) {
			p.goldenDir, p.imageID = "", ""
			trim = append(trim, p.trimWarm(0)...)
		}
	}
	m.mu.Unlock()
	m.out.Reloaded(view.out)
	m.destroyAll(context.WithoutCancel(ctx), trim).Wait()
	m.cfg = next
	m.recordAudit(ctx, "", auditFrame{Op: "config_reload", Changed: changed})
	log.WithFunc("pool.ReloadConfig").Infof(ctx, "config reloaded: %s; %d warm VMs of retired goldens destroyed", strings.Join(changed, ", "), len(trim))
	m.warnMissingClasses(ctx)
	m.warnLostInjects(ctx, view, injecting)
	m.kickRefill()
	return res, nil
}

func (m *Manager) warnLostInjects(ctx context.Context, v *configView, sbs []*types.Sandbox) {
	for _, sb := range sbs {
		m.mu.Lock()
		env := sb.Env
		m.mu.Unlock()
		if gap := v.out.InjectGap(sb, env, m.claimPooled); len(gap) > 0 {
			log.WithFunc("pool.warnLostInjects").Warnf(ctx, "claim %s: inject no longer intercepted: %s", sb.ID, strings.Join(gap, ", "))
		}
	}
}

func (m *Manager) refusePolicyDrop(old, next *outbound.View) error {
	denied := m.deniedClaims(next)
	for _, key := range slices.SortedFunc(maps.Keys(denied), comparePoolKeys) {
		if old.HasPolicy(key) {
			return fmt.Errorf("pool %s drops the egress policy of %d live claims: keep it until they are released", key.Template, denied[key])
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
