package pool

import (
	"os"
	"sync/atomic"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// configView is the config-owned state a reload replaces whole; readers load it once and never see a half-applied reload.
type configView struct {
	poolEgress  map[types.PoolKey]*egress.Policy
	classEgress map[string]*egress.Policy
	poolWarmups map[types.PoolKey][]string
	poolTrims   map[types.PoolKey]bool
	poolStorage map[types.PoolKey]string

	poolUpstream  map[types.PoolKey]string
	classUpstream map[string]string
	upstreamEnv   string
	upstreamAllow egress.UpstreamAllow
	internalAllow []egress.InternalAllow
	secrets       *egress.SecretStore
	guardedEgress bool
	usageBytes    bool
}

func newConfigView(cfg *config.Config, secrets *egress.SecretStore) *configView {
	v := &configView{
		poolEgress:    map[types.PoolKey]*egress.Policy{},
		classEgress:   map[string]*egress.Policy{},
		poolWarmups:   map[types.PoolKey][]string{},
		poolTrims:     map[types.PoolKey]bool{},
		poolStorage:   map[types.PoolKey]string{},
		poolUpstream:  map[types.PoolKey]string{},
		classUpstream: map[string]string{},
		internalAllow: parseInternalAllow(cfg.EgressInternalAllow),
		secrets:       secrets,
		usageBytes:    cfg.EgressUsageBytes,
	}
	if u := cfg.EgressUpstream; u != nil {
		v.upstreamEnv = u.ClaimEnv
		v.upstreamAllow, _ = egress.ParseUpstreamAllow(u.Allow) // config validation rejected a bad entry
	}
	for _, ec := range cfg.EgressClasses {
		if ec.EgressUpstreamEnv != "" {
			v.classUpstream[ec.Name] = os.Getenv(ec.EgressUpstreamEnv)
		}
		v.classEgress[ec.Name] = ec.Egress
		v.guardedEgress = true
	}
	for _, spec := range cfg.Pools {
		key := spec.PoolKey
		if spec.Egress != nil {
			v.poolEgress[key] = spec.Egress
			v.guardedEgress = true
		}
		if len(spec.Warmup) > 0 {
			v.poolWarmups[key] = spec.Warmup
		}
		if spec.CaptureTrim {
			v.poolTrims[key] = true
		}
		if spec.Storage != "" {
			v.poolStorage[key] = spec.Storage
		}
		if spec.EgressUpstreamEnv != "" {
			v.poolUpstream[key] = os.Getenv(spec.EgressUpstreamEnv)
		}
	}
	return v
}

type resolvedPolicy struct {
	view *configView
	eval egress.Evaluator
}

var _ egress.Evaluator = (*livePolicy)(nil)

// livePolicy is a claim's egress policy that follows a reload: a new view re-resolves its pool and tenant layers once.
type livePolicy struct {
	m   *Manager
	sb  *types.Sandbox
	cur atomic.Pointer[resolvedPolicy]
}

func newLivePolicy(m *Manager, sb *types.Sandbox, v *configView, eval egress.Evaluator) *livePolicy {
	l := &livePolicy{m: m, sb: sb}
	l.cur.Store(&resolvedPolicy{view: v, eval: eval})
	return l
}

func (l *livePolicy) Eval(host, method string, port uint16) (egress.Rule, egress.Decision) {
	return l.current().Eval(host, method, port)
}

func (l *livePolicy) EvalHost(host string, port uint16) (egress.Rule, egress.Decision) {
	return l.current().EvalHost(host, port)
}

func (l *livePolicy) EvalInner(host, method string, port uint16) (egress.Rule, egress.Decision) {
	return l.current().EvalInner(host, method, port)
}

func (l *livePolicy) ServesSocks() bool {
	return l.current().ServesSocks()
}

// current denies everything once a reload leaves the claim with no policy.
func (l *livePolicy) current() egress.Evaluator {
	v := l.m.view.Load()
	if r := l.cur.Load(); r.view == v {
		return r.eval
	}
	eval, ok := l.m.effectivePolicy(v, l.sb)
	if !ok {
		eval = egress.Policy{}
	}
	l.cur.Store(&resolvedPolicy{view: v, eval: eval})
	return eval
}
