package outbound

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// View is the egress half of the node config; a reload replaces it whole.
type View struct {
	poolEgress  map[types.PoolKey]*egress.Policy
	classEgress map[string]*egress.Policy

	poolUpstream  map[types.PoolKey]string
	classUpstream map[string]string
	upstreamEnv   string
	upstreamAllow egress.UpstreamAllow
	internalAllow []egress.InternalAllow
	secrets       *egress.SecretStore
	guarded       bool
	usageBytes    bool
}

// NewView reads cfg's egress settings; config validation already rejected bad entries.
func NewView(cfg *config.Config, secrets *egress.SecretStore) *View {
	v := &View{
		poolEgress:    map[types.PoolKey]*egress.Policy{},
		classEgress:   map[string]*egress.Policy{},
		poolUpstream:  map[types.PoolKey]string{},
		classUpstream: map[string]string{},
		internalAllow: parseInternalAllow(cfg.EgressInternalAllow),
		secrets:       secrets,
		usageBytes:    cfg.EgressUsageBytes,
	}
	if u := cfg.EgressUpstream; u != nil {
		v.upstreamEnv = u.ClaimEnv
		v.upstreamAllow, _ = egress.ParseUpstreamAllow(u.Allow)
	}
	for _, ec := range cfg.EgressClasses {
		if ec.EgressUpstreamEnv != "" {
			v.classUpstream[ec.Name] = os.Getenv(ec.EgressUpstreamEnv)
		}
		v.classEgress[ec.Name] = ec.Egress
		v.guarded = true
	}
	for _, spec := range cfg.Pools {
		if spec.Egress != nil {
			v.poolEgress[spec.PoolKey] = spec.Egress
			v.guarded = true
		}
		if spec.EgressUpstreamEnv != "" {
			v.poolUpstream[spec.PoolKey] = os.Getenv(spec.EgressUpstreamEnv)
		}
	}
	return v
}

// Intercepts reports whether key's pool policy may terminate HTTPS.
func (v *View) Intercepts(key types.PoolKey) bool {
	return v.poolEgress[key].Intercepts()
}

// HasClass reports whether the egress class name is configured.
func (v *View) HasClass(name string) bool {
	_, ok := v.classEgress[name]
	return ok
}

// Resolve is sb's effective policy, pool ∩ tenant: root has no tenant layer, an unpooled key no pool one, a NoEgress claim none at all; pooled runs only when the key has no pool policy.
func (v *View) Resolve(sb *types.Sandbox, pooled func() bool) (egress.Evaluator, bool) {
	if sb.NoEgress {
		return nil, false
	}
	poolPol := v.poolEgress[sb.PolicyKey()]
	tenantPol := v.classEgress[sb.EgressClass]
	if poolPol == nil {
		if sb.Tenant == "" || tenantPol == nil || pooled() {
			return nil, false
		}
		return *tenantPol, true
	}
	if sb.Tenant == "" {
		return *poolPol, true
	}
	if tenantPol == nil {
		return nil, false
	}
	return egress.Compose(*poolPol, *tenantPol), true
}

// InjectGap names, in env order, each inject host sb's egress does not intercept and each header a covering pool secret already sets.
func (v *View) InjectGap(eval egress.Evaluator, ok bool, sb *types.Sandbox, env types.Env) []string {
	var gap []string
	pool := v.poolEgress[sb.PolicyKey()]
	for _, name := range slices.Sorted(maps.Keys(env)) {
		inj := env[name].Inject
		if inj == nil {
			continue
		}
		for _, h := range inj.Hosts {
			if !ok || !eval.InterceptsHost(h) {
				gap = append(gap, fmt.Sprintf("%s: %s is not intercepted", name, h))
			} else if inj.Header != "" && v.secretSets(pool, h, inj.Header) {
				gap = append(gap, fmt.Sprintf("%s: the pool's secret already sets %s on %s", name, inj.Header, h))
			}
		}
	}
	return gap
}

// CheckEnv admits an env this node can serve: inject headers the proxy may set, and an upstream entry that is host-only and either direct or an allowed upstream.
func (v *View) CheckEnv(env types.Env) error {
	for name, e := range env {
		if e.Inject == nil || e.Inject.Header == "" {
			continue
		}
		if err := egress.CheckInjectHeader(e.Inject.Header); err != nil {
			return fmt.Errorf("env %s: inject %w", name, err)
		}
	}
	name := v.upstreamEnv
	if name == "" {
		return nil
	}
	e, ok := env[name]
	switch {
	case !ok:
		return nil
	case e.InGuest():
		return fmt.Errorf("%s must be guest: false, it carries the upstream's credentials", name)
	case e.Value == upstreamDirect:
		return nil
	}
	u, err := egress.ParseUpstream(e.Value)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if !v.upstreamAllow.Allows(u) {
		return fmt.Errorf("%s: upstream %s is not in egress_upstream.allow", name, u.Host)
	}
	return nil
}

func (v *View) secretSets(pool *egress.Policy, host, header string) bool {
	if pool == nil {
		return false
	}
	return slices.ContainsFunc(pool.Allow, func(r egress.Rule) bool {
		if r.Intercept == egress.InterceptOff || r.Secret == "" || !r.Covers(host) {
			return false
		}
		h, _, known := v.secrets.Header(r.Secret)
		return known && strings.EqualFold(h, header)
	})
}
