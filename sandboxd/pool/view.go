package pool

import (
	"os"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// configView is the config-owned state a reload replaces whole; readers load it once and never see a half-applied reload.
type configView struct {
	poolEgress   map[types.PoolKey]*egress.Policy
	tenantEgress map[string]*egress.Policy
	poolWarmups  map[types.PoolKey][]string
	poolTrims    map[types.PoolKey]bool
	poolStorage  map[types.PoolKey]string

	poolUpstream   map[types.PoolKey]string
	tenantUpstream map[string]string
	upstreamEnv    string
	upstreamAllow  egress.UpstreamAllow
	internalAllow  []egress.InternalAllow
	secrets        *egress.SecretStore
	guardedEgress  bool
	usageBytes     bool
}

func newConfigView(cfg *config.Config, secrets *egress.SecretStore) *configView {
	v := &configView{
		poolEgress:     map[types.PoolKey]*egress.Policy{},
		tenantEgress:   map[string]*egress.Policy{},
		poolWarmups:    map[types.PoolKey][]string{},
		poolTrims:      map[types.PoolKey]bool{},
		poolStorage:    map[types.PoolKey]string{},
		poolUpstream:   map[types.PoolKey]string{},
		tenantUpstream: map[string]string{},
		internalAllow:  parseInternalAllow(cfg.EgressInternalAllow),
		secrets:        secrets,
		usageBytes:     cfg.EgressUsageBytes,
	}
	if u := cfg.EgressUpstream; u != nil {
		v.upstreamEnv = u.ClaimEnv
		v.upstreamAllow, _ = egress.ParseUpstreamAllow(u.Allow) // config validation rejected a bad entry
	}
	for _, tn := range cfg.Tenants {
		if tn.EgressUpstreamEnv != "" {
			v.tenantUpstream[tn.Name] = os.Getenv(tn.EgressUpstreamEnv)
		}
		if tn.Egress != nil {
			v.tenantEgress[tn.Name] = tn.Egress
			v.guardedEgress = true
		}
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
