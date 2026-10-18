package pool

import (
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// configView is the config-owned state a reload replaces whole; readers load it once and never see a half-applied reload.
type configView struct {
	out           *outbound.View
	poolWarmups   map[types.PoolKey][]string
	poolTrims     map[types.PoolKey]bool
	poolStorage   map[types.PoolKey]string
	poolReplicate map[types.PoolKey]time.Duration
	poolReaders   map[types.PoolKey]bool
	diskReserve   int64
	diskDirs      []string
}

func newConfigView(cfg *config.Config, secrets *egress.SecretStore) *configView {
	v := &configView{
		out:           outbound.NewView(cfg, secrets),
		poolWarmups:   map[types.PoolKey][]string{},
		poolTrims:     map[types.PoolKey]bool{},
		poolStorage:   map[types.PoolKey]string{},
		poolReplicate: map[types.PoolKey]time.Duration{},
		poolReaders:   map[types.PoolKey]bool{},
	}
	if r := cfg.DiskReserve; r != nil {
		v.diskReserve, _ = config.StorageBytes(r.Free)
		v.diskDirs = r.Dirs
		if len(v.diskDirs) == 0 {
			v.diskDirs = []string{cfg.DataDir}
		}
	}
	for _, spec := range cfg.Pools {
		key := spec.PoolKey
		if len(spec.Warmup) > 0 {
			v.poolWarmups[key] = spec.Warmup
		}
		if spec.CaptureTrim {
			v.poolTrims[key] = true
		}
		if spec.Storage != "" {
			v.poolStorage[key] = spec.Storage
		}
		if spec.ReplicateSeconds > 0 {
			v.poolReplicate[key] = time.Duration(spec.ReplicateSeconds) * time.Second
		}
		if spec.Reader {
			v.poolReaders[key] = true
		}
	}
	return v
}
