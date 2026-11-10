package pool

import (
	"cmp"
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
	memoryAnon    int64
	memoryPSI     float64
	noBalloon     bool
	hostEnvMemory bool

	maxClaims            int
	maxRunning           int
	moveConcurrency      int
	maxFork              int
	releaseDelay         time.Duration
	vmmRestart           types.VMMRestart
	ckptTTL              time.Duration
	idleDefault          time.Duration
	archiveAfterDefault  time.Duration
	archiveDeleteDefault time.Duration
}

func newConfigView(cfg *config.Config, secrets *egress.SecretStore) *configView {
	v := &configView{
		out:           outbound.NewView(cfg, secrets),
		poolWarmups:   map[types.PoolKey][]string{},
		poolTrims:     map[types.PoolKey]bool{},
		poolStorage:   map[types.PoolKey]string{},
		poolReplicate: map[types.PoolKey]time.Duration{},
		poolReaders:   map[types.PoolKey]bool{},
		noBalloon:     cfg.NoBalloon,
		hostEnvMemory: cfg.HostEnvMemoryOnly,

		maxClaims:            cfg.MaxClaims,
		maxRunning:           cfg.MaxRunning,
		moveConcurrency:      cmp.Or(cfg.MoveConcurrency, defaultMoveConcurrency),
		maxFork:              cmp.Or(cfg.MaxForkCount, config.DefaultMaxForkCount),
		releaseDelay:         time.Duration(cfg.ReleaseDelaySeconds) * time.Second,
		vmmRestart:           cfg.VMMRestart,
		ckptTTL:              time.Duration(cfg.CheckpointTTLHours) * time.Hour,
		idleDefault:          time.Duration(cfg.IdleHibernateSeconds) * time.Second,
		archiveAfterDefault:  time.Duration(cfg.ArchiveAfterSeconds) * time.Second,
		archiveDeleteDefault: time.Duration(cfg.ArchiveDeleteAfterSeconds) * time.Second,
	}
	if r := cfg.DiskReserve; r != nil {
		v.diskReserve, _ = config.StorageBytes(r.Free)
		v.diskDirs = r.Dirs
		if len(v.diskDirs) == 0 {
			v.diskDirs = []string{cfg.DataDir}
		}
	}
	if l := cfg.MemoryLimit; l != nil {
		v.memoryPSI = l.PSISomeAvg10
		if l.Anon != "" {
			v.memoryAnon, _ = config.StorageBytes(l.Anon)
		}
	}
	for _, spec := range cfg.Pools {
		key := spec.PoolKey
		v.poolWarmups[key], v.poolTrims[key], v.poolStorage[key] = spec.Warmup, spec.CaptureTrim, spec.Storage
		v.poolReplicate[key], v.poolReaders[key] = time.Duration(spec.ReplicateSeconds)*time.Second, spec.Reader
	}
	return v
}
