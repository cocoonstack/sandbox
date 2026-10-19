// Package pool owns a node's warm pools and claimed sandboxes; refill keeps pools claim-ready, so a claim is ownership transfer only.
package pool

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/projecteru2/core/log"
	"golang.org/x/sync/singleflight"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/poolset"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/peer"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	refillInterval    = 2 * time.Second
	reapInterval      = 5 * time.Second
	livenessInterval  = 10 * time.Second
	imageInterval     = time.Minute
	buildRetryDelay   = 30 * time.Second
	claimProbeTimeout = 15 * time.Second
	coldProbeTimeout  = 90 * time.Second
	// A clone answers in ~1s even saturated, so a silent one is replaced, not waited on
	warmProbeTimeout = 5 * time.Second
	// One list; a wrong answer only costs an extra sweep.
	removeVerifyTimeout = 15 * time.Second
	cellRetry           = 5 * time.Second
	// A full node clears only when VMs go away; retrying sooner buys nothing.
	capacityBackoff   = buildRetryDelay
	vsockPollInterval = 100 * time.Millisecond
	defaultTTL        = 5 * time.Minute
	maxTTL            = 24 * time.Hour
	// A renewal missed once fences only if the next, 2*every after the last success, takes over ttl-margin-2*every to answer.
	leaseTTL    = 15 * time.Second
	leaseRenew  = 5 * time.Second
	leaseMargin = 3 * time.Second
	// One failed boot is ordinary; only an unbroken run predicts the next failure
	refillFailStreak  = 8
	refillBackoffBase = 250 * time.Millisecond
	refillBackoffMax  = buildRetryDelay
	// Fallbacks when a Manager is built from a Config that skipped config.Load's defaulting
	defaultMaxFork = 16
	defaultRefill  = 4

	// A heal pulls a whole guest memory image, so a few saturate disk and NIC.
	maxConcurrentHeals = 4

	vmPrefix        = "sbx-"
	goldenPrefix    = "sbx-golden-"
	hibernatePrefix = "sbx-hib-"
	forkPrefix      = "sbx-fork-"
	vmStateRunning  = "running"
	vmStateCreating = "creating"

	goldenStampSuffix = ".stamp"
)

var (
	ErrBadKey            = errors.New("invalid pool key")
	ErrBadCount          = errors.New("invalid fork count")
	ErrBadVolume         = errors.New("invalid volume request")
	ErrBadEnv            = errors.New("invalid env")
	ErrBadTenant         = tenants.ErrInvalid
	ErrUnknownTenant     = tenants.ErrUnknown
	ErrBadConfig         = errors.New("invalid config")
	ErrReloadRefused     = errors.New("config reload refused")
	ErrTenantStoreDown   = tenants.ErrUnavailable
	ErrPoolStoreDown     = poolset.ErrUnavailable
	ErrArchiveStoreDown  = archives.ErrUnavailable
	ErrTenantRemoved     = errors.New("the claim's tenant was removed: it runs to its deadline but cannot renew, fork, or wake from the archive")
	ErrVolumeUnavailable = fmt.Errorf("%w: unknown or unavailable volume", ErrBadVolume)
	ErrNoWarm            = errors.New("no warm sandbox for key")
	ErrUnknownSandbox    = errors.New("unknown sandbox or bad token")
	ErrUnknownTemplate   = errors.New("unknown promoted template")
	ErrPooledTemplate    = errors.New("template belongs to a configured pool")
	ErrTemplateOwned     = errors.New("template owned by another tenant")
	ErrTemplateReplaced  = errors.New("template replaced since it was observed")
	ErrNoEgress          = errors.New("node has no egress attachment (bridge or network)")
	ErrNoEgressHibernate = errors.New("egress-lane sandboxes do not hibernate")
	ErrNoEgressFork      = errors.New("egress-lane sandboxes cannot fork, checkpoint, or promote: a resumed guest egresses before its fresh tap can be locked")
	ErrVolumeCapture     = errors.New("sandboxes with volumes cannot hibernate, fork, checkpoint, or promote")
	ErrVolumeBusy        = errors.New("volume is held by another claim")
	// Replaying a journal takes a writable mount, so readers stay out.
	ErrVolumeNeedsRecovery = errors.New("volume needs recovery by a writable claim")
	ErrArchived            = errors.New("sandbox is archived; an exec or file call wakes it first")
	ErrLeaseLost           = errors.New("node lease lost; retry")
	ErrPaused              = errors.New("sandbox is paused; an operator call does not wake it")
	ErrFailed              = errors.New("sandbox failed: its VMM is down; release it, or wake it to cold-boot its disk")
	ErrNoInstanceMetadata  = engine.ErrNoInstanceMetadata
	ErrQuota               = errors.New("node claim quota reached")
	ErrSleeping            = errors.New("sandbox is hibernated or archived and a reader may serve it")
	ErrNoReader            = errors.New("no reader for the sandbox")

	errNotSleeping   = errors.New("sandbox is no longer hibernated or archived")
	errWokeMeanwhile = errors.New("woke between sweep and hibernate")
	errNoEgressTap   = errors.New("egress-lane claim has no lockable tap")
)

// Engine is the slice of the cocoon driver the manager consumes.
type Engine interface {
	Clone(ctx context.Context, fromDir, name string, key types.PoolKey) (types.VMRecord, error)
	CloneSnap(ctx context.Context, snap, name string, key types.PoolKey) (types.VMRecord, error)
	RunCold(ctx context.Context, name string, key types.PoolKey, storage string) (types.VMRecord, error)
	Remove(ctx context.Context, name string) error
	Start(ctx context.Context, name string) error
	Stop(ctx context.Context, name string) error
	ReconcileStaleCreate(ctx context.Context, name string) (engine.StaleCreateOutcome, error)
	SnapshotSave(ctx context.Context, vmName, snapName string) error
	SnapshotExport(ctx context.Context, snapName, toDir string) error
	SnapshotRemove(ctx context.Context, snapName string) error
	SnapshotList(ctx context.Context) ([]string, error)
	ImageIDs(ctx context.Context) (map[string]string, error)
	Hibernate(ctx context.Context, vmName, snapName string) error
	Restore(ctx context.Context, vmName, snapRef string) (string, error)
	List(ctx context.Context) ([]types.VMRecord, error)
	Inspect(ctx context.Context, name string) (types.VMRecord, bool, error)
	VMEvents(ctx context.Context, socket string, onSync engine.VMSyncFunc, onChange engine.VMChangeFunc) error
	Probe(ctx context.Context, vsockSocket string, timeout time.Duration) error
	DialGuestPort(ctx context.Context, vsockSocket string, port uint16) (net.Conn, error)
	InstallCACert(ctx context.Context, vsockSocket string, certPEM []byte) error
	MarkLane(ctx context.Context, vsockSocket string, lane types.NetRoute) error
	Warmup(ctx context.Context, vsockSocket string, argv []string) error
	TrimCow(ctx context.Context, vsockSocket string) error
	CheckLiveVolumeSupport(ctx context.Context) error
	DiskAttach(ctx context.Context, vmName string, spec engine.VolumeSpec) error
	DetachVolume(ctx context.Context, vmName, sock, name, mount string) error
	MountVolume(ctx context.Context, vsockSocket, name, mount string, rw bool) error
	MountLiveVolume(ctx context.Context, vsockSocket, name, mount string, rw bool) error
	UnmountVolume(ctx context.Context, vsockSocket, mount string) error
	SyncGuest(ctx context.Context, vsockSocket string) error
	DiskAttachSnapshot(ctx context.Context, vmName, snapName string) (string, error)
	DiskAttachDir(ctx context.Context, vmName, dir string) (string, error)
	StartReader(ctx context.Context, vsockSocket, cowDisk string) error
	DialReader(ctx context.Context, vsockSocket string) (net.Conn, error)
	WriteInstanceMetadata(ctx context.Context, vsockSocket string, doc []byte) error
	WriteGuestEnv(ctx context.Context, vsockSocket string, doc []byte) error
}

type VolumeMountConflict = engine.VolumeMountConflict

type LiveVolumeUnsupported = engine.LiveVolumeUnsupported

// SandboxSummary is the ops view of one live claim.
type SandboxSummary struct {
	ID     string        `json:"id"`
	Key    types.PoolKey `json:"key"`
	Tenant string        `json:"tenant,omitempty"`
	// Token is the sandbox's own bearer token; only the root by-id read carries it.
	Token string `json:"token,omitempty"`
	// ClaimRef echoes the caller reference; empty for checkpoint branches and unprefixed forks.
	ClaimRef       string             `json:"claim_ref,omitempty"`
	Metadata       types.Metadata     `json:"metadata,omitempty"`
	OnExpire       types.ExpireAction `json:"on_expire,omitempty"`
	CPUCount       int                `json:"cpu_count,omitzero"`
	MemTotalBytes  int64              `json:"mem_total_bytes,omitzero"`
	Volumes        []types.Volume     `json:"volumes,omitempty"`
	VolumesWant    []types.Volume     `json:"volumes_want,omitempty"`
	Hibernated     bool               `json:"hibernated"`
	Archived       bool               `json:"archived,omitzero"`
	FromCheckpoint string             `json:"from_checkpoint,omitempty"`
	Restarts       int                `json:"restarts,omitzero"`
	RestartedAt    time.Time          `json:"restarted_at,omitzero"`
	Failed         string             `json:"failed,omitempty"`
	ClaimedAt      time.Time          `json:"claimed_at,omitzero"`
	Deadline       time.Time          `json:"deadline"`
}

// PoolInfo is the ops view of one pool.
type PoolInfo struct {
	Key       types.PoolKey `json:"key"`
	Warm      int           `json:"warm"`
	Refilling int           `json:"refilling"`
	Target    int           `json:"target"`
	Golden    bool          `json:"golden"`
}

// Gauges are the manager's point-in-time claim counts and drain state.
type Gauges struct {
	Claimed    int
	Hibernated int
	Archived   int
	Failed     int
	Draining   bool
	// AtCapacity marks refill parked because the node refused another VM.
	AtCapacity       bool
	AtCapacityReason string
}

type pendingRemoval struct {
	sandboxID   string
	tap         string
	staleCreate bool
	volumes     volumeTeardown
	notBefore   time.Time
}

type pool struct {
	key  types.PoolKey
	hash string

	// floor and warmMax bound the demand-adaptive target computed in watermark.go
	floor           int
	warmMax         int
	idle            time.Duration
	archiveAfter    time.Duration
	archiveDelete   time.Duration
	rate            float64
	lead            time.Duration
	lastArrival     time.Time
	binStart        time.Time
	binCount        int
	overTargetSince time.Time

	goldenDir string
	imageID   string
	// goldenGen counts installs, so a clone of a retired golden never lands after a rebuild at the same path.
	goldenGen   uint64
	building    bool
	nextBuild   time.Time
	removed     bool // dropped from the desired set while unsettled; swept by refillOnce
	warm        []*types.Sandbox
	refilling   int
	claimClones atomic.Int32

	// Without a streak gate a node-wide dead cause is retried every tick at full concurrency
	refillFails int
	nextRefill  time.Time
}

func newPool(key types.PoolKey) *pool {
	return &pool{key: key, hash: key.Hash()}
}

func (p *pool) settled() bool {
	return !p.building && p.refilling == 0 && p.claimClones.Load() == 0
}

// refillGated reports whether a refill may not spawn: backoff wait, or one probe in flight.
func (p *pool) refillGated(now time.Time) bool {
	return now.Before(p.nextRefill) || (p.refillFails >= refillFailStreak && p.refilling > 0)
}

// noteRefillResult records one refill outcome and reports whether it started a backoff.
func (p *pool) noteRefillResult(now time.Time, failed bool) bool {
	if !failed {
		p.refillFails, p.nextRefill = 0, time.Time{}
		return false
	}
	p.refillFails++
	if p.refillFails < refillFailStreak {
		return false
	}
	first := !now.Before(p.nextRefill)
	backoff := refillBackoffBase << min(p.refillFails-refillFailStreak, 16)
	p.nextRefill = now.Add(min(backoff, refillBackoffMax))
	return first
}

func (p *pool) applySpec(spec config.PoolSpec) {
	p.floor = spec.Warm
	p.warmMax = spec.WarmMax
	p.idle = time.Duration(spec.IdleHibernateSeconds) * time.Second
	p.archiveAfter = time.Duration(spec.ArchiveAfterSeconds) * time.Second
	p.archiveDelete = time.Duration(spec.ArchiveDeleteAfterSeconds) * time.Second
}

func (p *pool) popWarm() *types.Sandbox {
	n := len(p.warm)
	if n == 0 {
		return nil
	}
	sb := p.warm[n-1]
	p.warm = p.warm[:n-1]
	return sb
}

// trimWarm shrinks p.warm to at most target and returns the trimmed VM names; callers hold m.mu.
func (p *pool) trimWarm(target int) []string {
	var trim []string
	for len(p.warm) > target {
		n := len(p.warm) - 1
		trim = append(trim, p.warm[n].VMName)
		p.warm = p.warm[:n]
	}
	return trim
}

type Backends struct {
	Checkpoints       store.Store
	Templates         store.Store
	SharedCheckpoints bool
	Tenants           tenants.Source
	CellPools         poolset.Shared
	Archives          archives.Registry
}

// PeerDeleteFunc broadcasts a checkpoint delete to every peer.
type PeerDeleteFunc func(ctx context.Context, id string)

// Manager owns the node's pools, claims, and their persistence.
type Manager struct {
	eng     Engine
	dataDir string
	egress  bool
	maxFork int
	store   *claimStore
	volumes map[string]catalogVolume
	// volumeAdmission counts the live holders of each volume name; guarded by m.mu.
	volumeAdmission map[string]volumeHolders
	managed         *volumeRegistry
	volumeRuns      map[*types.Sandbox]*volumeRun
	volumeErrors    map[*types.Sandbox]map[string]string

	poolStore *poolStore
	// poolShared is the cell's pool set with a meta store; poolVersion, under m.mu, is its newest version applied.
	poolShared     poolset.Shared
	poolVersion    int64
	configSeedHash string // config pools' hash, to warn when a file edit is overridden

	// idleDefault is the idle-hibernate threshold for unpooled keys; zero disables.
	idleDefault time.Duration
	idleSweep   atomic.Bool

	vmmRestart    types.VMMRestart
	livenessSweep atomic.Bool
	cocoondSocket string
	// vmmEvents is set while the cocoon daemon stream is live; the poll is then only a backstop.
	vmmEvents  atomic.Bool
	lastVMPoll atomic.Int64

	// archive*Default are the archive thresholds for unpooled keys.
	archiveAfterDefault  time.Duration
	archiveDeleteDefault time.Duration
	archiveSweep         atomic.Bool
	archiveDeleteSweep   atomic.Bool
	// archiving holds ids with an export or a registry drop in flight; pendingCks pins ids mid-commit; both under m.mu.
	archiving  map[string]struct{}
	pendingCks map[string]struct{}
	// archives is the cluster archive registry, nil unless archive_failover is set.
	archives     archives.Registry
	nodeID       string
	orphanSweep  atomic.Bool
	adoptFlights singleflight.Group
	// fence gates replicated claims while the lease is overdue; replicaPools is fixed at start; replicas belongs to the pass holding replicating.
	lease        leaseTiming
	fence        atomic.Pointer[fenceState]
	unfencing    atomic.Bool
	replicaPools map[types.PoolKey]struct{}
	replicating  atomic.Bool
	replicas     map[string]replicaState

	view atomic.Pointer[configView]
	// cfg is the config the view was built from; reloadMu orders reloads, the only writers of cfg and view.
	cfg      *config.Config
	reloadMu sync.Mutex

	maxClaims    int
	releaseDelay time.Duration
	draining     bool // guarded by m.mu; deliberately not persisted

	tenants    tenants.Source
	rootSum    string
	onTenants  func()
	tenantLive map[string]int
	usage      *journal
	audit      *journal
	counters   counters
	ckpts      store.Store
	// A cluster-wide backend makes heal and the delete broadcast no-ops
	ckptsShared bool
	healer      *peer.Healer
	healSem     chan struct{}
	healFlights singleflight.Group
	// healPending/healAbort, guarded by healMu, let a delete veto a heal still staging.
	healMu       sync.Mutex
	healPending  map[string]struct{}
	healAbort    map[string]struct{}
	peerDelete   PeerDeleteFunc
	tpls         store.Store
	ckptTTL      time.Duration
	ckptSweeping atomic.Bool
	genSweeping  atomic.Bool

	// tplSet caches each template id's key, digest and owning tenant ("" = operator).
	tplMu  sync.Mutex
	tplSet map[string]TemplateInfo

	// recLocks serializes same-id record mutations; an entry lives exactly as long as a recLock reference to it.
	recLocks   map[string]*sync.RWMutex
	recLocksMu sync.Mutex
	recRefs    map[string]int

	// notifyTemplates is set before serving starts and fires after a promote or template delete.
	notifyTemplates func()

	out *outbound.Host

	readers *readerSet

	mu              sync.Mutex
	pools           map[types.PoolKey]*pool
	claimed         map[string]*types.Sandbox
	pendingRemovals map[string]pendingRemoval

	// Node-wide, unlike the per-pool streak backoff: the exhausted resource is shared
	atCapacityUntil  time.Time
	atCapacityReason string

	diskLow        atomic.Bool
	diskStatFailed atomic.Bool

	refillSem  chan struct{}
	maintSem   chan struct{}
	probeSem   chan struct{}
	refillKick chan struct{}
}

// NewManager builds a manager from the node config over backends the caller selected from it.
func NewManager(ctx context.Context, cfg *config.Config, eng Engine, secrets *egress.SecretStore, b Backends) (*Manager, error) {
	if err := cfg.ValidateManagedVolumeStore(); err != nil {
		return nil, err
	}
	refill := cmp.Or(cfg.RefillConcurrency, defaultRefill)
	m := &Manager{
		eng:             eng,
		dataDir:         cfg.DataDir,
		egress:          cfg.HasEgress(),
		releaseDelay:    time.Duration(cfg.ReleaseDelaySeconds) * time.Second,
		vmmRestart:      cfg.VMMRestart,
		cocoondSocket:   cfg.CocoondSocket,
		maxFork:         cmp.Or(cfg.MaxForkCount, defaultMaxFork),
		store:           newClaimStore(cfg.DataDir, cfg.SyncClaims),
		volumes:         make(map[string]catalogVolume, len(cfg.Volumes)),
		volumeAdmission: map[string]volumeHolders{},
		volumeRuns:      map[*types.Sandbox]*volumeRun{},
		volumeErrors:    map[*types.Sandbox]map[string]string{},
		poolStore:       newPoolStore(cfg.DataDir),
		poolShared:      b.CellPools,
		archives:        b.Archives,
		lease:           leaseTiming{ttl: leaseTTL, every: leaseRenew, margin: leaseMargin},
		tenants:         b.Tenants,
		ckpts:           b.Checkpoints,
		ckptsShared:     b.SharedCheckpoints,
		tpls:            b.Templates,
		pools:           make(map[types.PoolKey]*pool, len(cfg.Pools)),
		claimed:         map[string]*types.Sandbox{},
		pendingRemovals: map[string]pendingRemoval{},
		tenantLive:      map[string]int{},
		readers:         &readerSet{live: map[string]*reader{}},
		archiving:       map[string]struct{}{},
		pendingCks:      map[string]struct{}{},
		recLocks:        map[string]*sync.RWMutex{},
		recRefs:         map[string]int{},
		healPending:     map[string]struct{}{},
		healAbort:       map[string]struct{}{},
		refillSem:       make(chan struct{}, refill),
		maintSem:        make(chan struct{}, max(1, refill-max(1, refill/4))),
		probeSem:        make(chan struct{}, refill),
		refillKick:      make(chan struct{}, 1),
		healSem:         make(chan struct{}, maxConcurrentHeals),
	}
	for _, volume := range cfg.Volumes {
		m.volumes[volume.Name] = catalogVolume{
			disk:     engine.VolumeSpec{Name: volume.Name, Path: volume.Path, DirectIO: volume.DirectIO},
			tenants:  slices.Clone(volume.Tenants),
			writable: volume.Writable,
		}
	}
	if err := os.MkdirAll(m.goldensDir(), 0o750); err != nil {
		return nil, fmt.Errorf("create goldens dir: %w", err)
	}
	m.ckptTTL = time.Duration(cfg.CheckpointTTLHours) * time.Hour
	m.tplSet = map[string]TemplateInfo{}
	metas, listErr := m.tpls.Metas(ctx)
	if listErr != nil {
		log.WithFunc("pool.NewManager").Warnf(ctx, "list templates skipped: %v", listErr)
	}
	for _, r := range metas {
		var rec templateRecord
		if json.Unmarshal(r.Meta, &rec) == nil && rec.ID != "" {
			info := TemplateInfo{Key: rec.Key, ContentDigest: r.Digest, Tenant: rec.Tenant, CreatedAt: rec.CreatedAt}
			_ = json.Unmarshal(r.Labels, &info.Labels)
			m.tplSet[rec.ID] = info
		}
	}
	usage, err := newJournal(filepath.Join(cfg.DataDir, "usage.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("open usage journal: %w", err)
	}
	m.usage = usage
	if cfg.AuditLog {
		audit, err := newJournal(filepath.Join(cfg.DataDir, "audit.jsonl"))
		if err != nil {
			return nil, fmt.Errorf("open audit journal: %w", err)
		}
		m.audit = audit
	}
	m.maxClaims = cfg.MaxClaims
	if cfg.APIToken != "" {
		m.rootSum = tenants.TokenSHA256(cfg.APIToken)
	}
	m.cfg = cfg
	m.view.Store(newConfigView(cfg, secrets))
	m.idleDefault = time.Duration(cfg.IdleHibernateSeconds) * time.Second
	m.archiveAfterDefault = time.Duration(cfg.ArchiveAfterSeconds) * time.Second
	m.archiveDeleteDefault = time.Duration(cfg.ArchiveDeleteAfterSeconds) * time.Second
	for _, spec := range cfg.Pools {
		p := newPool(spec.PoolKey)
		p.applySpec(spec)
		m.pools[spec.PoolKey] = p
	}
	ca, caErr := outbound.LoadCA(cfg)
	if caErr != nil {
		return nil, fmt.Errorf("load egress ca: %w", caErr)
	}
	m.out = outbound.New(m.outboundOptions(ca))
	m.configSeedHash = poolSeedHash(cfg.Pools)
	start := &fenceState{ch: make(chan struct{}), fenced: m.archives != nil}
	if m.archives != nil {
		m.nodeID = cfg.Mesh.ID()
		close(start.ch)
		m.replicaPools = map[types.PoolKey]struct{}{}
		for _, spec := range cfg.Pools {
			if spec.ReplicateSeconds > 0 {
				m.replicaPools[spec.PoolKey] = struct{}{}
			}
		}
	}
	m.fence.Store(start)
	if err := m.adoptPools(ctx); err != nil {
		return nil, err
	}
	m.warnVolumeTenants(ctx, cfg.Volumes)
	m.warnMissingClasses(ctx)
	if cfg.ManagedVolumeDir != "" {
		node, hostErr := os.Hostname()
		if cfg.Mesh != nil {
			node = cfg.Mesh.ID()
		}
		if node == "" {
			return nil, fmt.Errorf("managed volume node identity unavailable: %v", hostErr)
		}
		var err error
		m.managed, err = openVolumeRegistry(cfg.ManagedVolumeDir, node)
		if err != nil {
			return nil, err
		}
		for name, entry := range m.volumes {
			_, collision := m.managed.data.Volumes[name]
			if collision || m.managedCatalogAlias(entry.disk.Path) {
				m.managed.close()
				return nil, fmt.Errorf("catalog conflicts with existing managed resource")
			}
		}
	}
	return m, nil
}

// ClusterDigest fingerprints this node's must-match config as last loaded, with its egress root and tenant set.
func (m *Manager) ClusterDigest() string {
	m.reloadMu.Lock()
	cfg := m.cfg
	m.reloadMu.Unlock()
	return clusterDigest(cfg, m.out.CAFingerprint(), m.TenantRecords())
}

// Run drives the refill and reap loops until ctx is canceled.
func (m *Manager) Run(ctx context.Context) {
	refill := time.NewTicker(refillInterval)
	defer refill.Stop()
	reap := time.NewTicker(reapInterval)
	defer reap.Stop()
	liveness := time.NewTicker(livenessInterval)
	defer liveness.Stop()
	images := time.NewTicker(imageInterval)
	defer images.Stop()
	// Store retention is hourly: each sweep is cluster-visible I/O on a shared root
	storeSweep := time.NewTicker(time.Hour)
	defer storeSweep.Stop()
	go m.sweepExpiredCheckpoints(ctx)
	if m.cocoondSocket != "" {
		go m.watchVMMs(ctx)
	}
	if m.poolShared != nil {
		go m.followCellPools(ctx)
	}
	if m.archives != nil {
		go m.keepLease(ctx)
	}
	m.livenessOnce(ctx)
	m.checkDisk(ctx)
	m.refillOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-refill.C:
			if m.view.Load().out.Guarded() {
				m.out.RefreshSelf()
			}
			m.checkDisk(ctx)
			m.refillOnce(ctx)
		case <-m.refillKick:
			m.refillOnce(ctx)
		case <-reap.C:
			m.retryRemovals(ctx)
			m.reapOnce(ctx)
			m.idleOnce(ctx)
			m.archiveOnce(ctx)
			m.replicateOnce(ctx)
			m.shrinkOnce(ctx)
			go m.retryArchiveDeletes(ctx)
		case <-liveness.C:
			m.livenessOnce(ctx)
		case <-images.C:
			go m.checkImagesOnce(ctx)
		case <-storeSweep.C:
			go m.sweepStoreGenerations(ctx)
			go m.sweepExpiredCheckpoints(ctx)
			if m.archives != nil {
				go m.sweepOrphanArchives(ctx)
			}
		}
	}
}

// FlushClaims synchronously persists the current claim set at shutdown.
func (m *Manager) FlushClaims() error {
	return m.store.commit(m.store.mark())
}

// SetTemplateNotifier wires the immediate-republish hook; call it before serving starts.
func (m *Manager) SetTemplateNotifier(fn func()) {
	m.notifyTemplates = fn
}

// Info reports pool states (sorted for stable output) and the claim gauges.
func (m *Manager) Info() ([]PoolInfo, Gauges) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	pools := make([]PoolInfo, 0, len(m.pools))
	for _, p := range slices.SortedFunc(maps.Values(m.pools), func(a, b *pool) int { return strings.Compare(a.hash, b.hash) }) {
		if p.removed {
			continue
		}
		pools = append(pools, PoolInfo{
			Key:       p.key,
			Warm:      len(p.warm),
			Refilling: p.refilling,
			Target:    p.effectiveTarget(now),
			Golden:    p.goldenDir != "",
		})
	}
	g := Gauges{Claimed: len(m.claimed), Draining: m.draining}
	g.AtCapacityReason = m.capacityReasonLocked(now)
	g.AtCapacity = g.AtCapacityReason != ""
	for _, sb := range m.claimed {
		if sb.HibernateSnap != "" {
			g.Hibernated++
		}
		if sb.ArchiveCk != "" {
			g.Archived++
		}
		if sb.Failed != "" {
			g.Failed++
		}
	}
	return pools, g
}

// Sandboxes lists live claims visible to tenant (empty means root), narrowed to claimRef when set and to claims holding every metadata pair.
func (m *Manager) Sandboxes(tenant, claimRef string, metadata types.Metadata) []SandboxSummary {
	m.mu.Lock()
	size := len(m.claimed)
	if claimRef != "" {
		size = 1
	}
	out := make([]SandboxSummary, 0, size)
	for _, sb := range m.claimed {
		if !tenantOwns(tenant, sb.Tenant) || (claimRef != "" && sb.ClaimRef != claimRef) || !sb.Metadata.Matches(metadata) {
			continue
		}
		out = append(out, summarize(sb))
	}
	m.mu.Unlock()
	slices.SortFunc(out, func(a, b SandboxSummary) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// WarmCounts is the per-pool-key-hash warm count, for gossiping placement.
func (m *Manager) WarmCounts() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	counts := make(map[string]int, len(m.pools))
	low := m.diskLow.Load()
	for _, p := range m.pools {
		if !p.removed {
			counts[p.hash] = len(p.warm)
			if low {
				counts[p.hash] = 0
			}
		}
	}
	return counts
}

// WithPeerHeal installs the healer ClaimCheckpointHeal pulls through.
func (m *Manager) WithPeerHeal(enabled bool, owners peer.Owners, puller peer.Puller) {
	if !enabled || owners == nil || m.ckptsShared {
		return
	}
	m.healer = peer.NewHealer(owners, puller)
}

// WithPeerDelete wires the broadcast DeleteCheckpoint makes after a local delete.
func (m *Manager) WithPeerDelete(fn PeerDeleteFunc) {
	m.peerDelete = fn
}

func (m *Manager) sweepStoreGenerations(ctx context.Context) {
	if !m.genSweeping.CompareAndSwap(false, true) {
		return
	}
	defer m.genSweeping.Store(false)
	logger := log.WithFunc("pool.sweepStoreGenerations")
	if err := m.ckpts.SweepGenerations(ctx); err != nil {
		logger.Error(ctx, err, "sweep checkpoint generations")
	}
	if m.ckptsShared {
		return
	}
	if err := m.tpls.SweepGenerations(ctx); err != nil {
		logger.Error(ctx, err, "sweep template generations")
	}
}

func (m *Manager) untrack(set map[string]struct{}, key string) {
	m.mu.Lock()
	delete(set, key)
	m.mu.Unlock()
}

// activePool looks up key treating a removed pool as absent; callers hold m.mu.
func (m *Manager) activePool(key types.PoolKey) (*pool, bool) {
	p, ok := m.pools[key]
	return p, ok && !p.removed
}

func (m *Manager) validate(key types.PoolKey) error {
	if err := key.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrBadKey, err)
	}
	if key.Net == types.NetEgress && !m.egress {
		return ErrNoEgress
	}
	return nil
}

func (m *Manager) goldensDir() string {
	return filepath.Join(m.dataDir, "goldens")
}

func summarize(sb *types.Sandbox) SandboxSummary {
	spec, _ := sb.Key.Size.Spec()
	return SandboxSummary{
		ID: sb.ID, Key: sb.Key, Tenant: sb.Tenant, Deadline: sb.Deadline, ClaimedAt: sb.ClaimedAt,
		Hibernated: sb.HibernateSnap != "", Archived: sb.ArchiveCk != "",
		FromCheckpoint: sb.FromCheckpoint, ClaimRef: sb.ClaimRef, Metadata: sb.Metadata, OnExpire: sb.OnExpire,
		Restarts: sb.Restarts, RestartedAt: sb.RestartedAt, Failed: sb.Failed,
		CPUCount: spec.CPU, MemTotalBytes: spec.MemoryBytes,
		Volumes: slices.Clone(sb.Volumes), VolumesWant: slices.Clone(sb.VolumesWant),
	}
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// tenantOwns is the tenancy predicate: root (empty tenant) owns everything, a tenant its own.
func tenantOwns(tenant, owner string) bool {
	return tenant == "" || tenant == owner
}

func hasVolumes(sb *types.Sandbox) bool {
	return len(sb.Volumes) > 0 || len(sb.VolumesWant) > 0
}

// logSweepResult reports one background-sweep outcome; benign races stay silent.
func logSweepResult(ctx context.Context, logger *log.Fields, err error, okMsg, failMsg string) {
	switch {
	case err == nil:
		logger.Info(ctx, okMsg)
	case !benignSweepErr(err):
		logger.Error(ctx, err, failMsg)
	}
}

func benignSweepErr(err error) bool {
	return errors.Is(err, ErrUnknownSandbox) || errors.Is(err, errWokeMeanwhile) ||
		errors.Is(err, ErrNoEgressHibernate) || errors.Is(err, ErrQuota)
}

func vmName(key types.PoolKey) string {
	return vmPrefix + key.Hash() + "-" + randHex(6)
}

// lockedVMName reads the VM name under the lock that archive() clears it under.
func lockedVMName(sb *types.Sandbox) string {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	return sb.VMName
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b) // never fails per crypto/rand contract
	return hex.EncodeToString(b)
}
