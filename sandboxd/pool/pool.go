// Package pool owns a node's warm pools and claimed sandboxes; refill keeps pools claim-ready, so a claim is ownership transfer only.
package pool

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/netip"
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
	"github.com/cocoonstack/sandbox/sandboxd/moves"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/poolset"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/peer"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/tunnet"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

const (
	refillInterval    = 2 * time.Second
	reapInterval      = 5 * time.Second
	livenessInterval  = 10 * time.Second
	imageInterval     = time.Minute
	heldFenceInterval = time.Minute
	buildRetryDelay   = 30 * time.Second
	archiveRetryDelay = time.Minute
	claimProbeTimeout = 15 * time.Second
	wakeProbeTimeout  = 60 * time.Second
	coldProbeTimeout  = 90 * time.Second
	// A clone answers in ~1s even saturated, so a silent one is replaced, not waited on
	warmProbeTimeout = 5 * time.Second
	// One inspect; a wrong answer only costs an extra sweep.
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
	leaseWindow = leaseTTL - leaseMargin
	adoptGrace  = leaseRenew + leaseMargin
	// One failed boot is ordinary; only an unbroken run predicts the next failure
	refillFailStreak  = 8
	refillBackoffBase = 250 * time.Millisecond
	refillBackoffMax  = buildRetryDelay
	// Fallback when a Manager is built from a Config that skipped config.Load's defaulting
	defaultRefill = 4

	defaultMoveConcurrency = 2

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
	ErrTunOffPromote     = errors.New("a sandbox whose tun is turned off cannot be promoted: its template's claims would get tun back")
	ErrVolumeCapture     = errors.New("sandboxes with volumes cannot hibernate, fork, checkpoint, or promote")
	ErrVolumeBusy        = errors.New("volume is held by another claim")

	ErrVolumeRegistrationConflict = errors.New("volume registration conflicts with the catalog")
	ErrVolumeCatalogUnavailable   = errors.New("volume catalog requires a successful persistence retry")

	// Replaying a journal takes a writable mount, so readers stay out.
	ErrVolumeNeedsRecovery = errors.New("volume needs recovery by a writable claim")
	ErrArchived            = errors.New("sandbox is archived; a wake or an exec wakes it first")
	ErrLeaseLost           = errors.New("node lease lost; retry")
	ErrPaused              = errors.New("sandbox is paused; an operator call does not wake it")
	ErrEvacuating          = errors.New("sandbox is moving to another node; retry")
	ErrFailed              = errors.New("sandbox failed: its VMM is down; release it, or wake it to cold-boot its disk")
	ErrNoInstanceMetadata  = engine.ErrNoInstanceMetadata
	ErrQuota               = errors.New("admission refused")
	ErrSleeping            = errors.New("sandbox is hibernated or archived and a reader may serve it")
	ErrNoReader            = errors.New("no reader for the sandbox")
	ErrNotMovable          = errors.New("sandbox cannot move")
	ErrMoveTarget          = errors.New("move target failed")
	ErrMoveDowntime        = errors.New("the final round would exceed max_downtime_ms")
	ErrMoveStoreDown       = moves.ErrUnavailable
	ErrBalloonOff          = errors.New("cocoon on this node has no vm balloon")
	ErrNoBalloon           = errors.New("the sandbox's VM has no balloon device")
	ErrBalloonTarget       = engine.ErrBalloonTarget

	errNotSleeping   = errors.New("sandbox is no longer hibernated or archived")
	errWokeMeanwhile = errors.New("woke between sweep and hibernate")
	errNoEgressTap   = errors.New("egress-lane claim has no lockable tap")

	errFencedReplicating = fmt.Errorf("%w: node lease lost", ErrQuota)
	errMemoryLow         = fmt.Errorf("%w: %s", ErrQuota, memoryLowReason)
	errNoFailover        = errors.New("archive_failover is off, so no peer can adopt the claim")
	errNoArchiveRow      = errors.New("archived before archive_failover was on, so no registry row names it")
	errArchiveBusy       = errors.New("an archive of the claim is in flight")
	errNoAdopter         = errors.New("no peer adopted the claim")
	errMovesOff          = fmt.Errorf("%w: live moves need a meta store cell and an encrypted mesh", ErrNotMovable)
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
	WaitCapture(ctx context.Context, snap string) error
	DialGuestPort(ctx context.Context, vsockSocket string, port uint16) (net.Conn, error)
	InstallCACert(ctx context.Context, vsockSocket string, certPEM []byte) error
	MarkLane(ctx context.Context, vsockSocket string, lane types.NetRoute) error
	TunUp(ctx context.Context, vsockSocket string) error
	TunDown(ctx context.Context, vsockSocket string) error
	Warmup(ctx context.Context, vsockSocket string, argv []string) error
	SettleGuest(ctx context.Context, vsockSocket string) error
	TrimCow(ctx context.Context, vsockSocket string) error
	CheckLiveVolumeSupport(ctx context.Context) error
	DiskAttach(ctx context.Context, vmName string, spec engine.VolumeSpec) error
	DetachVolume(ctx context.Context, vmName, sock, name, mount string) (cleanRW bool, err error)
	MountVolume(ctx context.Context, vsockSocket, name, mount string, rw bool) error
	MountLiveVolume(ctx context.Context, vsockSocket, name, mount string, rw bool) error
	UnmountVolume(ctx context.Context, vsockSocket, mount string) (bool, error)
	SyncGuest(ctx context.Context, vsockSocket string) error
	DiskAttachSnapshot(ctx context.Context, vmName, snapName string) (string, error)
	DiskAttachDir(ctx context.Context, vmName, dir string) (string, error)
	StartReader(ctx context.Context, vsockSocket, cowDisk string) error
	DialReader(ctx context.Context, vsockSocket string) (net.Conn, error)
	WriteInstanceMetadata(ctx context.Context, vsockSocket string, doc []byte) error
	WriteGuestEnv(ctx context.Context, vsockSocket string, doc []byte) error
	CheckMigrateSupport(ctx context.Context) error
	CheckBalloonSupport(ctx context.Context) error
	Balloon(ctx context.Context, vmName, target string) (types.VMBalloon, error)
	MigrateCapture(ctx context.Context, vmName, series string, final bool) (types.MigrateRound, error)
	MigrateReceive(ctx context.Context, fromDir, name string, key types.PoolKey) (types.VMRecord, error)
	MigrateAbort(ctx context.Context, vmName, series string) error
}

type VolumeMountConflict = engine.VolumeMountConflict

type LiveVolumeUnsupported = engine.LiveVolumeUnsupported

// SandboxSummary is the ops view of one live claim.
type SandboxSummary struct {
	ID     string        `json:"id"`
	Key    types.PoolKey `json:"key"`
	Tenant string        `json:"tenant,omitempty"`
	// Token is the sandbox's own bearer token; only the root by-id read carries it, like the tun fields.
	Token      string     `json:"token,omitempty"`
	TunAddr    netip.Addr `json:"tun_addr,omitzero"`
	TunOff     bool       `json:"tun_off,omitzero"`
	TunProfile string     `json:"tun_profile,omitempty"`
	// EgressLimit is the claim's bandwidth cap; only the root by-id read carries it.
	EgressLimit types.EgressLimit `json:"egress_limit,omitzero"`
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
	// EnvMissing reports a host-only env entry whose value the node no longer holds.
	EnvMissing bool `json:"env_missing,omitzero"`
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
	Running    int
	MaxRunning int
	Hibernated int
	Archived   int
	Failed     int
	Draining   bool
	Cordoned   bool
	// Evacuating counts the claims an evacuation has yet to move.
	Evacuating int
	// AtCapacity marks refill parked by a refused VM or a low disk or memory reserve.
	AtCapacity       bool
	AtCapacityReason string
	MemoryLow        bool
	AnonBytes        int
}

type Backends struct {
	Checkpoints       store.Store
	Templates         store.Store
	SharedCheckpoints bool
	Tenants           tenants.Source
	CellPools         poolset.Shared
	Archives          archives.Registry
	Moves             moves.Registry
	// TunRouter and TunAddrs carry the advanced network mode; both nil when the node has no tun block.
	TunRouter *tunnet.Router
	TunAddrs  tunnet.Addresses
}

// PeerDeleteFunc broadcasts a checkpoint delete to every peer.
type PeerDeleteFunc func(ctx context.Context, id string)

// PeerAdoptFunc asks the peers to adopt a released claim and reports whether one holds it now; an empty token asks as the root.
type PeerAdoptFunc func(ctx context.Context, id, token string, room bool) bool

type pendingRemoval struct {
	sandboxID   string
	tap         string
	staleCreate bool
	volumes     volumeTeardown
	notBefore   time.Time
}

// Manager owns the node's pools, claims, and their persistence.
type Manager struct {
	eng          Engine
	dataDir      string
	egress       bool
	store        *claimStore
	catalog      volumeCatalog
	volumeRuns   map[*types.Sandbox]*volumeRun
	volumeErrors map[*types.Sandbox]map[string]string

	poolStore *poolStore
	// poolShared is the cell's pool set with a meta store; poolVersion, under m.mu, is its newest version applied.
	poolShared     poolset.Shared
	poolVersion    int64
	configSeedHash string // config pools' hash, to warn when a file edit is overridden

	sweeps    struct{ idle, liveness, archive, archiveDelete, orphan, held, ckpt, gen atomic.Bool }
	balloonOK atomic.Bool

	cocoondSocket string
	// vmmEvents is set while the cocoon daemon stream is live; the poll is then only a backstop.
	vmmEvents  atomic.Bool
	lastVMPoll atomic.Int64

	// archiving holds ids with an export or a registry drop in flight; pendingCks pins ids mid-commit; both under m.mu.
	archiving  map[string]struct{}
	pendingCks map[string]struct{}
	// archives is the cluster archive registry, nil unless archive_failover is set.
	archives     archives.Registry
	nodeID       string
	adoptFlights singleflight.Group
	// fence gates replicated claims while the lease is overdue; replicas belongs to the pass holding replicating.
	fence       atomic.Pointer[fenceState]
	unfencing   atomic.Bool
	replicating atomic.Bool
	replicas    map[string]time.Time
	// moves is the cluster move registry, nil unless the node can run live moves.
	moves    moves.Registry
	movePeer MovePeer

	view atomic.Pointer[configView]
	// cfg is the config the view was built from; reloadMu orders reloads, the only writers of cfg and view.
	cfg      *config.Config
	reloadMu sync.Mutex

	draining bool // guarded by m.mu; deliberately not persisted
	evac     *evacuation
	// cordoned, under m.mu, mirrors the cordon file; cordonMu orders its writers.
	cordoned bool
	cordonMu sync.Mutex

	tenants    tenants.Source
	reserved   tenants.Reserved
	onTenants  func()
	tenantLive map[string]int
	usage      *journal
	audit      *journal
	counters   counters
	ckpts      store.Store
	// A cluster-wide backend makes heal and the delete broadcast no-ops
	ckptsShared       bool
	healer            *peer.Healer
	healSem           chan struct{}
	healFlights       singleflight.Group
	heals             healVetoes
	peerDelete        PeerDeleteFunc
	peerAdopt         PeerAdoptFunc
	handOffQuietUntil atomic.Int64
	handOffBusy       atomic.Bool
	tpls              store.Store

	// tplSet caches each template id's key, digest and owning tenant ("" = operator).
	tplMu  sync.Mutex
	tplSet map[string]TemplateInfo

	recs recordLocks

	ckMarks ckMarkers

	// notifyTemplates is set before serving starts and fires after a promote or template delete.
	notifyTemplates func()

	out       *outbound.Host
	tunRouter *tunnet.Router
	tunAddrs  tunnet.Addresses

	readers *readerSet

	mu              sync.Mutex
	pools           map[types.PoolKey]*pool
	claimed         map[string]*types.Sandbox
	claimedVMs      map[string]*types.Sandbox
	pendingRemovals map[string]pendingRemoval
	tunReleases     map[string]netip.Addr
	// moving holds this node's outgoing moves, incoming its reservations for moves to it; neither enters claimed.
	moving   map[string]*outMove
	incoming map[string]*inMove

	held        int
	heldTenants map[string]int

	// Node-wide, unlike the per-pool streak backoff: the exhausted resource is shared
	atCapacityUntil  time.Time
	atCapacityReason string

	diskLow        atomic.Bool
	diskStatFailed atomic.Bool

	procDir          string
	memoryLow        atomic.Bool
	memoryReadFailed atomic.Bool
	anonBytes        atomic.Int64

	refillSem   chan struct{}
	maintSem    chan struct{}
	probeSem    chan struct{}
	refillKick  chan struct{}
	goldenDumps chan struct{}
}

// NewManager builds a manager from the node config over backends the caller selected from it.
func NewManager(ctx context.Context, cfg *config.Config, eng Engine, secrets *egress.SecretStore, b Backends) (*Manager, error) {
	refill := cmp.Or(cfg.RefillConcurrency, defaultRefill)
	m := &Manager{
		eng:             eng,
		dataDir:         cfg.DataDir,
		procDir:         "/proc",
		egress:          cfg.HasEgress(),
		cocoondSocket:   cfg.CocoondSocket,
		store:           newClaimStore(cfg.DataDir, cfg.SyncClaims),
		catalog:         volumeCatalog{entries: make(map[string]*catalogVolume, len(cfg.Volumes)), roots: cfg.VolumeRoots, holders: map[string]volumeHolders{}},
		volumeRuns:      map[*types.Sandbox]*volumeRun{},
		volumeErrors:    map[*types.Sandbox]map[string]string{},
		poolStore:       newPoolStore(cfg.DataDir),
		poolShared:      b.CellPools,
		archives:        b.Archives,
		moves:           b.Moves,
		tunRouter:       b.TunRouter,
		tunAddrs:        b.TunAddrs,
		tenants:         b.Tenants,
		ckpts:           b.Checkpoints,
		ckptsShared:     b.SharedCheckpoints,
		tpls:            b.Templates,
		pools:           make(map[types.PoolKey]*pool, len(cfg.Pools)),
		claimed:         map[string]*types.Sandbox{},
		claimedVMs:      map[string]*types.Sandbox{},
		pendingRemovals: map[string]pendingRemoval{},
		tunReleases:     map[string]netip.Addr{},
		moving:          map[string]*outMove{},
		incoming:        map[string]*inMove{},
		heldTenants:     map[string]int{},
		tenantLive:      map[string]int{},
		readers:         &readerSet{live: map[string]*reader{}},
		archiving:       map[string]struct{}{},
		pendingCks:      map[string]struct{}{},
		ckMarks:         ckMarkers{dir: filepath.Join(cfg.DataDir, archiveDeleteDir)},
		recs:            recordLocks{held: map[string]*sync.RWMutex{}, refs: map[string]int{}},
		heals:           healVetoes{pending: map[string]struct{}{}, aborted: map[string]struct{}{}},
		refillSem:       make(chan struct{}, refill),
		goldenDumps:     make(chan struct{}, 1),
		maintSem:        make(chan struct{}, max(1, refill-max(1, refill/4))),
		probeSem:        make(chan struct{}, refill),
		refillKick:      make(chan struct{}, 1),
		healSem:         make(chan struct{}, maxConcurrentHeals),
	}
	m.store.dropHostEnv = cfg.HostEnvMemoryOnly
	m.seedVolumes(cfg.Volumes)
	if err := m.loadCordon(); err != nil {
		return nil, err
	}
	if err := m.loadRuntimeVolumes(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(m.goldensDir(), 0o750); err != nil {
		return nil, fmt.Errorf("create goldens dir: %w", err)
	}
	m.loadTemplates(ctx)
	usageJournal, err := newJournal(filepath.Join(cfg.DataDir, usage.JournalFile))
	if err != nil {
		return nil, fmt.Errorf("open usage journal: %w", err)
	}
	m.usage = usageJournal
	if cfg.AuditLog {
		audit, err := newJournal(filepath.Join(cfg.DataDir, "audit.jsonl"))
		if err != nil {
			return nil, fmt.Errorf("open audit journal: %w", err)
		}
		m.audit = audit
	}
	m.reserved = tenants.ReservedOf(cfg.APIToken, cfg.APITokenNext, cfg.OperatorToken, cfg.OperatorTokenNext)
	m.cfg = cfg
	m.view.Store(newConfigView(cfg, secrets))
	for _, spec := range cfg.Pools {
		m.pools[spec.PoolKey] = newPool(spec)
	}
	ca, caErr := outbound.LoadCA(cfg)
	if caErr != nil {
		return nil, fmt.Errorf("load egress ca: %w", caErr)
	}
	m.out = outbound.New(m.outboundOptions(ca))
	m.configSeedHash = poolSeedHash(cfg.Pools)
	start := &fenceState{ch: make(chan struct{}), fenced: m.archives != nil}
	if m.archives != nil || m.moves != nil {
		m.nodeID = cfg.Mesh.ID()
	}
	if m.archives != nil {
		close(start.ch)
	}
	m.fence.Store(start)
	if err := m.adoptPools(ctx); err != nil {
		return nil, err
	}
	m.warnVolumeTenants(ctx, cfg.Volumes)
	m.warnMissingClasses(ctx)
	return m, nil
}

// ClusterDigest fingerprints this node's must-match config as last loaded, with its egress root and tenant set.
func (m *Manager) ClusterDigest() string {
	m.reloadMu.Lock()
	cfg := m.cfg
	m.reloadMu.Unlock()
	return clusterDigest(cfg, m.out.CAFingerprint(), m.tenants.Records())
}

// Run drives the refill and reap loops until ctx is canceled.
func (m *Manager) Run(ctx context.Context) {
	refill := time.Tick(refillInterval)
	reap := time.Tick(reapInterval)
	liveness := time.Tick(livenessInterval)
	images := time.Tick(imageInterval)
	heldFence := time.Tick(heldFenceInterval)
	// Store retention is hourly: each sweep is cluster-visible I/O on a shared root
	storeSweep := time.Tick(time.Hour)
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
	m.checkMemory(ctx)
	m.refillOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-refill:
			if m.view.Load().out.Guarded() {
				m.out.RefreshSelf()
			}
			m.checkDisk(ctx)
			m.checkMemory(ctx)
			m.refillOnce(ctx)
		case <-m.refillKick:
			m.refillOnce(ctx)
		case <-reap:
			m.dropIdleReservations()
			m.dropStaleReaders(ctx)
			m.retryRemovals(ctx)
			m.reapOnce(ctx)
			m.idleOnce(ctx)
			m.archiveOnce(ctx)
			m.replicateOnce(ctx)
			m.shrinkOnce(ctx)
			go m.retryArchiveDeletes(ctx)
		case <-liveness:
			m.livenessOnce(ctx)
		case <-images:
			go m.checkImagesOnce(ctx)
		case <-heldFence:
			if m.archives != nil {
				go m.fenceHeldArchives(ctx)
			}
		case <-storeSweep:
			go m.sweepStoreGenerations(ctx)
			go m.sweepExpiredCheckpoints(ctx)
			if m.archives != nil {
				go m.sweepOrphanArchives(ctx)
			}
			if m.moves != nil {
				go m.sweepStaleMoves(ctx, time.Now())
			}
			go m.retryTunReleases(ctx)
			go m.reconcileTunAddrs(ctx)
		}
	}
}

// FlushClaims synchronously persists the current claim set at shutdown.
func (m *Manager) FlushClaims() error {
	m.foldTunBytes()
	m.mu.Lock()
	js := m.store.setSeen(m.claimed, time.Now())
	m.mu.Unlock()
	return m.store.commit(js)
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
	g := Gauges{
		Claimed: len(m.claimed), Running: m.store.runningCount(), MaxRunning: m.view.Load().maxRunning, Draining: m.draining, Cordoned: m.cordoned,
		MemoryLow: m.memoryLow.Load(), AnonBytes: int(m.anonBytes.Load()),
	}
	if m.evac != nil {
		g.Evacuating = len(m.evac.pending)
	}
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

// WarmCounts is the per-pool-key-hash warm count, for gossiping placement; a node that would refuse a claim names no pool.
func (m *Manager) WarmCounts() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	counts := make(map[string]int, len(m.pools))
	if m.quotaErr(1, "") != nil {
		return counts
	}
	for _, p := range m.pools {
		if !p.removed {
			counts[p.hash] = len(p.warm)
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

// WithPeerAdopt wires the push an evacuation or a full owner's wake makes after it releases a row.
func (m *Manager) WithPeerAdopt(fn PeerAdoptFunc) {
	m.peerAdopt = fn
}

func (m *Manager) sweepStoreGenerations(ctx context.Context) {
	if !m.sweeps.gen.CompareAndSwap(false, true) {
		return
	}
	defer m.sweeps.gen.Store(false)
	logger := log.WithFunc("pool.sweepStoreGenerations")
	if err := m.ckpts.SweepGenerations(ctx); err != nil {
		logger.Error(ctx, err, "sweep checkpoint generations")
	}
	if err := m.tpls.SweepGenerations(ctx); err != nil {
		logger.Error(ctx, err, "sweep template generations")
	}
}

// runBounded fans f over n items on the refill semaphore, sharing the node-wide budget but never the share maintSem leaves to refill.
func (m *Manager) runBounded(ctx context.Context, n int, f func(context.Context, int)) *sync.WaitGroup {
	var wg sync.WaitGroup
	var next atomic.Int64
	// one budget of workers queues at most that many waiters, so a long sweep leaves other sweeps their FIFO turn
	for range min(n, cap(m.maintSem)) {
		wg.Go(func() {
			for i := int(next.Add(1) - 1); i < n; i = int(next.Add(1) - 1) {
				if !m.withSlots(ctx, func() { f(ctx, i) }) {
					return
				}
			}
		})
	}
	return &wg
}

func (m *Manager) withSlots(ctx context.Context, f func()) bool {
	for _, sem := range [...]chan struct{}{m.maintSem, m.refillSem} {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return false
		}
		defer func() { <-sem }()
	}
	f()
	return true
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
		Restarts: sb.Restarts, RestartedAt: sb.RestartedAt, Failed: sb.Failed, EnvMissing: sb.Env.HasMissing(),
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

func templateVisible(tenant, owner string) bool {
	return owner == "" || tenantOwns(tenant, owner)
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
		errors.Is(err, ErrNoEgressHibernate) || errors.Is(err, ErrQuota) || errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrEvacuating)
}

func vmName(key types.PoolKey) string {
	return vmPrefix + key.Hash() + "-" + randHex(6)
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b) // never fails per crypto/rand contract
	return hex.EncodeToString(b)
}
