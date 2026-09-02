// Package pool owns a node's warm pools and claimed sandboxes: refill keeps
// every configured pool topped up with claim-ready VMs (cloned, reseeded,
// probed), so a claim is ownership transfer only.
package pool

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/projecteru2/core/log"
	"golang.org/x/sync/singleflight"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/netfilter"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/dir"
	"github.com/cocoonstack/sandbox/sandboxd/store/peer"
	"github.com/cocoonstack/sandbox/sandboxd/store/s3"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	refillInterval    = 2 * time.Second
	reapInterval      = 5 * time.Second
	buildRetryDelay   = 30 * time.Second
	claimProbeTimeout = 15 * time.Second
	coldProbeTimeout  = 90 * time.Second
	// a clone answers in ~1s even saturated, so a silent one is replaced, not waited on
	warmProbeTimeout = 5 * time.Second
	// One list; a wrong answer only costs an extra sweep.
	removeVerifyTimeout = 15 * time.Second
	// A full node clears only when VMs go away; retrying sooner buys nothing.
	capacityBackoff    = buildRetryDelay
	vsockPollInterval  = 100 * time.Millisecond
	defaultTTL         = 5 * time.Minute
	maxTTL             = 24 * time.Hour
	recommitBackoff    = 20 * time.Millisecond
	recommitMaxBackoff = 5 * time.Second
	// one failed boot is ordinary; only an unbroken run predicts the next failure
	refillFailStreak = 8
	// Doubles per failure past the streak.
	refillBackoffBase = 250 * time.Millisecond
	refillBackoffMax  = buildRetryDelay
	// fallbacks when a Manager is built from a Config that skipped config.Load's defaulting
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

	caSidecarSuffix = ".cafp"
)

var (
	ErrBadKey            = errors.New("invalid pool key")
	ErrBadCount          = errors.New("invalid fork count")
	ErrBadVolume         = errors.New("invalid volume request")
	ErrVolumeUnavailable = fmt.Errorf("%w: unknown or unavailable volume", ErrBadVolume)
	ErrNoWarm            = errors.New("no warm sandbox for key")
	ErrUnknownSandbox    = errors.New("unknown sandbox or bad token")
	ErrUnknownTemplate   = errors.New("unknown promoted template")
	ErrPooledTemplate    = errors.New("template belongs to a configured pool")
	ErrTemplateOwned     = errors.New("template owned by another tenant")
	ErrNoEgress          = errors.New("node has no egress attachment (bridge or network)")
	ErrNoEgressHibernate = errors.New("egress-lane sandboxes do not hibernate")
	ErrNoEgressFork      = errors.New("egress-lane sandboxes cannot fork, checkpoint, or promote: a resumed guest egresses before its fresh tap can be locked")
	ErrVolumeCapture     = errors.New("sandboxes with volumes cannot hibernate, fork, checkpoint, or promote")
	ErrVolumeBusy        = errors.New("volume is held by another claim")
	// Replaying a journal takes a writable mount, so readers stay out.
	ErrVolumeNeedsRecovery = errors.New("volume needs recovery by a writable claim")
	ErrQuota               = errors.New("node claim quota reached")

	errWokeMeanwhile = errors.New("woke between sweep and hibernate")
	errNoEgressTap   = errors.New("egress-lane claim has no lockable tap")
)

// Engine is the slice of the cocoon driver the manager consumes.
type Engine interface {
	Clone(ctx context.Context, fromDir, name string, key types.PoolKey) (types.VMRecord, error)
	CloneSnap(ctx context.Context, snap, name string, key types.PoolKey) (types.VMRecord, error)
	RunCold(ctx context.Context, name string, key types.PoolKey) (types.VMRecord, error)
	Remove(ctx context.Context, name string) error
	ReconcileStaleCreate(ctx context.Context, name string) (engine.StaleCreateOutcome, error)
	SnapshotSave(ctx context.Context, vmName, snapName string) error
	SnapshotExport(ctx context.Context, snapName, toDir string) error
	SnapshotRemove(ctx context.Context, snapName string) error
	SnapshotList(ctx context.Context) ([]string, error)
	Hibernate(ctx context.Context, vmName, snapName string) error
	Restore(ctx context.Context, vmName, snapRef string) (string, error)
	List(ctx context.Context, filters ...string) ([]types.VMRecord, error)
	Probe(ctx context.Context, vsockSocket string, timeout time.Duration) error
	DialGuestPort(ctx context.Context, vsockSocket string, port uint16) (net.Conn, error)
	InstallCACert(ctx context.Context, vsockSocket string, certPEM []byte) error
	DiskAttach(ctx context.Context, vmName string, spec engine.VolumeSpec) error
	MountVolume(ctx context.Context, vsockSocket, name, mount string, rw bool) error
	UnmountVolume(ctx context.Context, vsockSocket, mount string) error
	SyncGuest(ctx context.Context, vsockSocket string) error
}

// SandboxSummary is the ops view of one live claim — no tokens.
type SandboxSummary struct {
	ID             string         `json:"id"`
	Key            types.PoolKey  `json:"key"`
	Deadline       time.Time      `json:"deadline"`
	Hibernated     bool           `json:"hibernated"`
	Archived       bool           `json:"archived,omitempty"`
	FromCheckpoint string         `json:"from_checkpoint,omitempty"`
	Volumes        []types.Volume `json:"volumes,omitempty"`
	// ClaimRef echoes the caller reference; empty for fork and checkpoint-branch claims.
	ClaimRef string `json:"claim_ref,omitempty"`
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
}

type pool struct {
	key  types.PoolKey
	hash string

	// floor and warmMax bound the demand-adaptive target computed in watermark.go
	floor         int
	warmMax       int
	idle          time.Duration
	archiveAfter  time.Duration
	archiveDelete time.Duration
	rate          float64
	lead          time.Duration
	lastArrival   time.Time

	goldenDir string
	building  bool
	nextBuild time.Time
	removed   bool // dropped from the desired set while building/refilling; swept by refillOnce
	warm      []*types.Sandbox
	refilling int

	// without a streak gate a node-wide dead cause is retried every tick at full concurrency
	refillFails int
	nextRefill  time.Time
}

func newPool(key types.PoolKey) *pool {
	return &pool{key: key, hash: key.Hash()}
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

	poolStore      *poolStore
	configSeedHash string // config pools' hash, to warn when a file edit is overridden

	// idleDefault is the idle-hibernate threshold for unpooled keys; zero disables.
	idleDefault time.Duration
	idleEnabled bool
	idleSweep   atomic.Bool

	// archive*Default are the archive thresholds for unpooled keys.
	archiveAfterDefault  time.Duration
	archiveDeleteDefault time.Duration
	archiveEnabled       bool
	archiveSweep         atomic.Bool
	archiveDeleteSweep   atomic.Bool
	// archiving holds ids with an export in flight; pendingCks pins ids mid-commit; both under m.mu.
	archiving  map[string]struct{}
	pendingCks map[string]struct{}
	// egressListeners holds the per-sandbox egress proxy accept point; guarded by m.mu.
	egressListeners map[string]*egressListener
	// egressTaps holds the nft-locked egress-lane tap per sandbox id; guarded by m.mu.
	egressTaps map[string]string

	// tenantMax doubles as the set of known tenants; a 0 cap means unlimited.
	maxClaims    int
	draining     bool // guarded by m.mu; deliberately not persisted
	tenantMax    map[string]int
	tenantLive   map[string]int
	tenantEgress map[string]*egress.Policy // per-tenant allow-list; nil = no tenant policy
	poolEgress   map[types.PoolKey]*egress.Policy
	usage        *journal
	audit        *journal
	counters     counters
	ckpts        store.Store
	// a cluster-wide backend makes heal and the delete broadcast no-ops
	ckptsShared bool
	healer      *peer.Healer
	// healPending/healAbort, guarded by recLocksMu, let a delete veto a heal still staging.
	healSem      chan struct{}
	healFlights  singleflight.Group
	healPending  map[string]struct{}
	healAbort    map[string]struct{}
	peerDelete   PeerDeleteFunc
	tpls         store.Store
	ckptTTL      time.Duration
	ckptSweeping atomic.Bool

	// tplSet caches each template id against its owning tenant ("" = operator).
	tplMu  sync.Mutex
	tplSet map[string]string

	// recLocks serializes same-id record mutations; an entry evicts only when no holder remains, never on a bare delete, since a peer heal can republish the id.
	recLocks   map[string]*sync.RWMutex
	recLocksMu sync.Mutex
	recRefs    map[string]int
	// recEvict defers a delete's eviction to the last holder, else the recLocks entry leaks.
	recEvict map[string]struct{}

	// notifyTemplates is set before serving starts and fires after a promote or template delete.
	notifyTemplates func()

	// egressSecrets is read-only after startup; egressCA is nil unless a pool rule intercepts.
	egressSecrets *egress.SecretStore
	egressCA      *egress.CA
	guardedEgress bool
	lockEgress    bool
	// dial and sweep are test seams: the SSRF guard blocks loopback, the nft sweep is netlink-only.
	dial  egress.DialFunc
	sweep func(map[string]bool) error

	mu              sync.Mutex
	pools           map[types.PoolKey]*pool
	claimed         map[string]*types.Sandbox
	pendingRemovals map[string]pendingRemoval

	// node-wide, unlike the per-pool streak backoff: the exhausted resource is shared
	atCapacityUntil  time.Time
	atCapacityReason string

	refillSem  chan struct{}
	probeSem   chan struct{}
	refillKick chan struct{}
}

// NewManager builds a manager from the node config; ctx bounds backend construction.
func NewManager(ctx context.Context, cfg *config.Config, eng Engine, secrets *egress.SecretStore) (*Manager, error) {
	maxFork := cfg.MaxForkCount
	if maxFork < 1 {
		maxFork = defaultMaxFork
	}
	refill := cfg.RefillConcurrency
	if refill < 1 {
		refill = defaultRefill
	}
	m := &Manager{
		eng:             eng,
		dataDir:         cfg.DataDir,
		egress:          cfg.HasEgress(),
		lockEgress:      len(cfg.Bridges) > 0,
		maxFork:         maxFork,
		store:           newClaimStore(cfg.DataDir),
		volumes:         make(map[string]catalogVolume, len(cfg.Volumes)),
		volumeAdmission: map[string]volumeHolders{},
		poolStore:       newPoolStore(cfg.DataDir),
		pools:           make(map[types.PoolKey]*pool, len(cfg.Pools)),
		claimed:         map[string]*types.Sandbox{},
		pendingRemovals: map[string]pendingRemoval{},
		tenantLive:      map[string]int{},
		archiving:       map[string]struct{}{},
		pendingCks:      map[string]struct{}{},
		egressListeners: map[string]*egressListener{},
		egressTaps:      map[string]string{},
		recLocks:        map[string]*sync.RWMutex{},
		recRefs:         map[string]int{},
		recEvict:        map[string]struct{}{},
		healPending:     map[string]struct{}{},
		healAbort:       map[string]struct{}{},
		egressSecrets:   secrets,
		dial:            newEgressDialer(parsePrefixes(cfg.EgressInternalAllow)).DialContext,
		sweep:           netfilter.SweepExcept,
		refillSem:       make(chan struct{}, refill),
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
	// checkpoints and templates are two id-namespaced views (ck_*, tp_*) over one store root
	var err error
	if m.ckpts, err = newStoreView(ctx, cfg, "checkpoint-staging", store.CheckpointIDRe); err != nil {
		return nil, err
	}
	m.ckptsShared = cfg.CheckpointStore != nil && cfg.CheckpointStore.Kind == "s3"
	if m.tpls, err = newStoreView(ctx, cfg, "template-staging", store.TemplateIDRe); err != nil {
		return nil, err
	}
	m.ckptTTL = time.Duration(cfg.CheckpointTTLHours) * time.Hour
	m.tplSet = map[string]string{}
	if metas, listErr := m.tpls.Metas(ctx); listErr == nil {
		for _, raw := range metas {
			var rec templateRecord
			if json.Unmarshal(raw, &rec) == nil && rec.ID != "" {
				m.tplSet[rec.ID] = rec.Tenant
			}
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
	m.tenantMax = make(map[string]int, len(cfg.Tenants))
	m.tenantEgress = make(map[string]*egress.Policy, len(cfg.Tenants))
	m.poolEgress = make(map[types.PoolKey]*egress.Policy, len(cfg.Pools))
	for _, tn := range cfg.Tenants {
		m.tenantMax[tn.Name] = tn.MaxClaims
		if tn.Egress != nil {
			m.tenantEgress[tn.Name] = tn.Egress
			m.guardedEgress = true
		}
	}
	m.idleDefault = time.Duration(cfg.IdleHibernateSeconds) * time.Second
	m.idleEnabled = m.idleDefault > 0
	m.archiveAfterDefault = time.Duration(cfg.ArchiveAfterSeconds) * time.Second
	m.archiveDeleteDefault = time.Duration(cfg.ArchiveDeleteAfterSeconds) * time.Second
	m.archiveEnabled = m.archiveAfterDefault > 0
	for _, spec := range cfg.Pools {
		p := newPool(spec.PoolKey)
		p.applySpec(spec)
		m.pools[spec.PoolKey] = p
		if spec.IdleHibernateSeconds > 0 {
			m.idleEnabled = true
		}
		if spec.ArchiveAfterSeconds > 0 {
			m.archiveEnabled = true
		}
		if spec.Egress != nil {
			m.poolEgress[spec.PoolKey] = spec.Egress
			m.guardedEgress = true
		}
	}
	if slices.ContainsFunc(cfg.Pools, func(s config.PoolSpec) bool { return s.Egress.Intercepts() }) {
		ca, err := loadEgressCA(cfg.EgressCA)
		if err != nil {
			return nil, fmt.Errorf("load egress ca: %w", err)
		}
		m.egressCA = ca
	}
	m.configSeedHash = poolSeedHash(cfg.Pools)
	if err := m.adoptPersistedPools(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

// EgressCAFingerprint is the egress root's fingerprint, or "" when the node intercepts nothing.
func (m *Manager) EgressCAFingerprint() string {
	if m.egressCA == nil {
		return ""
	}
	return m.egressCA.Fingerprint()
}

// Run drives the refill and reap loops until ctx is canceled.
func (m *Manager) Run(ctx context.Context) {
	refill := time.NewTicker(refillInterval)
	defer refill.Stop()
	reap := time.NewTicker(reapInterval)
	defer reap.Stop()
	// store retention is hourly: each sweep is cluster-visible I/O on a shared root
	storeSweep := time.NewTicker(time.Hour)
	defer storeSweep.Stop()
	if m.ckptTTL > 0 {
		m.sweepExpiredCheckpoints(ctx)
	}
	m.refillOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-refill.C:
			m.refillOnce(ctx)
		case <-m.refillKick:
			m.refillOnce(ctx)
		case <-reap.C:
			m.retryRemovals(ctx)
			m.reapOnce(ctx)
			m.idleOnce(ctx)
			m.archiveOnce(ctx)
			go m.retryArchiveDeletes(ctx)
		case <-storeSweep.C:
			m.sweepStoreGenerations(ctx)
			if m.ckptTTL > 0 {
				m.sweepExpiredCheckpoints(ctx)
			}
		}
	}
}

// FlushClaims synchronously persists the current claim set at shutdown.
func (m *Manager) FlushClaims() error {
	return m.store.commit(m.claimsSnapshot())
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
	live := make([]*pool, 0, len(m.pools))
	for _, p := range m.pools {
		if !p.removed {
			live = append(live, p)
		}
	}
	slices.SortFunc(live, func(a, b *pool) int { return strings.Compare(a.hash, b.hash) })
	pools := make([]PoolInfo, 0, len(live))
	for _, p := range live {
		pools = append(pools, PoolInfo{
			Key:       p.key,
			Warm:      len(p.warm),
			Refilling: p.refilling,
			Target:    p.effectiveTarget(now),
			Golden:    p.goldenDir != "",
		})
	}
	g := Gauges{Claimed: len(m.claimed), Draining: m.draining}
	if now.Before(m.atCapacityUntil) {
		g.AtCapacity, g.AtCapacityReason = true, m.atCapacityReason
	}
	for _, sb := range m.claimed {
		if sb.HibernateSnap != "" {
			g.Hibernated++
		}
		if sb.ArchiveCk != "" {
			g.Archived++
		}
	}
	return pools, g
}

// Sandboxes lists live claims visible to tenant; empty tenant means root.
func (m *Manager) Sandboxes(tenant string) []SandboxSummary {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SandboxSummary, 0, len(m.claimed))
	for _, sb := range m.claimed {
		if !tenantOwns(tenant, sb.Tenant) {
			continue
		}
		out = append(out, summarize(sb))
	}
	slices.SortFunc(out, func(a, b SandboxSummary) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// WarmCounts is the per-pool-key-hash warm count, for gossiping placement.
func (m *Manager) WarmCounts() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	counts := make(map[string]int, len(m.pools))
	for _, p := range m.pools {
		if !p.removed {
			counts[p.hash] = len(p.warm)
		}
	}
	return counts
}

// WithPeerHeal installs the healer ClaimCheckpointHeal pulls through.
func (m *Manager) WithPeerHeal(enabled bool, owners peer.Owners, token string) {
	if !enabled || owners == nil || m.ckptsShared {
		return
	}
	m.healer = peer.NewHealer(owners, &peer.HTTPPuller{Token: token})
}

// WithPeerDelete wires the broadcast DeleteCheckpoint makes after a local delete.
func (m *Manager) WithPeerDelete(fn PeerDeleteFunc) {
	m.peerDelete = fn
}

func (m *Manager) sweepStoreGenerations(ctx context.Context) {
	logger := log.WithFunc("pool.sweepStoreGenerations")
	if err := m.ckpts.SweepGenerations(); err != nil {
		logger.Error(ctx, err, "sweep checkpoint generations")
	}
	if err := m.tpls.SweepGenerations(); err != nil {
		logger.Error(ctx, err, "sweep template generations")
	}
}

func (m *Manager) claimsSnapshot() claimSnapshot {
	return m.store.mark()
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
		return fmt.Errorf("%w: %v", ErrBadKey, err)
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
	return SandboxSummary{
		ID: sb.ID, Key: sb.Key, Deadline: sb.Deadline,
		Hibernated: sb.HibernateSnap != "", Archived: sb.ArchiveCk != "",
		FromCheckpoint: sb.FromCheckpoint, ClaimRef: sb.ClaimRef,
		Volumes: slices.Clone(sb.Volumes),
	}
}

func loadEgressCA(cfg *config.EgressCAConfig) (*egress.CA, error) {
	root, err := os.ReadFile(cfg.RootCert) //nolint:gosec // operator-configured ca path
	if err != nil {
		return nil, fmt.Errorf("read root cert: %w", err)
	}
	interCert, err := os.ReadFile(cfg.IntermediateCert) //nolint:gosec // operator-configured ca path
	if err != nil {
		return nil, fmt.Errorf("read intermediate cert: %w", err)
	}
	interKey, err := os.ReadFile(cfg.IntermediateKey) //nolint:gosec // operator-configured ca path
	if err != nil {
		return nil, fmt.Errorf("read intermediate key: %w", err)
	}
	return egress.LoadCA(root, interCert, interKey)
}

// The dir default lives here, not config.applyDefaults: tests build Config directly.
func newStoreView(ctx context.Context, cfg *config.Config, staging string, idRe *regexp.Regexp) (store.Store, error) {
	if cs := cfg.CheckpointStore; cs != nil && cs.Kind == "s3" {
		return s3.New(ctx, *cs.S3, filepath.Join(cfg.DataDir, staging), idRe)
	}
	return dir.New(cmp.Or(cfg.CheckpointDir, filepath.Join(cfg.DataDir, "checkpoints")), idRe)
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// tenantOwns is the tenancy predicate: root (empty tenant) owns everything, a tenant its own.
func tenantOwns(tenant, owner string) bool {
	return tenant == "" || tenant == owner
}

func hasAppliedVolumes(sb *types.Sandbox) bool {
	return len(sb.Volumes) > 0
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

// benignSweepErr reports whether err is the expected outcome of a housekeeping sweep.
func benignSweepErr(err error) bool {
	return errors.Is(err, ErrUnknownSandbox) || errors.Is(err, errWokeMeanwhile) ||
		errors.Is(err, ErrNoEgressHibernate)
}

func vmName(key types.PoolKey) string {
	return vmPrefix + key.Hash() + "-" + randHex(6)
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b) // never fails per crypto/rand contract
	return hex.EncodeToString(b)
}
