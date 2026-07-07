// Package pool owns a node's warm pools and claimed sandboxes: refill keeps
// every configured pool topped up with claim-ready VMs (cloned, reseeded,
// probed), so a claim is ownership transfer only.
package pool

import (
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

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/dir"
	"github.com/cocoonstack/sandbox/sandboxd/store/s3"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	refillInterval       = 2 * time.Second
	reapInterval         = 5 * time.Second
	buildRetryDelay      = 30 * time.Second
	claimProbeTimeout    = 15 * time.Second
	coldProbeTimeout     = 90 * time.Second
	maxConcurrentRefills = 4
	defaultTTL           = 5 * time.Minute
	maxTTL               = 24 * time.Hour
	// defaultMaxFork is the fork ceiling when a Manager is built from a Config
	// that skipped config.Load's defaulting (direct construction in tests).
	defaultMaxFork = 16

	vmPrefix        = "sbx-"
	goldenPrefix    = "sbx-golden-"
	hibernatePrefix = "sbx-hib-"
	forkPrefix      = "sbx-fork-"
	vmStateRunning  = "running"
)

var (
	ErrBadKey          = errors.New("invalid pool key")
	ErrBadCount        = errors.New("invalid fork count")
	ErrNoWarm          = errors.New("no warm sandbox for key")
	ErrUnknownSandbox  = errors.New("unknown sandbox or bad token")
	ErrUnknownTemplate = errors.New("unknown promoted template")
	ErrPooledTemplate  = errors.New("template belongs to a configured pool")
	ErrNoEgress        = errors.New("node has no egress attachment (bridge or network)")
	ErrQuota           = errors.New("node claim quota reached")

	// errWokeMeanwhile aborts an idle-hibernate whose victim saw a
	// data-plane connection after the sweep snapshot; internal only.
	errWokeMeanwhile = errors.New("woke between sweep and hibernate")

	// templateNameRe mirrors cocoon's snapshot-name rule so a promoted
	// template's derived snapshot and golden names are always accepted.
	templateNameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,62}$`)
)

// Engine is the slice of the cocoon driver the manager consumes.
type Engine interface {
	Clone(ctx context.Context, fromDir, name string, key types.PoolKey) error
	RunCold(ctx context.Context, name string, key types.PoolKey) error
	Remove(ctx context.Context, name string) error
	SnapshotSave(ctx context.Context, vmName, snapName string) error
	SnapshotExport(ctx context.Context, snapName, toDir string) error
	SnapshotRemove(ctx context.Context, snapName string) error
	SnapshotList(ctx context.Context) ([]string, error)
	Hibernate(ctx context.Context, vmName, snapName string) error
	Restore(ctx context.Context, vmName, snapRef string) error
	List(ctx context.Context, filters ...string) ([]types.VMRecord, error)
	Probe(ctx context.Context, vsockSocket string, timeout time.Duration) error
	DialGuestPort(ctx context.Context, vsockSocket string, port uint16) (net.Conn, error)
}

// SandboxSummary is the ops view of one live claim — no tokens.
type SandboxSummary struct {
	ID             string        `json:"id"`
	Key            types.PoolKey `json:"key"`
	Deadline       time.Time     `json:"deadline"`
	Hibernated     bool          `json:"hibernated"`
	FromCheckpoint string        `json:"from_checkpoint,omitempty"`
}

// PoolInfo is the ops view of one pool.
type PoolInfo struct {
	Key       types.PoolKey `json:"key"`
	Warm      int           `json:"warm"`
	Refilling int           `json:"refilling"`
	Target    int           `json:"target"`
	Golden    bool          `json:"golden"`
}

type pool struct {
	key types.PoolKey

	// floor and warmMax bound the demand-adaptive target (watermark.go);
	// rate/lead/lastArrival are its EWMA inputs, guarded by the manager
	// mutex like everything else here.
	floor       int
	warmMax     int
	idle        time.Duration
	rate        float64
	lead        time.Duration
	lastArrival time.Time

	goldenDir string
	building  bool
	nextBuild time.Time
	warm      []*types.Sandbox
	refilling int
}

func (p *pool) applySpec(spec config.PoolSpec) {
	p.floor = spec.Warm
	p.warmMax = spec.WarmMax
	p.idle = time.Duration(spec.IdleHibernateSeconds) * time.Second
}

// Manager owns the node's pools, claims, and their persistence.
type Manager struct {
	eng     Engine
	dataDir string
	egress  bool
	maxFork int
	store   *claimStore

	// idleDefault is the idle-hibernate threshold for unpooled keys; pooled
	// keys carry theirs on the pool struct. Zero means disabled.
	idleDefault time.Duration
	idleEnabled bool
	idleSweep   atomic.Bool

	// maxClaims caps live claims node-wide (0 = unlimited); tenantMax holds
	// every configured tenant's cap (0 = unlimited) and doubles as the set of
	// known tenants. usage is the always-on billing event stream, audit the
	// config-gated request tap.
	maxClaims    int
	tenantMax    map[string]int
	usage        *journal
	audit        *journal
	counters     counters
	ckpts        store.Store
	tpls         store.Store
	ckptTTL      time.Duration
	ckptSweeping atomic.Bool

	// tplSet caches the template ids visible in the store so the 1s gossip
	// tick never touches the backend (an s3 listing is network I/O); local
	// promotes/deletes update it, startup loads it.
	tplMu  sync.Mutex
	tplSet map[string]struct{}

	// notifyTemplates, when set (before serving starts), fires after a
	// promote or template delete so the mesh republishes immediately
	// instead of waiting out a gossip tick.
	notifyTemplates func()

	mu      sync.Mutex
	pools   map[types.PoolKey]*pool
	claimed map[string]*types.Sandbox

	refillSem chan struct{}
}

// NewManager builds a manager from the node config; ctx bounds backend
// construction (the s3 store resolves its credential chain).
func NewManager(ctx context.Context, cfg *config.Config, eng Engine) (*Manager, error) {
	maxFork := cfg.MaxForkCount
	if maxFork < 1 {
		maxFork = defaultMaxFork
	}
	m := &Manager{
		eng:       eng,
		dataDir:   cfg.DataDir,
		egress:    cfg.HasEgress(),
		maxFork:   maxFork,
		store:     newClaimStore(cfg.DataDir),
		pools:     make(map[types.PoolKey]*pool, len(cfg.Pools)),
		claimed:   map[string]*types.Sandbox{},
		refillSem: make(chan struct{}, maxConcurrentRefills),
	}
	if err := os.MkdirAll(m.goldensDir(), 0o750); err != nil {
		return nil, fmt.Errorf("create goldens dir: %w", err)
	}
	// Checkpoints and templates are two id-namespaced views over ONE store
	// root (ck_* vs tp_*): a shared mount or bucket carries both, and each
	// instance's listing filters to its own records.
	var err error
	if m.ckpts, err = newStoreView(ctx, cfg, "checkpoint-staging", store.CheckpointIDRe); err != nil {
		return nil, err
	}
	if m.tpls, err = newStoreView(ctx, cfg, "template-staging", store.TemplateIDRe); err != nil {
		return nil, err
	}
	m.ckptTTL = time.Duration(cfg.CheckpointTTLHours) * time.Hour
	m.tplSet = map[string]struct{}{}
	if metas, listErr := m.tpls.Metas(ctx); listErr == nil {
		for _, raw := range metas {
			var rec templateRecord
			if json.Unmarshal(raw, &rec) == nil && rec.ID != "" {
				m.tplSet[rec.ID] = struct{}{}
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
	for _, tn := range cfg.Tenants {
		m.tenantMax[tn.Name] = tn.MaxClaims
	}
	m.idleDefault = time.Duration(cfg.IdleHibernateSeconds) * time.Second
	m.idleEnabled = m.idleDefault > 0
	for _, spec := range cfg.Pools {
		p := &pool{key: spec.PoolKey}
		p.applySpec(spec)
		m.pools[spec.PoolKey] = p
		if spec.IdleHibernateSeconds > 0 {
			m.idleEnabled = true
		}
	}
	return m, nil
}

// Run drives the refill and reap loops until ctx is canceled.
func (m *Manager) Run(ctx context.Context) {
	refill := time.NewTicker(refillInterval)
	defer refill.Stop()
	reap := time.NewTicker(reapInterval)
	defer reap.Stop()
	// Retention is hourly, not per reap tick: on the s3 backend a sweep is
	// a LIST + per-checkpoint GETs.
	ckptSweep := make(<-chan time.Time)
	if m.ckptTTL > 0 {
		t := time.NewTicker(time.Hour)
		defer t.Stop()
		ckptSweep = t.C
		m.sweepExpiredCheckpoints(ctx)
	}
	m.refillOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-refill.C:
			m.refillOnce(ctx)
		case <-reap.C:
			m.reapOnce(ctx)
			m.idleOnce(ctx)
		case <-ckptSweep:
			m.sweepExpiredCheckpoints(ctx)
		}
	}
}

// Reconcile aligns state after a daemon restart: re-adopt persisted claims
// whose VMs are still running (or hibernated), drop the rest, and remove any
// sbx-prefixed VM nobody owns (stale pool VMs and golden builders from a
// previous life). It must run once at startup, before the server: it swaps
// in fresh records, which would bypass in-flight Transition locks.
func (m *Manager) Reconcile(ctx context.Context) error {
	claims, err := m.store.load()
	if err != nil {
		return err
	}
	vms, err := m.eng.List(ctx)
	if err != nil {
		return fmt.Errorf("list vms: %w", err)
	}
	live := make(map[string]types.VMRecord, len(vms))
	for _, vm := range vms {
		live[vm.Config.Name] = vm
	}

	owned := map[string]bool{}
	referenced := map[string]bool{}
	m.mu.Lock()
	for id, sb := range claims {
		rec, ok := live[sb.VMName]
		switch {
		case ok && rec.State == vmStateRunning:
			sb.VsockSocket = rec.VsockSocket
			// Running with the flag set = a wake crashed between restore and
			// commit; clearing it un-bricks the claim and unreferences the
			// snapshot for the sweep below.
			sb.HibernateSnap = ""
		case ok && sb.HibernateSnap != "":
			// Hibernated: the VM is stopped by design and wakes on demand.
		default:
			continue
		}
		m.claimed[id] = sb
		owned[sb.VMName] = true
		referenced[sb.HibernateSnap] = true
	}
	saveErr := m.store.save(m.claimed)
	for _, p := range m.pools {
		dir := filepath.Join(m.goldensDir(), p.key.Hash())
		if dirExists(dir) {
			p.goldenDir = dir
		}
	}
	m.mu.Unlock()
	// A crash mid-export leaves a *.tmp staging dir no build or promote of
	// this life will reuse.
	tmps, _ := filepath.Glob(filepath.Join(m.goldensDir(), "*.tmp"))
	if err := m.ckpts.SweepStaging(); err != nil {
		log.WithFunc("pool.Reconcile").Error(ctx, err, "sweep checkpoint staging")
	}
	if err := m.tpls.SweepStaging(); err != nil {
		log.WithFunc("pool.Reconcile").Error(ctx, err, "sweep template staging")
	}
	m.migrateLegacyTemplates(ctx)
	for _, tmp := range tmps {
		_ = os.RemoveAll(tmp)
	}

	logger := log.WithFunc("pool.Reconcile")
	var stale []string
	for name := range live {
		if strings.HasPrefix(name, vmPrefix) && !owned[name] {
			stale = append(stale, name)
		}
	}
	m.runBounded(ctx, len(stale), func(ctx context.Context, i int) {
		m.destroy(ctx, stale[i])
		logger.Infof(ctx, "removed stale VM %s", stale[i])
	}).Wait()

	// Snapshot sweep, symmetric to the VM sweep: a hibernate snapshot no
	// adopted claim references is an orphan (a crash between `vm hibernate`
	// and the journal commit), and fork/golden-build snapshots are transient
	// by construction — none can span a restart. A list failure only skips
	// the sweep: GC must not brick startup.
	if snaps, listErr := m.eng.SnapshotList(ctx); listErr != nil {
		logger.Warnf(ctx, "snapshot sweep skipped: %v", listErr)
	} else {
		var orphans []string
		for _, snap := range snaps {
			orphanHib := strings.HasPrefix(snap, hibernatePrefix) && !referenced[snap]
			if orphanHib || strings.HasPrefix(snap, forkPrefix) || strings.HasPrefix(snap, goldenPrefix) {
				orphans = append(orphans, snap)
			}
		}
		m.runBounded(ctx, len(orphans), func(ctx context.Context, i int) {
			m.dropSnap(ctx, orphans[i])
			logger.Infof(ctx, "removed orphan snapshot %s", orphans[i])
		}).Wait()
	}
	now := time.Now()
	for _, sb := range m.claimed {
		sb.LastActivity = now
	}
	logger.Infof(ctx, "adopted %d claims, %d VMs live", len(m.claimed), len(live))
	return saveErr
}

// SetPools replaces the node's desired warm targets. Existing claims are not
// affected; only unclaimed warm VMs are trimmed or refilled.
func (m *Manager) SetPools(ctx context.Context, specs []config.PoolSpec) error {
	desired := make(map[types.PoolKey]config.PoolSpec, len(specs))
	hashes := make(map[string]types.PoolKey, len(specs))
	for _, spec := range specs {
		spec = normalizePoolSpec(spec)
		if err := m.validate(spec.PoolKey); err != nil {
			return err
		}
		if err := spec.ValidateLimits(); err != nil {
			return fmt.Errorf("%w: %v", ErrBadCount, err)
		}
		if existing, ok := hashes[spec.Hash()]; ok && existing != spec.PoolKey {
			return fmt.Errorf("%w: pool key hash collision between %q and %q", ErrBadKey, existing.Template, spec.Template)
		}
		if _, ok := desired[spec.PoolKey]; ok {
			return fmt.Errorf("%w: duplicate pool %q", ErrBadKey, spec.Template)
		}
		hashes[spec.Hash()] = spec.PoolKey
		desired[spec.PoolKey] = spec
	}

	var trim []string
	m.mu.Lock()
	now := time.Now()
	for key, p := range m.pools {
		spec, ok := desired[key]
		if !ok {
			p.floor = 0
			p.warmMax = 0
			p.idle = 0
		} else {
			p.applySpec(spec)
		}
		target := p.effectiveTarget(now)
		for len(p.warm) > target {
			n := len(p.warm) - 1
			trim = append(trim, p.warm[n].VMName)
			p.warm = p.warm[:n]
		}
		if !ok && !p.building && p.refilling == 0 {
			delete(m.pools, key)
		}
	}
	for key, spec := range desired {
		if p := m.pools[key]; p != nil {
			continue
		}
		p := &pool{key: key}
		p.applySpec(spec)
		// A golden already on disk (from this pool's earlier life) is
		// adopted; buildGolden covers the rest.
		if g := filepath.Join(m.goldensDir(), key.Hash()); dirExists(g) {
			p.goldenDir = g
		}
		m.pools[key] = p
	}
	// Recompute rather than latch: removing every idle pool turns the
	// sweep off again.
	m.idleEnabled = m.idleDefault > 0
	for _, p := range m.pools {
		if p.idle > 0 {
			m.idleEnabled = true
		}
	}
	m.mu.Unlock()

	runCtx := context.WithoutCancel(ctx)
	for _, name := range trim {
		m.destroy(runCtx, name)
	}
	m.refillOnce(runCtx)
	return nil
}

// SetTemplateNotifier wires the immediate-republish hook; call it before
// the server starts serving.
func (m *Manager) SetTemplateNotifier(fn func()) {
	m.notifyTemplates = fn
}

// Info reports pool states (sorted for stable output), the claim count, and
// how many claims are hibernated.
func (m *Manager) Info() ([]PoolInfo, int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	infos := make([]PoolInfo, 0, len(m.pools))
	for _, p := range m.pools {
		infos = append(infos, PoolInfo{
			Key:       p.key,
			Warm:      len(p.warm),
			Refilling: p.refilling,
			Target:    p.effectiveTarget(time.Now()),
			Golden:    p.goldenDir != "",
		})
	}
	slices.SortFunc(infos, func(a, b PoolInfo) int { return strings.Compare(a.Key.Hash(), b.Key.Hash()) })
	hibernated := 0
	for _, sb := range m.claimed {
		if sb.HibernateSnap != "" {
			hibernated++
		}
	}
	return infos, len(m.claimed), hibernated
}

// Sandboxes lists the live claims, for the operator index.
func (m *Manager) Sandboxes() []SandboxSummary {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SandboxSummary, 0, len(m.claimed))
	for _, sb := range m.claimed {
		out = append(out, SandboxSummary{
			ID: sb.ID, Key: sb.Key, Deadline: sb.Deadline,
			Hibernated: sb.HibernateSnap != "", FromCheckpoint: sb.FromCheckpoint,
		})
	}
	slices.SortFunc(out, func(a, b SandboxSummary) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// WarmCounts is the per-pool-key-hash warm count, for gossiping placement.
func (m *Manager) WarmCounts() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	counts := make(map[string]int, len(m.pools))
	for key, p := range m.pools {
		counts[key.Hash()] = len(p.warm)
	}
	return counts
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

// normalizePoolSpec fills the wire defaults, mirroring ClaimRequest.Key():
// API requests default net/size; config files stay explicit (Validate
// rejects empty there).
func normalizePoolSpec(spec config.PoolSpec) config.PoolSpec {
	if spec.Net == "" {
		spec.Net = types.NetNone
	}
	if spec.Size == "" {
		spec.Size = types.SizeSmall
	}
	return spec
}

// newStoreView builds one id-namespaced view of the configured backend.
// The dir default lives here rather than config.applyDefaults: tests build
// Config directly, skipping Load.
func newStoreView(ctx context.Context, cfg *config.Config, staging string, idRe *regexp.Regexp) (store.Store, error) {
	if cs := cfg.CheckpointStore; cs != nil && cs.Kind == "s3" {
		return s3.New(ctx, *cs.S3, filepath.Join(cfg.DataDir, staging), idRe)
	}
	ckptDir := cfg.CheckpointDir
	if ckptDir == "" {
		ckptDir = filepath.Join(cfg.DataDir, "checkpoints")
	}
	return dir.New(ckptDir, idRe)
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

func (m *Manager) goldensDir() string {
	return filepath.Join(m.dataDir, "goldens")
}

func vmName(key types.PoolKey) string {
	return vmPrefix + key.Hash() + "-" + randHex(3)
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b) // never fails per crypto/rand contract
	return hex.EncodeToString(b)
}
