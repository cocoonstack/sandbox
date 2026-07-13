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
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/netfilter"
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
	vsockPollInterval    = 100 * time.Millisecond
	maxConcurrentRefills = 4
	defaultTTL           = 5 * time.Minute
	maxTTL               = 24 * time.Hour
	recommitBackoff      = 20 * time.Millisecond
	recommitMaxBackoff   = 5 * time.Second
	// defaultMaxFork is the fork ceiling when a Manager is built from a Config
	// that skipped config.Load's defaulting (direct construction in tests).
	defaultMaxFork = 16

	vmPrefix        = "sbx-"
	goldenPrefix    = "sbx-golden-"
	hibernatePrefix = "sbx-hib-"
	forkPrefix      = "sbx-fork-"
	vmStateRunning  = "running"

	caSidecarSuffix = ".cafp"
)

var (
	ErrBadKey            = errors.New("invalid pool key")
	ErrBadCount          = errors.New("invalid fork count")
	ErrNoWarm            = errors.New("no warm sandbox for key")
	ErrUnknownSandbox    = errors.New("unknown sandbox or bad token")
	ErrUnknownTemplate   = errors.New("unknown promoted template")
	ErrPooledTemplate    = errors.New("template belongs to a configured pool")
	ErrTemplateOwned     = errors.New("template owned by another tenant")
	ErrNoEgress          = errors.New("node has no egress attachment (bridge or network)")
	ErrNoEgressHibernate = errors.New("egress-lane sandboxes do not hibernate")
	ErrNoEgressFork      = errors.New("egress-lane sandboxes cannot fork, checkpoint, or promote: a resumed guest egresses before its fresh tap can be locked")
	ErrQuota             = errors.New("node claim quota reached")

	errWokeMeanwhile = errors.New("woke between sweep and hibernate")
	errNoEgressTap   = errors.New("egress-lane claim has no lockable tap")
)

// Engine is the slice of the cocoon driver the manager consumes.
type Engine interface {
	Clone(ctx context.Context, fromDir, name string, key types.PoolKey) (types.VMRecord, error)
	CloneSnap(ctx context.Context, snap, name string, key types.PoolKey) (types.VMRecord, error)
	RunCold(ctx context.Context, name string, key types.PoolKey) (types.VMRecord, error)
	Remove(ctx context.Context, name string) error
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
}

// SandboxSummary is the ops view of one live claim — no tokens.
type SandboxSummary struct {
	ID             string        `json:"id"`
	Key            types.PoolKey `json:"key"`
	Deadline       time.Time     `json:"deadline"`
	Hibernated     bool          `json:"hibernated"`
	Archived       bool          `json:"archived,omitempty"`
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

// Gauges are the manager's point-in-time claim counts.
type Gauges struct {
	Claimed    int
	Hibernated int
	Archived   int
}

type pool struct {
	key types.PoolKey

	// floor and warmMax bound the demand-adaptive target (watermark.go);
	// rate/lead/lastArrival are its EWMA inputs, guarded by the manager
	// mutex like everything else here.
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
	warm      []*types.Sandbox
	refilling int
}

func (p *pool) applySpec(spec config.PoolSpec) {
	p.floor = spec.Warm
	p.warmMax = spec.WarmMax
	p.idle = time.Duration(spec.IdleHibernateSeconds) * time.Second
	p.archiveAfter = time.Duration(spec.ArchiveAfterSeconds) * time.Second
	p.archiveDelete = time.Duration(spec.ArchiveDeleteAfterSeconds) * time.Second
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

	// archive*Default are the archive thresholds for unpooled keys; pooled keys
	// carry theirs on the pool struct. archiveEnabled is set when any pool or
	// the node default has archiving on; archiveSweep guards the sweep loop.
	archiveAfterDefault  time.Duration
	archiveDeleteDefault time.Duration
	archiveEnabled       bool
	archiveSweep         atomic.Bool
	// archiving holds ids with an archive() export in flight, so the reap tick
	// and archive sweep don't both re-export the same sandbox; pendingCks pins
	// checkpoint ids an archive is publishing before ArchiveCk can (a delete
	// in that window would strand the claim). Both guarded by m.mu.
	archiving  map[string]struct{}
	pendingCks map[string]struct{}
	// egressListeners holds the per-sandbox egress proxy accept point, keyed by
	// sandbox id; guarded by m.mu.
	egressListeners map[string]*egressListener
	// egressTaps holds the nft-locked egress-lane tap per sandbox id, tracked
	// apart from the proxy so a locked-but-policyless NIC still unlocks on
	// release; guarded by m.mu.
	egressTaps map[string]string

	// maxClaims caps live claims node-wide (0 = unlimited); tenantMax holds
	// every configured tenant's cap (0 = unlimited) and doubles as the set of
	// known tenants; tenantLive counts live claims per tenant so admission
	// stays O(1). usage is the always-on billing event stream, audit the
	// config-gated request tap.
	maxClaims    int
	tenantMax    map[string]int
	tenantLive   map[string]int
	tenantEgress map[string]*egress.Policy // per-tenant allow-list; nil = no tenant policy
	poolEgress   map[types.PoolKey]*egress.Policy
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

	// recLocks serializes same-id store record mutations and holds off a
	// re-publish swap while a clone reads the old generation (per id, RW).
	recLocks sync.Map

	// notifyTemplates, when set (before serving starts), fires after a
	// promote or template delete so the mesh republishes immediately
	// instead of waiting out a gossip tick.
	notifyTemplates func()

	// egressSecrets resolves a rule's secret name to the injected header, read-only
	// after startup. guardedEgress arms the proxy (some policy exists); lockEgress
	// nft-locks every egress-lane NIC default-deny (a bridge lane exists). egressCA
	// signs HTTPS-interception leaves and is baked into interception pools' goldens;
	// nil unless a pool rule sets intercept.
	egressSecrets *egress.SecretStore
	egressCA      *egress.CA
	guardedEgress bool
	lockEgress    bool
	// dial and sweep are fields as test seams: the SSRF guard blocks loopback
	// test origins, and the nft sweep is netlink-only.
	dial  egress.DialFunc
	sweep func(map[string]bool) error

	mu      sync.Mutex
	pools   map[types.PoolKey]*pool
	claimed map[string]*types.Sandbox

	refillSem chan struct{}
}

// NewManager builds a manager from the node config; ctx bounds backend
// construction (the s3 store resolves its credential chain). secrets resolves
// egress rule references and is shared by every per-sandbox proxy.
func NewManager(ctx context.Context, cfg *config.Config, eng Engine, secrets *egress.SecretStore) (*Manager, error) {
	maxFork := cfg.MaxForkCount
	if maxFork < 1 {
		maxFork = defaultMaxFork
	}
	m := &Manager{
		eng:             eng,
		dataDir:         cfg.DataDir,
		egress:          cfg.HasEgress(),
		lockEgress:      cfg.Bridge != "",
		maxFork:         maxFork,
		store:           newClaimStore(cfg.DataDir),
		pools:           make(map[types.PoolKey]*pool, len(cfg.Pools)),
		claimed:         map[string]*types.Sandbox{},
		tenantLive:      map[string]int{},
		archiving:       map[string]struct{}{},
		pendingCks:      map[string]struct{}{},
		egressListeners: map[string]*egressListener{},
		egressTaps:      map[string]string{},
		egressSecrets:   secrets,
		dial:            egressDialer.DialContext,
		sweep:           netfilter.SweepExcept,
		refillSem:       make(chan struct{}, maxConcurrentRefills),
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
		p := &pool{key: spec.PoolKey}
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
	if m.archiveEnabled && m.ckptTTL == 0 {
		log.WithFunc("pool.NewManager").Warn(ctx, "archive enabled with checkpoint_ttl_hours=0: a checkpoint whose delete fails is not reclaimed")
	}
	return m, nil
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
			m.archiveOnce(ctx)
		case <-ckptSweep:
			m.sweepExpiredCheckpoints(ctx)
		}
	}
}

// FlushClaims synchronously persists the current claim set — the shutdown
// hook that closes the window where a detached recommit has not converged yet.
func (m *Manager) FlushClaims() error {
	return m.store.commit(m.claimsSnapshot())
}

func (m *Manager) claimsSnapshot() claimSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.store.snapshot(m.claimed)
}

func (m *Manager) untrack(set map[string]struct{}, key string) {
	m.mu.Lock()
	delete(set, key)
	m.mu.Unlock()
}

// SetTemplateNotifier wires the immediate-republish hook; call it before
// the server starts serving.
func (m *Manager) SetTemplateNotifier(fn func()) {
	m.notifyTemplates = fn
}

// Info reports pool states (sorted for stable output) and the claim gauges.
func (m *Manager) Info() ([]PoolInfo, Gauges) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pools := make([]PoolInfo, 0, len(m.pools))
	for _, p := range m.pools {
		pools = append(pools, PoolInfo{
			Key:       p.key,
			Warm:      len(p.warm),
			Refilling: p.refilling,
			Target:    p.effectiveTarget(time.Now()),
			Golden:    p.goldenDir != "",
		})
	}
	slices.SortFunc(pools, func(a, b PoolInfo) int { return strings.Compare(a.Key.Hash(), b.Key.Hash()) })
	g := Gauges{Claimed: len(m.claimed)}
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

// Sandboxes lists the live claims, for the operator index.
func (m *Manager) Sandboxes() []SandboxSummary {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SandboxSummary, 0, len(m.claimed))
	for _, sb := range m.claimed {
		out = append(out, SandboxSummary{
			ID: sb.ID, Key: sb.Key, Deadline: sb.Deadline,
			Hibernated: sb.HibernateSnap != "", Archived: sb.ArchiveCk != "", FromCheckpoint: sb.FromCheckpoint,
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

func (m *Manager) goldensDir() string {
	return filepath.Join(m.dataDir, "goldens")
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

// tenantOwns is THE tenancy predicate — every read/delete/overwrite scope
// check goes through it: root (empty tenant) owns everything, a tenant only
// records stamped with its own name.
func tenantOwns(tenant, owner string) bool {
	return tenant == "" || tenant == owner
}

// benignSweepErr reports whether err is the expected outcome of a housekeeping
// sweep (victim released, woke mid-sweep, or a lane that never hibernates).
func benignSweepErr(err error) bool {
	return errors.Is(err, ErrUnknownSandbox) || errors.Is(err, errWokeMeanwhile) ||
		errors.Is(err, ErrNoEgressHibernate)
}

func vmName(key types.PoolKey) string {
	return vmPrefix + key.Hash() + "-" + randHex(3)
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b) // never fails per crypto/rand contract
	return hex.EncodeToString(b)
}
