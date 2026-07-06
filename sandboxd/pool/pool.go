// Package pool owns a node's warm pools and claimed sandboxes: refill keeps
// every configured pool topped up with claim-ready VMs (cloned, reseeded,
// probed), so a claim is ownership transfer only.
package pool

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
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

// Manager owns the node's pools, claims, and their persistence.
type Manager struct {
	eng     Engine
	dataDir string
	egress  bool
	maxFork int
	store   *store

	// idleDefault is the idle-hibernate threshold for unpooled keys; pooled
	// keys carry theirs on the pool struct. Zero means disabled.
	idleDefault time.Duration
	idleEnabled bool
	idleSweep   atomic.Bool

	// maxClaims caps live claims node-wide (0 = unlimited); usage is the
	// always-on billing event stream, audit the config-gated request tap.
	maxClaims int
	usage     *journal
	audit     *journal
	counters  counters
	ckpts     CheckpointStore

	// notifyTemplates, when set (before serving starts), fires after a
	// promote or template delete so the mesh republishes immediately
	// instead of waiting out a gossip tick.
	notifyTemplates func()

	mu      sync.Mutex
	pools   map[types.PoolKey]*pool
	claimed map[string]*types.Sandbox

	refillSem chan struct{}
}

// NewManager builds a manager from the node config.
func NewManager(cfg *config.Config, eng Engine) (*Manager, error) {
	maxFork := cfg.MaxForkCount
	if maxFork < 1 {
		maxFork = defaultMaxFork
	}
	m := &Manager{
		eng:       eng,
		dataDir:   cfg.DataDir,
		egress:    cfg.HasEgress(),
		maxFork:   maxFork,
		store:     newStore(cfg.DataDir),
		pools:     make(map[types.PoolKey]*pool, len(cfg.Pools)),
		claimed:   map[string]*types.Sandbox{},
		refillSem: make(chan struct{}, maxConcurrentRefills),
	}
	if err := os.MkdirAll(m.goldensDir(), 0o750); err != nil {
		return nil, fmt.Errorf("create goldens dir: %w", err)
	}
	// Default here too: tests build Config directly, skipping Load.
	ckptDir := cfg.CheckpointDir
	if ckptDir == "" {
		ckptDir = filepath.Join(cfg.DataDir, "checkpoints")
	}
	ckpts, err := newDirCheckpointStore(ckptDir)
	if err != nil {
		return nil, err
	}
	m.ckpts = ckpts
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
	m.idleDefault = time.Duration(cfg.IdleHibernateSeconds) * time.Second
	m.idleEnabled = m.idleDefault > 0
	for _, spec := range cfg.Pools {
		m.pools[spec.PoolKey] = &pool{
			key: spec.PoolKey, floor: spec.Warm, warmMax: spec.WarmMax,
			idle: time.Duration(spec.IdleHibernateSeconds) * time.Second,
		}
		if spec.IdleHibernateSeconds > 0 {
			m.idleEnabled = true
		}
	}
	return m, nil
}

// Claim hands out a claim-ready sandbox: a warm hit when one exists, else a
// provision (golden clone ~40ms, or a cold boot for an unpooled key). The mesh
// redirect is layered above this in the server; Claim itself is node-local.
func (m *Manager) Claim(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
	sb, err := m.ClaimWarm(ctx, key, ttl)
	if errors.Is(err, ErrNoWarm) {
		return m.ClaimProvision(ctx, key, ttl)
	}
	return sb, err
}

// ClaimWarm transfers ownership of a warm sandbox without provisioning;
// ErrNoWarm means the pool is empty (the caller may redirect or provision).
func (m *Manager) ClaimWarm(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
	start := time.Now()
	if err := m.validate(key); err != nil {
		return nil, err
	}
	if m.overQuota(1) {
		return nil, fmt.Errorf("%w: cap %d", ErrQuota, m.maxClaims)
	}
	m.mu.Lock()
	var sb *types.Sandbox
	if p := m.pools[key]; p != nil {
		p.noteArrival(start)
		if n := len(p.warm); n > 0 {
			sb = p.warm[n-1]
			p.warm = p.warm[:n-1]
		}
	}
	m.mu.Unlock()
	if sb == nil {
		return nil, ErrNoWarm
	}
	out, err := m.finalize(ctx, sb, ttl)
	if err == nil {
		m.counters.claimsWarm.Add(1)
		m.counters.claimNanos.Add(uint64(time.Since(start))) //nolint:gosec // durations are positive
	}
	return out, err
}

// ClaimProvision creates a claim-ready sandbox (golden clone or cold boot).
func (m *Manager) ClaimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
	start := time.Now()
	if err := m.validate(key); err != nil {
		return nil, err
	}
	if m.overQuota(1) {
		return nil, fmt.Errorf("%w: cap %d", ErrQuota, m.maxClaims)
	}
	golden := m.goldenDirFor(key)
	sb, err := m.provision(ctx, key, golden)
	if err != nil {
		return nil, err
	}
	out, err := m.finalize(ctx, sb, ttl)
	if err == nil {
		if golden != "" {
			m.counters.claimsClone.Add(1)
		} else {
			m.counters.claimsCold.Add(1)
		}
		m.counters.claimNanos.Add(uint64(time.Since(start))) //nolint:gosec // durations are positive
	}
	return out, err
}

// overQuota is the cheap advisory precheck: the authoritative check stays
// in finalizeBatch (admission races resolve there), this one just spares a
// doomed request the provision cost.
func (m *Manager) overQuota(extra int) bool {
	if m.maxClaims <= 0 {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.claimed)+extra > m.maxClaims
}

// SetTemplateNotifier wires the immediate-republish hook; call it before
// the server starts serving.
func (m *Manager) SetTemplateNotifier(fn func()) {
	m.notifyTemplates = fn
}

// HasGolden reports whether this node can provision the key without a cold
// boot — a configured pool golden or a promoted template on disk.
func (m *Manager) HasGolden(key types.PoolKey) bool {
	return m.goldenDirFor(key) != ""
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

// TemplateHashes lists the promoted-template key hashes on disk — goldens
// not backing a configured pool — for the mesh's template gossip.
func (m *Manager) TemplateHashes() []string {
	entries, err := os.ReadDir(m.goldensDir())
	if err != nil {
		return nil
	}
	m.mu.Lock()
	pooled := make(map[string]struct{}, len(m.pools))
	for key := range m.pools {
		pooled[key.Hash()] = struct{}{}
	}
	m.mu.Unlock()
	var hashes []string
	for _, e := range entries {
		if !e.IsDir() || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		if _, ok := pooled[e.Name()]; !ok {
			hashes = append(hashes, e.Name())
		}
	}
	return hashes
}

// Release destroys a claimed sandbox after validating its token.
func (m *Manager) Release(ctx context.Context, id, token string) error {
	m.mu.Lock()
	sb, ok := m.authed(id, token)
	if !ok {
		m.mu.Unlock()
		return ErrUnknownSandbox
	}
	delete(m.claimed, id)
	snap := sb.HibernateSnap
	saveErr := m.store.save(m.claimed)
	m.mu.Unlock()
	if saveErr != nil {
		log.WithFunc("pool.Release").Errorf(ctx, saveErr, "persist release of %s", id)
	}
	// The claim is already dropped; removal must survive the caller hanging up.
	err := m.eng.Remove(context.WithoutCancel(ctx), sb.VMName)
	m.dropSnap(ctx, snap)
	m.counters.releases.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "release", ID: id, VMName: sb.VMName})
	return err
}

// AgentSocket resolves a claimed sandbox's vsock UDS without waking it (the
// ownership probe must not restore a hibernated VM).
func (m *Manager) AgentSocket(id, token string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb, ok := m.authed(id, token)
	if !ok {
		return "", ErrUnknownSandbox
	}
	// No activity stamp here: owner/Lookup probes use this path, and a
	// control-plane poll must not keep an idle sandbox awake. The relay's
	// stamp lives in WakeAgentSocket.
	return sb.VsockSocket, nil
}

// Hibernate atomically snapshots a claimed sandbox and stops its VM, freeing
// memory; the next agent access wakes it. Idempotent on an already-hibernated
// sandbox. When to hibernate is the caller's policy — the node only provides
// the transition.
func (m *Manager) Hibernate(ctx context.Context, id, token string) error {
	sb, ok := m.claim(id, token)
	if !ok {
		return ErrUnknownSandbox
	}
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	return m.hibernateLocked(ctx, sb)
}

// hibernateLocked is Hibernate's body; the caller holds sb.Transition.
func (m *Manager) hibernateLocked(ctx context.Context, sb *types.Sandbox) error {
	if sb.HibernateSnap != "" {
		return nil
	}
	// A started transition must finish even if the caller hangs up (the
	// engine bounds every step), or the record would disagree with the VM.
	ctx = context.WithoutCancel(ctx)
	// Derived from VMName, not sb.ID: cocoon snapshot names reject the
	// underscore in the "sb_" prefix.
	snap := hibernatePrefix + strings.TrimPrefix(sb.VMName, vmPrefix)
	if err := m.eng.Hibernate(ctx, sb.VMName, snap); err != nil {
		return err
	}
	if !m.commitTransition(ctx, sb, snap, sb.VsockSocket) {
		// Released mid-transition: the VM is gone, drop our snapshot.
		m.dropSnap(ctx, snap)
		return ErrUnknownSandbox
	}
	m.counters.hibernates.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "hibernate", ID: sb.ID, VMName: sb.VMName})
	return nil
}

// WakeAgentSocket resolves the sandbox's vsock UDS for the relay, first
// restoring the VM if it is hibernated; concurrent wakes queue on the
// transition lock and find the fast path.
func (m *Manager) WakeAgentSocket(ctx context.Context, id, token string) (string, error) {
	sb, ok := m.claim(id, token)
	if !ok {
		return "", ErrUnknownSandbox
	}
	m.touch(sb)
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if sb.HibernateSnap == "" {
		return sb.VsockSocket, nil
	}
	// See Hibernate: a half-restored VM is worse than a wasted wake.
	ctx = context.WithoutCancel(ctx)
	wakeStart := time.Now()
	snap := sb.HibernateSnap
	if err := m.eng.Restore(ctx, sb.VMName, snap); err != nil {
		return "", fmt.Errorf("wake %s: %w", id, err)
	}
	sock, err := m.probeReady(ctx, sb.VMName, claimProbeTimeout)
	if err != nil {
		return "", fmt.Errorf("wake %s: %w", id, err)
	}
	if !m.commitTransition(ctx, sb, "", sock) {
		// Released mid-transition: destroy the VM we just resurrected.
		m.destroy(ctx, sb.VMName)
		m.dropSnap(ctx, snap)
		return "", ErrUnknownSandbox
	}
	// The memory image is consumed by the resume; drop it to free disk.
	m.dropSnap(ctx, snap)
	m.counters.wakes.Add(1)
	m.counters.wakeNanos.Add(uint64(time.Since(wakeStart))) //nolint:gosec // durations are positive
	m.recordUsage(ctx, usageEvent{Event: "wake", ID: sb.ID, VMName: sb.VMName})
	return sock, nil
}

// Fork clones a claimed sandbox into count children, each a fresh claim with
// its own lease: memory, disk, and guest state (sessions, processes, tmpfs)
// duplicate at the snapshot point, and cocoon's clone reseed gives every
// child a distinct machine identity. All-or-nothing: any child failing
// destroys the ones already built, so an error means no child survived.
func (m *Manager) Fork(ctx context.Context, id, token string, count int, ttl time.Duration) ([]*types.Sandbox, error) {
	if count < 1 || count > m.maxFork {
		return nil, fmt.Errorf("%w: %d not in 1..%d", ErrBadCount, count, m.maxFork)
	}
	if m.overQuota(count) {
		return nil, fmt.Errorf("%w: cap %d", ErrQuota, m.maxClaims)
	}
	sb, ok := m.claim(id, token)
	if !ok {
		return nil, ErrUnknownSandbox
	}
	// See Hibernate: a started fork must finish even if the caller hangs up.
	ctx = context.WithoutCancel(ctx)

	dir, err := os.MkdirTemp(m.dataDir, "fork-")
	if err != nil {
		return nil, fmt.Errorf("fork %s: %w", id, err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	exportDir := filepath.Join(dir, "export") // cocoon wants the target absent
	if err = m.exportSource(ctx, sb, exportDir); err != nil {
		return nil, fmt.Errorf("fork %s: %w", id, err)
	}

	children, err := m.cloneBatch(ctx, sb.Key, exportDir, count)
	if err != nil {
		return nil, fmt.Errorf("fork %s: %w", id, err)
	}
	if err := m.finalizeBatch(ctx, children, ttl); err != nil {
		return nil, fmt.Errorf("fork %s: %w", id, err)
	}
	m.counters.forks.Add(1)
	m.counters.claimsClone.Add(uint64(len(children))) //nolint:gosec // count is bounded by maxFork
	ids := make([]string, len(children))
	for i, c := range children {
		ids[i] = c.ID
	}
	m.recordUsage(ctx, usageEvent{Event: "fork", ID: sb.ID, VMName: sb.VMName, Children: ids})
	return children, nil
}

// Promote publishes a claimed sandbox as a template: its state is exported
// as a golden under (template, parent net, parent size), and later claims
// for that key clone from it — provision-on-demand, no warm pool unless the
// node config adds one. Re-promoting to the same name replaces the golden.
// The caller owns the template's lifecycle (DeleteTemplate); it lives only
// on this node, so the returned key is what a cluster client needs to reach
// it again.
func (m *Manager) Promote(ctx context.Context, id, token, template string) (types.PoolKey, error) {
	if !templateNameRe.MatchString(template) {
		return types.PoolKey{}, fmt.Errorf("%w: template %q must match %s", ErrBadKey, template, templateNameRe)
	}
	sb, ok := m.claim(id, token)
	if !ok {
		return types.PoolKey{}, ErrUnknownSandbox
	}
	key := types.PoolKey{Template: template, Net: sb.Key.Net, Size: sb.Key.Size}
	if m.pooledHash(key.Hash()) {
		// The same goldens/<hash> path backs a configured pool's golden —
		// promoting over it would silently change what refills produce.
		return types.PoolKey{}, ErrPooledTemplate
	}
	// See Fork: the transition lock pins the source snapshot, and a started
	// promote must finish even if the caller hangs up.
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	ctx = context.WithoutCancel(ctx)

	snap, cleanup, err := m.sourceSnap(ctx, sb)
	if err != nil {
		return types.PoolKey{}, fmt.Errorf("promote %s: %w", id, err)
	}
	defer cleanup()
	if err := m.exportGolden(ctx, snap, filepath.Join(m.goldensDir(), key.Hash())); err != nil {
		return types.PoolKey{}, fmt.Errorf("promote %s: %w", id, err)
	}
	if m.notifyTemplates != nil {
		m.notifyTemplates()
	}
	m.counters.promotes.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "promote", ID: sb.ID, VMName: sb.VMName, Reference: key.Template})
	return key, nil
}

// DeleteTemplate removes a promoted template's golden. Configured pools are
// refused: their goldens are owned by the node config, not an API caller.
func (m *Manager) DeleteTemplate(key types.PoolKey) error {
	if m.pooledHash(key.Hash()) {
		return ErrPooledTemplate
	}
	dir, ok := m.goldenOnDisk(key.Hash())
	if !ok {
		return ErrUnknownTemplate
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("delete template: %w", err)
	}
	if m.notifyTemplates != nil {
		m.notifyTemplates()
	}
	return nil
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
		if fi, statErr := os.Stat(dir); statErr == nil && fi.IsDir() {
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
	for _, tmp := range tmps {
		_ = os.RemoveAll(tmp)
	}

	logger := log.WithFunc("pool.Reconcile")
	for name := range live {
		if strings.HasPrefix(name, vmPrefix) && !owned[name] {
			m.destroy(ctx, name)
			logger.Infof(ctx, "removed stale VM %s", name)
		}
	}

	// Snapshot sweep, symmetric to the VM sweep: a hibernate snapshot no
	// adopted claim references is an orphan (a crash between `vm hibernate`
	// and the journal commit), and fork/golden-build snapshots are transient
	// by construction — none can span a restart. A list failure only skips
	// the sweep: GC must not brick startup.
	if snaps, listErr := m.eng.SnapshotList(ctx); listErr != nil {
		logger.Warnf(ctx, "snapshot sweep skipped: %v", listErr)
	} else {
		for _, snap := range snaps {
			orphanHib := strings.HasPrefix(snap, hibernatePrefix) && !referenced[snap]
			if orphanHib || strings.HasPrefix(snap, forkPrefix) || strings.HasPrefix(snap, goldenPrefix) {
				m.dropSnap(ctx, snap)
				logger.Infof(ctx, "removed orphan snapshot %s", snap)
			}
		}
	}
	now := time.Now()
	for _, sb := range m.claimed {
		sb.LastActivity = now
	}
	logger.Infof(ctx, "adopted %d claims, %d VMs live", len(m.claimed), len(live))
	return saveErr
}

// Run drives the refill and reap loops until ctx is canceled.
func (m *Manager) Run(ctx context.Context) {
	refill := time.NewTicker(refillInterval)
	defer refill.Stop()
	reap := time.NewTicker(reapInterval)
	defer reap.Stop()
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
		}
	}
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

// pooledHash reports whether a configured pool occupies this hash — the
// guard is on the HASH, not the key: goldens are stored by hash, so a
// colliding key would reach a pool's golden dir even though the keys differ.
func (m *Manager) pooledHash(hash string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.pools {
		if key.Hash() == hash {
			return true
		}
	}
	return false
}

// goldenDirFor resolves a key's golden: the pool's when configured, else an
// on-disk golden a Promote published for an unpooled key; "" cold-boots.
func (m *Manager) goldenDirFor(key types.PoolKey) string {
	m.mu.Lock()
	var dir string
	if p := m.pools[key]; p != nil {
		dir = p.goldenDir
	}
	m.mu.Unlock()
	if dir != "" {
		return dir
	}
	if onDisk, ok := m.goldenOnDisk(key.Hash()); ok {
		return onDisk
	}
	return ""
}

func (m *Manager) goldenOnDisk(hash string) (string, bool) {
	dir := filepath.Join(m.goldensDir(), hash)
	fi, err := os.Stat(dir)
	return dir, err == nil && fi.IsDir()
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

// finalize stamps identity, persists the claim, and destroys the VM if the
// store write fails so a durable claim always matches a live VM.
func (m *Manager) finalize(ctx context.Context, sb *types.Sandbox, ttl time.Duration) (*types.Sandbox, error) {
	if err := m.finalizeBatch(ctx, []*types.Sandbox{sb}, ttl); err != nil {
		return nil, err
	}
	return sb, nil
}

// finalizeBatch stamps identities and persists the claims as one journal
// write; on a failed write every VM in the batch is destroyed — a durable
// claim always matches a live VM, and a batch lands all-or-nothing.
func (m *Manager) finalizeBatch(ctx context.Context, sbs []*types.Sandbox, ttl time.Duration) error {
	now := time.Now()
	for _, sb := range sbs {
		stampIdentity(sb, clampTTL(ttl))
		sb.LastActivity = now
	}
	m.mu.Lock()
	if live := len(m.claimed); m.maxClaims > 0 && live+len(sbs) > m.maxClaims {
		m.mu.Unlock()
		for _, sb := range sbs {
			m.destroy(ctx, sb.VMName)
		}
		return fmt.Errorf("%w: %d live claims, cap %d", ErrQuota, live, m.maxClaims)
	}
	for _, sb := range sbs {
		m.claimed[sb.ID] = sb
	}
	saveErr := m.store.save(m.claimed)
	if saveErr != nil {
		for _, sb := range sbs {
			delete(m.claimed, sb.ID)
		}
	}
	m.mu.Unlock()
	if saveErr != nil {
		for _, sb := range sbs {
			m.destroy(ctx, sb.VMName)
		}
		return fmt.Errorf("persist claim: %w", saveErr)
	}
	for _, sb := range sbs {
		m.recordUsage(ctx, usageEvent{Event: "claim", ID: sb.ID, VMName: sb.VMName, KeyHash: sb.Key.Hash()})
	}
	return nil
}

// exportSource captures a claimed sandbox's state into exportDir. Only this
// window holds the transition lock — it pins the source snapshot against a
// concurrent wake consuming it; the minutes-long clone fan-out after it must
// not block the source's own wake/hibernate traffic.
func (m *Manager) exportSource(ctx context.Context, sb *types.Sandbox, exportDir string) error {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	snap, cleanup, err := m.sourceSnap(ctx, sb)
	if err != nil {
		return err
	}
	defer cleanup()
	return m.eng.SnapshotExport(ctx, snap, exportDir)
}

func (m *Manager) refillOnce(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for _, p := range m.pools {
		target := p.effectiveTarget(now)
		if p.goldenDir == "" {
			if !p.building && time.Now().After(p.nextBuild) {
				p.building = true
				go m.buildGolden(ctx, p)
			}
			continue
		}
		golden := p.goldenDir
		for len(p.warm)+p.refilling < target {
			select {
			case m.refillSem <- struct{}{}:
				p.refilling++
				go m.refillOne(ctx, p, golden)
			default:
				return
			}
		}
	}
}

func (m *Manager) refillOne(ctx context.Context, p *pool, golden string) {
	defer func() { <-m.refillSem }()
	start := time.Now()
	sb, err := m.provision(ctx, p.key, golden)
	m.mu.Lock()
	p.refilling--
	if err == nil {
		p.warm = append(p.warm, sb)
		p.noteLead(time.Since(start))
	}
	m.mu.Unlock()
	if err != nil {
		log.WithFunc("pool.refillOne").Errorf(ctx, err, "refill %s", p.key.Hash())
	}
}

func (m *Manager) buildGolden(ctx context.Context, p *pool) {
	logger := log.WithFunc("pool.buildGolden")
	hash := p.key.Hash()
	name := vmPrefix + "gb-" + hash
	snap := goldenPrefix + hash
	final := filepath.Join(m.goldensDir(), hash)

	err := m.buildGoldenSteps(ctx, p.key, name, snap, final)
	if rmErr := m.eng.SnapshotRemove(ctx, snap); rmErr != nil && err == nil {
		logger.Debugf(ctx, "drop golden snapshot %s: %v", snap, rmErr)
	}
	m.destroy(ctx, name)

	m.mu.Lock()
	p.building = false
	if err == nil {
		p.goldenDir = final
	} else {
		p.nextBuild = time.Now().Add(buildRetryDelay)
	}
	m.mu.Unlock()
	if err != nil {
		logger.Errorf(ctx, err, "build golden for %s", hash)
		return
	}
	logger.Infof(ctx, "golden ready for %s (%s)", hash, p.key.Template)
}

func (m *Manager) buildGoldenSteps(ctx context.Context, key types.PoolKey, name, snap, final string) error {
	if err := m.eng.RunCold(ctx, name, key); err != nil {
		return err
	}
	if _, err := m.probeReady(ctx, name, coldProbeTimeout); err != nil {
		return err
	}
	if err := m.eng.SnapshotSave(ctx, name, snap); err != nil {
		return err
	}
	return m.exportGolden(ctx, snap, final)
}

// exportGolden exports snap into final through a unique sibling *.tmp dir:
// a crash mid-export never leaves a half-written dir that would pass for a
// golden, and concurrent promotes to one name cannot clobber each other's
// staging (last rename wins whole).
func (m *Manager) exportGolden(ctx context.Context, snap, final string) error {
	staging, err := os.MkdirTemp(filepath.Dir(final), filepath.Base(final)+"-*.tmp")
	if err != nil {
		return fmt.Errorf("stage golden: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	tmp := filepath.Join(staging, "export") // cocoon wants the target absent
	if err := m.eng.SnapshotExport(ctx, snap, tmp); err != nil {
		return err
	}
	if err := os.RemoveAll(final); err != nil {
		return fmt.Errorf("clear golden dir: %w", err)
	}
	return os.Rename(tmp, final)
}

// sourceSnap picks the snapshot to export a claimed sandbox from: the wake
// image of a hibernated one (kept — the wake still needs it), or a transient
// capture of a running one, dropped by the returned cleanup.
func (m *Manager) sourceSnap(ctx context.Context, sb *types.Sandbox) (string, func(), error) {
	if sb.HibernateSnap != "" {
		return sb.HibernateSnap, func() {}, nil
	}
	snap := forkPrefix + strings.TrimPrefix(sb.VMName, vmPrefix) + "-" + randHex(3)
	if err := m.eng.SnapshotSave(ctx, sb.VMName, snap); err != nil {
		return "", nil, err
	}
	return snap, func() { m.dropSnap(ctx, snap) }, nil
}

func (m *Manager) reapOnce(ctx context.Context) {
	now := time.Now()
	type victim struct {
		id, vmName, snap string
	}
	m.mu.Lock()
	var expired []victim
	for id, sb := range m.claimed {
		if now.After(sb.Deadline) {
			expired = append(expired, victim{id: id, vmName: sb.VMName, snap: sb.HibernateSnap})
			delete(m.claimed, id)
		}
	}
	var saveErr error
	if len(expired) > 0 {
		saveErr = m.store.save(m.claimed)
	}
	m.mu.Unlock()

	logger := log.WithFunc("pool.reapOnce")
	if saveErr != nil {
		logger.Errorf(ctx, saveErr, "persist reap")
	}
	for _, v := range expired {
		m.destroy(ctx, v.vmName)
		m.dropSnap(ctx, v.snap)
		m.counters.reaps.Add(1)
		m.recordUsage(ctx, usageEvent{Event: "reap", ID: v.id, VMName: v.vmName})
		logger.Infof(ctx, "reaped expired sandbox %s (%s)", v.id, v.vmName)
	}
}

// touch records data-plane activity for the idle policy.
func (m *Manager) touch(sb *types.Sandbox) {
	m.mu.Lock()
	sb.LastActivity = time.Now()
	m.mu.Unlock()
}

// idleOnce hibernates claims idle past their pool's (or the node's)
// threshold. Best-effort: a connection racing the sweep may see its sandbox
// hibernate right after — the next call wakes it transparently.
func (m *Manager) idleOnce(ctx context.Context) {
	if !m.idleEnabled {
		return
	}
	if !m.idleSweep.CompareAndSwap(false, true) {
		return // the previous sweep's hibernates are still draining
	}
	now := time.Now()
	type victim struct{ id, token string }
	var victims []victim
	m.mu.Lock()
	for _, sb := range m.claimed {
		idle := m.idleDefault
		if p, pooled := m.pools[sb.Key]; pooled {
			idle = p.idle // pooled keys never take the node default
		}
		if idle <= 0 || sb.HibernateSnap != "" || now.Sub(sb.LastActivity) < idle {
			continue
		}
		victims = append(victims, victim{sb.ID, sb.Token})
	}
	m.mu.Unlock()
	if len(victims) == 0 {
		m.idleSweep.Store(false)
		return
	}
	// Hibernates are seconds-long engine snapshots: run them off the
	// housekeeping loop so refill ticks keep flowing during a big sweep.
	go func() {
		defer m.idleSweep.Store(false)
		logger := log.WithFunc("pool.idleOnce")
		for _, v := range victims {
			switch err := m.idleHibernate(ctx, v.id, v.token, now); {
			case err == nil:
				logger.Infof(ctx, "idle-hibernated %s", v.id)
			case !errors.Is(err, ErrUnknownSandbox) && !errors.Is(err, errWokeMeanwhile):
				logger.Errorf(ctx, err, "idle-hibernate %s", v.id)
			}
		}
	}()
}

// idleHibernate re-validates a sweep victim under the Transition lock: a
// data-plane connection that arrived after the sweep's snapshot refreshes
// LastActivity, and hibernating underneath it would cut a live call.
func (m *Manager) idleHibernate(ctx context.Context, id, token string, sweepStart time.Time) error {
	sb, ok := m.claim(id, token)
	if !ok {
		return ErrUnknownSandbox
	}
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	m.mu.Lock()
	woke := sb.LastActivity.After(sweepStart) || sb.HibernateSnap != ""
	m.mu.Unlock()
	if woke {
		return errWokeMeanwhile
	}
	return m.hibernateLocked(ctx, sb)
}

// provision creates one claim-ready VM: clone from a golden when available,
// cold-boot the template otherwise. The VM is destroyed on any failure —
// including create-command failures, which can leave a half-created VM
// behind (e.g. the CLI killed by timeout after the VMM spawned).
func (m *Manager) provision(ctx context.Context, key types.PoolKey, golden string) (*types.Sandbox, error) {
	name := vmName(key)
	probeTimeout := claimProbeTimeout
	var err error
	if golden != "" {
		err = m.eng.Clone(ctx, golden, name, key)
	} else {
		err = m.eng.RunCold(ctx, name, key)
		probeTimeout = coldProbeTimeout
	}
	var sock string
	if err == nil {
		sock, err = m.probeReady(ctx, name, probeTimeout)
	}
	if err != nil {
		m.destroy(ctx, name)
		return nil, err
	}
	return &types.Sandbox{VMName: name, Key: key, VsockSocket: sock}, nil
}

// cloneBatch builds count claim-ready clones from an exported snapshot dir,
// bounded by the refill semaphore — forks and refills contend for the same
// node resources, so they share one gate. One failure destroys the batch.
func (m *Manager) cloneBatch(ctx context.Context, key types.PoolKey, dir string, count int) ([]*types.Sandbox, error) {
	children := make([]*types.Sandbox, count)
	errs := make([]error, count)
	var wg sync.WaitGroup
	for i := range count {
		m.refillSem <- struct{}{}
		wg.Go(func() {
			defer func() { <-m.refillSem }()
			children[i], errs[i] = m.provision(ctx, key, dir)
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		for _, child := range children {
			if child != nil {
				m.destroy(ctx, child.VMName)
			}
		}
		return nil, err
	}
	return children, nil
}

// probeReady resolves a VM's vsock socket and waits until its silkd answers,
// returning the socket — the claim-ready gate after create, clone, or restore.
func (m *Manager) probeReady(ctx context.Context, name string, timeout time.Duration) (string, error) {
	sock, err := m.vsockOf(ctx, name)
	if err != nil {
		return "", err
	}
	if err := m.eng.Probe(ctx, sock, timeout); err != nil {
		return "", err
	}
	return sock, nil
}

func (m *Manager) vsockOf(ctx context.Context, name string) (string, error) {
	vms, err := m.eng.List(ctx, name)
	if err != nil {
		return "", err
	}
	for _, vm := range vms {
		if vm.Config.Name != name {
			continue
		}
		if vm.VsockSocket == "" {
			return "", fmt.Errorf("vm %s has no vsock socket", name)
		}
		return vm.VsockSocket, nil
	}
	return "", fmt.Errorf("vm %s not found after create", name)
}

// destroy removes a VM on a cancellation-immune ctx: cleanup is usually
// triggered by a failed or abandoned request, and running `cocoon vm rm` on
// the caller's canceled ctx would no-op and orphan a live VM.
func (m *Manager) destroy(ctx context.Context, name string) {
	ctx = context.WithoutCancel(ctx)
	if err := m.eng.Remove(ctx, name); err != nil {
		log.WithFunc("pool.destroy").Errorf(ctx, err, "remove vm %s", name)
	}
}

func (m *Manager) dropSnap(ctx context.Context, snap string) {
	if snap == "" {
		return
	}
	ctx = context.WithoutCancel(ctx)
	if err := m.eng.SnapshotRemove(ctx, snap); err != nil {
		log.WithFunc("pool.dropSnap").Warnf(ctx, "drop snapshot %s: %v", snap, err)
	}
}

func (m *Manager) claim(id, token string) (*types.Sandbox, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authed(id, token)
}

// commitTransition publishes a hibernate/wake result and persists the
// journal, but only if the claim is still live — Release and reap do not
// take the transition lock, so a sandbox can be destroyed mid-transition
// and publishing then would resurrect state nobody owns. Returns liveness;
// a failed journal write only warns (the live state is authoritative).
func (m *Manager) commitTransition(ctx context.Context, sb *types.Sandbox, snap, sock string) bool {
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb
	var saveErr error
	if live {
		sb.HibernateSnap = snap
		sb.VsockSocket = sock
		saveErr = m.store.save(m.claimed)
	}
	m.mu.Unlock()
	if saveErr != nil {
		log.WithFunc("pool.commitTransition").Warnf(ctx, "persist claims: %v", saveErr)
	}
	return live
}

// authed looks up a claim by id and token; callers hold m.mu.
func (m *Manager) authed(id, token string) (*types.Sandbox, bool) {
	sb := m.claimed[id]
	if sb == nil || subtle.ConstantTimeCompare([]byte(sb.Token), []byte(token)) != 1 {
		return nil, false
	}
	return sb, true
}

func (m *Manager) goldensDir() string {
	return filepath.Join(m.dataDir, "goldens")
}

func stampIdentity(sb *types.Sandbox, ttl time.Duration) {
	sb.ID = "sb_" + randHex(8)
	sb.Token = randHex(16)
	sb.Deadline = time.Now().Add(ttl)
}

func clampTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return defaultTTL
	}
	return min(ttl, maxTTL)
}

func vmName(key types.PoolKey) string {
	return vmPrefix + key.Hash() + "-" + randHex(3)
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b) // never fails per crypto/rand contract
	return hex.EncodeToString(b)
}
