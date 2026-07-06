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
	"slices"
	"strings"
	"sync"
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

	vmPrefix        = "sbx-"
	goldenPrefix    = "sbx-golden-"
	hibernatePrefix = "sbx-hib-"
	vmStateRunning  = "running"
)

var (
	ErrBadKey         = errors.New("invalid pool key")
	ErrNoWarm         = errors.New("no warm sandbox for key")
	ErrUnknownSandbox = errors.New("unknown sandbox or bad token")
	ErrNoEgress       = errors.New("node has no egress attachment (bridge or network)")
)

// Engine is the slice of the cocoon driver the manager consumes.
type Engine interface {
	Clone(ctx context.Context, fromDir, name string, key types.PoolKey) error
	RunCold(ctx context.Context, name string, key types.PoolKey) error
	Remove(ctx context.Context, name string) error
	SnapshotSave(ctx context.Context, vmName, snapName string) error
	SnapshotExport(ctx context.Context, snapName, toDir string) error
	SnapshotRemove(ctx context.Context, snapName string) error
	Hibernate(ctx context.Context, vmName, snapName string) error
	Restore(ctx context.Context, vmName, snapRef string) error
	List(ctx context.Context, filters ...string) ([]types.VMRecord, error)
	Probe(ctx context.Context, vsockSocket string, timeout time.Duration) error
}

// Manager owns the node's pools, claims, and their persistence.
type Manager struct {
	eng     Engine
	dataDir string
	egress  bool
	store   *store

	mu      sync.Mutex
	pools   map[types.PoolKey]*pool
	claimed map[string]*types.Sandbox

	refillSem chan struct{}
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
	key    types.PoolKey
	target int

	goldenDir string
	building  bool
	nextBuild time.Time
	warm      []*types.Sandbox
	refilling int
}

// NewManager builds a manager from the node config.
func NewManager(cfg *config.Config, eng Engine) (*Manager, error) {
	m := &Manager{
		eng:       eng,
		dataDir:   cfg.DataDir,
		egress:    cfg.HasEgress(),
		store:     newStore(cfg.DataDir),
		pools:     make(map[types.PoolKey]*pool, len(cfg.Pools)),
		claimed:   map[string]*types.Sandbox{},
		refillSem: make(chan struct{}, maxConcurrentRefills),
	}
	if err := os.MkdirAll(m.goldensDir(), 0o750); err != nil {
		return nil, fmt.Errorf("create goldens dir: %w", err)
	}
	for _, spec := range cfg.Pools {
		m.pools[spec.PoolKey] = &pool{key: spec.PoolKey, target: spec.Warm}
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
	if err := m.validate(key); err != nil {
		return nil, err
	}
	m.mu.Lock()
	var sb *types.Sandbox
	if p := m.pools[key]; p != nil {
		if n := len(p.warm); n > 0 {
			sb = p.warm[n-1]
			p.warm = p.warm[:n-1]
		}
	}
	m.mu.Unlock()
	if sb == nil {
		return nil, ErrNoWarm
	}
	return m.finalize(ctx, sb, ttl)
}

// ClaimProvision creates a claim-ready sandbox (golden clone or cold boot).
func (m *Manager) ClaimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
	if err := m.validate(key); err != nil {
		return nil, err
	}
	m.mu.Lock()
	golden := ""
	if p := m.pools[key]; p != nil {
		golden = p.goldenDir
	}
	m.mu.Unlock()

	sb, err := m.provision(ctx, key, golden)
	if err != nil {
		return nil, err
	}
	return m.finalize(ctx, sb, ttl)
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
	stampIdentity(sb, clampTTL(ttl))
	m.mu.Lock()
	m.claimed[sb.ID] = sb
	saveErr := m.store.save(m.claimed)
	if saveErr != nil {
		delete(m.claimed, sb.ID)
	}
	m.mu.Unlock()
	if saveErr != nil {
		m.destroy(ctx, sb.VMName)
		return nil, fmt.Errorf("persist claim: %w", saveErr)
	}
	return sb, nil
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
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if sb.HibernateSnap == "" {
		return sb.VsockSocket, nil
	}
	// See Hibernate: a half-restored VM is worse than a wasted wake.
	ctx = context.WithoutCancel(ctx)
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
	return sock, nil
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
	m.mu.Lock()
	for id, sb := range claims {
		rec, ok := live[sb.VMName]
		switch {
		case ok && rec.State == vmStateRunning:
			sb.VsockSocket = rec.VsockSocket
		case ok && sb.HibernateSnap != "":
			// Hibernated: the VM is stopped by design and wakes on demand.
		default:
			continue
		}
		m.claimed[id] = sb
		owned[sb.VMName] = true
	}
	saveErr := m.store.save(m.claimed)
	for _, p := range m.pools {
		dir := filepath.Join(m.goldensDir(), p.key.Hash())
		if fi, statErr := os.Stat(dir); statErr == nil && fi.IsDir() {
			p.goldenDir = dir
		}
	}
	m.mu.Unlock()

	logger := log.WithFunc("pool.Reconcile")
	for name := range live {
		if strings.HasPrefix(name, vmPrefix) && !owned[name] {
			m.destroy(ctx, name)
			logger.Infof(ctx, "removed stale VM %s", name)
		}
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
			Target:    p.target,
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

func (m *Manager) refillOnce(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.pools {
		if p.goldenDir == "" {
			if !p.building && time.Now().After(p.nextBuild) {
				p.building = true
				go m.buildGolden(ctx, p)
			}
			continue
		}
		golden := p.goldenDir
		for len(p.warm)+p.refilling < p.target {
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
	sb, err := m.provision(ctx, p.key, golden)
	m.mu.Lock()
	p.refilling--
	if err == nil {
		p.warm = append(p.warm, sb)
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
	// A crash mid-export must never leave a half-written dir that Reconcile
	// would adopt as a valid golden.
	tmp := final + ".tmp"
	if err := os.RemoveAll(tmp); err != nil {
		return fmt.Errorf("clear golden tmp: %w", err)
	}
	if err := m.eng.SnapshotExport(ctx, snap, tmp); err != nil {
		return err
	}
	if err := os.RemoveAll(final); err != nil {
		return fmt.Errorf("clear golden dir: %w", err)
	}
	return os.Rename(tmp, final)
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
		logger.Infof(ctx, "reaped expired sandbox %s (%s)", v.id, v.vmName)
	}
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

// dropSnap deletes a no-longer-needed memory snapshot; empty is a no-op.
func (m *Manager) dropSnap(ctx context.Context, snap string) {
	if snap == "" {
		return
	}
	ctx = context.WithoutCancel(ctx)
	if err := m.eng.SnapshotRemove(ctx, snap); err != nil {
		log.WithFunc("pool.dropSnap").Warnf(ctx, "drop snapshot %s: %v", snap, err)
	}
}

// claim authenticates a sandbox id/token pair and returns its record.
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
