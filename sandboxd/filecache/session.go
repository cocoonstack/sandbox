package filecache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/projecteru2/core/log"
)

// Config tunes the sync cadence. Zero values fall back to defaults that keep
// cross-writer visibility within a 30s budget (push ≤8s + poll ≤4s + NAS
// attribute cache; the workspace NAS mount should use actimeo=1).
type Config struct {
	PushInterval time.Duration
	PullInterval time.Duration
	Mount        string // guest workspace dir; default /workspace

	// VMName is the cocoon VM the sandbox runs as; required for the
	// dedicated-disk and uncached modes (attach acts on the VM, not the guest).
	VMName string
	// DedicatedDisk, when true, hot-attaches a fresh ext4 virtio-blk disk and
	// mounts it at Mount before hydration, so the workspace is isolated from
	// the guest rootfs layer. Requires the Manager to have a Disk driver.
	DedicatedDisk bool
	// NoCache turns the cache off for this session: instead of a local disk
	// the node syncs, the guest mounts the shared workspace itself over
	// vhost-user-fs, so its writes reach the NAS as they happen and peers see
	// them without waiting for a push. There is no journal, no hydration, and
	// no barrier — and no local-disk speed either. Requires the Manager to
	// have a Share driver; without one the request falls back to the cache.
	NoCache bool
}

func (c Config) withDefaults() Config {
	if c.PushInterval <= 0 {
		c.PushInterval = 8 * time.Second
	}
	if c.PullInterval <= 0 {
		c.PullInterval = 4 * time.Second
	}
	if c.Mount == "" {
		c.Mount = "/workspace"
	}
	return c
}

// Session is one sandbox↔workspace binding. A cached session runs the
// push/pull loops over a local workspace disk; an uncached one holds the
// vhost-user-fs share instead and has no loops to run. A Manager owns one per
// claimed sandbox that requested a workspace.
type Session struct {
	sy        *syncer // nil when uncached: there is nothing to sync
	cfg       Config
	stop      context.CancelFunc
	done      chan struct{}
	id        string
	dedicated bool
	share     *shareHandle // non-nil when uncached
	sock      string       // guest vsock, kept for the uncached teardown

	mu      sync.Mutex
	stopped bool
}

// Manager tracks live workspace sessions, keyed by sandbox id. It mirrors the
// egress-listener bookkeeping in the pool Manager: arm on claim, barrier on
// release, all guarded by mu.
type Manager struct {
	guest Guest
	disk  *diskProvisioner  // nil disables dedicated-disk mode
	share *shareProvisioner // nil disables the uncached mode
	mu    sync.Mutex
	byID  map[string]*Session
}

// NewManager returns a filecache manager driving the guest via g.
func NewManager(g Guest) *Manager {
	return &Manager{guest: g, byID: map[string]*Session{}}
}

// EnableDedicatedDisk turns on the dedicated-workspace-disk mode: sessions that
// request it get a fresh ext4 virtio-blk disk (backed by an image under root,
// sized sizeMB) attached and mounted before hydration. Call once before
// serving. Without this, DedicatedDisk requests fall back to the rootfs layer.
func (m *Manager) EnableDedicatedDisk(d Disk, root string, sizeMB int) {
	m.disk = &diskProvisioner{disk: d, root: root, sizeMB: sizeMB}
}

// EnableShare turns on the uncached mode: sessions that ask for it get the
// shared workspace mounted straight into the guest over a vhost-user-fs share
// served by virtiofsd (binary), with its socket under runDir. Call once before
// serving. Without this, NoCache requests fall back to the cache.
func (m *Manager) EnableShare(s Share, binary, runDir string) {
	m.share = &shareProvisioner{share: s, guest: m.guest, binary: binary, runDir: runDir}
}

// Arm binds sandbox id (reachable at vsockSocket) to the NAS workspace ws as
// writer, hydrates the guest, and starts the sync loops. Bootstrap runs
// synchronously so the guest workspace is populated before Arm returns; the
// loops then run in the background until Barrier. Arming an id already armed
// is a no-op.
func (m *Manager) Arm(ctx context.Context, id, vsockSocket, ws, writer string, cfg Config) error {
	cfg = cfg.withDefaults()
	m.mu.Lock()
	if _, ok := m.byID[id]; ok {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	if err := os.MkdirAll(ws, 0o755); err != nil { //nolint:gosec // shared NAS tree; peer nodes traverse it
		return fmt.Errorf("workspace dir: %w", err)
	}
	// Uncached mode: the guest mounts ws itself, so there is no disk to attach,
	// nothing to hydrate, and no loops to run. Everything below is the cache.
	if cfg.NoCache && m.share != nil {
		return m.armShare(ctx, id, vsockSocket, ws, cfg)
	}
	// Dedicated-disk mode: attach and mount a fresh ext4 virtio-blk disk at the
	// mount before hydration, so the workspace is isolated from the rootfs
	// layer. Falls back to the rootfs layer if the feature is off.
	dedicated := cfg.DedicatedDisk && m.disk != nil
	if dedicated {
		if err := m.disk.attachAndMount(ctx, cfg.VMName, vsockSocket, cfg.Mount); err != nil {
			return fmt.Errorf("workspace disk: %w", err)
		}
	}
	sy := newSyncer(m.guest, vsockSocket, cfg.Mount, ws, writer)
	if err := sy.bootstrap(ctx); err != nil {
		if dedicated {
			m.disk.unmountAndDetach(ctx, cfg.VMName, vsockSocket, cfg.Mount)
		}
		return fmt.Errorf("bootstrap: %w", err)
	}

	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	sess := &Session{sy: sy, cfg: cfg, stop: cancel, done: make(chan struct{}), id: id, dedicated: dedicated}
	m.mu.Lock()
	m.byID[id] = sess
	m.mu.Unlock()
	go sess.run(loopCtx)
	log.WithFunc("filecache.Arm").Infof(ctx, "workspace armed for %s (ws=%s writer=%s)", id, ws, writer)
	return nil
}

// armShare binds the sandbox to ws over a vhost-user-fs share and records the
// session so the barrier can tear the share down. No goroutine runs for it:
// the guest talks to the NAS directly, so there is nothing for the node to
// carry between the two.
func (m *Manager) armShare(ctx context.Context, id, vsockSocket, ws string, cfg Config) error {
	h, err := m.share.serveAndMount(ctx, id, cfg.VMName, vsockSocket, ws, cfg.Mount)
	if err != nil {
		return err
	}
	sess := &Session{cfg: cfg, stop: func() {}, done: closedChan(), id: id, share: h, sock: vsockSocket}
	m.mu.Lock()
	m.byID[id] = sess
	m.mu.Unlock()
	log.WithFunc("filecache.Arm").Infof(ctx, "workspace shared uncached for %s (ws=%s)", id, ws)
	return nil
}

// PreAttach provisions and hot-attaches a workspace disk to a warm VM ahead
// of any claim (no in-guest mount — a workspace-less claim must not see a
// surprise /workspace). No-op unless dedicated-disk mode is enabled.
func (m *Manager) PreAttach(ctx context.Context, vmName string) error {
	if m == nil || m.disk == nil {
		return nil
	}
	return m.disk.preAttach(ctx, vmName)
}

// CleanupVM removes the workspace disk image of a VM that is gone without a
// barrier (warm trim, quarantine, failed claim). Idempotent.
func (m *Manager) CleanupVM(vmName string) {
	if m == nil || m.disk == nil {
		return
	}
	m.disk.cleanupVM(vmName)
}

// Barrier stops the sync loops for id and runs a final push so every local
// change is on the NAS and visible to other clients (close-to-open) before it
// returns. A no-op if id has no session. Bounded internally so a hung guest
// cannot wedge release.
func (m *Manager) Barrier(ctx context.Context, id string) {
	m.mu.Lock()
	sess := m.byID[id]
	delete(m.byID, id)
	m.mu.Unlock()
	if sess == nil {
		return
	}
	sess.mu.Lock()
	sess.stopped = true
	sess.mu.Unlock()
	sess.stop()
	<-sess.done // loops exited; no concurrent cycle

	// An uncached session has no delta to publish — every write already went
	// to the NAS — so the barrier is just teardown of the share.
	if sess.share != nil {
		bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		m.share.unmountAndStop(bctx, sess.share, sess.cfg.VMName, sess.sock, sess.cfg.Mount)
		return
	}

	// The barrier is the durability edge: a workspace file not on the NAS when
	// the VM dies is lost. Budget for a large final delta (a 100k-file
	// node_modules publishes for minutes) instead of truncating at 60s.
	bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Minute)
	defer cancel()
	if puts, dels, err := sess.sy.pushCycle(bctx, 0); err != nil { // settle=0: the barrier must publish hot files too
		log.WithFunc("filecache.Barrier").Errorf(ctx, err, "final push for %s", id)
	} else if puts+dels > 0 {
		log.WithFunc("filecache.Barrier").Infof(ctx, "barrier %s: %d puts %d dels", id, puts, dels)
	}
	// Dedicated disk: after the final push has drained the workspace to the
	// NAS, unmount and detach it (the VM is about to be torn down anyway).
	if sess.dedicated && m.disk != nil {
		m.disk.unmountAndDetach(bctx, sess.cfg.VMName, sess.sy.sock, sess.cfg.Mount)
	}
}

// Has reports whether id currently has a workspace session.
func (m *Manager) Has(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.byID[id]
	return ok
}

// run drives the push/pull/heartbeat tickers until ctx is canceled.
func (s *Session) run(ctx context.Context) {
	defer close(s.done)
	pushT := time.NewTicker(s.cfg.PushInterval)
	pullT := time.NewTicker(s.cfg.PullInterval)
	hbT := time.NewTicker(10 * time.Second)
	defer pushT.Stop()
	defer pullT.Stop()
	defer hbT.Stop()
	logger := log.WithFunc("filecache.run")
	for {
		select {
		case <-ctx.Done():
			return
		case <-pushT.C:
			if puts, dels, err := s.sy.pushCycle(ctx, settleWindow); err != nil {
				if ctx.Err() == nil {
					logger.Warnf(ctx, "push %s: %v", s.id, err)
				}
			} else if puts+dels > 0 {
				logger.Debugf(ctx, "push %s: %d puts %d dels", s.id, puts, dels)
			}
		case <-pullT.C:
			if n, puts, dels, err := s.sy.pullCycle(ctx); err != nil {
				if ctx.Err() == nil {
					logger.Warnf(ctx, "pull %s: %v", s.id, err)
				}
			} else if n > 0 {
				logger.Debugf(ctx, "pull %s: %d entries (%d puts %d dels)", s.id, n, puts, dels)
			}
		case <-hbT.C:
			s.sy.heartbeat()
		}
	}
}

// closedChan returns an already-closed channel, so a session with no loops
// satisfies the same "wait for the loops to exit" barrier step as one with.
func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// WorkspaceDir returns the on-NAS path a sandbox's workspace token maps to
// under root. Kept here so the arming call site and any inspector agree on
// layout: <root>/<token>.
func WorkspaceDir(root, token string) string {
	return filepath.Join(root, token)
}
