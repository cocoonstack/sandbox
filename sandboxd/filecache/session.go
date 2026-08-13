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

	// VMName is the cocoon VM the sandbox runs as; required only for the
	// dedicated-disk mode (attach acts on the VM, not the guest).
	VMName string
	// DedicatedDisk, when true, hot-attaches a fresh ext4 virtio-blk disk and
	// mounts it at Mount before hydration, so the workspace is isolated from
	// the guest rootfs layer. Requires the Manager to have a Disk driver.
	DedicatedDisk bool
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

// Session runs the push/pull loops for one sandbox↔workspace binding. A
// Manager owns one per claimed sandbox that requested a workspace.
type Session struct {
	sy        *syncer
	cfg       Config
	stop      context.CancelFunc
	done      chan struct{}
	id        string
	dedicated bool

	mu      sync.Mutex
	stopped bool
}

// Manager tracks live workspace sessions, keyed by sandbox id. It mirrors the
// egress-listener bookkeeping in the pool Manager: arm on claim, barrier on
// release, all guarded by mu.
type Manager struct {
	guest Guest
	disk  *diskProvisioner // nil disables dedicated-disk mode
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

// WorkspaceDir returns the on-NAS path a sandbox's workspace token maps to
// under root. Kept here so the arming call site and any inspector agree on
// layout: <root>/<token>.
func WorkspaceDir(root, token string) string {
	return filepath.Join(root, token)
}
