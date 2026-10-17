package pool

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/projecteru2/core/log"
	"golang.org/x/sync/singleflight"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const (
	readerIdle         = 30 * time.Second
	readerSetupTimeout = 5 * time.Minute
	readerDir          = "readers"
	readerCowDisk      = "rcow"
	readerCowFile      = "cow.raw"
	readerSidecarFile  = "cocoon.json"
	layerRole          = "layer"
)

var linkFile = os.Link

type readerSet struct {
	key     types.PoolKey
	mu      sync.Mutex
	live    map[string]*reader
	flights singleflight.Group
}

type reader struct {
	claim  string
	source string
	vm     string
	sock   string
	dir    string
	conns  int
	idle   *time.Timer
}

// DialReader opens a silkd connection to a sleeping claim's reader VM without waking the claim.
func (m *Manager) DialReader(ctx context.Context, id, token string) (conn net.Conn, source string, err error) {
	sb, ok := m.claim(id, token)
	if !ok {
		return nil, "", ErrUnknownSandbox
	}
	if m.readers == nil {
		return nil, "", ErrNoReader
	}
	r, err := m.acquireReader(ctx, sb)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", ErrNoReader, err)
	}
	conn, err = m.eng.DialReader(ctx, r.sock)
	if err != nil {
		m.releaseReader(r)
		m.dropReader(ctx, r, true)
		return nil, "", fmt.Errorf("%w: %w", ErrNoReader, err)
	}
	return &heldConn{Conn: conn, release: sync.OnceFunc(func() { m.releaseReader(r) })}, r.source, nil
}

// SleepingSource names the hibernate snapshot or archive record a reader would serve, or "" when the claim is awake, failed or mid-transition.
func (m *Manager) SleepingSource(id, token string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb, ok := m.authed(id, token)
	if !ok {
		return ""
	}
	return sleepSource(sb)
}

func (m *Manager) acquireReader(ctx context.Context, sb *types.Sandbox) (*reader, error) {
	rs := m.readers
	for {
		m.mu.Lock()
		want := sleepSource(sb)
		m.mu.Unlock()
		if want == "" {
			return nil, errNotSleeping
		}
		rs.mu.Lock()
		r := rs.live[sb.ID]
		if r != nil && r.source == want {
			r.conns++
			if r.idle != nil {
				r.idle.Stop()
				r.idle = nil
			}
			rs.mu.Unlock()
			return r, nil
		}
		rs.mu.Unlock()
		if r != nil {
			m.dropReader(ctx, r, true)
		}
		_, err, _ := rs.flights.Do(sb.ID, func() (any, error) {
			setupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readerSetupTimeout)
			defer cancel()
			return nil, m.ensureReader(setupCtx, sb)
		})
		if err != nil {
			return nil, err
		}
	}
}

func (m *Manager) releaseReader(r *reader) {
	rs := m.readers
	rs.mu.Lock()
	defer rs.mu.Unlock()
	r.conns--
	if r.conns == 0 && rs.live[r.claim] == r {
		r.idle = time.AfterFunc(readerIdle, func() { m.dropReader(context.Background(), r, false) })
	}
}

func (m *Manager) dropReader(ctx context.Context, r *reader, force bool) {
	rs := m.readers
	rs.mu.Lock()
	if rs.live[r.claim] != r || (!force && r.conns > 0) {
		rs.mu.Unlock()
		return
	}
	delete(rs.live, r.claim)
	rs.mu.Unlock()
	m.destroyReader(context.WithoutCancel(ctx), r)
}

func (m *Manager) ensureReader(ctx context.Context, sb *types.Sandbox) error {
	m.mu.Lock()
	want := sleepSource(sb)
	m.mu.Unlock()
	rs := m.readers
	rs.mu.Lock()
	r := rs.live[sb.ID]
	rs.mu.Unlock()
	if r != nil && r.source == want {
		return nil
	}
	return m.openReader(ctx, sb)
}

func (m *Manager) openReader(ctx context.Context, sb *types.Sandbox) error {
	start := time.Now()
	r := &reader{claim: sb.ID, dir: filepath.Join(m.dataDir, readerDir, sb.ID+"-"+randHex(3))}
	layers, err := m.readerSource(ctx, sb, r)
	if err != nil {
		_ = os.RemoveAll(r.dir)
		return err
	}
	vm, err := m.takeReaderVM()
	if err != nil {
		_ = os.RemoveAll(r.dir)
		return err
	}
	r.vm, r.sock = vm.VMName, vm.VsockSocket
	if err = m.startReader(ctx, r, layers); err != nil {
		m.destroyReader(ctx, r)
		return err
	}
	rs := m.readers
	rs.mu.Lock()
	old := rs.live[sb.ID]
	rs.live[sb.ID] = r
	r.idle = time.AfterFunc(readerIdle, func() { m.dropReader(context.Background(), r, false) })
	rs.mu.Unlock()
	if old != nil {
		m.destroyReader(context.WithoutCancel(ctx), old)
	}
	log.WithFunc("pool.openReader").Infof(ctx, "reader %s for sleeping claim %s ready in %s", r.vm, sb.ID, time.Since(start).Round(time.Millisecond))
	return nil
}

func (m *Manager) readerSource(ctx context.Context, sb *types.Sandbox, r *reader) ([]string, error) {
	archived, err := m.captureSleeping(ctx, sb, r)
	if err != nil {
		return nil, err
	}
	sidecar := filepath.Join(r.dir, readerSidecarFile)
	if archived {
		if sidecar, err = m.fetchReaderDisk(ctx, r); err != nil {
			return nil, err
		}
	}
	return readerLayers(sidecar)
}

// captureSleeping copies a hibernated snapshot under the Transition lock, which the wake that consumes the snapshot takes.
func (m *Manager) captureSleeping(ctx context.Context, sb *types.Sandbox, r *reader) (archived bool, err error) {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if (sb.HibernateSnap == "" && sb.ArchiveCk == "") || !sb.ReaderSafe {
		return false, errNotSleeping
	}
	if sb.HibernateSnap == "" {
		if err = m.tenantRemoved(ctx, sb.Tenant); err != nil {
			return false, err
		}
	}
	if err = m.diskErr(); err != nil {
		return false, err
	}
	if sb.HibernateSnap == "" {
		r.source = sb.ArchiveCk
		return true, nil
	}
	r.source = sb.HibernateSnap
	if err = m.eng.SnapshotExport(ctx, r.source, r.dir); err != nil {
		return false, fmt.Errorf("export snapshot: %w", err)
	}
	return false, utils.RemoveDirEntries(r.dir, func(name string) bool { return name != readerCowFile && name != readerSidecarFile })
}

func (m *Manager) fetchReaderDisk(ctx context.Context, r *reader) (string, error) {
	if err := os.MkdirAll(r.dir, 0o750); err != nil {
		return "", err
	}
	for _, name := range []string{readerCowFile, readerSidecarFile} {
		if err := m.ckpts.FetchFile(ctx, r.source, name, filepath.Join(r.dir, name)); err != nil {
			return "", archiveFetchErr(err)
		}
	}
	return filepath.Join(r.dir, readerSidecarFile), nil
}

func (m *Manager) takeReaderVM() (*types.Sandbox, error) {
	m.mu.Lock()
	var sb *types.Sandbox
	if p, ok := m.activePool(m.readers.key); ok {
		sb = p.popWarm()
	}
	m.mu.Unlock()
	m.kickRefill()
	if sb == nil {
		return nil, ErrNoWarm
	}
	return sb, nil
}

func (m *Manager) startReader(ctx context.Context, r *reader, layers []string) error {
	disks := engine.ReaderDisks{Cow: readerCowDisk}
	for i, blob := range layers {
		name := fmt.Sprintf("rl%d", i)
		linked := filepath.Join(r.dir, name+".erofs")
		if err := linkFile(blob, linked); err != nil {
			if err = utils.CloneFile(blob, linked); err != nil {
				return fmt.Errorf("link layer %d into the reader dir: %w", i, err)
			}
		}
		if err := m.eng.DiskAttach(ctx, r.vm, engine.VolumeSpec{Name: name, Path: linked}); err != nil {
			return fmt.Errorf("attach layer %d: %w", i, err)
		}
		disks.Layers = append(disks.Layers, name)
	}
	if err := m.eng.DiskAttach(ctx, r.vm, engine.VolumeSpec{Name: readerCowDisk, Path: filepath.Join(r.dir, readerCowFile), RW: true}); err != nil {
		return fmt.Errorf("attach claim disk: %w", err)
	}
	return m.eng.StartReader(ctx, r.sock, disks)
}

func (m *Manager) destroyReader(ctx context.Context, r *reader) {
	m.destroy(ctx, r.vm)
	if err := os.RemoveAll(r.dir); err != nil {
		log.WithFunc("pool.destroyReader").Warnf(ctx, "remove reader dir %s: %v", r.dir, err)
	}
}

// sleepSource names a settled sleep's snapshot or record; callers hold m.mu.
func sleepSource(sb *types.Sandbox) string {
	if sb.Failed != "" || sb.PendingSnap != "" || !sb.ReaderSafe {
		return ""
	}
	return cmp.Or(sb.HibernateSnap, sb.ArchiveCk)
}

func archiveFetchErr(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return errNotSleeping
	}
	return fmt.Errorf("fetch archive: %w", err)
}

func readerLayers(sidecar string) ([]string, error) {
	raw, err := os.ReadFile(sidecar) //nolint:gosec // path from our own export or fetch
	if err != nil {
		return nil, fmt.Errorf("read snapshot sidecar: %w", err)
	}
	var meta struct {
		StorageConfigs []struct {
			Path string `json:"path"`
			Role string `json:"role"`
		} `json:"storage_configs"`
	}
	if err = json.Unmarshal(raw, &meta); err != nil {
		return nil, fmt.Errorf("parse snapshot sidecar: %w", err)
	}
	var layers []string
	for _, sc := range meta.StorageConfigs {
		if sc.Role != layerRole {
			continue
		}
		if _, err = os.Stat(sc.Path); err != nil {
			return nil, fmt.Errorf("layer blob: %w", err)
		}
		layers = append(layers, sc.Path)
	}
	if len(layers) == 0 {
		return nil, errors.New("snapshot sidecar names no layers")
	}
	return layers, nil
}
