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

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	readerIdle         = 30 * time.Second
	readerSetupTimeout = 5 * time.Minute
	readerDir          = "readers"
	envelopeFile       = "snapshot.json"
)

// DialReader opens a silkd connection to a sleeping claim's reader VM without waking the claim.
func (m *Manager) DialReader(ctx context.Context, id, token string) (conn net.Conn, source string, err error) {
	sb, ok := m.claim(id, token)
	if !ok {
		return nil, "", ErrUnknownSandbox
	}
	fenced, err := m.fenceWatch(sb)
	if err != nil {
		return nil, "", err
	}
	if !m.view.Load().poolReaders[sb.Key] {
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
	stop := CloseOnFence(fenced, conn)
	return &heldConn{Conn: conn, release: sync.OnceFunc(func() { stop(); m.releaseReader(r) })}, r.source, nil
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
	archived, err := m.stageReader(ctx, sb, r)
	if err != nil {
		_ = os.RemoveAll(r.dir)
		return err
	}
	vm, err := m.takeReaderVM(sb.Key)
	if err != nil {
		_ = os.RemoveAll(r.dir)
		return err
	}
	r.vm, r.sock = vm.VMName, vm.VsockSocket
	if err = m.startReader(ctx, sb, r, archived); err != nil {
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

func (m *Manager) stageReader(ctx context.Context, sb *types.Sandbox, r *reader) (archived bool, err error) {
	sb.Transition.Lock()
	r.source, archived, err = m.readerSource(ctx, sb)
	sb.Transition.Unlock()
	if err != nil || !archived {
		return archived, err
	}
	return true, m.fetchReaderArchive(ctx, r)
}

// readerSource names the sleep a reader would serve; callers hold sb.Transition, which the wake that consumes the sleep takes.
func (m *Manager) readerSource(ctx context.Context, sb *types.Sandbox) (source string, archived bool, err error) {
	if (sb.HibernateSnap == "" && sb.ArchiveCk == "") || !sb.ReaderSafe {
		return "", false, errNotSleeping
	}
	archived = sb.HibernateSnap == ""
	if archived {
		if err = m.tenantRemoved(ctx, sb.Tenant); err != nil {
			return "", false, err
		}
	}
	if err = m.diskErr(); err != nil {
		return "", false, err
	}
	return cmp.Or(sb.HibernateSnap, sb.ArchiveCk), archived, nil
}

func (m *Manager) fetchReaderArchive(ctx context.Context, r *reader) error {
	if err := os.MkdirAll(r.dir, 0o750); err != nil {
		return err
	}
	if err := m.fetchReaderFile(ctx, r, envelopeFile); err != nil {
		return err
	}
	names, err := diskAttachFiles(filepath.Join(r.dir, envelopeFile))
	if err != nil {
		return err
	}
	for _, name := range names {
		if err = m.fetchReaderFile(ctx, r, name); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) fetchReaderFile(ctx context.Context, r *reader, name string) error {
	if err := m.ckpts.FetchFile(ctx, r.source, name, filepath.Join(r.dir, name)); err != nil {
		return archiveFetchErr(err)
	}
	return nil
}

func (m *Manager) takeReaderVM(key types.PoolKey) (*types.Sandbox, error) {
	m.mu.Lock()
	var sb *types.Sandbox
	if p, ok := m.activePool(key); ok {
		sb = p.popWarm()
	}
	m.mu.Unlock()
	m.kickRefill()
	if sb == nil {
		return nil, ErrNoWarm
	}
	return sb, nil
}

func (m *Manager) startReader(ctx context.Context, sb *types.Sandbox, r *reader, archived bool) error {
	disk, err := m.attachReaderDisk(ctx, sb, r, archived)
	if err != nil {
		return fmt.Errorf("attach claim disk: %w", err)
	}
	return m.eng.StartReader(ctx, r.sock, disk)
}

func (m *Manager) attachReaderDisk(ctx context.Context, sb *types.Sandbox, r *reader, archived bool) (string, error) {
	if archived {
		defer func() { _ = os.RemoveAll(r.dir) }()
		return m.eng.DiskAttachDir(ctx, r.vm, r.dir)
	}
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if source, _, err := m.readerSource(ctx, sb); err != nil || source != r.source {
		return "", cmp.Or(err, errNotSleeping)
	}
	return m.eng.DiskAttachSnapshot(ctx, r.vm, r.source)
}

func (m *Manager) destroyReader(ctx context.Context, r *reader) {
	m.destroy(ctx, r.vm)
	if err := os.RemoveAll(r.dir); err != nil {
		log.WithFunc("pool.destroyReader").Warnf(ctx, "remove reader dir %s: %v", r.dir, err)
	}
}

type readerSet struct {
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

func diskAttachFiles(envelope string) ([]string, error) {
	raw, err := os.ReadFile(envelope) //nolint:gosec // path from our own fetch
	if err != nil {
		return nil, fmt.Errorf("read archive envelope: %w", err)
	}
	var doc struct {
		Files []string `json:"disk_attach_files"`
	}
	if err = json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse archive envelope: %w", err)
	}
	if len(doc.Files) == 0 {
		return nil, errors.New("archive envelope lists no disk_attach_files")
	}
	for _, name := range doc.Files {
		if filepath.Base(name) != name || !filepath.IsLocal(name) {
			return nil, fmt.Errorf("archive envelope lists disk attach file %q outside its directory", name)
		}
	}
	return doc.Files, nil
}
