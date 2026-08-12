package e2e

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sdk/go/silkd/silkdtest"
)

// fakeEngine replaces only the cocoon CLI: every "VM" it creates is a
// silkdtest daemon behind a real hybrid-vsock UDS, so probing and the data
// plane run the production code paths end to end.
type fakeEngine struct {
	real *engine.Engine
	dir  string

	mu        sync.Mutex
	listeners map[string]io.Closer
	socks     map[string]string
	seq       int
}

func newFakeEngine(dir string) *fakeEngine {
	return &fakeEngine{
		real:      engine.New("cocoon", nil, nil, false, ""),
		dir:       dir,
		listeners: map[string]io.Closer{},
		socks:     map[string]string{},
	}
}

func (f *fakeEngine) Clone(_ context.Context, _, name string, _ types.PoolKey) (types.VMRecord, error) {
	return f.createRecord(name)
}

func (f *fakeEngine) CloneSnap(_ context.Context, _, name string, _ types.PoolKey) (types.VMRecord, error) {
	return f.createRecord(name)
}

func (f *fakeEngine) RunCold(_ context.Context, name string, _ types.PoolKey) (types.VMRecord, error) {
	return f.createRecord(name)
}

func (f *fakeEngine) Remove(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if l := f.listeners[name]; l != nil {
		_ = l.Close()
	}
	delete(f.listeners, name)
	delete(f.socks, name)
	return nil
}

func (f *fakeEngine) ReconcileStaleCreate(context.Context, string) (engine.StaleCreateOutcome, error) {
	return engine.StaleCreateNotCreating, nil
}

func (f *fakeEngine) SnapshotSave(_ context.Context, _, _ string) error { return nil }

func (f *fakeEngine) SnapshotExport(_ context.Context, _, toDir string) error {
	return os.MkdirAll(toDir, 0o750)
}

func (f *fakeEngine) SnapshotRemove(_ context.Context, _ string) error { return nil }

func (f *fakeEngine) SnapshotList(_ context.Context) ([]string, error) { return nil, nil }

// Hibernate closes the VM's silkd listener (the "VMM died") and Restore
// brings a fresh one up, mirroring the real stop/resume observable from the
// control plane.
func (f *fakeEngine) Hibernate(ctx context.Context, name, _ string) error {
	return f.Remove(ctx, name)
}

func (f *fakeEngine) Restore(_ context.Context, name, _ string) (string, error) {
	return f.create(name)
}

func (f *fakeEngine) List(_ context.Context, filters ...string) ([]types.VMRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var vms []types.VMRecord
	for name, sock := range f.socks {
		if len(filters) > 0 && !slices.Contains(filters, name) {
			continue
		}
		vms = append(vms, types.VMRecord{State: "running", VsockSocket: sock, Config: types.VMConfig{Name: name}})
	}
	return vms, nil
}

func (f *fakeEngine) Probe(ctx context.Context, vsockSocket string, timeout time.Duration) error {
	return f.real.Probe(ctx, vsockSocket, timeout)
}

func (f *fakeEngine) DialGuestPort(context.Context, string, uint16) (net.Conn, error) {
	return nil, fmt.Errorf("fakeEngine has no guest port")
}

func (f *fakeEngine) InstallCACert(context.Context, string, []byte) error { return nil }

func (f *fakeEngine) DiskAttach(context.Context, string, engine.VolumeSpec) error { return nil }

func (f *fakeEngine) MountVolume(context.Context, string, string, string) error { return nil }

func (f *fakeEngine) create(name string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	sock := filepath.Join(f.dir, fmt.Sprintf("v%d.sock", f.seq))
	l, err := silkdtest.ListenHybrid(sock, 2048)
	if err != nil {
		return "", err
	}
	f.listeners[name] = l
	f.socks[name] = sock
	return sock, nil
}

func (f *fakeEngine) createRecord(name string) (types.VMRecord, error) {
	sock, err := f.create(name)
	if err != nil {
		return types.VMRecord{}, err
	}
	return types.VMRecord{VsockSocket: sock, Config: types.VMConfig{Name: name}}, nil
}
