package e2e

import (
	"context"
	"errors"
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

type fakeVolumeMount struct {
	name string
	rw   bool
}

type fakeEngine struct {
	real *engine.Engine
	dir  string

	mu        sync.Mutex
	listeners map[string]io.Closer
	socks     map[string]string
	volumeOps []string
	volumeVMs map[string]bool
	guestEnvs map[string]string
	mounts    map[string]map[string]fakeVolumeMount
	seq       int

	readerRoot     string
	readerStarts   []string
	readerAttaches []string
}

func newFakeEngine(dir string) *fakeEngine {
	return &fakeEngine{
		real:      engine.New("cocoon", nil, nil, false, false, ""),
		dir:       dir,
		listeners: map[string]io.Closer{},
		socks:     map[string]string{},
		volumeVMs: map[string]bool{},
		guestEnvs: map[string]string{},
		mounts:    map[string]map[string]fakeVolumeMount{},
	}
}

func (f *fakeEngine) CheckLiveVolumeSupport(context.Context) error { return nil }

func (f *fakeEngine) Clone(_ context.Context, _, name string, _ types.PoolKey) (types.VMRecord, error) {
	return f.createRecord(name)
}

func (f *fakeEngine) CloneSnap(_ context.Context, _, name string, _ types.PoolKey) (types.VMRecord, error) {
	return f.createRecord(name)
}

func (f *fakeEngine) RunCold(_ context.Context, name string, _ types.PoolKey, _ string) (types.VMRecord, error) {
	return f.createRecord(name)
}

func (f *fakeEngine) Remove(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if l := f.listeners[name]; l != nil {
		_ = l.Close()
	}
	delete(f.listeners, name)
	delete(f.mounts, f.socks[name])
	delete(f.socks, name)
	if f.volumeVMs[name] {
		delete(f.volumeVMs, name)
		f.volumeOps = append(f.volumeOps, "remove")
	}
	return nil
}

func (f *fakeEngine) Start(_ context.Context, name string) error {
	return fmt.Errorf("fakeEngine cannot cold-boot %s", name)
}

func (f *fakeEngine) Stop(ctx context.Context, name string) error {
	return f.Remove(ctx, name)
}

func (f *fakeEngine) ReconcileStaleCreate(context.Context, string) (engine.StaleCreateOutcome, error) {
	return engine.StaleCreateNotCreating, nil
}

func (f *fakeEngine) SnapshotSave(_ context.Context, _, _ string) error { return nil }

func (f *fakeEngine) SnapshotExport(_ context.Context, _, toDir string) error {
	return os.MkdirAll(toDir, 0o750)
}

func (f *fakeEngine) DiskAttachSnapshot(_ context.Context, vmName, snapName string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readerAttaches = append(f.readerAttaches, vmName+":"+snapName)
	return "snap-" + snapName, nil
}

func (f *fakeEngine) DiskAttachDir(context.Context, string, string) (string, error) {
	return "", errors.New("no archive in this fake")
}

func (f *fakeEngine) StartReader(_ context.Context, vsockSocket, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readerStarts = append(f.readerStarts, vsockSocket)
	return nil
}

func (f *fakeEngine) DialReader(_ context.Context, _ string) (net.Conn, error) {
	f.mu.Lock()
	root := f.readerRoot
	f.mu.Unlock()
	client, server := net.Pipe()
	go silkdtest.NewFake(root).ServeConn(server)
	return client, nil
}

func (f *fakeEngine) SnapshotRemove(_ context.Context, _ string) error { return nil }

func (f *fakeEngine) SnapshotList(_ context.Context) ([]string, error) { return nil, nil }

func (f *fakeEngine) ImageIDs(context.Context) (map[string]string, error) { return nil, nil }

func (f *fakeEngine) WriteInstanceMetadata(context.Context, string, []byte) error { return nil }

func (f *fakeEngine) WriteGuestEnv(_ context.Context, vsockSocket string, doc []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.guestEnvs[vsockSocket] = string(doc)
	return nil
}

func (f *fakeEngine) Hibernate(ctx context.Context, name, _ string) error {
	return f.Remove(ctx, name)
}

func (f *fakeEngine) Restore(_ context.Context, name, _ string) (string, error) {
	return f.create(name)
}

func (f *fakeEngine) List(_ context.Context) ([]types.VMRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var vms []types.VMRecord
	for name, sock := range f.socks {
		vms = append(vms, types.VMRecord{State: "running", VsockSocket: sock, Config: types.VMConfig{Name: name}})
	}
	return vms, nil
}

func (f *fakeEngine) Inspect(ctx context.Context, name string) (types.VMRecord, bool, error) {
	vms, _ := f.List(ctx)
	i := slices.IndexFunc(vms, func(vm types.VMRecord) bool { return vm.Config.Name == name })
	if i < 0 {
		return types.VMRecord{}, false, nil
	}
	return vms[i], true, nil
}

func (f *fakeEngine) VMEvents(ctx context.Context, _ string, _ engine.VMSyncFunc, _ engine.VMChangeFunc) error {
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeEngine) Probe(ctx context.Context, vsockSocket string, timeout time.Duration) error {
	return f.real.Probe(ctx, vsockSocket, timeout)
}

func (f *fakeEngine) DialGuestPort(context.Context, string, uint16) (net.Conn, error) {
	return nil, errors.New("fakeEngine has no guest port")
}

func (f *fakeEngine) InstallCACert(context.Context, string, []byte) error { return nil }

func (f *fakeEngine) MarkLane(context.Context, string, types.NetRoute) error { return nil }

func (f *fakeEngine) Warmup(context.Context, string, []string) error { return nil }

func (f *fakeEngine) TrimCow(context.Context, string) error { return nil }

func (f *fakeEngine) DiskAttach(_ context.Context, vmName string, spec engine.VolumeSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volumeVMs[vmName] = true
	f.volumeOps = append(f.volumeOps, "attach:"+spec.Name+":"+volumeMode(spec.RW))
	return nil
}

func (f *fakeEngine) MountVolume(_ context.Context, sock, name, mount string, rw bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volumeOps = append(f.volumeOps, "mount:"+name+":"+mount+":"+volumeMode(rw))
	if f.mounts[sock] == nil {
		f.mounts[sock] = map[string]fakeVolumeMount{}
	}
	f.mounts[sock][mount] = fakeVolumeMount{name: name, rw: rw}
	return nil
}

func (f *fakeEngine) MountLiveVolume(ctx context.Context, sock, name, mount string, rw bool) error {
	return f.MountVolume(ctx, sock, name, mount, rw)
}

func (f *fakeEngine) UnmountVolume(_ context.Context, sock, mount string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unmountVolumeLocked(sock, mount)
	return nil
}

func (f *fakeEngine) SyncGuest(_ context.Context, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volumeOps = append(f.volumeOps, "sync")
	return nil
}

func (f *fakeEngine) DetachVolume(_ context.Context, _, sock, name, mount string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, mounted := f.mounts[sock][mount]
	normal := mount != "" && mounted && entry.name == name && entry.rw
	if mount != "" && mounted && entry.name == name {
		f.unmountVolumeLocked(sock, mount)
	}
	for _, other := range f.mounts[sock] {
		if other.name == name {
			return false, &engine.VolumeMountConflict{Message: "volume still has a simulated guest mount"}
		}
	}
	f.volumeOps = append(f.volumeOps, "detach:"+name)
	return normal, nil
}

func (f *fakeEngine) unmountNamedVolume(name, mount string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for sock, mounts := range f.mounts {
		if mounts[mount].name == name {
			f.unmountVolumeLocked(sock, mount)
			return nil
		}
	}
	return fmt.Errorf("volume %q is not mounted at %q", name, mount)
}

func (f *fakeEngine) unmountVolumeLocked(sock, mount string) {
	f.volumeOps = append(f.volumeOps, "umount:"+mount)
	delete(f.mounts[sock], mount)
}

func (f *fakeEngine) volumeOpsLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.volumeOps)
}

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

func volumeMode(rw bool) string {
	if rw {
		return types.VolumeModeRW
	}
	return types.VolumeModeRO
}
