package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

type (
	volumeID    string
	volumePhase string
)

const (
	volumeCreating volumePhase = "creating"
	volumeReady    volumePhase = "ready"
	volumeDeleting volumePhase = "deleting"
	volumeRecovery volumePhase = "recovery_required"
)

type backingIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	Size   int64  `json:"size"`
}

type managedVolume struct {
	ID        volumeID        `json:"id"`
	Name      string          `json:"name"`
	Tenant    string          `json:"tenant"`
	Node      string          `json:"node"`
	Size      int64           `json:"size"`
	GrowTo    int64           `json:"grow_to,omitempty"`
	Backing   backingIdentity `json:"backing"`
	State     volumePhase     `json:"state"`
	CreatedAt time.Time       `json:"created_at"`
}

// volumeRegistryData is registry.json: resources only, since claims.json rows are the holds.
type volumeRegistryData struct {
	Volumes map[string]managedVolume `json:"volumes"`
}

type volumeRegistry struct {
	rootDir, disksDir os.FileInfo
	mu                sync.Mutex
	writeMu           sync.Mutex
	seq, written      uint64
	writeFault        error
	retrying          atomic.Bool
	closed            bool
	aliases           map[backingIdentity]bool
	dir, node         string
	lock              *os.File
	data              volumeRegistryData
	commitFile        func(string, []byte) error
	// busy names a create or delete in flight, so its file I/O excludes only that name
	busy map[string]bool
	idle *sync.Cond
}

var (
	errBackingMismatch  = errors.New("managed backing is missing or changed")
	errManagedDirectory = errors.New("managed directory changed")
)

var managedIDPattern = regexp.MustCompile(`^vol_[0-9a-f]{32}$`)

func openVolumeRegistry(dir, node string) (*volumeRegistry, error) {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || dir == "/" {
		return nil, errors.New("invalid managed volume directory")
	}
	if err := privateVolumeDir(dir); err != nil {
		return nil, err
	}
	if err := privateVolumeDir(filepath.Join(dir, "disks")); err != nil {
		return nil, err
	}
	lock, err := openPrivateFile(filepath.Join(dir, "lock"), syscall.O_CREAT|syscall.O_RDWR)
	if err != nil {
		return nil, fmt.Errorf("open managed volume lock: %w", err)
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, errors.Join(fmt.Errorf("managed volume directory is already in use: %w", err), lock.Close())
	}
	r := &volumeRegistry{dir: dir, node: node, lock: lock, commitFile: writeVolumeSnapshot, busy: map[string]bool{}}
	r.idle = sync.NewCond(&r.mu)
	if err = r.load(); err != nil {
		return nil, errors.Join(fmt.Errorf("open managed volume registry: %w", err), lock.Close())
	}
	return r, nil
}

func (r *volumeRegistry) load() error {
	var err error
	if r.rootDir, err = os.Lstat(r.dir); err != nil {
		return err
	}
	if r.disksDir, err = os.Lstat(filepath.Join(r.dir, "disks")); err != nil {
		return err
	}
	r.data = volumeRegistryData{Volumes: map[string]managedVolume{}}
	f, err := openPrivateFile(filepath.Join(r.dir, "registry.json"), syscall.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		entries, readErr := os.ReadDir(filepath.Join(r.dir, "disks"))
		if readErr != nil || len(entries) != 0 {
			return errors.Join(errors.New("missing registry with existing backing files"), readErr)
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		// a failed first write leaves writeFault for the retrier, like any later write
		_ = r.commitAndRelock()
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &r.data); err != nil {
		return err
	}
	if err = r.validate(); err != nil {
		return err
	}
	for name, v := range r.data.Volumes {
		if v.GrowTo != 0 {
			actual, _ := inspectBacking(r.path(v.ID))
			r.data.Volumes[name] = settleGrowth(v, actual)
		}
	}
	r.indexBackings()
	return nil
}

func privateVolumeDir(path string) error {
	for p := path; p != "/"; p = filepath.Dir(p) {
		st, err := os.Lstat(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		owner := st.Sys().(*syscall.Stat_t).Uid
		if owner != 0 && int64(owner) != int64(os.Geteuid()) {
			return errors.New("managed directory ancestors must belong to root or the daemon user")
		}
		if st.Mode().Perm()&0o022 != 0 && st.Mode()&os.ModeSticky == 0 {
			return errors.New("managed directory ancestors must not be writable by others")
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return errors.New("managed directory component is not a directory")
		}
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !st.IsDir() || st.Mode().Perm()&0o077 != 0 {
		return errors.New("managed directory must be private (0700)")
	}
	if int64(st.Sys().(*syscall.Stat_t).Uid) != int64(os.Geteuid()) {
		return errors.New("managed directory must belong to the daemon user")
	}
	return nil
}

func openPrivateFile(path string, flags int) (*os.File, error) {
	fd, err := syscall.Open(path, flags|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0o077 != 0 || st.Sys().(*syscall.Stat_t).Nlink != 1 || int64(st.Sys().(*syscall.Stat_t).Uid) != int64(os.Geteuid()) {
		_ = f.Close()
		return nil, errors.New("managed file must be private, owned, regular, and unlinked elsewhere")
	}
	return f, nil
}

func (r *volumeRegistry) path(id volumeID) string {
	return filepath.Join(r.dir, "disks", string(id)+".raw")
}

func inspectBacking(path string) (backingIdentity, error) {
	f, err := openPrivateFile(path, syscall.O_RDONLY)
	if err != nil {
		return backingIdentity{}, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return backingIdentity{}, err
	}
	raw := st.Sys().(*syscall.Stat_t)
	return backingIdentity{Device: backingDeviceID(raw.Dev), Inode: raw.Ino, Size: st.Size()}, nil
}

// Darwin uses signed device numbers; retain their existing uint64 representation.
func backingDeviceID[T ~uint64 | ~int32](device T) uint64 {
	if device < 0 {
		return ^uint64(^device)
	}
	return uint64(device)
}

func (r *volumeRegistry) verify(v managedVolume) error {
	actual, err := r.inspectManaged(r.path(v.ID))
	if errors.Is(err, errManagedDirectory) {
		return err
	}
	if errors.Is(err, os.ErrNotExist) || err == nil && (v.Backing.Inode == 0 || actual.Device != v.Backing.Device || actual.Inode != v.Backing.Inode || (actual.Size != v.Size && (v.GrowTo == 0 || actual.Size != v.GrowTo))) {
		return errBackingMismatch
	}
	return err
}

func (r *volumeRegistry) inspectManaged(path string) (backingIdentity, error) {
	if err := r.verifyDirectories(); err != nil {
		return backingIdentity{}, errors.Join(errManagedDirectory, err)
	}
	actual, err := inspectBacking(path)
	if errors.Is(err, os.ErrNotExist) {
		// a directory renamed between the check and the open must stay a transient error
		if dirErr := r.verifyDirectories(); dirErr != nil {
			return actual, errors.Join(errManagedDirectory, dirErr)
		}
	}
	return actual, err
}

func (r *volumeRegistry) validate() error {
	if r.data.Volumes == nil {
		return errors.New("invalid volume registry format")
	}
	ids := map[volumeID]bool{}
	for name, v := range r.data.Volumes {
		if err := validateVolumeRecord(name, v, ids); err != nil {
			return err
		}
	}
	entries, err := os.ReadDir(filepath.Join(r.dir, "disks"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := strings.TrimSuffix(entry.Name(), volumeDirtySuffix)
		id := volumeID(strings.TrimSuffix(name, ".raw"))
		if name != string(id)+".raw" || !ids[id] {
			return errors.New("unregistered managed backing requires quarantine")
		}
	}
	return nil
}

func validateVolumeRecord(name string, v managedVolume, ids map[volumeID]bool) error {
	if name != v.Name || !types.ValidVolumeName(name) || !managedIDPattern.MatchString(string(v.ID)) || !types.NameRe.MatchString(v.Tenant) || v.Size < 1<<30 || v.Size > 1024<<30 || v.Size%(1<<30) != 0 || v.CreatedAt.IsZero() || v.Node == "" || ids[v.ID] {
		return errors.New("invalid managed volume record")
	}
	switch v.State {
	case volumeCreating, volumeReady, volumeDeleting, volumeRecovery:
	default:
		return errors.New("invalid managed volume state")
	}
	if (v.State == volumeReady || v.Backing.Inode != 0) && (v.Backing.Inode == 0 || v.Backing.Size != v.Size) {
		return errors.New("invalid managed backing record")
	}
	if v.GrowTo != 0 && (v.State != volumeReady || v.GrowTo <= v.Size || v.GrowTo > 1024<<30 || v.GrowTo%(1<<30) != 0) {
		return errors.New("invalid managed growth intent")
	}
	ids[v.ID] = true
	return nil
}

func (r *volumeRegistry) commitAndRelock() error {
	if r.closed {
		return managedStorageError()
	}
	r.seq++
	seq := r.seq
	r.indexBackings()
	r.mu.Unlock()
	r.writeMu.Lock()
	r.mu.Lock()
	if r.closed {
		r.writeMu.Unlock()
		return managedStorageError()
	}
	if r.written >= seq {
		r.writeMu.Unlock()
		if r.writeFault != nil {
			return managedStorageError()
		}
		return nil
	}
	seq = r.seq
	raw, err := json.Marshal(r.data)
	write := r.commitFile
	r.mu.Unlock()
	if err == nil {
		err = r.verifyDirectories()
	}
	if err == nil {
		err = write(filepath.Join(r.dir, "registry.json"), raw)
	}
	r.mu.Lock()
	previousFault := r.writeFault
	r.writeFault = err
	if err == nil {
		r.written = seq
	}
	r.writeMu.Unlock()
	r.mu.Unlock()
	if previousFault == nil && err != nil {
		log.WithFunc("pool.volumeRegistry").Error(context.Background(), err, "managed registry write failed; retaining ownership for retry")
	} else if previousFault != nil && err == nil {
		log.WithFunc("pool.volumeRegistry").Info(context.Background(), "managed registry persistence recovered")
	}
	r.mu.Lock()
	if err != nil || r.writeFault != nil {
		return managedStorageError()
	}
	return nil
}

func (r *volumeRegistry) indexBackings() {
	r.aliases = make(map[backingIdentity]bool, len(r.data.Volumes))
	for _, v := range r.data.Volumes {
		if v.Backing.Inode != 0 {
			r.aliases[backingIdentity{Device: v.Backing.Device, Inode: v.Backing.Inode}] = true
		}
	}
}

func (m *Manager) retryManagedSnapshot() {
	if m.managed == nil {
		return
	}
	r := m.managed
	r.mu.Lock()
	retry := r.writeFault != nil && !r.closed && r.retrying.CompareAndSwap(false, true)
	r.mu.Unlock()
	if !retry {
		return
	}
	go func() {
		defer r.retrying.Store(false)
		r.mu.Lock()
		defer r.mu.Unlock()
		_ = r.commitAndRelock()
	}()
}

func writeVolumeSnapshot(path string, raw []byte) error {
	return utils.WriteFileSync(path, raw, 0o600)
}

// claimName waits out another create or delete of name, then marks it busy; callers hold r.mu.
func (r *volumeRegistry) claimName(name string) error {
	for r.busy[name] && !r.closed {
		r.idle.Wait()
	}
	if r.closed {
		return managedStorageError()
	}
	r.busy[name] = true
	return nil
}

func (r *volumeRegistry) releaseName(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.busy, name)
	r.idle.Broadcast()
}

func (r *volumeRegistry) close() error {
	r.mu.Lock()
	r.closed = true
	for len(r.busy) > 0 {
		r.idle.Wait()
	}
	r.mu.Unlock()
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	if r.lock == nil {
		return nil
	}
	err := r.lock.Close()
	r.lock = nil
	return err
}

// Close releases the managed directory lock after requests and workers have stopped.
func (m *Manager) Close() error {
	if m.managed == nil {
		return nil
	}
	return m.managed.close()
}

func (r *volumeRegistry) verifyDirectories() error {
	for _, pair := range []struct {
		path     string
		original os.FileInfo
	}{{r.dir, r.rootDir}, {filepath.Join(r.dir, "disks"), r.disksDir}} {
		st, err := os.Lstat(pair.path)
		if err != nil {
			return err
		}
		if !st.IsDir() || st.Mode().Perm()&0o077 != 0 || !os.SameFile(st, pair.original) {
			return errors.New("managed directory identity or permissions changed")
		}
	}
	return nil
}
