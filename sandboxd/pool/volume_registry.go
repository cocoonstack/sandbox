package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type (
	volumeID    string
	volumePhase string
	holdPhase   string
)

const (
	volumeCreating volumePhase = "creating"
	volumeReady    volumePhase = "ready"
	volumeGrowing  volumePhase = "growing"
	volumeDeleting volumePhase = "deleting"
	volumeRecovery volumePhase = "recovery_required"
	holdAttaching  holdPhase   = "attaching"
	holdClaimed    holdPhase   = "claimed"
	holdRetiring   holdPhase   = "retiring"
)

type backingIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	Size   int64  `json:"size"`
}

type managedVolume struct {
	CapacityGeneration uint64          `json:"capacity_generation,omitempty"`
	Growth             *managedGrowth  `json:"growth,omitempty"`
	ID                 volumeID        `json:"id"`
	Name               string          `json:"name"`
	Tenant             string          `json:"tenant"`
	Node               string          `json:"node"`
	Size               int64           `json:"size"`
	Backing            backingIdentity `json:"backing"`
	State              volumePhase     `json:"state"`
	CreatedAt          time.Time       `json:"created_at"`
}

type volumeRef struct {
	Volume  types.Volume    `json:"volume"`
	ID      volumeID        `json:"id,omitempty"`
	Path    string          `json:"path"`
	Backing backingIdentity `json:"backing"`
}

type processIdentity struct {
	BootID string `json:"boot_id"`
	PID    int    `json:"pid"`
	Start  uint64 `json:"start"`
}

type volumeHold struct {
	Sleep     *managedSleep   `json:"sleep,omitempty"`
	SandboxID string          `json:"sandbox_id"`
	Tenant    string          `json:"tenant"`
	VMName    string          `json:"vm_name,omitempty"`
	Process   processIdentity `json:"process"`
	State     holdPhase       `json:"state"`
	Refs      []volumeRef     `json:"refs"`
}

type volumeTombstone struct {
	ID     volumeID `json:"id"`
	Name   string   `json:"name"`
	Tenant string   `json:"tenant"`
}

type volumeRegistryData struct {
	Orphans map[string]processIdentity   `json:"orphans,omitempty"`
	Version int                          `json:"version"`
	Volumes map[string]managedVolume     `json:"volumes"`
	Holds   map[string]volumeHold        `json:"holds"`
	Deleted map[volumeID]volumeTombstone `json:"deleted"`
}

type volumeRegistry struct {
	rootDir, disksDir os.FileInfo
	mu                sync.Mutex
	writeMu           sync.Mutex
	fileMu            sync.Mutex
	seq, written      uint64
	writeFault        error
	writeFailures     atomic.Uint64
	writeRecoveries   atomic.Uint64
	retrying          atomic.Bool
	closed            bool
	uncertainVMs      map[string]bool
	discharging       map[string]bool
	aliases           map[backingIdentity]bool
	dir, node         string
	lock              *os.File
	data              volumeRegistryData
	original          []byte
	fault             error
	commitFile        func(string, []byte) error
	process           func(int) (processIdentity, error)
}

var errBackingMismatch = errors.New("managed backing identity changed")

var managedIDPattern = regexp.MustCompile(`^vol_[0-9a-f]{32}$`)

func openVolumeRegistry(dir, node string) (*volumeRegistry, error) {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || dir == "/" {
		return nil, fmt.Errorf("invalid managed volume directory")
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
	r := &volumeRegistry{dir: dir, node: node, lock: lock, commitFile: writeVolumeSnapshot, process: readProcessIdentity}
	r.rootDir, err = os.Lstat(dir)
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	r.disksDir, err = os.Lstat(filepath.Join(dir, "disks"))
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	r.data = volumeRegistryData{Version: 1, Volumes: map[string]managedVolume{}, Holds: map[string]volumeHold{}, Deleted: map[volumeID]volumeTombstone{}}
	f, err := openPrivateFile(filepath.Join(dir, "registry.json"), syscall.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		entries, readErr := os.ReadDir(filepath.Join(dir, "disks"))
		if readErr != nil || len(entries) != 0 {
			r.fault = errors.New("missing registry with existing backing files")
			return r, nil
		}
		r.mu.Lock()
		err = r.commitAndRelock()
		r.mu.Unlock()
		if err != nil {
			return r, nil
		}
		return r, nil
	}
	if err != nil {
		r.fault = err
		return r, nil
	}
	defer func() { _ = f.Close() }()
	raw, readErr := io.ReadAll(f)
	if readErr != nil {
		r.fault = readErr
		return r, nil
	}
	r.original = raw
	if err := json.Unmarshal(raw, &r.data); err != nil {
		r.fault = err
		return r, nil
	}
	if err := r.validate(); err != nil {
		r.fault = err
	}
	r.indexBackings()
	return r, nil
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
			return fmt.Errorf("managed directory ancestors must belong to root or the daemon user")
		}
		if st.Mode().Perm()&0o022 != 0 && st.Mode()&os.ModeSticky == 0 {
			return fmt.Errorf("managed directory ancestors must not be writable by others")
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("managed directory component is not a directory")
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
		return fmt.Errorf("managed directory must be private (0700)")
	}
	if int64(st.Sys().(*syscall.Stat_t).Uid) != int64(os.Geteuid()) {
		return fmt.Errorf("managed directory must belong to the daemon user")
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
		return nil, fmt.Errorf("managed file must be private, owned, regular, and unlinked elsewhere")
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
	if err := r.verifyDirectories(); err != nil {
		return err
	}
	actual, err := inspectBacking(r.path(v.ID))
	if err != nil {
		return err
	}
	if v.Backing.Inode == 0 || actual != v.Backing || actual.Size != v.Size {
		return errBackingMismatch
	}
	return nil
}

func (r *volumeRegistry) validate() error {
	d := r.data
	if (d.Version != 1 && d.Version != 2 && d.Version != 3) || d.Volumes == nil || d.Holds == nil || d.Deleted == nil {
		return errors.New("invalid volume registry format")
	}
	ids := map[volumeID]bool{}
	for name, v := range d.Volumes {
		if err := r.validateGrowth(v); err != nil {
			return err
		}
		if d.Version < 3 && v.CapacityGeneration != 0 {
			return errors.New("capacity generation requires version 3")
		}
		if err := validateVolumeRecord(name, v, ids); err != nil {
			return err
		}
	}
	vms := map[string]bool{}
	holders := map[string]volumeHolders{}
	for id, h := range d.Holds {
		if err := r.validateHoldRecord(id, h, vms, holders); err != nil {
			return err
		}
	}
	for name, p := range d.Orphans {
		if !strings.HasPrefix(name, vmPrefix) || p.PID <= 0 || p.Start == 0 || p.BootID == "" {
			return errors.New("invalid orphan process record")
		}
	}
	for id, t := range d.Deleted {
		if id != t.ID || !managedIDPattern.MatchString(string(id)) || !types.ValidVolumeName(t.Name) || !types.NameRe.MatchString(t.Tenant) || ids[id] {
			return errors.New("invalid volume tombstone")
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
	case volumeCreating, volumeReady, volumeGrowing, volumeDeleting, volumeRecovery:
	default:
		return errors.New("invalid managed volume state")
	}
	if (v.State == volumeReady || v.Backing.Inode != 0) && (v.Backing.Inode == 0 || v.Backing.Size != v.Size) {
		return errors.New("invalid managed backing record")
	}
	ids[v.ID] = true
	return nil
}

func (r *volumeRegistry) validateHoldRecord(id string, h volumeHold, vms map[string]bool, holders map[string]volumeHolders) error {
	if id != h.SandboxID || !strings.HasPrefix(id, "sb_") || len(h.Refs) == 0 {
		return errors.New("invalid volume holder")
	}
	switch h.State {
	case holdAttaching, holdClaimed, holdRetiring:
	default:
		return errors.New("invalid volume holder state")
	}
	if h.VMName != "" {
		if !strings.HasPrefix(h.VMName, vmPrefix) || vms[h.VMName] || h.Process.PID <= 0 || h.Process.Start == 0 || h.Process.BootID == "" {
			return errors.New("invalid volume process identity")
		}
		vms[h.VMName] = true
	} else if h.State == holdClaimed {
		return errors.New("claimed volume lacks VM")
	}
	if h.Sleep != nil {
		if r.data.Version < 2 {
			return errors.New("sleep requires registry version 2")
		}
		if err := validateManagedSleep(h); err != nil {
			return err
		}
	}
	names := map[string]bool{}
	managed := false
	for _, ref := range h.Refs {
		isManaged, err := r.validateHoldRef(h, ref, names, holders)
		if err != nil {
			return err
		}
		managed = managed || isManaged
	}
	if !managed {
		return errors.New("managed holder has no managed resource")
	}
	return nil
}

func (r *volumeRegistry) validateHoldRef(h volumeHold, ref volumeRef, names map[string]bool, holders map[string]volumeHolders) (bool, error) {
	if !types.ValidVolumeName(ref.Volume.Name) || names[ref.Volume.Name] || !filepath.IsAbs(ref.Path) || (ref.Volume.Mode != "" && ref.Volume.Mode != types.VolumeModeRW) {
		return false, errors.New("invalid volume reference")
	}
	names[ref.Volume.Name] = true
	counts := holders[ref.Volume.Name]
	if counts.writers > 0 || ref.Volume.RW() && counts.readers > 0 {
		return false, errors.New("conflicting volume holder records")
	}
	if ref.Volume.RW() {
		counts.writers++
	} else {
		counts.readers++
	}
	holders[ref.Volume.Name] = counts
	if ref.ID == "" {
		return false, nil
	}
	v, ok := r.data.Volumes[ref.Volume.Name]
	if !ok || (v.State != volumeReady && v.State != volumeRecovery && v.State != volumeGrowing) || ref.ID != v.ID || ref.Path != r.path(v.ID) || ref.Backing != v.Backing || (h.Tenant != "" && h.Tenant != v.Tenant) {
		return false, errors.New("inconsistent managed volume reference")
	}
	return true, nil
}

func (r *volumeRegistry) commitAndRelock() error {
	if r.fault != nil || r.closed {
		return managedStorageError()
	}
	r.seq++
	seq := r.seq
	r.indexBackings()
	r.mu.Unlock()
	r.writeMu.Lock()
	r.mu.Lock()
	if r.closed || r.fault != nil {
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
	if err == nil && len(r.original) != 0 {
		raw, err = mergeRegistryFields(raw, r.original, reflect.TypeFor[volumeRegistryData]())
	}
	write := r.commitFile
	r.mu.Unlock()
	if err == nil {
		err = r.verifyDirectories()
		if err != nil {
			r.mu.Lock()
			r.fault = err
			r.mu.Unlock()
		}
	}
	if err == nil {
		err = write(filepath.Join(r.dir, "registry.json"), raw)
	}
	r.mu.Lock()
	previousFault := r.writeFault
	r.writeFault = err
	if err != nil {
		r.writeFailures.Add(1)
	}
	if err == nil && previousFault != nil {
		r.writeRecoveries.Add(1)
	}
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
	if err != nil || r.writeFault != nil || r.fault != nil {
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
	retry := (r.writeFault != nil || len(r.unboundRetirements()) > 0) && r.fault == nil && !r.closed && r.retrying.CompareAndSwap(false, true)
	r.mu.Unlock()
	if !retry {
		return
	}
	go func() {
		defer r.retrying.Store(false)
		r.mu.Lock()
		if r.writeFault != nil {
			_ = r.commitAndRelock()
		}
		ids := r.unboundRetirements()
		r.mu.Unlock()
		for _, id := range ids {
			m.finishManaged(id, nil)
		}
	}()
}

func (r *volumeRegistry) unboundRetirements() []string {
	var ids []string
	for id, h := range r.data.Holds {
		if h.State == holdRetiring && h.VMName == "" {
			ids = append(ids, id)
		}
	}
	return ids
}

func writeVolumeSnapshot(path string, raw []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".registry-")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(raw); err != nil {
		return errors.Join(err, tmp.Close())
	}
	if err := errors.Join(tmp.Sync(), tmp.Close()); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func (r *volumeRegistry) close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.fileMu.Lock()
	defer r.fileMu.Unlock()
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
