package pool

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type volumeID string
type volumePhase string
type holdPhase string

const (
	volumeCreating volumePhase = "creating"
	volumeReady    volumePhase = "ready"
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
	ID        volumeID        `json:"id"`
	Name      string          `json:"name"`
	Tenant    string          `json:"tenant"`
	Node      string          `json:"node"`
	Size      int64           `json:"size"`
	Backing   backingIdentity `json:"backing"`
	State     volumePhase     `json:"state"`
	CreatedAt time.Time       `json:"created_at"`
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
	dir, node         string
	lock              *os.File
	data              volumeRegistryData
	fault             error
	commitFile        func(string, []byte) error
	process           func(int) (processIdentity, error)
}

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
		lock.Close()
		return nil, fmt.Errorf("managed volume directory is already in use: %w", err)
	}
	r := &volumeRegistry{dir: dir, node: node, lock: lock, commitFile: writeVolumeSnapshot, process: readProcessIdentity}
	r.rootDir, err = os.Lstat(dir)
	if err != nil {
		lock.Close()
		return nil, err
	}
	r.disksDir, err = os.Lstat(filepath.Join(dir, "disks"))
	if err != nil {
		lock.Close()
		return nil, err
	}
	r.data = volumeRegistryData{Version: 1, Volumes: map[string]managedVolume{}, Holds: map[string]volumeHold{}, Deleted: map[volumeID]volumeTombstone{}}
	f, err := openPrivateFile(filepath.Join(dir, "registry.json"), syscall.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		entries, readErr := os.ReadDir(filepath.Join(dir, "disks"))
		if readErr != nil || len(entries) != 0 {
			r.fault = errors.New("missing registry with existing backing files")
			return r, nil
		}
		if err := r.commit(); err != nil {
			return r, nil
		}
		return r, nil
	}
	if err != nil {
		r.fault = err
		return r, nil
	}
	defer f.Close()
	if err := json.UnmarshalRead(f, &r.data, json.RejectUnknownMembers(true)); err != nil {
		r.fault = err
		return r, nil
	}
	if err := r.validate(); err != nil {
		r.fault = err
	}
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
		if owner != 0 && owner != uint32(os.Geteuid()) {
			return fmt.Errorf("managed directory ancestors must belong to root or the daemon user")
		}
		if st.Mode().Perm()&0022 != 0 && st.Mode()&os.ModeSticky == 0 {
			return fmt.Errorf("managed directory ancestors must not be writable by others")
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("managed directory component is not a directory")
		}
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !st.IsDir() || st.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("managed directory must be private (0700)")
	}
	if st.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("managed directory must belong to the daemon user")
	}
	return nil
}

func openPrivateFile(path string, flags int) (*os.File, error) {
	fd, err := syscall.Open(path, flags|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Sys().(*syscall.Stat_t).Nlink != 1 || st.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		f.Close()
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
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return backingIdentity{}, err
	}
	raw := st.Sys().(*syscall.Stat_t)
	return backingIdentity{Device: uint64(raw.Dev), Inode: raw.Ino, Size: st.Size()}, nil
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
		return errors.New("managed backing identity changed")
	}
	return nil
}

func (r *volumeRegistry) validate() error {
	d := r.data
	if d.Version != 1 || d.Volumes == nil || d.Holds == nil || d.Deleted == nil {
		return errors.New("invalid volume registry format")
	}
	ids := map[volumeID]bool{}
	for name, v := range d.Volumes {
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
		ids[v.ID] = true
	}
	vms := map[string]bool{}
	holders := map[string]volumeHolders{}
	for id, h := range d.Holds {
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
		names := map[string]bool{}
		managed := false
		for _, ref := range h.Refs {
			if !types.ValidVolumeName(ref.Volume.Name) || names[ref.Volume.Name] || !filepath.IsAbs(ref.Path) || (ref.Volume.Mode != "" && ref.Volume.Mode != types.VolumeModeRW) {
				return errors.New("invalid volume reference")
			}
			names[ref.Volume.Name] = true
			counts := holders[ref.Volume.Name]
			if counts.writers > 0 || ref.Volume.RW() && counts.readers > 0 {
				return errors.New("conflicting volume holder records")
			}
			if ref.Volume.RW() {
				counts.writers++
			} else {
				counts.readers++
			}
			holders[ref.Volume.Name] = counts
			if ref.ID != "" {
				v, ok := d.Volumes[ref.Volume.Name]
				if !ok || (v.State != volumeReady && v.State != volumeRecovery) || ref.ID != v.ID || ref.Path != r.path(v.ID) || ref.Backing != v.Backing || (h.Tenant != "" && h.Tenant != v.Tenant) {
					return errors.New("inconsistent managed volume reference")
				}
				managed = true
			}
		}
		if !managed {
			return errors.New("managed holder has no managed resource")
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

// Any failed snapshot poisons mutations until restart. The attempted state remains
// owned in memory because rename may already have published it.
func (r *volumeRegistry) commit() error {
	if r.fault != nil {
		return managedStorageError()
	}
	if err := r.verifyDirectories(); err != nil {
		r.fault = err
		return managedStorageError()
	}
	raw, err := json.Marshal(r.data)
	if err == nil {
		err = r.commitFile(filepath.Join(r.dir, "registry.json"), raw)
	}
	if err != nil {
		r.fault = err
		return managedStorageError()
	}
	return nil
}

func writeVolumeSnapshot(path string, raw []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".registry-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
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
	defer r.mu.Unlock()
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
		if !st.IsDir() || st.Mode().Perm()&0077 != 0 || !os.SameFile(st, pair.original) {
			return errors.New("managed directory identity or permissions changed")
		}
	}
	return nil
}
