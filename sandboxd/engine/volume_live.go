package engine

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const volumeMutationTimeout = 30 * time.Second

// AttachVolume converges a read-only disk and its mount. The caller journals
// intent before calling: failures may leave a disk attached or mounted.
func (e *Engine) AttachVolume(ctx context.Context, vmName, sock string, spec VolumeSpec, mount string) error {
	ctx, cancel := context.WithTimeout(ctx, volumeMutationTimeout)
	defer cancel()
	if spec.RW || mount == "" {
		return fmt.Errorf("live attach requires a read-only mount")
	}
	found, err := e.attachedVolume(ctx, vmName, spec)
	if err != nil {
		return err
	}
	if !found {
		if err = e.DiskAttach(ctx, vmName, spec); err != nil {
			return err
		}
	}
	device, err := e.waitForVolumeDevice(ctx, sock, spec.Name)
	if err != nil {
		return err
	}
	mounted, err := e.volumeMounted(ctx, sock, device, mount)
	if err != nil || mounted {
		return err
	}
	if err = e.MountVolume(ctx, sock, spec.Name, mount, false); err != nil {
		return err
	}
	mounted, err = e.volumeMounted(ctx, sock, device, mount)
	if err != nil {
		return err
	}
	if !mounted {
		return fmt.Errorf("volume %q did not appear at its mount point", spec.Name)
	}
	return nil
}

// attachedVolume uses inspect only as positive evidence. A missing entry may
// also mean cocoon failed to query the VMM; DiskAttach then fails closed.
func (e *Engine) attachedVolume(ctx context.Context, vmName string, spec VolumeSpec) (bool, error) {
	out, err := e.run(ctx, "vm", "inspect", vmName)
	if err != nil {
		return false, err
	}
	var rec struct {
		AttachedDevices struct {
			Disks []struct {
				Name     string `json:"name"`
				Path     string `json:"path"`
				ReadOnly bool   `json:"readonly"`
			} `json:"disks"`
		} `json:"attached_devices"`
	}
	if err := json.Unmarshal(out, &rec); err != nil {
		return false, fmt.Errorf("parse attached disks: %w", err)
	}
	for _, disk := range rec.AttachedDevices.Disks {
		if disk.Name != spec.Name {
			continue
		}
		path, err := filepath.EvalSymlinks(spec.Path)
		if err != nil {
			return false, err
		}
		if !disk.ReadOnly || disk.Path != path {
			return false, fmt.Errorf("attached volume %q has a different image or mode", spec.Name)
		}
		return true, nil
	}
	return false, nil
}

// DetachVolume refuses busy or foreign mounts and confirms host removal even
// when the guest serial has disappeared. It never uses lazy or forced umount.
func (e *Engine) DetachVolume(ctx context.Context, vmName, sock, name, mount string) error {
	ctx, cancel := context.WithTimeout(ctx, volumeMutationTimeout)
	defer cancel()
	device, found, err := e.findVolumeDevice(ctx, sock, name)
	if err != nil {
		return err
	}
	if found {
		if err = e.unmountLiveVolume(ctx, sock, device, mount); err != nil {
			return err
		}
	}
	_, err = e.run(ctx, "vm", "disk", "detach", vmName, argName, name)
	// Both CH and FC emit this only after checking the device set under their
	// operation lock. Inspect's absent field alone is not proof of removal.
	if err != nil && !strings.Contains(err.Error(), fmt.Sprintf("disk %q not attached", name)) {
		return err
	}
	return nil
}

func (e *Engine) unmountLiveVolume(ctx context.Context, sock, device, mount string) error {
	mounted, err := e.volumeMounted(ctx, sock, device, mount)
	if err != nil {
		return err
	}
	if mounted {
		if err = e.UnmountVolume(ctx, sock, mount); err != nil {
			return err
		}
	}
	// Refuse a second/bind mount of the same device before hot-unplug.
	return e.volumeUnmounted(ctx, sock, device)
}

type volumeMount struct {
	device, root, path, options string
}

func (e *Engine) volumeMounts(ctx context.Context, sock, device string) (string, []volumeMount, error) {
	dev, err := e.silkdReadFile(ctx, sock, "/sys/block/"+filepath.Base(device)+"/dev")
	if err != nil {
		return "", nil, err
	}
	raw, err := e.silkdReadFile(ctx, sock, "/proc/self/mountinfo")
	if err != nil {
		return "", nil, err
	}
	var mounts []volumeMount
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 || !slices.Contains(fields[6:], "-") {
			return "", nil, fmt.Errorf("invalid guest mountinfo")
		}
		mounts = append(mounts, volumeMount{fields[2], unescapeMount(fields[3]), unescapeMount(fields[4]), fields[5]})
	}
	return strings.TrimSpace(string(dev)), mounts, nil
}

func (e *Engine) volumeMounted(ctx context.Context, sock, device, mount string) (bool, error) {
	dev, mounts, err := e.volumeMounts(ctx, sock, device)
	if err != nil {
		return false, err
	}
	matched := false
	for _, entry := range mounts {
		if entry.path != mount {
			continue
		}
		if matched || entry.device != dev || entry.root != "/" || !slices.Contains(strings.Split(entry.options, ","), "ro") {
			return false, fmt.Errorf("volume mount %q has a different device or mode", mount)
		}
		matched = true
	}
	return matched, nil
}

func (e *Engine) volumeUnmounted(ctx context.Context, sock, device string) error {
	dev, mounts, err := e.volumeMounts(ctx, sock, device)
	if err != nil {
		return err
	}
	for _, mount := range mounts {
		if mount.device == dev {
			return fmt.Errorf("volume device is still mounted at %q", mount.path)
		}
	}
	return nil
}

func unescapeMount(s string) string {
	return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(s)
}
