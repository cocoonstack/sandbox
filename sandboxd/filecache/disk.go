package filecache

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// Disk drives VM-level workspace-disk hotplug for the dedicated-disk mode; the
// engine implements it, reusing the same cocoon disk-attach and by-serial
// device discovery as operator catalog volumes, but read-write. Separate from
// Guest (in-guest silkd ops) because attach/detach/mount act on the VM.
type Disk interface {
	// Attach hot-attaches a read-write raw image to vmName as a virtio-blk
	// device with the given serial name.
	Attach(ctx context.Context, vmName, rawPath, name string) error
	// Mount discovers the device by serial and mounts it read-write at mount.
	Mount(ctx context.Context, vsockSocket, name, mount string) error
	// Detach unmounts and detaches the disk from vmName.
	Detach(ctx context.Context, vmName, name string) error
}

// diskSerial is the attach serial and by-id key for a sandbox's workspace disk.
// One per sandbox, so a constant is fine.
const diskSerial = "fcws"

// diskProvisioner creates the host-side raw ext4 image, hot-attaches it
// read-write, and mounts it in the guest at the workspace mount before
// hydration. On barrier it unmounts, detaches, and removes the image. A nil
// disk driver means the feature is off (workspace stays on the rootfs layer).
type diskProvisioner struct {
	disk   Disk
	guest  Guest
	root   string // host dir for raw images
	sizeMB int
}

func (d *diskProvisioner) rawPath(id string) string {
	return filepath.Join(d.root, id+".raw")
}

// attachAndMount creates a fresh ext4 image, attaches it read-write to vmName,
// and mounts it at mount inside the guest before hydration.
func (d *diskProvisioner) attachAndMount(ctx context.Context, id, vmName, vsockSocket, mount string) error {
	raw := d.rawPath(id)
	if err := os.MkdirAll(d.root, 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(raw); os.IsNotExist(err) {
		if err := createExt4(ctx, raw, d.sizeMB); err != nil {
			return fmt.Errorf("create workspace disk: %w", err)
		}
	}
	if err := d.disk.Attach(ctx, vmName, raw, diskSerial); err != nil {
		return fmt.Errorf("attach workspace disk: %w", err)
	}
	if err := d.disk.Mount(ctx, vsockSocket, diskSerial, mount); err != nil {
		return fmt.Errorf("mount workspace disk: %w", err)
	}
	return nil
}

// unmountAndDetach reverses attachAndMount for barrier. Best-effort per step so
// one failure does not strand the rest; a gone VM (already reaped) is fine.
func (d *diskProvisioner) unmountAndDetach(ctx context.Context, id, vmName, vsockSocket, mount string) {
	d.guest.Run(ctx, vsockSocket, "/bin/sh", "-c", "/usr/bin/umount "+mount+" 2>/dev/null || true")
	_ = d.disk.Detach(ctx, vmName, diskSerial)
	os.Remove(d.rawPath(id))
}

// createExt4 makes a sparse raw image of sizeMB and formats it ext4. mkfs is
// host tooling the sandbox rootfs already relies on at bake time.
func createExt4(ctx context.Context, path string, sizeMB int) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := f.Truncate(int64(sizeMB) * 1024 * 1024); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	f.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "mkfs.ext4", "-qF", path)
	if out, err := cmd.CombinedOutput(); err != nil {
		os.Remove(path)
		return fmt.Errorf("mkfs.ext4: %w: %s", err, out)
	}
	return nil
}
