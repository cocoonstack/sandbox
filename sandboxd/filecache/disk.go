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
// engine implements it over the same disk-attach, by-serial discovery, and
// unmount primitives as writable catalog volumes. Separate from Guest
// (in-guest silkd ops) because attach/detach act on the VM.
type Disk interface {
	// Attach hot-attaches a read-write raw image to vmName as a virtio-blk
	// device with the given serial name.
	Attach(ctx context.Context, vmName, rawPath, name string) error
	// Mount discovers the device by serial and mounts it read-write at mount.
	Mount(ctx context.Context, vsockSocket, name, mount string) error
	// Unmount flushes and unmounts the guest mount.
	Unmount(ctx context.Context, vsockSocket, mount string) error
	// Detach detaches the disk from vmName.
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
	root   string // host dir for raw images
	sizeMB int
}

func (d *diskProvisioner) rawPath(vmName string) string {
	return filepath.Join(d.root, vmName+".raw")
}

// preAttach creates and hot-attaches a workspace disk to a warm VM ahead of
// any claim, so Arm pays only the in-guest mount. No mount happens here — a
// claim that never asks for a workspace must not see a surprise /workspace.
func (d *diskProvisioner) preAttach(ctx context.Context, vmName string) error {
	raw := d.rawPath(vmName)
	if err := os.MkdirAll(d.root, 0o750); err != nil {
		return err
	}
	if _, err := os.Stat(raw); err == nil {
		return nil // already provisioned (reconcile re-ran refill bookkeeping)
	}
	if err := createExt4(ctx, raw, d.sizeMB); err != nil {
		return fmt.Errorf("create workspace disk: %w", err)
	}
	if err := d.disk.Attach(ctx, vmName, raw, diskSerial); err != nil {
		_ = os.Remove(raw)
		return fmt.Errorf("attach workspace disk: %w", err)
	}
	return nil
}

// cleanupVM drops the disk image of a VM that died without a barrier (warm
// trim, quarantine); idempotent, and a barrier already removed its image.
func (d *diskProvisioner) cleanupVM(vmName string) {
	_ = os.Remove(d.rawPath(vmName))
}

// attachAndMount brings the workspace disk up at mount. The fast path finds
// the image pre-attached by refill and only mounts; a VM without one (adopted
// from an older daemon, or its pre-attach failed) gets the full provision.
func (d *diskProvisioner) attachAndMount(ctx context.Context, vmName, vsockSocket, mount string) error {
	if _, err := os.Stat(d.rawPath(vmName)); err != nil {
		if err := d.preAttach(ctx, vmName); err != nil {
			return err
		}
	}
	if err := d.disk.Mount(ctx, vsockSocket, diskSerial, mount); err != nil {
		return fmt.Errorf("mount workspace disk: %w", err)
	}
	return nil
}

// unmountAndDetach reverses attachAndMount for barrier. Best-effort per step so
// one failure does not strand the rest; a gone VM (already reaped) is fine, and
// a failed unmount loses nothing durable — the image is scratch, discarded
// below, and the workspace's contents were already published by the barrier.
func (d *diskProvisioner) unmountAndDetach(ctx context.Context, vmName, vsockSocket, mount string) {
	_ = d.disk.Unmount(ctx, vsockSocket, mount)
	_ = d.disk.Detach(ctx, vmName, diskSerial)
	_ = os.Remove(d.rawPath(vmName))
}

// createExt4 makes a sparse raw image of sizeMB and formats it ext4. mkfs is
// host tooling the sandbox rootfs already relies on at bake time.
func createExt4(ctx context.Context, path string, sizeMB int) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // provisioner-owned image path under its own root
	if err != nil {
		return err
	}
	if err := f.Truncate(int64(sizeMB) * 1024 * 1024); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "mkfs.ext4", "-qF", path)
	if out, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("mkfs.ext4: %w: %s", err, out)
	}
	return nil
}
