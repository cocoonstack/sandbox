package engine

import (
	"cmp"
	"context"
	"fmt"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	volumeDevicePrefix = "/dev/disk/by-id/virtio-"
	volumePollInterval = 100 * time.Millisecond
	volumeSetupTimeout = 10 * time.Second
)

// VolumeSpec describes one operator-owned disk image attached to a sandbox.
type VolumeSpec struct {
	Name     string
	Path     string
	DirectIO string
}

func (s VolumeSpec) directIO() (string, error) {
	mode := cmp.Or(s.DirectIO, types.DirectIOOff)
	if !types.ValidDirectIO(mode) {
		return "", fmt.Errorf("volume directio must be on, off, or auto, got %q", s.DirectIO)
	}
	return mode, nil
}

// DiskAttach hot-attaches an operator-owned disk read-only through cocoon.
func (e *Engine) DiskAttach(ctx context.Context, vmName string, spec VolumeSpec) error {
	args, err := e.diskAttachArgs(vmName, spec)
	if err != nil {
		return err
	}
	_, err = e.run(ctx, args...)
	return err
}

// MountVolume settles and mounts a hot-attached disk read-only at mount.
func (e *Engine) MountVolume(ctx context.Context, vsockSocket, name, mount string) error {
	ctx, cancel := context.WithTimeout(ctx, volumeSetupTimeout)
	defer cancel()
	if err := e.silkdExec(ctx, vsockSocket, "udevadm", "trigger", "--action=add", "--subsystem-match=block"); err != nil {
		return fmt.Errorf("trigger volume device %s: %w", name, err)
	}
	device := volumeDevicePrefix + name
	if err := e.silkdExec(ctx, vsockSocket, "udevadm", "settle", "--timeout=5", "--exit-if-exists="+device); err != nil {
		return fmt.Errorf("settle volume device %s: %w", name, err)
	}
	if err := e.waitForVolumeDevice(ctx, vsockSocket, device); err != nil {
		return fmt.Errorf("wait for volume device %s: %w", name, err)
	}
	if err := e.silkdExec(ctx, vsockSocket, "mkdir", "-p", "--", mount); err != nil {
		return fmt.Errorf("create volume mount point %s: %w", mount, err)
	}
	if err := e.silkdExec(ctx, vsockSocket, "mount", "-o", "ro", "--", device, mount); err != nil {
		return fmt.Errorf("mount volume %s: %w", name, err)
	}
	return nil
}

func (e *Engine) waitForVolumeDevice(ctx context.Context, vsockSocket, device string) error {
	var lastErr error
	for {
		if lastErr = e.silkdExec(ctx, vsockSocket, "test", "-e", device); lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last: %v)", ctx.Err(), lastErr)
		case <-time.After(volumePollInterval):
		}
	}
}

func (e *Engine) diskAttachArgs(vmName string, spec VolumeSpec) ([]string, error) {
	directIO, err := spec.directIO()
	if err != nil {
		return nil, err
	}
	return []string{
		"vm", "disk", "attach", vmName,
		"--path", spec.Path,
		argName, spec.Name,
		"--readonly",
		"--directio", directIO,
	}, nil
}
