package engine

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	// VolumeCallTimeout bounds one guest teardown exec; the pool scales its
	// whole quiesce budget from it.
	VolumeCallTimeout = 2 * time.Second

	volumePollInterval = 10 * time.Millisecond
	volumeProbeTimeout = 2 * time.Second
	volumeSetupTimeout = 10 * time.Second
)

// VolumeSpec describes one operator-owned disk image attached to a sandbox.
type VolumeSpec struct {
	Name     string
	Path     string
	DirectIO string
	RW       bool
}

func (s VolumeSpec) directIO() (string, error) {
	mode := cmp.Or(s.DirectIO, types.DirectIOOff)
	if !types.ValidDirectIO(mode) {
		return "", fmt.Errorf("volume directio must be on, off, or auto, got %q", s.DirectIO)
	}
	return mode, nil
}

// DiskAttach hot-attaches an operator-owned disk through cocoon.
func (e *Engine) DiskAttach(ctx context.Context, vmName string, spec VolumeSpec) error {
	args, err := e.diskAttachArgs(vmName, spec)
	if err != nil {
		return err
	}
	_, err = e.run(ctx, args...)
	return err
}

// MountVolume discovers and mounts a hot-attached disk at mount, read-only unless rw.
func (e *Engine) MountVolume(ctx context.Context, vsockSocket, name, mount string, rw bool) error {
	ctx, cancel := context.WithTimeout(ctx, volumeSetupTimeout)
	defer cancel()
	device, err := e.waitForVolumeDevice(ctx, vsockSocket, name)
	if err != nil {
		return fmt.Errorf("wait for volume device %s: %w", name, err)
	}
	if err := e.silkdExec(ctx, vsockSocket, "mkdir", "-p", "--", mount); err != nil {
		return fmt.Errorf("create volume mount point %s: %w", mount, err)
	}
	mode := types.VolumeModeRO
	if rw {
		mode = types.VolumeModeRW
	}
	if err := e.silkdExec(ctx, vsockSocket, "mount", "-o", mode, "--", device, mount); err != nil {
		return fmt.Errorf("mount volume %s: %w", name, err)
	}
	return nil
}

// UnmountVolume flushes and detaches a guest mount, so a writable image's
// dirty state reaches the backing file before the VM is removed.
func (e *Engine) UnmountVolume(ctx context.Context, vsockSocket, mount string) error {
	ctx, cancel := context.WithTimeout(ctx, VolumeCallTimeout)
	defer cancel()
	if err := e.silkdExec(ctx, vsockSocket, "umount", "--", mount); err != nil {
		return fmt.Errorf("unmount volume %s: %w", mount, err)
	}
	return nil
}

// SyncGuest flushes the guest page cache — the only flush left for a mount that
// refused to unmount, since a lazy unmount would keep writing behind us.
func (e *Engine) SyncGuest(ctx context.Context, vsockSocket string) error {
	ctx, cancel := context.WithTimeout(ctx, VolumeCallTimeout)
	defer cancel()
	if err := e.silkdExec(ctx, vsockSocket, "sync"); err != nil {
		return fmt.Errorf("sync guest: %w", err)
	}
	return nil
}

func (e *Engine) waitForVolumeDevice(ctx context.Context, vsockSocket, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, volumeProbeTimeout)
	defer cancel()
	ticker := time.NewTicker(volumePollInterval)
	defer ticker.Stop()
	for {
		device, found, err := e.findVolumeDevice(ctx, vsockSocket, name)
		if err != nil {
			return "", cmp.Or(ctx.Err(), err)
		}
		if found {
			return device, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e *Engine) findVolumeDevice(ctx context.Context, vsockSocket, name string) (string, bool, error) {
	entries, err := e.silkdList(ctx, vsockSocket, "/sys/block")
	if err != nil {
		return "", false, err
	}
	for _, entry := range entries {
		serial, err := e.silkdReadFile(ctx, vsockSocket, "/sys/block/"+entry.Name+"/serial")
		if isNotFound(err) {
			continue
		}
		if err != nil {
			return "", false, err
		}
		if strings.TrimSpace(string(serial)) != name {
			continue
		}
		device := "/dev/" + entry.Name
		// The kernel publishes /sys/block before devtmpfs creates the node, so
		// a serial match can still precede the device actually existing.
		if _, err := e.silkdStat(ctx, vsockSocket, device); err != nil {
			if isNotFound(err) {
				return "", false, nil
			}
			return "", false, err
		}
		return device, true, nil
	}
	return "", false, nil
}

func (e *Engine) diskAttachArgs(vmName string, spec VolumeSpec) ([]string, error) {
	directIO, err := spec.directIO()
	if err != nil {
		return nil, err
	}
	args := []string{
		"vm", "disk", "attach", vmName,
		"--path", spec.Path,
		argName, spec.Name,
	}
	if !spec.RW {
		args = append(args, "--readonly")
	}
	return append(args, "--directio", directIO), nil
}

func isNotFound(err error) bool {
	var respErr *wire.ErrorResp
	return errors.As(err, &respErr) && respErr.Kind == wire.KindNotFound
}
