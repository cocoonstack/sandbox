package engine

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	// VolumeCallTimeout bounds one guest teardown exec.
	VolumeCallTimeout = 2 * time.Second
	// VolumeMutationTimeout bounds one live attach or detach, guest steps included.
	VolumeMutationTimeout = 30 * time.Second

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

// DiskAttachSnapshot hot-attaches a private read-write copy of a local snapshot's copy-on-write disk and returns its guest serial.
func (e *Engine) DiskAttachSnapshot(ctx context.Context, vmName, snapName string) (string, error) {
	return e.diskAttachCopy(ctx, vmName, "--from-snapshot", snapName)
}

// DiskAttachDir is DiskAttachSnapshot from an export directory.
func (e *Engine) DiskAttachDir(ctx context.Context, vmName, dir string) (string, error) {
	return e.diskAttachCopy(ctx, vmName, "--from-dir", dir)
}

// MountVolume discovers and mounts a hot-attached disk at mount, read-only unless rw.
func (e *Engine) MountVolume(ctx context.Context, vsockSocket, name, mount string, rw bool) error {
	ctx, cancel := context.WithTimeout(ctx, volumeSetupTimeout)
	defer cancel()
	device, err := e.waitForVolumeDevice(ctx, vsockSocket, name)
	if err != nil {
		return fmt.Errorf("wait for volume device %s: %w", name, err)
	}
	return e.mountDevice(ctx, vsockSocket, device, name, mount, rw)
}

// UnmountVolume flushes and detaches a guest mount before the VM is removed.
func (e *Engine) UnmountVolume(ctx context.Context, vsockSocket, mount string) error {
	ctx, cancel := context.WithTimeout(ctx, VolumeCallTimeout)
	defer cancel()
	if err := e.silkdExec(ctx, vsockSocket, "umount", "--", mount); err != nil {
		return fmt.Errorf("unmount volume %s: %w", mount, err)
	}
	return nil
}

// SyncGuest flushes the guest page cache, the only flush left for a failed unmount.
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
		serial, err := e.silkdReadFile(ctx, vsockSocket, "/sys/block/"+entry.Name+"/serial", silkdReadMax)
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
		// the kernel publishes /sys/block before devtmpfs creates the node.
		if err := e.silkdStat(ctx, vsockSocket, device); err != nil {
			if isNotFound(err) {
				return "", false, nil
			}
			return "", false, err
		}
		return device, true, nil
	}
	return "", false, nil
}

func (e *Engine) mountDevice(ctx context.Context, vsockSocket, device, name, mount string, rw bool) error {
	if err := e.silkdExec(ctx, vsockSocket, "mkdir", "-p", "--", mount); err != nil {
		return fmt.Errorf("create volume mount point %s: %w", mount, err)
	}
	if err := e.silkdExec(ctx, vsockSocket, "mount", "-o", mountMode(rw), "--", device, mount); err != nil {
		return fmt.Errorf("mount volume %s: %w", name, err)
	}
	return nil
}

func (e *Engine) diskAttachCopy(ctx context.Context, vmName, sourceFlag, source string) (string, error) {
	out, err := e.run(ctx, "vm", "disk", "attach", vmName, sourceFlag, source, argOutput, formatJSON)
	if err != nil {
		return "", err
	}
	var attached struct {
		Name string `json:"name"`
	}
	if err = json.Unmarshal(out, &attached); err != nil || attached.Name == "" {
		return "", fmt.Errorf("parse disk attach result %q: %w", out, cmp.Or(err, errors.New("no disk name")))
	}
	return attached.Name, nil
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
	respErr, ok := errors.AsType[*wire.ErrorResp](err)
	return ok && respErr.Kind == wire.KindNotFound
}

func mountMode(rw bool) string {
	if rw {
		return types.VolumeModeRW
	}
	return types.VolumeModeRO
}
