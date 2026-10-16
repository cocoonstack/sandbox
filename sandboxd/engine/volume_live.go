package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/projecteru2/core/log"
)

var mountUnescaper = strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)

// VolumeMountConflict reports a guest mount that must be resolved before retrying.
type VolumeMountConflict struct {
	Message string
}

func (e *VolumeMountConflict) Error() string { return e.Message }

// LiveVolumeUnsupported reports a runtime that cannot safely change live volumes.
type LiveVolumeUnsupported struct {
	Version string
}

func (e *LiveVolumeUnsupported) Error() string {
	if e.Version != "" {
		return fmt.Sprintf("live volume changes require Cocoon >= %s; installed version is %s", LiveVolumeCocoon, e.Version)
	}
	return "live volume changes require Cocoon >= " + LiveVolumeCocoon + "; runtime version unavailable"
}

type volumeMount struct {
	device, root, path, options string
}

// CheckLiveVolumeSupport rejects runtimes without idempotent live disk attach and detach.
func (e *Engine) CheckLiveVolumeSupport(ctx context.Context) error {
	version, err := e.Version(ctx)
	if err != nil {
		log.WithFunc("engine.CheckLiveVolumeSupport").Warnf(ctx, "query cocoon version: %v", err)
		return &LiveVolumeUnsupported{}
	}
	if below, known := belowFloor(version, LiveVolumeCocoon); known && below {
		return &LiveVolumeUnsupported{Version: version}
	}
	return nil
}

// MountLiveVolume mounts an attached disk unless the same mount already exists.
func (e *Engine) MountLiveVolume(ctx context.Context, sock, name, mount string, rw bool) error {
	ctx, cancel := context.WithTimeout(ctx, volumeSetupTimeout)
	defer cancel()
	device, err := e.waitForVolumeDevice(ctx, sock, name)
	if err != nil {
		return fmt.Errorf("wait for volume device %s: %w", name, err)
	}
	mounted, err := e.volumeMounted(ctx, sock, device, mount, rw)
	if err != nil || mounted {
		return err
	}
	return e.mountDevice(ctx, sock, device, name, mount, rw)
}

// DetachVolume unmounts a guest volume before confirming host removal.
func (e *Engine) DetachVolume(ctx context.Context, vmName, sock, name, mount string) error {
	ctx, cancel := context.WithTimeout(ctx, VolumeMutationTimeout)
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
	return err
}

func (e *Engine) unmountLiveVolume(ctx context.Context, sock, device, mount string) error {
	mounted, err := e.volumeMounted(ctx, sock, device, mount, false)
	if err != nil {
		return err
	}
	if mounted {
		if err = e.UnmountVolume(ctx, sock, mount); err != nil {
			if _, ok := errors.AsType[*guestExitError](err); ok {
				return &VolumeMountConflict{Message: fmt.Sprintf("cannot unmount %q; close guest references and retry", mount)}
			}
			return err
		}
	}
	// Refuse a second/bind mount of the same device before hot-unplug.
	return e.volumeUnmounted(ctx, sock, device)
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
			return "", nil, errors.New("invalid guest mountinfo")
		}
		mounts = append(mounts, volumeMount{fields[2], mountUnescaper.Replace(fields[3]), mountUnescaper.Replace(fields[4]), fields[5]})
	}
	return strings.TrimSpace(string(dev)), mounts, nil
}

func (e *Engine) volumeMounted(ctx context.Context, sock, device, mount string, rw bool) (bool, error) {
	dev, mounts, err := e.volumeMounts(ctx, sock, device)
	if err != nil {
		return false, err
	}
	mode := "ro"
	if rw {
		mode = "rw"
	}
	matched := false
	for _, entry := range mounts {
		if entry.path != mount {
			if entry.device == dev {
				return false, bindConflict(entry.path)
			}
			continue
		}
		if matched || entry.device != dev || entry.root != "/" || !slices.Contains(strings.Split(entry.options, ","), mode) {
			return false, &VolumeMountConflict{Message: fmt.Sprintf("volume mount %q has a different device, root, or mode; restore the expected mount before retrying", mount)}
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
			return bindConflict(mount.path)
		}
	}
	return nil
}

func bindConflict(path string) *VolumeMountConflict {
	return &VolumeMountConflict{Message: fmt.Sprintf("volume device has another mount at %q; remove guest mount references before retrying", path)}
}
