package engine

import (
	"cmp"
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
	return fmt.Sprintf("live volume changes require Cocoon >= %s; installed version is %s", LiveVolumeCocoon, cmp.Or(e.Version, "unavailable"))
}

type volumeMount struct {
	devNum, root, path, options string
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
func (e *Engine) MountLiveVolume(ctx context.Context, vsockSocket, name, mount string, rw bool) error {
	ctx, cancel := context.WithTimeout(ctx, volumeSetupTimeout)
	defer cancel()
	device, err := e.waitForVolumeDevice(ctx, vsockSocket, name)
	if err != nil {
		return fmt.Errorf("wait for volume device %s: %w", name, err)
	}
	mounted, err := e.volumeMounted(ctx, vsockSocket, device, mount, rw)
	if err != nil || mounted {
		return err
	}
	return e.mountDevice(ctx, vsockSocket, device, name, mount, rw)
}

// DetachVolume unmounts a guest volume before confirming host removal.
func (e *Engine) DetachVolume(ctx context.Context, vmName, vsockSocket, name, mount string) error {
	ctx, cancel := context.WithTimeout(ctx, VolumeMutationTimeout)
	defer cancel()
	device, found, err := e.findVolumeDevice(ctx, vsockSocket, name)
	if err != nil {
		return err
	}
	if found {
		if err = e.unmountLiveVolume(ctx, vsockSocket, device, mount); err != nil {
			return err
		}
	}
	_, err = e.run(ctx, "vm", "disk", "detach", vmName, argName, name)
	return err
}

func (e *Engine) unmountLiveVolume(ctx context.Context, vsockSocket, device, mount string) error {
	mounted, err := e.volumeMounted(ctx, vsockSocket, device, mount, false)
	if err != nil {
		return err
	}
	if mounted {
		if err = e.UnmountVolume(ctx, vsockSocket, mount); err != nil {
			if _, ok := errors.AsType[*guestExitError](err); ok {
				return &VolumeMountConflict{Message: fmt.Sprintf("cannot unmount %q; close guest references and retry", mount)}
			}
			return err
		}
	}
	return e.refuseOtherMounts(ctx, vsockSocket, device)
}

func (e *Engine) volumeMounts(ctx context.Context, vsockSocket, device string) (string, []volumeMount, error) {
	devNum, err := e.silkdReadFile(ctx, vsockSocket, "/sys/block/"+filepath.Base(device)+"/dev", silkdReadMax)
	if err != nil {
		return "", nil, err
	}
	raw, err := e.silkdReadFile(ctx, vsockSocket, "/proc/self/mountinfo", mountInfoMax)
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
	return strings.TrimSpace(string(devNum)), mounts, nil
}

func (e *Engine) volumeMounted(ctx context.Context, vsockSocket, device, mount string, rw bool) (bool, error) {
	devNum, mounts, err := e.volumeMounts(ctx, vsockSocket, device)
	if err != nil {
		return false, err
	}
	target := e.resolveMount(ctx, vsockSocket, mount, mounts)
	matched := false
	for _, entry := range mounts {
		if entry.path != target {
			if entry.devNum == devNum {
				return false, bindConflict(entry.path)
			}
			continue
		}
		if mode, _, _ := strings.Cut(entry.options, ","); matched || entry.devNum != devNum || entry.root != "/" || mode != mountMode(rw) {
			return false, &VolumeMountConflict{Message: fmt.Sprintf("volume mount %q has a different device, root, or mode; restore the expected mount before retrying", target)}
		}
		matched = true
	}
	return matched, nil
}

func (e *Engine) resolveMount(ctx context.Context, vsockSocket, mount string, mounts []volumeMount) string {
	if slices.ContainsFunc(mounts, func(m volumeMount) bool { return m.path == mount }) {
		return mount
	}
	resolved, err := e.silkdOutput(ctx, vsockSocket, "realpath", mount)
	if err != nil {
		log.WithFunc("engine.resolveMount").Warnf(ctx, "resolve volume mount %s, checking it as given: %v", mount, err)
		return mount
	}
	return strings.TrimSuffix(string(resolved), "\n")
}

func (e *Engine) refuseOtherMounts(ctx context.Context, vsockSocket, device string) error {
	devNum, mounts, err := e.volumeMounts(ctx, vsockSocket, device)
	if err != nil {
		return err
	}
	for _, mount := range mounts {
		if mount.devNum == devNum {
			return bindConflict(mount.path)
		}
	}
	return nil
}

func bindConflict(path string) *VolumeMountConflict {
	return &VolumeMountConflict{Message: fmt.Sprintf("volume device has another mount at %q; remove guest mount references before retrying", path)}
}
