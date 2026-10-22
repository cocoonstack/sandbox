package engine

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

type VolumeHookCall struct {
	Argv                                           []string
	Action, Operation, VolumeID, VolumeName, Mount string
	Timeout                                        time.Duration
}

// VolumeHookExit is a guest volume hook that ran and exited non-zero; Output is the guest's own.
type VolumeHookExit struct {
	Code   int32
	Output string
}

func (e *VolumeHookExit) Error() string { return fmt.Sprintf("exit code %d: %s", e.Code, e.Output) }

type HibernateSource struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (e *Engine) RunVolumeHook(ctx context.Context, socket string, call VolumeHookCall) error {
	seconds := strconv.FormatFloat(call.Timeout.Seconds(), 'f', -1, 64)
	argv := []string{"/usr/bin/flock", "--timeout", seconds, "/run/sandboxd-volume-hook.lock", "/usr/bin/timeout", "--signal=KILL", "--", seconds + "s"}
	argv = append(argv, call.Argv...)
	argv = append(argv, call.Action, call.Operation, call.VolumeID, call.VolumeName, call.Mount)
	ctx, cancel := context.WithTimeout(ctx, 2*call.Timeout+10*time.Second)
	defer cancel()
	_, err := e.silkdOutput(ctx, socket, argv...)
	if exit, ok := errors.AsType[*guestExitError](err); ok {
		return &VolumeHookExit{Code: exit.code, Output: exit.output}
	}
	return err
}

func (e *Engine) InspectHibernateSource(ctx context.Context, name string) (HibernateSource, bool, error) {
	names, err := e.SnapshotList(ctx)
	if err != nil {
		return HibernateSource{}, false, err
	}
	if !slices.Contains(names, name) {
		return HibernateSource{}, false, nil
	}
	raw, err := e.run(ctx, "snapshot", "inspect", name)
	if err != nil {
		return HibernateSource{}, false, err
	}
	var source HibernateSource
	if err = json.Unmarshal(raw, &source); err != nil {
		return source, false, fmt.Errorf("parse snapshot inspect: %w", err)
	}
	if source.ID == "" || source.Name != name {
		return source, false, errors.New("snapshot identity unavailable")
	}
	return source, true, nil
}

func (e *Engine) VerifyMountedVolume(ctx context.Context, socket, name, mount string) error {
	ctx, cancel := context.WithTimeout(ctx, volumeSetupTimeout)
	defer cancel()
	device, found, err := e.findVolumeDevice(ctx, socket, name)
	if err != nil {
		return err
	}
	if !found {
		return &VolumeMountConflict{Message: "managed volume device is absent"}
	}
	devNum, mounts, err := e.volumeMounts(ctx, socket, device)
	if err != nil {
		return err
	}
	target := e.resolveMount(ctx, socket, mount, mounts)
	matched := false
	for _, entry := range mounts {
		if entry.path != target && entry.devNum != devNum {
			continue
		}
		if matched || entry.path != target || entry.devNum != devNum || entry.root != "/" || !slices.Contains(strings.Split(entry.options, ","), "rw") {
			return &VolumeMountConflict{Message: fmt.Sprintf("managed volume must have one exact writable root mount at %q", target)}
		}
		matched = true
	}
	if !matched {
		return &VolumeMountConflict{Message: fmt.Sprintf("managed volume mount %q is absent", target)}
	}
	return nil
}
