package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const VolumeMutationTimeout = 30 * time.Second

// DetachVolume refuses busy or foreign mounts and confirms host removal even
// when the guest serial has disappeared. It never uses lazy or forced umount.
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
				return false, fmt.Errorf("volume device is already mounted at %q", entry.path)
			}
			continue
		}
		if matched || entry.device != dev || entry.root != "/" || !slices.Contains(strings.Split(entry.options, ","), mode) {
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
