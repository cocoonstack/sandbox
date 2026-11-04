package types

import (
	"cmp"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

const (
	MaxClaimVolumes = 8

	// Also the guest `mount -o` option literals: renaming them changes the mount flags.
	VolumeModeRO = "ro"
	VolumeModeRW = "rw"

	DirectIOOn   = "on"
	DirectIOOff  = "off"
	DirectIOAuto = "auto"
)

var (
	// VolumeNameRe is the virtio disk serial grammar shared with cocoon.
	VolumeNameRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,19}$`)

	guestOSMountRoots = []string{
		"/bin", "/boot", "/dev", "/etc", "/lib", "/lib64", "/proc",
		"/run", "/sbin", "/sys", "/usr", "/var",
	}
)

// Volume is one dataset mount; Mode is normalized to "" (read-only) or VolumeModeRW.
type Volume struct {
	Name  string `json:"name"`
	Mount string `json:"mount,omitempty"`
	Mode  string `json:"mode,omitempty"`
	// AttachOnly leaves mounting and unmounting to the caller.
	AttachOnly bool `json:"attach_only,omitzero"`
}

// RW reports whether the entry asks for write access.
func (v Volume) RW() bool { return v.Mode == VolumeModeRW }

// ValidVolumeName reports whether name is a legal cocoon data-disk serial.
func ValidVolumeName(name string) bool {
	return VolumeNameRe.MatchString(name) && !strings.HasPrefix(name, "cocoon-")
}

// DefaultVolumeMount returns the guest mount used when a request omits one.
func DefaultVolumeMount(name string) string {
	return "/volumes/" + name
}

// ValidDirectIO reports whether mode is a legal volume direct-I/O setting.
func ValidDirectIO(mode string) bool {
	return mode == DirectIOOn || mode == DirectIOOff || mode == DirectIOAuto
}

// VolumeNames projects the entries' names in order; nil for none.
func VolumeNames(volumes []Volume) []string {
	if len(volumes) == 0 {
		return nil
	}
	names := make([]string, len(volumes))
	for i, volume := range volumes {
		names[i] = volume.Name
	}
	return names
}

// VolumeRWNames projects the names of the write-enabled entries; nil for none.
func VolumeRWNames(volumes []Volume) []string {
	var names []string
	for _, volume := range volumes {
		if volume.RW() {
			names = append(names, volume.Name)
		}
	}
	return names
}

// ValidateVolumes returns detached entries with defaults filled and modes normalized.
func ValidateVolumes(volumes []Volume) ([]Volume, error) {
	if len(volumes) > MaxClaimVolumes {
		return nil, fmt.Errorf("volumes must contain at most %d entries, got %d", MaxClaimVolumes, len(volumes))
	}
	applied := make([]Volume, len(volumes))
	names := make(map[string]struct{}, len(volumes))
	for i, volume := range volumes {
		if !ValidVolumeName(volume.Name) {
			return nil, fmt.Errorf("volumes[%d] name %q must match %s and not start with cocoon-", i, volume.Name, VolumeNameRe)
		}
		if _, ok := names[volume.Name]; ok {
			return nil, fmt.Errorf("volumes[%d] duplicates name %q", i, volume.Name)
		}
		names[volume.Name] = struct{}{}
		mode := volume.Mode
		switch mode {
		case VolumeModeRO:
			mode = ""
		case "", VolumeModeRW:
		default:
			return nil, fmt.Errorf("volumes[%d] mode %q must be %s or %s", i, mode, VolumeModeRO, VolumeModeRW)
		}
		if volume.AttachOnly {
			if volume.Mount != "" {
				return nil, fmt.Errorf("volumes[%d] mount %q is meaningless when the caller mounts the device itself", i, volume.Mount)
			}
			applied[i] = Volume{Name: volume.Name, Mode: mode, AttachOnly: true}
			continue
		}
		mount := cmp.Or(volume.Mount, DefaultVolumeMount(volume.Name))
		if err := validateVolumeMount(mount); err != nil {
			return nil, fmt.Errorf("volumes[%d] mount %q: %w", i, mount, err)
		}
		for j := range i {
			other := applied[j].Mount
			if other == "" {
				continue
			}
			switch {
			case mount == other:
				return nil, fmt.Errorf("volumes[%d] mount %q duplicates volumes[%d]", i, mount, j)
			case pathWithin(other, mount), pathWithin(mount, other):
				return nil, fmt.Errorf("volumes[%d] mount %q nests with volumes[%d] mount %q", i, mount, j, other)
			}
		}
		applied[i] = Volume{Name: volume.Name, Mount: mount, Mode: mode}
	}
	return applied, nil
}

func validateVolumeMount(mount string) error {
	if !filepath.IsAbs(mount) {
		return errors.New("must be absolute")
	}
	if filepath.Clean(mount) != mount {
		return errors.New("must be clean")
	}
	if mount == "/" || slices.ContainsFunc(guestOSMountRoots, func(root string) bool { return mount == root || pathWithin(root, mount) }) {
		return errors.New("must be outside the guest OS tree")
	}
	return nil
}

func pathWithin(parent, child string) bool {
	return strings.HasPrefix(child, parent+"/")
}
