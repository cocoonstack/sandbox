package engine

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	diskObserved   = "observed"
	diskUnobserved = "unobserved"
)

type DiskRef struct {
	VMID   string `json:"vm_id"`
	DiskID string `json:"disk_id"`
}
type DiskProcess struct {
	BootID string `json:"boot_id"`
	PID    int    `json:"pid"`
	Start  uint64 `json:"start"`
}
type DiskFD struct {
	Number int    `json:"number"`
	Access string `json:"access"`
}
type DiskBacking struct {
	Device uint64   `json:"device"`
	Inode  uint64   `json:"inode"`
	FDs    []DiskFD `json:"fds"`
}
type DiskBinding struct {
	Ref           DiskRef     `json:"ref"`
	VMM           DiskProcess `json:"vmm"`
	Serial        string      `json:"serial"`
	Path          string      `json:"path"`
	Mode          string      `json:"mode"`
	Format        string      `json:"format"`
	Backing       DiskBacking `json:"backing"`
	ObservedBytes int64       `json:"observed_bytes,string"`
	Revision      uint64      `json:"revision"`
}
type DiskGrowRequest struct {
	Expected    DiskBinding `json:"expected"`
	ID          string      `json:"id"`
	TargetBytes int64       `json:"target_bytes,string"`
}
type DiskReceipt struct {
	Request  DiskGrowRequest `json:"request"`
	Revision uint64          `json:"revision"`
	Outcome  string          `json:"outcome"`
}
type DiskObservation struct {
	State   string       `json:"state"`
	Binding *DiskBinding `json:"binding,omitempty"`
	Reason  string       `json:"reason,omitempty"`
}
type DiskError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type DiskInspection struct {
	Requested        DiskRef         `json:"requested"`
	Live             DiskObservation `json:"live"`
	LatestReceipt    *DiskReceipt    `json:"latest_receipt,omitempty"`
	GuestBlockStatus string          `json:"guest_block_status"`
	Error            *DiskError      `json:"error,omitempty"`
}
type DiskGrowResult struct {
	Receipt          *DiskReceipt    `json:"receipt,omitempty"`
	Live             DiskObservation `json:"live"`
	GuestBlockStatus string          `json:"guest_block_status"`
	Error            *DiskError      `json:"error,omitempty"`
}

var growUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func ValidGrowID(id string) bool {
	return growUUID.MatchString(id) && id != "00000000-0000-0000-0000-000000000000"
}

func (b DiskBinding) Validate() error {
	if b.Ref.VMID == "" || b.Ref.DiskID == "" || b.VMM.BootID == "" || b.VMM.PID <= 0 || b.VMM.Start == 0 || b.Serial == "" || !filepath.IsAbs(b.Path) || filepath.Clean(b.Path) != b.Path || b.Mode != "read_write" || b.Format != "raw" || b.Backing.Inode == 0 || len(b.Backing.FDs) == 0 || b.ObservedBytes <= 0 {
		return errors.New("invalid disk binding")
	}
	for i, fd := range b.Backing.FDs {
		if fd.Number < 0 || fd.Access != "read_write" || i > 0 && b.Backing.FDs[i-1].Number >= fd.Number {
			return errors.New("invalid disk FD witnesses")
		}
	}
	return nil
}

func (b DiskBinding) SameIdentity(other DiskBinding) bool {
	return b.Ref == other.Ref && b.VMM == other.VMM && b.Serial == other.Serial && b.Path == other.Path && b.Mode == other.Mode && b.Format == other.Format && b.Backing.Device == other.Backing.Device && b.Backing.Inode == other.Backing.Inode && slices.Equal(b.Backing.FDs, other.Backing.FDs)
}

func (b DiskBinding) Equal(other DiskBinding) bool {
	return b.SameIdentity(other) && b.ObservedBytes == other.ObservedBytes && b.Revision == other.Revision
}

func (r DiskGrowRequest) Equal(other DiskGrowRequest) bool {
	return r.ID == other.ID && r.TargetBytes == other.TargetBytes && r.Expected.Equal(other.Expected)
}

func (e *Engine) InspectDisk(ctx context.Context, ref DiskRef) (DiskInspection, error) {
	var result DiskInspection
	raw, err := e.run(ctx, "vm", "disk", "inspect", ref.VMID, "--disk-id", ref.DiskID, "--output", "json")
	if parseErr := json.Unmarshal(raw, &result, json.RejectUnknownMembers(true)); parseErr != nil {
		return DiskInspection{}, errors.Join(err, parseErr)
	}
	if result.Requested != ref || result.GuestBlockStatus != diskUnobserved {
		return DiskInspection{}, errors.New("unsupported disk inspection")
	}
	if result.Live.State != diskObserved && result.Live.State != "unavailable" {
		return DiskInspection{}, errors.New("invalid disk observation")
	}
	if result.Live.State == diskObserved && (result.Live.Binding == nil || result.Live.Binding.Validate() != nil || result.Live.Binding.Ref != ref) {
		return DiskInspection{}, errors.New("invalid observed binding")
	}
	return result, err
}

func (e *Engine) GrowDisk(ctx context.Context, request DiskGrowRequest) (DiskGrowResult, error) {
	if request.Expected.Validate() != nil || !ValidGrowID(request.ID) || request.TargetBytes <= request.Expected.ObservedBytes || request.TargetBytes%4096 != 0 {
		return DiskGrowResult{}, errors.New("invalid disk grow request")
	}
	expected := DiskInspection{Requested: request.Expected.Ref, Live: DiskObservation{State: diskObserved, Binding: &request.Expected}, GuestBlockStatus: diskUnobserved}
	raw, err := json.Marshal(expected)
	if err != nil {
		return DiskGrowResult{}, err
	}
	f, err := os.CreateTemp("", "sandbox-grow-")
	if err != nil {
		return DiskGrowResult{}, err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	_, writeErr := f.Write(raw)
	if err = errors.Join(writeErr, f.Close()); err != nil {
		return DiskGrowResult{}, err
	}
	var result DiskGrowResult
	raw, err = e.run(ctx, "vm", "disk", "grow", "--expect", f.Name(), "--request-id", request.ID, "--target-bytes", strconv.FormatInt(request.TargetBytes, 10), "--output", "json")
	if parseErr := json.Unmarshal(raw, &result, json.RejectUnknownMembers(true)); parseErr != nil {
		return DiskGrowResult{}, errors.Join(err, parseErr)
	}
	if result.GuestBlockStatus != diskUnobserved {
		return DiskGrowResult{}, errors.New("unsupported disk grow response")
	}
	return result, err
}

type GuestVolumeObservation struct {
	BootID    string `json:"boot_id"`
	Serial    string `json:"serial"`
	Device    string `json:"device"`
	DevNum    string `json:"dev_num"`
	MountID   string `json:"mount_id"`
	Mount     string `json:"mount"`
	Root      string `json:"root"`
	Mode      string `json:"mode"`
	SizeBytes int64  `json:"size_bytes"`
}

func (g GuestVolumeObservation) SameIdentity(other GuestVolumeObservation) bool {
	g.SizeBytes = other.SizeBytes
	return g == other
}

func (g GuestVolumeObservation) Valid() bool {
	return g.BootID != "" && g.Serial != "" && strings.HasPrefix(g.Device, "/dev/") && g.DevNum != "" && g.MountID != "" && filepath.IsAbs(g.Mount) && g.Root == "/" && g.Mode == "rw" && g.SizeBytes > 0
}

// ObserveVolume reads block and mount identity without changing the filesystem.
func (e *Engine) ObserveVolume(ctx context.Context, socket, serial, mount string) (GuestVolumeObservation, error) {
	first, err := e.observeVolume(ctx, socket, serial, mount)
	if err != nil {
		return GuestVolumeObservation{}, err
	}
	second, err := e.observeVolume(ctx, socket, serial, mount)
	if err != nil {
		return GuestVolumeObservation{}, err
	}
	if !first.SameIdentity(second) {
		return GuestVolumeObservation{}, errors.New("guest identity changed during block observation")
	}
	return second, nil
}

func (e *Engine) observeVolume(ctx context.Context, socket, serial, mount string) (GuestVolumeObservation, error) {
	ctx, cancel := context.WithTimeout(ctx, volumeSetupTimeout)
	defer cancel()
	device, found, err := e.findVolumeDevice(ctx, socket, serial)
	if err != nil || !found {
		return GuestVolumeObservation{}, errors.New("guest disk unavailable")
	}
	boot, err := e.silkdReadFile(ctx, socket, "/proc/sys/kernel/random/boot_id", silkdReadMax)
	if err != nil {
		return GuestVolumeObservation{}, err
	}
	devNum, mounts, err := e.volumeMounts(ctx, socket, device)
	if err != nil {
		return GuestVolumeObservation{}, err
	}
	target := e.resolveMount(ctx, socket, mount, mounts)
	g := GuestVolumeObservation{BootID: strings.TrimSpace(string(boot)), Serial: serial, Device: device, DevNum: devNum}
	for _, m := range mounts {
		if m.devNum != devNum && m.path != target {
			continue
		}
		mode, _, _ := strings.Cut(m.options, ",")
		if g.MountID != "" || m.devNum != devNum || m.path != target || m.root != "/" || mode != "rw" {
			return GuestVolumeObservation{}, errors.New("guest mount identity mismatch")
		}
		g.MountID, g.Mount, g.Root, g.Mode = m.id, m.path, m.root, mode
	}
	size, err := e.silkdReadFile(ctx, socket, "/sys/block/"+filepath.Base(device)+"/size", silkdReadMax)
	if err != nil {
		return GuestVolumeObservation{}, err
	}
	sectors, err := strconv.ParseInt(strings.TrimSpace(string(size)), 10, 64)
	if err != nil || sectors <= 0 || sectors > (1<<63-1)/512 {
		return GuestVolumeObservation{}, fmt.Errorf("invalid guest block size")
	}
	g.SizeBytes = sectors * 512
	if !g.Valid() {
		return GuestVolumeObservation{}, errors.New("incomplete guest volume observation")
	}
	return g, nil
}
