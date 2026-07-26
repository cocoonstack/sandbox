package pool

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// SandboxStats is one sandbox's resource usage. The declared fields (CPUCount,
// MemTotalBytes) come from the size tier, which is what the VM was actually
// booted with, so they are authoritative. The measured field (MemUsedBytes) is
// the host VMM process's resident set — for a microVM that is the guest's
// backing memory plus a small VMM overhead, and it is the only usage signal
// available without a guest agent that reports its own /proc.
//
// MemUsedBytes is zero when the VMM process cannot be found (a hibernated
// sandbox has no process at all); callers must treat zero as "not measured"
// rather than "idle".
type SandboxStats struct {
	ID              string    `json:"id"`
	CPUCount        int       `json:"cpu_count"`
	MemTotalBytes   int64     `json:"mem_total_bytes"`
	MemUsedBytes    int64     `json:"mem_used_bytes"`
	Hibernated      bool      `json:"hibernated"`
	MeasuredAt      time.Time `json:"measured_at"`
	MemUsedMeasured bool      `json:"mem_used_measured"`
}

// Stats reports one live claim's resource usage.
func (m *Manager) Stats(id string) (SandboxStats, bool) {
	sb, ok := m.byID(id)
	if !ok {
		return SandboxStats{}, false
	}
	spec, _ := sb.Key.Size.Spec()
	st := SandboxStats{
		ID:            sb.ID,
		CPUCount:      spec.CPU,
		MemTotalBytes: parseMemSize(spec.Memory),
		Hibernated:    sb.HibernateSnap != "",
		MeasuredAt:    time.Now().UTC(),
	}
	if !st.Hibernated {
		if rss, ok := vmmResidentBytes(sb.VMName); ok {
			st.MemUsedBytes, st.MemUsedMeasured = rss, true
		}
	}
	return st, true
}

// parseMemSize converts a size tier's memory string ("512M", "8G") to bytes.
func parseMemSize(s string) int64 {
	if s == "" {
		return 0
	}
	mult := int64(1)
	switch s[len(s)-1] {
	case 'K', 'k':
		mult, s = 1<<10, s[:len(s)-1]
	case 'M', 'm':
		mult, s = 1<<20, s[:len(s)-1]
	case 'G', 'g':
		mult, s = 1<<30, s[:len(s)-1]
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0
	}
	return n * mult
}

// vmmResidentBytes finds the hypervisor process serving vmName and reports its
// resident set. The VM name appears in the VMM's argv (its run directory), so
// the scan matches on that; it is O(processes) and therefore meant for a
// single-sandbox read, never a whole-fleet sweep.
func vmmResidentBytes(vmName string) (int64, bool) {
	if vmName == "" {
		return 0, false
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, false
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid := e.Name()
		if pid[0] < '0' || pid[0] > '9' {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", pid, "cmdline"))
		if err != nil || !strings.Contains(string(cmdline), vmName) {
			continue
		}
		if rss, ok := residentBytes(pid); ok {
			return rss, true
		}
	}
	return 0, false
}

// residentBytes reads a process's resident set from /proc/<pid>/statm, whose
// second field is the resident page count.
func residentBytes(pid string) (int64, bool) {
	b, err := os.ReadFile(filepath.Join("/proc", pid, "statm"))
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * int64(os.Getpagesize()), true
}
