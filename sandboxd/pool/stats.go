package pool

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// SandboxStats is one sandbox's resource usage. CPUCount and MemTotalBytes come
// from the size tier the VM was booted with. MemUsedBytes is the host VMM
// process's resident set, the only usage signal available without a guest agent;
// MemUsedMeasured is false when no VMM process was found (a hibernated sandbox
// has none), so a zero is never read as idle.
type SandboxStats struct {
	ID              string    `json:"id"`
	CPUCount        int       `json:"cpu_count"`
	MemTotalBytes   int64     `json:"mem_total_bytes"`
	MemUsedBytes    int64     `json:"mem_used_bytes"`
	MemUsedMeasured bool      `json:"mem_used_measured"`
	Hibernated      bool      `json:"hibernated"`
	MeasuredAt      time.Time `json:"measured_at"`
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
		MemTotalBytes: spec.MemoryBytes,
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

// vmmResidentBytes reports the resident set of the hypervisor process serving
// vmName, matched on the run directory in the VMM's argv. The scan is
// O(processes), so this is a single-sandbox read, never a fleet sweep.
func vmmResidentBytes(vmName string) (int64, bool) {
	if vmName == "" {
		return 0, false
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, false
	}
	for _, e := range entries {
		pid := e.Name()
		if !e.IsDir() || pid[0] < '0' || pid[0] > '9' {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", pid, "cmdline")) //nolint:gosec // pid enumerated from /proc
		if err != nil || !strings.Contains(string(cmdline), vmName) {
			continue
		}
		if rss, ok := residentBytes(pid); ok {
			return rss, true
		}
	}
	return 0, false
}

// residentBytes reads a process's resident page count from statm's second field.
func residentBytes(pid string) (int64, bool) {
	b, err := os.ReadFile(filepath.Join("/proc", pid, "statm")) //nolint:gosec // pid enumerated from /proc
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
