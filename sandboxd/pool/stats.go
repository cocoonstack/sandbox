package pool

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// SandboxStats is one sandbox's resource usage; MemUsedMeasured false means the RSS was unreadable.
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
func (m *Manager) Stats(ctx context.Context, id string) (SandboxStats, bool) {
	// hibernate and archive rewrite HibernateSnap and VMName under m.mu; the /proc read needs none
	m.mu.Lock()
	sb := m.claimed[id]
	if sb == nil {
		m.mu.Unlock()
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
	vmName := sb.VMName
	m.mu.Unlock()
	if !st.Hibernated {
		if rss, ok := m.vmResidentBytes(ctx, vmName); ok {
			st.MemUsedBytes, st.MemUsedMeasured = rss, true
		}
	}
	return st, true
}

// vmResidentBytes reports the resident set of the hypervisor process serving vmName.
func (m *Manager) vmResidentBytes(ctx context.Context, vmName string) (int64, bool) {
	if vmName == "" {
		return 0, false
	}
	vm, ok, err := m.findVM(ctx, vmName)
	if err != nil || !ok || vm.PID <= 0 {
		return 0, false
	}
	return residentBytes(vm.PID)
}

// residentBytes reads a process's resident page count from statm's second field.
func residentBytes(pid int) (int64, bool) {
	b, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "statm")) //nolint:gosec // pid comes from cocoon's own VM record
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
