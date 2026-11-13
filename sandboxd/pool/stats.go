package pool

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// userHZ is the unit of the times in /proc/PID/stat: USER_HZ, 100 on every Linux arch whatever the kernel's HZ.
const userHZ = 100

// SandboxStats is one sandbox's resource usage; MemUsedMeasured or CPUMeasured false means that reading was unavailable.
// CPUSeconds is the VMM process's user+system time and starts over with the process (wake, restart, move).
type SandboxStats struct {
	ID              string    `json:"id"`
	CPUCount        int       `json:"cpu_count"`
	MemTotalBytes   int64     `json:"mem_total_bytes"`
	MemUsedBytes    int64     `json:"mem_used_bytes"`
	MemUsedMeasured bool      `json:"mem_used_measured"`
	CPUSeconds      float64   `json:"cpu_seconds"`
	CPUMeasured     bool      `json:"cpu_measured"`
	Hibernated      bool      `json:"hibernated"`
	MeasuredAt      time.Time `json:"measured_at"`
}

// Stats reports one live claim's resource usage.
func (m *Manager) Stats(ctx context.Context, id string) (SandboxStats, bool) {
	// Hibernate and archive rewrite HibernateSnap and VMName under m.mu; the /proc read needs none
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
		if pid, ok := m.vmPID(ctx, vmName); ok {
			st.MemUsedBytes, st.MemUsedMeasured = residentBytes(m.procDir, pid)
			st.CPUSeconds, st.CPUMeasured = cpuSeconds(m.procDir, pid)
		}
	}
	return st, true
}

func (m *Manager) vmPID(ctx context.Context, vmName string) (int, bool) {
	if vmName == "" {
		return 0, false
	}
	vm, ok, err := m.eng.Inspect(ctx, vmName)
	if err != nil || !ok || vm.PID <= 0 {
		return 0, false
	}
	return vm.PID, true
}

func residentBytes(proc string, pid int) (int64, bool) {
	b, err := os.ReadFile(filepath.Join(proc, strconv.Itoa(pid), "statm")) //nolint:gosec // an engine pid under the proc root
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

// cpuSeconds sums utime and stime, fields 14 and 15, which for a process cover all its threads.
func cpuSeconds(proc string, pid int) (float64, bool) {
	b, err := os.ReadFile(filepath.Join(proc, strconv.Itoa(pid), "stat")) //nolint:gosec // an engine pid under the proc root
	if err != nil {
		return 0, false
	}
	// comm, field 2, may hold spaces and parens; the fields after its last ')' start at state, field 3
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, false
	}
	fields := strings.Fields(s[i+1:])
	if len(fields) < 13 {
		return 0, false
	}
	utime, uerr := strconv.ParseUint(fields[11], 10, 64)
	stime, serr := strconv.ParseUint(fields[12], 10, 64)
	if uerr != nil || serr != nil {
		return 0, false
	}
	return float64(utime+stime) / userHZ, true
}
