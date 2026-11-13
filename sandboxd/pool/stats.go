package pool

import (
	"cmp"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// userHZ is the unit of the times in /proc/PID/stat: USER_HZ, 100 on every Linux arch sandboxd builds for.
const userHZ = 100

// SandboxStats is one sandbox's resource usage; a false *Measured means that reading was unavailable.
type SandboxStats struct {
	ID              string    `json:"id"`
	CPUCount        int       `json:"cpu_count"`
	MemTotalBytes   int64     `json:"mem_total_bytes"`
	MemUsedBytes    int64     `json:"mem_used_bytes"`
	MemUsedMeasured bool      `json:"mem_used_measured"`
	CPUSeconds      float64   `json:"cpu_seconds"`
	CPUStartedAt    time.Time `json:"cpu_started_at,omitzero"`
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
	st, vmName := statsOfLocked(sb)
	m.mu.Unlock()
	if !st.Hibernated {
		if pid, ok := m.vmPID(ctx, vmName); ok {
			readProcStats(&st, m.procDir, pid, bootTime(m.procDir))
		}
	}
	st.MeasuredAt = time.Now().UTC()
	return st, true
}

// AllStats reports every live claim's resource usage from one engine listing.
func (m *Manager) AllStats(ctx context.Context) []SandboxStats {
	pids := map[string]int{}
	if vms, err := m.eng.List(ctx); err == nil {
		for _, vm := range vms {
			if vm.PID > 0 {
				pids[vm.Config.Name] = vm.PID
			}
		}
	}
	m.mu.Lock()
	all := make([]SandboxStats, 0, len(m.claimed))
	vmNames := make([]string, 0, len(m.claimed))
	for _, sb := range m.claimed {
		st, vmName := statsOfLocked(sb)
		all, vmNames = append(all, st), append(vmNames, vmName)
	}
	m.mu.Unlock()
	boot := bootTime(m.procDir)
	for i := range all {
		if pid, ok := pids[vmNames[i]]; ok && !all[i].Hibernated {
			readProcStats(&all[i], m.procDir, pid, boot)
		}
		all[i].MeasuredAt = time.Now().UTC()
	}
	slices.SortFunc(all, func(a, b SandboxStats) int { return cmp.Compare(a.ID, b.ID) })
	return all
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

func statsOfLocked(sb *types.Sandbox) (SandboxStats, string) {
	spec, _ := sb.Key.Size.Spec()
	return SandboxStats{ID: sb.ID, CPUCount: spec.CPU, MemTotalBytes: spec.MemoryBytes, Hibernated: sb.HibernateSnap != ""}, sb.VMName
}

func readProcStats(st *SandboxStats, proc string, pid int, boot time.Time) {
	st.MemUsedBytes, st.MemUsedMeasured = residentBytes(proc, pid)
	st.CPUSeconds, st.CPUStartedAt, st.CPUMeasured = processCPU(proc, pid, boot)
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

// processCPU reads utime+stime (fields 14 and 15, all threads) and the start time (field 22) of pid.
func processCPU(proc string, pid int, boot time.Time) (float64, time.Time, bool) {
	b, err := os.ReadFile(filepath.Join(proc, strconv.Itoa(pid), "stat")) //nolint:gosec // an engine pid under the proc root
	if err != nil {
		return 0, time.Time{}, false
	}
	// comm, field 2, may hold spaces and parens, so the fields after its last ')' start at state, field 3
	_, rest, ok := strings.CutLast(string(b), ")")
	if !ok {
		return 0, time.Time{}, false
	}
	fields := strings.Fields(rest)
	if len(fields) < 13 {
		return 0, time.Time{}, false
	}
	utime, uerr := strconv.ParseUint(fields[11], 10, 64)
	stime, serr := strconv.ParseUint(fields[12], 10, 64)
	if uerr != nil || serr != nil {
		return 0, time.Time{}, false
	}
	var started time.Time
	if len(fields) > 19 && !boot.IsZero() {
		if ticks, err := strconv.ParseInt(fields[19], 10, 64); err == nil {
			started = boot.Add(time.Duration(ticks) * (time.Second / userHZ)).UTC()
		}
	}
	return float64(utime+stime) / userHZ, started, true
}

func bootTime(proc string) time.Time {
	b, err := os.ReadFile(filepath.Join(proc, "stat")) //nolint:gosec // the proc root's own stat
	if err != nil {
		return time.Time{}
	}
	for line := range strings.Lines(string(b)) {
		if v, ok := strings.CutPrefix(line, "btime "); ok {
			if sec, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
				return time.Unix(sec, 0)
			}
		}
	}
	return time.Time{}
}
