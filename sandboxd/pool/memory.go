package pool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/docker/go-units"
	"github.com/projecteru2/core/log"
)

const (
	memoryLowReason = "memory above limit"

	procRoot = "/proc"
)

// Page cache and clean guest pages are reclaimable, so only anon and stall time show a squeezed node.
func (m *Manager) checkMemory(ctx context.Context) {
	logger := log.WithFunc("pool.checkMemory")
	v := m.view.Load()
	var anon int64
	var psi float64
	var anonErr, psiErr error
	if v.memoryAnon > 0 {
		anon, anonErr = readAnonBytes(m.procDir)
	}
	if v.memoryPSI > 0 {
		psi, psiErr = readPSISomeAvg10(m.procDir)
	}
	if anonErr == nil {
		m.anonBytes.Store(anon)
	}
	err := errors.Join(anonErr, psiErr)
	switch {
	case err != nil && !m.memoryReadFailed.Swap(true):
		logger.Error(ctx, err, "read memory signals")
	case err == nil && m.memoryReadFailed.Swap(false):
		logger.Info(ctx, "memory signals read again")
	}
	low := anon > v.memoryAnon || psi > v.memoryPSI
	switch was := m.memoryLow.Swap(low); {
	case low && !was:
		logger.Warnf(ctx, "anon %s limit %s, PSI some avg10 %.2f limit %.2f: refusing claims and refills",
			units.BytesSize(float64(anon)), units.BytesSize(float64(v.memoryAnon)), psi, v.memoryPSI)
	case was && !low:
		logger.Info(ctx, "memory back under limit: admitting claims and refills")
	}
}

func (m *Manager) memoryErr() error {
	if m.memoryLow.Load() {
		return fmt.Errorf("%w: %s", ErrQuota, memoryLowReason)
	}
	return nil
}

func readAnonBytes(proc string) (int64, error) {
	raw, err := os.ReadFile(filepath.Join(proc, "meminfo")) //nolint:gosec // a fixed proc file
	if err != nil {
		return 0, err
	}
	_, rest, found := bytes.Cut(raw, []byte("AnonPages:"))
	fields := bytes.Fields(rest)
	if !found || len(fields) == 0 {
		return 0, errors.New("meminfo has no AnonPages")
	}
	kb, err := strconv.ParseInt(string(fields[0]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse AnonPages: %w", err)
	}
	return kb * units.KiB, nil
}

func readPSISomeAvg10(proc string) (float64, error) {
	raw, err := os.ReadFile(filepath.Join(proc, "pressure", "memory")) //nolint:gosec // a fixed proc file
	if err != nil {
		return 0, err
	}
	var avg10 float64
	if _, err := fmt.Sscanf(string(raw), "some avg10=%f", &avg10); err != nil {
		return 0, fmt.Errorf("parse memory pressure: %w", err)
	}
	return avg10, nil
}
