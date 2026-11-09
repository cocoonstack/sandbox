package pool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/docker/go-units"
	"github.com/projecteru2/core/log"
)

const memoryLowReason = "memory above limit"

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
		return errMemoryLow
	}
	return nil
}

// readAnonBytes counts resident anon plus used swap, since anon pushed to swap still has to fit back.
func readAnonBytes(proc string) (int64, error) {
	raw, err := os.ReadFile(filepath.Join(proc, "meminfo")) //nolint:gosec // a fixed proc file
	if err != nil {
		return 0, err
	}
	var kb [3]int64
	for i, field := range []string{"AnonPages:", "SwapTotal:", "SwapFree:"} {
		_, rest, _ := bytes.Cut(raw, []byte(field))
		if _, err := fmt.Sscan(string(rest), &kb[i]); err != nil {
			return 0, fmt.Errorf("parse %s %w", field, err)
		}
	}
	return (kb[0] + kb[1] - kb[2]) * units.KiB, nil
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
