package pool

import (
	"context"
	"fmt"

	"github.com/docker/go-units"
	"github.com/projecteru2/core/log"
	"golang.org/x/sys/unix"
)

const diskLowReason = "disk below reserve"

func (m *Manager) checkDisk(ctx context.Context) {
	logger := log.WithFunc("pool.checkDisk")
	v := m.view.Load()
	low, failed := false, false
	var lowDir string
	var lowFree int64
	for _, dir := range v.diskDirs {
		free, err := freeBytes(dir)
		if err != nil {
			if !m.diskStatFailed.Swap(true) {
				logger.Errorf(ctx, err, "statfs %s", dir)
			}
			failed = true
			continue
		}
		if free < v.diskReserve {
			low, lowDir, lowFree = true, dir, free
			break
		}
	}
	if !failed && m.diskStatFailed.Swap(false) {
		logger.Info(ctx, "statfs of the reserve dirs works again")
	}
	if failed && !low {
		return
	}
	switch was := m.diskLow.Swap(low); {
	case low && !was:
		logger.Warnf(ctx, "%s has %s free, reserve %s: refusing claims, wakes, snapshots and refills",
			lowDir, units.BytesSize(float64(lowFree)), units.BytesSize(float64(v.diskReserve)))
	case was && !low:
		logger.Info(ctx, "disk back above reserve: admitting claims, wakes, snapshots and refills")
	}
}

func (m *Manager) diskErr() error {
	if m.diskLow.Load() {
		return fmt.Errorf("%w: %s", ErrQuota, diskLowReason)
	}
	return nil
}

func freeBytes(dir string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail * uint64(st.Bsize)), nil //nolint:gosec // a mounted filesystem's free bytes fit int64
}
