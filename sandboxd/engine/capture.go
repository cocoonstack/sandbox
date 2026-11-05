package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"time"
)

const captureExitPoll = time.Second

func (e *Engine) WaitCapture(ctx context.Context, snap string) error {
	for captureRunning(snap) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(captureExitPoll):
		}
	}
	return nil
}

func captureRunning(snap string) bool {
	procs, _ := os.ReadDir("/proc")
	arg := []byte("\x00" + snap + "\x00")
	return slices.ContainsFunc(procs, func(proc os.DirEntry) bool {
		cmdline, err := os.ReadFile(filepath.Join("/proc", proc.Name(), "cmdline"))
		return err == nil && bytes.Contains(cmdline, arg)
	})
}
