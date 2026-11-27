package engine

import (
	"cmp"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// A grown disk is checked and resized offline; cocoon lets those tools finish, so the call must outlast them.
const resizeTimeout = 15 * time.Minute

// CheckResizeSupport rejects runtimes without vm resize.
func (e *Engine) CheckResizeSupport(ctx context.Context) error {
	if version, misses := e.missesFloor(ctx, resizeFloor); misses {
		return fmt.Errorf("resize requires Cocoon >= %s; installed version is %s", resizeFloor, cmp.Or(version, "unavailable"))
	}
	return nil
}

// Resize sets a stopped VM's vCPUs and memory for its next cold boot and grows its disk to storage; an empty storage keeps the disk.
func (e *Engine) Resize(ctx context.Context, name string, spec types.SizeSpec, storage string) error {
	args := []string{"vm", "resize", "--cpu", strconv.Itoa(spec.CPU), "--memory", spec.Memory}
	if storage != "" {
		args = append(args, "--storage", storage)
	}
	_, err := e.runFor(ctx, resizeTimeout, saveTermGrace, append(args, name)...)
	return err
}
