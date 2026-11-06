package engine

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"fmt"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const argSeries = "--series"

// CheckMigrateSupport rejects runtimes without live migration.
func (e *Engine) CheckMigrateSupport(ctx context.Context) error {
	if version, misses := e.missesFloor(ctx, MigrateCocoon); misses {
		return fmt.Errorf("live migration requires Cocoon >= %s; installed version is %s", MigrateCocoon, cmp.Or(version, "unavailable"))
	}
	return nil
}

// MigrateCapture captures one round of a VM's series; an empty series starts one, and final leaves the guest paused.
func (e *Engine) MigrateCapture(ctx context.Context, vmName, series string, final bool) (types.MigrateRound, error) {
	args := []string{"vm", "migrate", "capture", vmName}
	if series != "" {
		args = append(args, argSeries, series)
	}
	if final {
		args = append(args, "--final")
	}
	out, err := e.runDump(ctx, append(args, argOutput, formatJSON)...)
	if err != nil {
		return types.MigrateRound{}, err
	}
	var round types.MigrateRound
	if err = json.Unmarshal(out, &round); err != nil {
		return types.MigrateRound{}, fmt.Errorf("parse migrate capture: %w", err)
	}
	return round, nil
}

// MigrateReceive restores a transferred series into a VM whose guest stays paused until Start.
func (e *Engine) MigrateReceive(ctx context.Context, fromDir, name string, key types.PoolKey) (types.VMRecord, error) {
	restore := e.restoreArgs()
	if e.restoreMode == types.RestoreOnDemand {
		// Cocoon's hypervisor rejects ondemand for a series with deltas, so its default applies
		restore = nil
	}
	args := append([]string{"vm", "migrate", "receive", "--from-dir", fromDir}, e.cloneFlags(name, key, restore)...)
	return e.runRecord(ctx, dumpTimeout, dumpTermGrace, args...)
}

// MigrateAbort ends a series and resumes the guest; a repeat or an old series does nothing.
func (e *Engine) MigrateAbort(ctx context.Context, vmName, series string) error {
	_, err := e.run(ctx, "vm", "migrate", "abort", argSeries, series, vmName)
	return err
}
