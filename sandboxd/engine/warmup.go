package engine

import (
	"context"
	"errors"
	"time"
)

const guestSettleTimeout = 30 * time.Second

// Warmup runs argv in the guest so the golden snapshot carries what it touches.
func (e *Engine) Warmup(ctx context.Context, vsockSocket string, argv []string) error {
	ctx, cancel := context.WithTimeout(ctx, cmdTimeout)
	defer cancel()
	return e.silkdExec(ctx, vsockSocket, argv...)
}

// SettleGuest waits until the guest's init has finished starting units; a degraded guest has finished too.
func (e *Engine) SettleGuest(ctx context.Context, vsockSocket string) error {
	ctx, cancel := context.WithTimeout(ctx, guestSettleTimeout)
	defer cancel()
	err := e.silkdExec(ctx, vsockSocket, "systemctl", "is-system-running", "--wait")
	if _, finished := errors.AsType[*guestExitError](err); finished {
		return nil
	}
	return err
}
