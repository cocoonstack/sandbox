package engine

import "context"

// Warmup runs argv in the guest so the golden snapshot carries what it touches.
func (e *Engine) Warmup(ctx context.Context, vsockSocket string, argv []string) error {
	ctx, cancel := context.WithTimeout(ctx, cmdTimeout)
	defer cancel()
	return e.silkdExec(ctx, vsockSocket, argv...)
}
