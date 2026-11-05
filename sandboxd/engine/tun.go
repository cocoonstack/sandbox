package engine

import (
	"context"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

// TunUp brings the guest's tun0 up and starts its packet stream; the host hello still decides whether packets flow.
func (e *Engine) TunUp(ctx context.Context, vsockSocket string) error {
	return e.silkdOnce[*wire.Done](ctx, vsockSocket, wire.TunUp{})
}

// TunDown removes the guest's tun0 and its routes so clients fail fast; it enforces nothing.
func (e *Engine) TunDown(ctx context.Context, vsockSocket string) error {
	return e.silkdOnce[*wire.Done](ctx, vsockSocket, wire.TunDown{})
}
