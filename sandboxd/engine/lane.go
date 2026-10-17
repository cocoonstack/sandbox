package engine

import (
	"context"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const laneFile = "/etc/silkd-lane"

// MarkLane writes the verdict into the guest, where silkd and the image's units read it; the nft lock is invisible from inside.
func (e *Engine) MarkLane(ctx context.Context, vsockSocket string, lane types.NetRoute) error {
	ctx, cancel := context.WithTimeout(ctx, cmdTimeout)
	defer cancel()
	return e.silkdWriteFile(ctx, vsockSocket, laneFile, 0o644, []byte(lane+"\n"))
}
