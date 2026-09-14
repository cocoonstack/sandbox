package engine

import "context"

const (
	laneFile = "/etc/silkd-lane"

	LaneRelay  Lane = "relay"
	LaneDirect Lane = "direct"
)

// Lane is the host's verdict on how a guest's traffic leaves: through the relay when its NIC is nft-locked, directly when the NIC routes.
type Lane string

// MarkLane writes the verdict into the guest, where silkd and the image's units read it; the nft lock is invisible from inside.
func (e *Engine) MarkLane(ctx context.Context, vsockSocket string, lane Lane) error {
	ctx, cancel := context.WithTimeout(ctx, cmdTimeout)
	defer cancel()
	return e.silkdWriteFile(ctx, vsockSocket, laneFile, 0o644, []byte(lane+"\n"))
}
