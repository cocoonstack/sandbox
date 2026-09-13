package engine

import "context"

const nicLockedMark = "/run/silkd-nic-locked"

// MarkNICLocked tells silkd the guest's NIC is nft-locked, so it steers execs at the relay.
func (e *Engine) MarkNICLocked(ctx context.Context, vsockSocket string) error {
	ctx, cancel := context.WithTimeout(ctx, cmdTimeout)
	defer cancel()
	return e.silkdWriteFile(ctx, vsockSocket, nicLockedMark, 0o644, nil)
}
