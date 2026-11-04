package engine

import "context"

// guestEnvPath is the env file silkd applies to every child it spawns.
const guestEnvPath = "/run/silkd.env"

// WriteGuestEnv replaces the guest's env file, root-only.
func (e *Engine) WriteGuestEnv(ctx context.Context, vsockSocket string, doc []byte) error {
	return e.silkdWriteFile(ctx, vsockSocket, guestEnvPath, 0o600, doc)
}
