package engine

import (
	"context"
	"fmt"
)

const (
	caCertGuestPath = "/usr/local/share/ca-certificates/sandbox-egress.crt"
	caBundlePath    = "/etc/ssl/certs/ca-certificates.crt"
)

// InstallCACert makes the guest trust the cluster root without update-ca-certificates.
func (e *Engine) InstallCACert(ctx context.Context, vsockSocket string, certPEM []byte) error {
	ctx, cancel := context.WithTimeout(ctx, cmdTimeout)
	defer cancel()
	if err := e.silkdWriteFile(ctx, vsockSocket, caCertGuestPath, 0o644, certPEM); err != nil {
		return fmt.Errorf("write ca cert: %w", err)
	}
	if err := e.silkdExec(ctx, vsockSocket, "sh", "-c", "cat "+caCertGuestPath+" >> "+caBundlePath+" && sync "+caCertGuestPath+" "+caBundlePath); err != nil {
		return fmt.Errorf("append ca cert to bundle: %w", err)
	}
	return nil
}
