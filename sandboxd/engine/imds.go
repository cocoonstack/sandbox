package engine

import (
	"context"
	"errors"
)

const instanceMetadataPath = "/run/silkd/instance-metadata.json"

// ErrNoInstanceMetadata reports a guest image that serves no instance metadata.
var ErrNoInstanceMetadata = errors.New("image serves no instance metadata")

// WriteInstanceMetadata replaces the instance-metadata document silkd serves on the guest's 169.254.169.254.
func (e *Engine) WriteInstanceMetadata(ctx context.Context, vsockSocket string, doc []byte) error {
	ctx, cancel := context.WithTimeout(ctx, cmdTimeout)
	defer cancel()
	err := e.silkdWriteFile(ctx, vsockSocket, instanceMetadataPath, 0o600, doc)
	if isNotFound(err) {
		return ErrNoInstanceMetadata
	}
	return err
}
