package sandbox

import (
	"context"
	"io"

	"github.com/cocoonstack/sandbox/sdk/silkd"
)

// Push extracts a tar stream under dest in the sandbox — the no-network lane's
// way to move a project in. tarStream is a reader of tar bytes (e.g. from
// archive/tar); silkd runs `tar -x` under dest.
func (s *Sandbox) Push(ctx context.Context, dest string, tarStream io.Reader) error {
	return s.uploadRPC(ctx, &silkd.FsPush{Dest: dest}, tarStream)
}

// Pull streams the tree at path back as a tar archive, written to tarStream.
func (s *Sandbox) Pull(ctx context.Context, path string, tarStream io.Writer) error {
	return s.downloadRPC(ctx, &silkd.FsPull{Path: path}, func(b []byte) error {
		_, err := tarStream.Write(b)
		return err
	})
}
