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
	conn, done, err := s.dial(ctx)
	if err != nil {
		return err
	}
	defer done()
	if err := conn.Send(&silkd.FsPush{Dest: dest}); err != nil {
		return err
	}
	if err := uploadStream(conn, tarStream); err != nil {
		return err
	}
	return terminalErr(ctx, conn)
}

// Pull streams the tree at path back as a tar archive, written to tarStream.
func (s *Sandbox) Pull(ctx context.Context, path string, tarStream io.Writer) error {
	conn, done, err := s.dial(ctx)
	if err != nil {
		return err
	}
	defer done()
	if err := conn.Send(&silkd.FsPull{Path: path}); err != nil {
		return err
	}
	return drainData(ctx, conn, func(b []byte) error {
		_, err := tarStream.Write(b)
		return err
	})
}
