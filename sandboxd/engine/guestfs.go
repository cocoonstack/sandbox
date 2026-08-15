package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

// GuestFS-style helpers over silkd for the workspace filecache. They reuse the
// same session plumbing as the volume and CA helpers; all operate on a claimed
// VM's vsock UDS and stay off the warm-claim hot path.

// GuestRun executes argv in the guest and returns its stdout (stderr folds into
// the error on non-zero exit). silkd starts the child with an empty
// environment, so PATH is set like silkdExec.
func (e *Engine) GuestRun(ctx context.Context, vsockSocket string, argv ...string) (string, error) {
	s, err := e.dialSilkdSession(ctx, vsockSocket)
	if err != nil {
		return "", err
	}
	defer s.close()
	sendErr := s.send(wire.Exec{Argv: argv, Env: map[string]string{"PATH": guestExecPATH}})
	if sendErr == nil {
		sendErr = s.send(wire.StdinClose{})
	}
	var stdout, stderr bytes.Buffer
	for {
		frame, err := s.recv()
		if err != nil {
			return "", errors.Join(sendErr, err)
		}
		switch resp := frame.(type) {
		case *wire.Started:
		case *wire.Stdout:
			stdout.Write(resp.Data)
		case *wire.Stderr:
			stderr.Write(resp.Data)
		case *wire.Exit:
			if resp.Code != 0 {
				return stdout.String(), fmt.Errorf("exit code %d: %s", resp.Code, bytes.TrimSpace(stderr.Bytes()))
			}
			return stdout.String(), nil
		case *wire.ErrorResp:
			return "", fmt.Errorf("silkd %w", resp)
		default:
			return "", fmt.Errorf("unexpected silkd frame %q", resp.RespType())
		}
	}
}

// GuestWriteFile writes data to path in the guest (mode applied at create).
func (e *Engine) GuestWriteFile(ctx context.Context, vsockSocket, path string, mode uint32, data []byte) error {
	return e.silkdWriteFile(ctx, vsockSocket, path, mode, data)
}

// GuestReadFile reads a guest file, returning its bytes.
func (e *Engine) GuestReadFile(ctx context.Context, vsockSocket, path string) ([]byte, error) {
	return e.silkdReadFile(ctx, vsockSocket, path)
}

// GuestRemove deletes a guest path (recursive for trees).
func (e *Engine) GuestRemove(ctx context.Context, vsockSocket, path string, recursive bool) error {
	return e.silkdStream(ctx, vsockSocket, wire.FsRm{Path: path, Recursive: recursive}, func(wire.Response) error {
		return nil
	})
}

// GuestPushTar extracts a tar stream under dest in the guest (silkd runs
// `tar -x`). The reader supplies tar bytes.
func (e *Engine) GuestPushTar(ctx context.Context, vsockSocket, dest string, r io.Reader) error {
	s, err := e.dialSilkdSession(ctx, vsockSocket)
	if err != nil {
		return err
	}
	defer s.close()
	sendErr := func() error {
		if serr := s.send(wire.FsPush{Dest: dest}); serr != nil {
			return serr
		}
		buf := make([]byte, silkdChunk)
		for {
			n, rerr := r.Read(buf)
			if n > 0 {
				if serr := s.send(wire.Data{Data: slices.Clone(buf[:n])}); serr != nil {
					return serr
				}
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				return rerr
			}
		}
		return s.send(wire.DataEnd{})
	}()
	frame, err := s.recv()
	if err != nil {
		return errors.Join(sendErr, err)
	}
	switch resp := frame.(type) {
	case *wire.Done:
		return nil
	case *wire.ErrorResp:
		return fmt.Errorf("silkd %w", resp)
	default:
		return fmt.Errorf("unexpected silkd frame %q", resp.RespType())
	}
}

// WorkspaceDiskDetach detaches a workspace disk from vmName. Attach, mount,
// and unmount reuse the catalog-volume primitives (DiskAttach with a
// read-write VolumeSpec, MountVolume rw, UnmountVolume); detach has no volume
// counterpart because catalog images outlive their VM while a workspace disk
// must come off a VM that stays up (arm rollback keeps the claim alive).
func (e *Engine) WorkspaceDiskDetach(ctx context.Context, vmName, name string) error {
	_, err := e.run(ctx, "vm", "disk", "detach", vmName, argName, name)
	return err
}

// WorkspaceShareAttach hot-attaches the vhost-user-fs share served on socket
// to vmName under tag; the guest then mounts it as `mount -t virtiofs <tag>`.
// The VM must have been booted with shared guest memory — vhost-user maps the
// guest's memory into the device backend, and cocoon refuses to flip that on a
// running VM — which is what the node's SharedMemory setting is for.
func (e *Engine) WorkspaceShareAttach(ctx context.Context, vmName, socket, tag string) error {
	_, err := e.run(ctx, "vm", "fs", "attach", vmName, argSocket, socket, argTag, tag)
	return err
}

// WorkspaceShareDetach removes the share tagged tag from vmName.
func (e *Engine) WorkspaceShareDetach(ctx context.Context, vmName, tag string) error {
	_, err := e.run(ctx, "vm", "fs", "detach", vmName, argTag, tag)
	return err
}
