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

// WorkspaceDiskAttach hot-attaches a read-write ext4 image to vmName as a
// virtio-blk device with serial name (guest device discovered by that serial).
// Unlike DiskAttach (operator catalog volumes are read-only), the workspace
// disk is writable so the filecache can stage the guest's working set on it.
func (e *Engine) WorkspaceDiskAttach(ctx context.Context, vmName, rawPath, name string) error {
	_, err := e.run(ctx, "vm", "disk", "attach", vmName,
		"--path", rawPath, argName, name, "--directio", "auto")
	return err
}

// WorkspaceDiskMount discovers the attached workspace disk by serial and mounts
// it read-write at mount inside the guest.
func (e *Engine) WorkspaceDiskMount(ctx context.Context, vsockSocket, name, mount string) error {
	ctx, cancel := context.WithTimeout(ctx, volumeSetupTimeout)
	defer cancel()
	device, err := e.waitForVolumeDevice(ctx, vsockSocket, name)
	if err != nil {
		return fmt.Errorf("wait for workspace device %s: %w", name, err)
	}
	if err := e.silkdExec(ctx, vsockSocket, "mkdir", "-p", "--", mount); err != nil {
		return fmt.Errorf("create workspace mount point %s: %w", mount, err)
	}
	if err := e.silkdExec(ctx, vsockSocket, "mount", "-t", "ext4", "--", device, mount); err != nil {
		return fmt.Errorf("mount workspace disk %s: %w", name, err)
	}
	return nil
}

// WorkspaceDiskDetach unmounts and detaches the workspace disk from vmName.
func (e *Engine) WorkspaceDiskDetach(ctx context.Context, vmName, name string) error {
	_, err := e.run(ctx, "vm", "disk", "detach", vmName, argName, name)
	return err
}
