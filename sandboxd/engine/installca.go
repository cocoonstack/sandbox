package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

const (
	caCertGuestPath = "/usr/local/share/ca-certificates/sandbox-egress.crt"
	caBundlePath    = "/etc/ssl/certs/ca-certificates.crt"
	// guestExecPATH is set on the guest command because silkd starts it with an
	// empty environment.
	guestExecPATH = "/usr/sbin:/usr/bin:/sbin:/bin"
)

// InstallCACert makes the guest trust the cluster root: drop the cert in the
// source dir and append it to the active bundle. It avoids update-ca-certificates
// on purpose — silkd runs before systemd-tmpfiles clears /tmp, where that script
// stages temp files. Off the claim path (golden build / cold provision).
func (e *Engine) InstallCACert(ctx context.Context, vsockSocket string, certPEM []byte) error {
	ctx, cancel := context.WithTimeout(ctx, cmdTimeout)
	defer cancel()
	if err := e.silkdWriteFile(ctx, vsockSocket, caCertGuestPath, 0o644, certPEM); err != nil {
		return fmt.Errorf("write ca cert: %w", err)
	}
	if err := e.silkdExec(ctx, vsockSocket, "sh", "-c", "cat "+caCertGuestPath+" >> "+caBundlePath); err != nil {
		return fmt.Errorf("append ca cert to bundle: %w", err)
	}
	return nil
}

func (e *Engine) silkdWriteFile(ctx context.Context, vsockSocket, path string, mode uint32, data []byte) error {
	s, err := e.dialSilkdSession(ctx, vsockSocket)
	if err != nil {
		return err
	}
	defer s.close()
	// A send racing the guest's early answer+close hits EPIPE; prefer the buffered verdict.
	sendErr := func() error {
		if serr := s.send(wire.FsWrite{Path: path, Mode: &mode}); serr != nil {
			return serr
		}
		for chunk := range slices.Chunk(data, silkdChunk) {
			if serr := s.send(wire.Data{Data: chunk}); serr != nil {
				return serr
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

func (e *Engine) silkdExec(ctx context.Context, vsockSocket string, argv ...string) error {
	s, err := e.dialSilkdSession(ctx, vsockSocket)
	if err != nil {
		return err
	}
	defer s.close()
	sendErr := s.send(wire.Exec{Argv: argv, Env: map[string]string{"PATH": guestExecPATH}})
	if sendErr == nil {
		// A child exiting without reading stdin races this close; prefer the buffered exit frame.
		sendErr = s.send(wire.StdinClose{})
	}
	var out []byte
	for {
		frame, err := s.recv()
		if err != nil {
			return errors.Join(sendErr, err)
		}
		switch resp := frame.(type) {
		case *wire.Started:
		case *wire.Stdout:
			out = appendCapped(out, resp.Data)
		case *wire.Stderr:
			out = appendCapped(out, resp.Data)
		case *wire.Exit:
			if resp.Code != 0 {
				return fmt.Errorf("exit code %d: %s", resp.Code, bytes.TrimSpace(out))
			}
			return nil
		case *wire.ErrorResp:
			return fmt.Errorf("silkd %w", resp)
		default:
			return fmt.Errorf("unexpected silkd frame %q", resp.RespType())
		}
	}
}

// appendCapped keeps the first 4096 bytes of combined output for error context.
func appendCapped(out, data []byte) []byte {
	if room := 4096 - len(out); room > 0 {
		out = append(out, data[:min(len(data), room)]...)
	}
	return out
}
