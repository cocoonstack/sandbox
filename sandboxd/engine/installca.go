package engine

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
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
		if serr := s.send(map[string]any{"v": 1, "op": "fs_write", "path": path, "mode": mode}); serr != nil {
			return serr
		}
		for chunk := range slices.Chunk(data, silkdChunk) {
			enc := base64.StdEncoding.EncodeToString(chunk)
			if serr := s.send(map[string]any{"v": 1, "op": "data", "data": enc}); serr != nil {
				return serr
			}
		}
		return s.send(map[string]any{"v": 1, "op": "data_end"})
	}()
	frame, err := s.recv()
	if err != nil {
		return errors.Join(sendErr, err)
	}
	switch frame.Type {
	case "done":
		return nil
	case "error":
		return fmt.Errorf("silkd %s: %s", frame.Kind, frame.Message)
	default:
		return fmt.Errorf("unexpected silkd frame %q", frame.Type)
	}
}

func (e *Engine) silkdExec(ctx context.Context, vsockSocket string, argv ...string) error {
	s, err := e.dialSilkdSession(ctx, vsockSocket)
	if err != nil {
		return err
	}
	defer s.close()
	sendErr := s.send(map[string]any{
		"v": 1, "op": "exec", "argv": argv, "detach": false,
		"env": map[string]string{"PATH": guestExecPATH},
	})
	if sendErr == nil {
		// A child exiting without reading stdin races this close; prefer the buffered exit frame.
		sendErr = s.send(map[string]any{"v": 1, "op": "stdin_close"})
	}
	var out []byte
	for {
		frame, err := s.recv()
		if err != nil {
			return errors.Join(sendErr, err)
		}
		switch frame.Type {
		case "started":
		case "stdout", "stderr":
			if len(out) < 4096 {
				out = append(out, frame.Data...)
			}
		case "exit":
			if frame.Code != 0 {
				return fmt.Errorf("exit code %d: %s", frame.Code, bytes.TrimSpace(out))
			}
			return nil
		case "error":
			return fmt.Errorf("silkd %s: %s", frame.Kind, frame.Message)
		default:
			return fmt.Errorf("unexpected silkd frame %q", frame.Type)
		}
	}
}
