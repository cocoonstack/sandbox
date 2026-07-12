package engine

import (
	"bytes"
	"context"
	"encoding/base64"
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

// InstallCACert makes the guest trust the cluster root: it drops the cert in
// the source dir (so a later guest-run update-ca-certificates keeps it) and
// appends it to the active bundle. It does NOT run update-ca-certificates —
// silkd is reachable before boot's systemd-tmpfiles finishes clearing /tmp, and
// that script stages temp files there, so they vanish mid-run. Called once per
// interception-pool golden build, off the claim path.
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
	if err = s.send(map[string]any{"v": 1, "op": "fs_write", "path": path, "mode": mode}); err != nil {
		return err
	}
	for chunk := range slices.Chunk(data, silkdChunk) {
		enc := base64.StdEncoding.EncodeToString(chunk)
		if err = s.send(map[string]any{"v": 1, "op": "data", "data": enc}); err != nil {
			return err
		}
	}
	if err = s.send(map[string]any{"v": 1, "op": "data_end"}); err != nil {
		return err
	}
	frame, err := s.recv()
	if err != nil {
		return err
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
	if err = s.send(map[string]any{
		"v": 1, "op": "exec", "argv": argv, "detach": false,
		"env": map[string]string{"PATH": guestExecPATH},
	}); err != nil {
		return err
	}
	if err = s.send(map[string]any{"v": 1, "op": "stdin_close"}); err != nil {
		return err
	}
	var out []byte
	for {
		frame, err := s.recv()
		if err != nil {
			return err
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
