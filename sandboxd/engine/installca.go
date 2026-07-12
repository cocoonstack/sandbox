package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"slices"
)

const (
	caCertGuestPath = "/usr/local/share/ca-certificates/sandbox-egress.crt"
	caBundlePath    = "/etc/ssl/certs/ca-certificates.crt"
	silkdChunk      = 256 * 1024
	maxSilkdFrame   = 8 << 20
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
	conn, err := e.DialSilkd(ctx, vsockSocket)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	sc := silkdScanner(conn)
	if err = writeFrame(conn, map[string]any{"v": 1, "op": "fs_write", "path": path, "mode": mode}); err != nil {
		return err
	}
	for chunk := range slices.Chunk(data, silkdChunk) {
		enc := base64.StdEncoding.EncodeToString(chunk)
		if err = writeFrame(conn, map[string]any{"v": 1, "op": "data", "data": enc}); err != nil {
			return err
		}
	}
	if err = writeFrame(conn, map[string]any{"v": 1, "op": "data_end"}); err != nil {
		return err
	}
	frame, err := recvFrame(sc)
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
	conn, err := e.DialSilkd(ctx, vsockSocket)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	sc := silkdScanner(conn)
	if err = writeFrame(conn, map[string]any{
		"v": 1, "op": "exec", "argv": argv, "detach": false,
		"env": map[string]string{"PATH": guestExecPATH},
	}); err != nil {
		return err
	}
	if err = writeFrame(conn, map[string]any{"v": 1, "op": "stdin_close"}); err != nil {
		return err
	}
	var out []byte
	for {
		frame, err := recvFrame(sc)
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

type silkdFrame struct {
	Type    string `json:"type"`
	Code    int32  `json:"code"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
	Data    []byte `json:"data"`
}

func writeFrame(conn net.Conn, frame map[string]any) error {
	buf, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	if _, err := conn.Write(append(buf, '\n')); err != nil {
		return fmt.Errorf("write silkd frame: %w", err)
	}
	return nil
}

func recvFrame(sc *bufio.Scanner) (silkdFrame, error) {
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return silkdFrame{}, fmt.Errorf("read silkd frame: %w", err)
		}
		return silkdFrame{}, io.ErrUnexpectedEOF
	}
	var frame silkdFrame
	if err := json.Unmarshal(sc.Bytes(), &frame); err != nil {
		return silkdFrame{}, fmt.Errorf("parse silkd frame: %w", err)
	}
	return frame, nil
}

func silkdScanner(conn net.Conn) *bufio.Scanner {
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 64*1024), maxSilkdFrame)
	return sc
}
