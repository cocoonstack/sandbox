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
	caCertGuestPath  = "/usr/local/share/ca-certificates/sandbox-egress.crt"
	caBundlePath     = "/etc/ssl/certs/ca-certificates.crt"
	installOutputMax = 4096
	// guestExecPATH is set because silkd starts the guest command with an empty environment.
	guestExecPATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)

type guestExitError struct {
	code   int32
	output string
}

func (e *guestExitError) Error() string {
	return fmt.Sprintf("exit code %d: %s", e.code, e.output)
}

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

func (e *Engine) silkdWriteFile(ctx context.Context, vsockSocket, path string, mode uint32, data []byte) error {
	s, err := e.dialSilkdSession(ctx, vsockSocket)
	if err != nil {
		return err
	}
	defer s.close()
	// a send racing the guest's early answer+close hits EPIPE; prefer the buffered verdict.
	sendErr := func() error {
		if serr := s.send(wire.FsWrite{Path: path, Mode: &mode}); serr != nil {
			return serr
		}
		for chunk := range slices.Chunk(data, wire.BulkChunk) {
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
	_, err := e.silkdOutput(ctx, vsockSocket, argv...)
	return err
}

func (e *Engine) silkdOutput(ctx context.Context, vsockSocket string, argv ...string) ([]byte, error) {
	s, err := e.dialSilkdSession(ctx, vsockSocket)
	if err != nil {
		return nil, err
	}
	defer s.close()
	sendErr := s.send(wire.Exec{Argv: argv, Env: map[string]string{"PATH": guestExecPATH}})
	if sendErr == nil {
		// a child exiting without reading stdin races this close; prefer the buffered exit frame.
		sendErr = s.send(wire.StdinClose{})
	}
	var out, stdout []byte
	for {
		frame, err := s.recv()
		if err != nil {
			return nil, errors.Join(sendErr, err)
		}
		switch resp := frame.(type) {
		case *wire.Started:
		case *wire.Stdout:
			out, stdout = appendCapped(out, resp.Data), appendCapped(stdout, resp.Data)
		case *wire.Stderr:
			out = appendCapped(out, resp.Data)
		case *wire.Exit:
			if resp.Code != 0 {
				return nil, &guestExitError{code: resp.Code, output: string(bytes.TrimSpace(out))}
			}
			return stdout, nil
		case *wire.ErrorResp:
			return nil, fmt.Errorf("silkd %w", resp)
		default:
			return nil, fmt.Errorf("unexpected silkd frame %q", resp.RespType())
		}
	}
}

func appendCapped(out, data []byte) []byte {
	if room := installOutputMax - len(out); room > 0 {
		out = append(out, data[:min(len(data), room)]...)
	}
	return out
}
