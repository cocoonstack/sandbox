package server

import (
	"cmp"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

const (
	execOutputCap = 8 << 20
	execKillWait  = 5 * time.Second
)

var errExecOutputCap = errors.New("command output exceeds the buffered exec cap")

// ExecRequest is a buffered exec: one command whose whole output comes back in the response.
type ExecRequest struct {
	Argv           []string          `json:"argv"`
	Cwd            string            `json:"cwd,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitzero"`
}

// ExecResponse is the exit code and complete output of a buffered exec.
type ExecResponse struct {
	ExitCode int32  `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

func (s *Server) handleExec(w http.ResponseWriter, r *http.Request) {
	token, ok := sandboxToken(w, r)
	if !ok {
		return
	}
	req, ok := decodeBodyStrict[ExecRequest](w, r)
	if !ok {
		return
	}
	if len(req.Argv) == 0 {
		writeErr(w, http.StatusBadRequest, "argv must not be empty")
		return
	}
	if req.TimeoutSeconds < 0 {
		writeErr(w, http.StatusBadRequest, "timeout_seconds must not be negative")
		return
	}
	ctx := r.Context()
	id := r.PathValue("id")
	guest, done, ok := s.wakeGuest(ctx, w, id, token)
	if !ok {
		return
	}
	defer done()
	if req.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutSeconds)*time.Second)
		defer cancel()
	}
	defer closeOnCancel(ctx, guest)()
	frame, _ := wire.EncodeRequest(wire.Exec{Argv: req.Argv, Cwd: req.Cwd, Env: req.Env})
	frame = append(frame, '\n')
	if s.mgr.AuditEnabled() {
		s.mgr.Audit(ctx, id, frame)
	}
	stdinClose, _ := wire.EncodeRequest(wire.StdinClose{})
	frame = append(frame, stdinClose...)
	frame = append(frame, '\n')
	if _, err := guest.Write(frame); err != nil {
		writeErr(w, http.StatusBadGateway, "guest agent unreachable")
		return
	}
	resp, pid, err := collectExec(guest)
	if err != nil && pid != 0 {
		// a dropped connection only reaches a child that writes; a silent one needs the kill
		if killErr := s.killExec(ctx, id, token, pid); killErr != nil {
			log.WithFunc("server.handleExec").Errorf(ctx, killErr, "kill exec pid %d in %s", pid, id)
		}
	}
	silkdErr, isSilkd := errors.AsType[*wire.ErrorResp](err)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, resp)
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		writeErr(w, http.StatusGatewayTimeout, "command timed out")
	case errors.Is(ctx.Err(), context.Canceled):
		return
	case isSilkd && silkdErr.Kind == wire.KindBadRequest:
		writeErr(w, http.StatusBadRequest, silkdErr.Message)
	case isSilkd:
		writeErr(w, http.StatusBadGateway, silkdErr.Error())
	case errors.Is(err, errExecOutputCap):
		writeErr(w, http.StatusRequestEntityTooLarge, err.Error())
	default:
		log.WithFunc("server.handleExec").Errorf(ctx, err, "exec relay for %s", id)
		writeErr(w, http.StatusBadGateway, "guest agent closed before the command exited")
	}
}

func (s *Server) killExec(ctx context.Context, id, token string, pid uint32) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), execKillWait)
	defer cancel()
	sock, done, err := s.mgr.WakeAgentSocket(ctx, id, token)
	if err != nil {
		return err
	}
	defer done()
	guest, err := s.dialer.DialSilkd(ctx, sock)
	if err != nil {
		return err
	}
	defer closeOnCancel(ctx, guest)()
	frame, _ := wire.EncodeRequest(wire.Kill{PID: pid})
	if _, err = guest.Write(append(frame, '\n')); err != nil {
		return err
	}
	sc := wire.NewFrameScanner(guest)
	if !sc.Scan() {
		return cmp.Or(sc.Err(), io.ErrUnexpectedEOF)
	}
	reply, err := wire.DecodeResponse(sc.Bytes())
	if err != nil {
		return err
	}
	if silkdErr, ok := reply.(*wire.ErrorResp); ok && silkdErr.Kind != wire.KindNotFound {
		return silkdErr
	}
	return nil
}

func collectExec(guest net.Conn) (resp ExecResponse, pid uint32, err error) {
	var stdout, stderr strings.Builder
	sc := wire.NewFrameScanner(guest)
	for sc.Scan() {
		frame, err := wire.DecodeResponse(sc.Bytes())
		if err != nil {
			return ExecResponse{}, pid, err
		}
		switch f := frame.(type) {
		case *wire.Started:
			pid = f.PID
		case *wire.Stdout:
			if stdout.Len()+stderr.Len()+len(f.Data) > execOutputCap {
				return ExecResponse{}, pid, errExecOutputCap
			}
			stdout.Write(f.Data)
		case *wire.Stderr:
			if stdout.Len()+stderr.Len()+len(f.Data) > execOutputCap {
				return ExecResponse{}, pid, errExecOutputCap
			}
			stderr.Write(f.Data)
		case *wire.Exit:
			return ExecResponse{ExitCode: f.Code, Stdout: stdout.String(), Stderr: stderr.String()}, 0, nil
		case *wire.ErrorResp:
			return ExecResponse{}, 0, f
		}
	}
	if err := sc.Err(); err != nil {
		return ExecResponse{}, pid, err
	}
	return ExecResponse{}, pid, io.ErrUnexpectedEOF
}

// closeOnCancel closes conn when ctx ends and once more when the returned func runs.
func closeOnCancel(ctx context.Context, conn net.Conn) func() {
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	return func() {
		stop()
		_ = conn.Close()
	}
}
