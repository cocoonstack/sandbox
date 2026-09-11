package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
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
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
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
	sock, err := s.mgr.WakeAgentSocket(ctx, id, token)
	switch {
	case writePoolErr(w, err):
		return
	case err != nil:
		log.WithFunc("server.handleExec").Errorf(ctx, err, "agent socket for %s", id)
		writeErr(w, http.StatusInternalServerError, "sandbox lookup failed")
		return
	}
	guest, err := s.dialer.DialSilkd(ctx, sock)
	if err != nil {
		log.WithFunc("server.handleExec").Errorf(ctx, err, "dial silkd for %s", id)
		writeErr(w, http.StatusBadGateway, "guest agent unreachable")
		return
	}
	if req.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutSeconds)*time.Second)
		defer cancel()
	}
	stop := context.AfterFunc(ctx, func() { _ = guest.Close() })
	defer func() {
		stop()
		_ = guest.Close()
	}()
	frame, err := wire.EncodeRequest(wire.Exec{Argv: req.Argv, Cwd: req.Cwd, Env: req.Env})
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	frame = append(frame, '\n')
	if s.mgr.AuditEnabled() {
		s.mgr.Audit(ctx, id, frame)
	}
	stdinClose, err := wire.EncodeRequest(wire.StdinClose{})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "command setup failed")
		return
	}
	frame = append(frame, stdinClose...)
	frame = append(frame, '\n')
	if _, err = guest.Write(frame); err != nil {
		writeErr(w, http.StatusBadGateway, "guest agent unreachable")
		return
	}
	resp, pid, err := collectExec(guest)
	if err != nil && pid != 0 {
		// a dropped connection only reaches a child that writes; a silent one needs the kill
		s.killExec(ctx, id, token, pid)
	}
	var silkdErr *wire.ErrorResp
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, resp)
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		writeErr(w, http.StatusGatewayTimeout, "command timed out")
	case errors.Is(ctx.Err(), context.Canceled):
		return
	case errors.As(err, &silkdErr) && silkdErr.Kind == "bad_request":
		writeErr(w, http.StatusBadRequest, silkdErr.Message)
	case errors.As(err, &silkdErr):
		writeErr(w, http.StatusBadGateway, silkdErr.Error())
	case errors.Is(err, errExecOutputCap):
		writeErr(w, http.StatusRequestEntityTooLarge, err.Error())
	default:
		log.WithFunc("server.handleExec").Errorf(ctx, err, "exec relay for %s", id)
		writeErr(w, http.StatusBadGateway, "guest agent closed before the command exited")
	}
}

func (s *Server) killExec(ctx context.Context, id, token string, pid uint32) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), execKillWait)
	defer cancel()
	sock, err := s.mgr.WakeAgentSocket(ctx, id, token)
	if err == nil {
		var guest net.Conn
		if guest, err = s.dialer.DialSilkd(ctx, sock); err == nil {
			defer func() { _ = guest.Close() }()
			frame, _ := wire.EncodeRequest(wire.Kill{PID: pid})
			if _, err = guest.Write(append(frame, '\n')); err == nil {
				wire.NewFrameScanner(guest).Scan()
				return
			}
		}
	}
	log.WithFunc("server.killExec").Errorf(ctx, err, "kill exec pid %d in %s", pid, id)
}

func collectExec(guest net.Conn) (resp ExecResponse, pid uint32, err error) {
	var stdout, stderr []byte
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
			stdout = append(stdout, f.Data...)
		case *wire.Stderr:
			stderr = append(stderr, f.Data...)
		case *wire.Exit:
			return ExecResponse{ExitCode: f.Code, Stdout: string(stdout), Stderr: string(stderr)}, 0, nil
		case *wire.ErrorResp:
			return ExecResponse{}, 0, f
		}
		if len(stdout)+len(stderr) > execOutputCap {
			return ExecResponse{}, pid, errExecOutputCap
		}
	}
	if err := sc.Err(); err != nil {
		return ExecResponse{}, pid, err
	}
	return ExecResponse{}, pid, io.ErrUnexpectedEOF
}
