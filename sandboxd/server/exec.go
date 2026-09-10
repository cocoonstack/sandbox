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

const execOutputCap = 8 << 20

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
	req, ok := decodeBody[ExecRequest](w, r)
	if !ok {
		return
	}
	if len(req.Argv) == 0 {
		writeErr(w, http.StatusBadRequest, "argv must not be empty")
		return
	}
	ctx := r.Context()
	if req.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutSeconds)*time.Second)
		defer cancel()
	}
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
	// silkd kills a non-detached child when its connection drops, so a canceled ctx ends the command
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
	if _, err = guest.Write(frame); err != nil {
		writeErr(w, http.StatusBadGateway, "guest agent unreachable")
		return
	}
	resp, err := collectExec(guest)
	var silkdErr *wire.ErrorResp
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, resp)
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		writeErr(w, http.StatusGatewayTimeout, "command timed out")
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

func collectExec(guest net.Conn) (ExecResponse, error) {
	var stdout, stderr []byte
	sc := wire.NewFrameScanner(guest)
	for sc.Scan() {
		frame, err := wire.DecodeResponse(sc.Bytes())
		if err != nil {
			return ExecResponse{}, err
		}
		switch f := frame.(type) {
		case *wire.Stdout:
			stdout = append(stdout, f.Data...)
		case *wire.Stderr:
			stderr = append(stderr, f.Data...)
		case *wire.Exit:
			return ExecResponse{ExitCode: f.Code, Stdout: string(stdout), Stderr: string(stderr)}, nil
		case *wire.ErrorResp:
			return ExecResponse{}, f
		}
		if len(stdout)+len(stderr) > execOutputCap {
			return ExecResponse{}, errExecOutputCap
		}
	}
	if err := sc.Err(); err != nil {
		return ExecResponse{}, err
	}
	return ExecResponse{}, io.ErrUnexpectedEOF
}
