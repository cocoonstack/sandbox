package sandbox

import (
	"context"
	"fmt"
	"io"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

// Spawn starts cmd detached and returns its guest pid; stdio is ignored and Session must be empty.
func (s *Sandbox) Spawn(ctx context.Context, cmd Cmd) (uint32, error) {
	if len(cmd.Argv) == 0 {
		return 0, fmt.Errorf("empty argv")
	}
	if cmd.Session != "" {
		return 0, fmt.Errorf("spawn does not support session")
	}
	req := &wire.Exec{Argv: cmd.Argv, Cwd: cmd.Cwd, Env: cmd.Env, User: cmd.User, Detach: true}
	started, err := s.oneShotRPC[wire.Started](ctx, req)
	if err != nil {
		return 0, err
	}
	return started.PID, nil
}

// Ps lists the guest's tracked processes with state and exit codes.
func (s *Sandbox) Ps(ctx context.Context) ([]wire.ProcInfo, error) {
	procs, err := s.oneShotRPC[wire.Procs](ctx, wire.Ps{})
	if err != nil {
		return nil, err
	}
	return procs.Procs, nil
}

// Kill signals the process group a tracked pid leads; sig 0 sends SIGKILL, and an exited process is a no-op success.
func (s *Sandbox) Kill(ctx context.Context, pid uint32, sig int32) error {
	req := &wire.Kill{PID: pid}
	if sig != 0 {
		req.Signal = &sig
	}
	return s.doneRPC(ctx, req)
}

// Logs replays a tracked process's ring-buffered output; code is valid only when exited.
func (s *Sandbox) Logs(ctx context.Context, pid uint32, stdout, stderr io.Writer) (code int32, exited bool, err error) {
	return s.drainProc(ctx, &wire.Logs{PID: pid}, stdout, stderr)
}

// Attach replays the buffered output, then follows live output until the process exits.
func (s *Sandbox) Attach(ctx context.Context, pid uint32, stdout, stderr io.Writer) (code int32, exited bool, err error) {
	return s.drainProc(ctx, &wire.Attach{PID: pid}, stdout, stderr)
}

func (s *Sandbox) drainProc(ctx context.Context, req wire.Request, stdout, stderr io.Writer) (int32, bool, error) {
	conn, l, err := s.call(ctx, req)
	if err != nil {
		return 0, false, err
	}
	defer l.close()
	_, code, exited, err := pumpStdio(ctx, conn, stdout, stderr)
	// Logs closes with done after the exit frame
	if _, replay := req.(*wire.Logs); replay && exited && err == nil {
		err = terminalErr(ctx, conn)
	}
	return code, exited, l.done(err)
}
