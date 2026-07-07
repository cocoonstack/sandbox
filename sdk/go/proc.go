package sandbox

import (
	"context"
	"fmt"
	"io"

	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

// Spawn starts cmd detached: it returns the guest pid as soon as the
// process starts, and the process keeps running with a bounded output ring
// readable later via Logs or Attach. cmd's Stdin/Stdout/Stderr are ignored.
func (s *Sandbox) Spawn(ctx context.Context, cmd Cmd) (uint32, error) {
	if len(cmd.Argv) == 0 {
		return 0, fmt.Errorf("empty argv")
	}
	req := &silkd.Exec{Argv: cmd.Argv, Cwd: cmd.Cwd, Env: cmd.Env, User: cmd.User, Detach: true, Session: cmd.Session}
	started, err := oneShotRPC[silkd.Started](ctx, s, req)
	if err != nil {
		return 0, err
	}
	return started.PID, nil
}

// Ps lists the guest's tracked processes — execs, spawns, and ptys — with
// state and exit codes.
func (s *Sandbox) Ps(ctx context.Context) ([]silkd.ProcInfo, error) {
	procs, err := oneShotRPC[silkd.Procs](ctx, s, silkd.Ps{})
	if err != nil {
		return nil, err
	}
	return procs.Procs, nil
}

// Kill signals a tracked process; sig 0 sends SIGKILL. Killing one that
// already exited is a no-op success — its OS pid may be recycled, so silkd
// never signals a reaped child.
func (s *Sandbox) Kill(ctx context.Context, pid uint32, sig int32) error {
	req := &silkd.Kill{PID: pid}
	if sig != 0 {
		req.Signal = &sig
	}
	return s.doneRPC(ctx, req)
}

// Logs replays a tracked process's ring-buffered output into the writers
// (nil discards). exited reports whether the process has ended; code is its
// exit code when it has.
func (s *Sandbox) Logs(ctx context.Context, pid uint32, stdout, stderr io.Writer) (code int32, exited bool, err error) {
	return s.drainProc(ctx, &silkd.Logs{PID: pid}, stdout, stderr)
}

// Attach replays the buffered output, then follows live output until the
// process exits, returning its exit code. exited is false only when the
// proc table dropped the process mid-attach (reap race).
func (s *Sandbox) Attach(ctx context.Context, pid uint32, stdout, stderr io.Writer) (code int32, exited bool, err error) {
	return s.drainProc(ctx, &silkd.Attach{PID: pid}, stdout, stderr)
}

// drainProc pumps stdout/stderr frames until the terminal frame: exit
// carries the code, done means the stream ended without one.
func (s *Sandbox) drainProc(ctx context.Context, req silkd.Request, stdout, stderr io.Writer) (int32, bool, error) {
	conn, done, err := s.call(ctx, req)
	if err != nil {
		return 0, false, err
	}
	defer done()
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	for {
		resp, err := recv(ctx, conn)
		if err != nil {
			return 0, false, err
		}
		switch resp := resp.(type) {
		case *silkd.Stdout:
			if _, err := stdout.Write(resp.Data); err != nil {
				return 0, false, err
			}
		case *silkd.Stderr:
			if _, err := stderr.Write(resp.Data); err != nil {
				return 0, false, err
			}
		case *silkd.Exit:
			return resp.Code, true, nil
		case *silkd.Done:
			return 0, false, nil
		case *silkd.ErrorResp:
			return 0, false, resp
		default:
			return 0, false, unexpected(resp)
		}
	}
}
