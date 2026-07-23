package sandbox

import (
	"cmp"
	"context"
	"fmt"
	"io"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

// Spawn starts cmd detached: it returns the guest pid as soon as the
// process starts, and the process keeps running with a bounded output ring
// readable later via Logs or Attach. cmd's Stdin/Stdout/Stderr are ignored;
// cmd.Session must be empty — the session exec path cannot detach.
func (s *Sandbox) Spawn(ctx context.Context, cmd Cmd) (uint32, error) {
	if len(cmd.Argv) == 0 {
		return 0, fmt.Errorf("empty argv")
	}
	if cmd.Session != "" {
		return 0, fmt.Errorf("spawn does not support session")
	}
	req := &wire.Exec{Argv: cmd.Argv, Cwd: cmd.Cwd, Env: cmd.Env, User: cmd.User, Detach: true}
	started, err := oneShotRPC[wire.Started](ctx, s, req)
	if err != nil {
		return 0, err
	}
	return started.PID, nil
}

// Ps lists the guest's tracked processes — execs, spawns, and ptys — with
// state and exit codes.
func (s *Sandbox) Ps(ctx context.Context) ([]wire.ProcInfo, error) {
	procs, err := oneShotRPC[wire.Procs](ctx, s, wire.Ps{})
	if err != nil {
		return nil, err
	}
	return procs.Procs, nil
}

// Kill signals a tracked process; sig 0 sends SIGKILL. Killing one that
// already exited is a no-op success — its OS pid may be recycled, so silkd
// never signals a reaped child.
func (s *Sandbox) Kill(ctx context.Context, pid uint32, sig int32) error {
	req := &wire.Kill{PID: pid}
	if sig != 0 {
		req.Signal = &sig
	}
	return s.doneRPC(ctx, req)
}

// Logs replays a tracked process's ring-buffered output into the writers
// (nil discards). exited reports whether the process has ended; code is its
// exit code when it has.
func (s *Sandbox) Logs(ctx context.Context, pid uint32, stdout, stderr io.Writer) (code int32, exited bool, err error) {
	return s.drainProc(ctx, &wire.Logs{PID: pid}, stdout, stderr)
}

// Attach replays the buffered output, then follows live output until the
// process exits, returning its exit code. exited is false only when the
// proc table dropped the process mid-attach (reap race).
func (s *Sandbox) Attach(ctx context.Context, pid uint32, stdout, stderr io.Writer) (code int32, exited bool, err error) {
	return s.drainProc(ctx, &wire.Attach{PID: pid}, stdout, stderr)
}

// drainProc pumps stdout/stderr frames until the terminal frame: exit
// carries the code, done means the stream ended without one.
func (s *Sandbox) drainProc(ctx context.Context, req wire.Request, stdout, stderr io.Writer) (int32, bool, error) {
	conn, done, err := s.call(ctx, req)
	if err != nil {
		return 0, false, err
	}
	defer done()
	stdout = cmp.Or(stdout, io.Discard)
	stderr = cmp.Or(stderr, io.Discard)
	for {
		resp, err := recv(ctx, conn)
		if err != nil {
			return 0, false, err
		}
		switch resp := resp.(type) {
		case *wire.Stdout:
			if _, err := stdout.Write(resp.Data); err != nil {
				return 0, false, err
			}
		case *wire.Stderr:
			if _, err := stderr.Write(resp.Data); err != nil {
				return 0, false, err
			}
		case *wire.Exit:
			return resp.Code, true, nil
		case *wire.Done:
			return 0, false, nil
		case *wire.ErrorResp:
			return 0, false, resp
		default:
			return 0, false, unexpected(resp)
		}
	}
}
