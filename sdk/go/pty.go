package sandbox

import (
	"context"
	"io"
	"sync"

	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

// PtyOpts configures OpenPty.
type PtyOpts struct {
	Cols uint16
	Rows uint16
	Cwd  string
	Env  map[string]string
	User string
}

// Pty is an open pseudo-terminal in the sandbox. Read yields terminal output
// (EOF when the shell exits), Write feeds input, Resize adjusts the window,
// and Close (or canceling the OpenPty ctx) ends the session.
type Pty struct {
	PID uint32

	sb   *Sandbox
	conn *silkd.Conn
	stop func()
	out  *io.PipeReader

	mu       sync.Mutex
	exitCode int
	exited   bool
}

// Read returns terminal output; io.EOF signals the shell has exited (check
// ExitCode after).
func (p *Pty) Read(b []byte) (int, error) {
	return p.out.Read(b)
}

// Write feeds input to the terminal.
func (p *Pty) Write(b []byte) (int, error) {
	if err := p.conn.Send(&silkd.Stdin{Data: b}); err != nil {
		return 0, err
	}
	return len(b), nil
}

// Resize adjusts the terminal window; it is a separate RPC keyed by pid.
func (p *Pty) Resize(ctx context.Context, cols, rows uint16) error {
	conn, done, err := p.sb.dial(ctx)
	if err != nil {
		return err
	}
	defer done()
	if err := conn.Send(&silkd.PtyResize{PID: p.PID, Cols: cols, Rows: rows}); err != nil {
		return err
	}
	return terminalErr(ctx, conn)
}

// Close ends the pty session (silkd sees the disconnect and kills the shell).
func (p *Pty) Close() error {
	p.stop()
	return nil
}

// ExitCode reports the shell's exit code once Read has returned io.EOF; ok is
// false while the shell is still running.
func (p *Pty) ExitCode() (code int, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exitCode, p.exited
}

// drain relays response frames: output into the pipe, the exit code into
// state, and a terminal error to unblock Read.
func (p *Pty) drain(pw *io.PipeWriter) {
	for {
		resp, err := p.conn.Recv()
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		switch r := resp.(type) {
		case *silkd.Started:
		case *silkd.Stdout:
			if _, err := pw.Write(r.Data); err != nil {
				return
			}
		case *silkd.Exit:
			p.mu.Lock()
			p.exitCode, p.exited = int(r.Code), true
			p.mu.Unlock()
			_ = pw.Close()
			return
		case *silkd.ErrorResp:
			_ = pw.CloseWithError(r)
			return
		default:
			_ = pw.CloseWithError(unexpected(resp))
			return
		}
	}
}

// OpenPty starts a shell under a pty. The ctx governs the pty's lifetime:
// canceling it (or calling Close) tears the session down.
func (s *Sandbox) OpenPty(ctx context.Context, opts PtyOpts) (*Pty, error) {
	req := &silkd.PtyOpen{Cols: opts.Cols, Rows: opts.Rows, Cwd: opts.Cwd, Env: opts.Env, User: opts.User}
	conn, done, err := s.call(ctx, req)
	if err != nil {
		return nil, err
	}
	started, err := expect[silkd.Started](ctx, conn)
	if err != nil {
		done()
		return nil, err
	}

	pr, pw := io.Pipe()
	p := &Pty{PID: started.PID, sb: s, conn: conn, stop: done, out: pr}
	go p.drain(pw)
	return p, nil
}
