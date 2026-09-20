package sandbox

import (
	"context"
	"io"
	"sync"

	"github.com/cocoonstack/sandbox/protocol/wire"
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

// Pty is an open pseudo-terminal in the sandbox.
type Pty struct {
	PID uint32

	sb   *Sandbox
	conn *silkd.Conn
	stop func()
	out  *io.PipeReader

	mu        sync.Mutex
	exitCode  int
	exited    bool
	closeOnce sync.Once
}

// Read returns terminal output; io.EOF means ExitCode is ready.
func (p *Pty) Read(b []byte) (int, error) {
	return p.out.Read(b)
}

// Write feeds input to the terminal, chunked to stay under the frame cap.
func (p *Pty) Write(b []byte) (int, error) {
	return sendChunks(p.conn, stdinChunk, func(chunk []byte) wire.Request { return &wire.Stdin{Data: chunk} }, b)
}

// Resize adjusts the terminal window; it is a separate RPC keyed by pid.
func (p *Pty) Resize(ctx context.Context, cols, rows uint16) error {
	return p.sb.doneRPC(ctx, &wire.PtyResize{PID: p.PID, Cols: cols, Rows: rows})
}

// Close ends the pty session (silkd sees the disconnect and kills the shell).
func (p *Pty) Close() error {
	p.closeOnce.Do(func() {
		p.stop()
		_ = p.out.Close()
	})
	return nil
}

// ExitCode reports the shell's exit code after Read returns io.EOF.
func (p *Pty) ExitCode() (code int, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exitCode, p.exited
}

// drain relays response frames into the output pipe and terminal state, releasing the relay when they end.
func (p *Pty) drain(ctx context.Context, pw *io.PipeWriter, stop func()) {
	defer stop()
	for {
		resp, err := recv(ctx, p.conn)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		switch r := resp.(type) {
		case *wire.Started:
		case *wire.Stdout:
			if _, err := pw.Write(r.Data); err != nil {
				return
			}
		case *wire.Exit:
			p.mu.Lock()
			p.exitCode, p.exited = int(r.Code), true
			p.mu.Unlock()
			_ = pw.Close()
			return
		case *wire.ErrorResp:
			_ = pw.CloseWithError(r)
			return
		default:
			_ = pw.CloseWithError(unexpected(resp))
			return
		}
	}
}

// OpenPty starts a shell whose lifetime is governed by ctx or Close.
func (s *Sandbox) OpenPty(ctx context.Context, opts PtyOpts) (*Pty, error) {
	req := &wire.PtyOpen{Cols: opts.Cols, Rows: opts.Rows, Cwd: opts.Cwd, Env: opts.Env, User: opts.User}
	conn, l, err := s.call(ctx, req)
	if err != nil {
		return nil, err
	}
	started, err := expect[wire.Started](ctx, conn)
	if err != nil {
		return nil, l.done(err)
	}

	pr, pw := io.Pipe()
	p := &Pty{PID: started.PID, sb: s, conn: conn, stop: l.close, out: pr}
	go p.drain(ctx, pw, l.close)
	return p, nil
}
