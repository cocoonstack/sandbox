package sandbox

import (
	"context"
	"strings"

	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

// Session is a persistent shell in the sandbox: cwd, env, and shell state
// survive across Exec calls until Close.
type Session struct {
	ID string

	sb *Sandbox
}

// Exec runs argv in the session and returns its combined output; state (cwd,
// env, shell variables) persists to the next call.
func (sess *Session) Exec(ctx context.Context, argv ...string) (string, error) {
	var out strings.Builder
	code, err := sess.sb.Run(ctx, Cmd{Argv: argv, Session: sess.ID, Stdout: &out, Stderr: &out})
	if err != nil {
		return out.String(), err
	}
	if code != 0 {
		return out.String(), &ExitError{Code: code, Stderr: out.String()}
	}
	return out.String(), nil
}

// Close terminates the session's shell and its process group.
func (sess *Session) Close(ctx context.Context) error {
	return sess.sb.doneRPC(ctx, &silkd.SessionRm{ID: sess.ID})
}

// SessionOption configures NewSession.
type SessionOption func(*silkd.SessionCreate)

// WithSessionCwd sets the session's initial working directory.
func WithSessionCwd(dir string) SessionOption {
	return func(r *silkd.SessionCreate) { r.Cwd = dir }
}

// WithSessionEnv sets the session's initial environment.
func WithSessionEnv(env map[string]string) SessionOption {
	return func(r *silkd.SessionCreate) { r.Env = env }
}

// NewSession opens a persistent shell.
func (s *Sandbox) NewSession(ctx context.Context, opts ...SessionOption) (*Session, error) {
	req := &silkd.SessionCreate{}
	for _, opt := range opts {
		opt(req)
	}
	created, err := oneShotRPC[silkd.SessionCreated](ctx, s, req)
	if err != nil {
		return nil, err
	}
	return &Session{ID: created.ID, sb: s}, nil
}

// Sessions lists the sandbox's live session ids.
func (s *Sandbox) Sessions(ctx context.Context) ([]string, error) {
	list, err := oneShotRPC[silkd.Sessions](ctx, s, silkd.SessionList{})
	if err != nil {
		return nil, err
	}
	return list.Sessions, nil
}
