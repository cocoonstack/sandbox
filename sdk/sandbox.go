package sandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/sdk/silkd"
)

const (
	releaseTimeout = 30 * time.Second
	stdinChunk     = 32 * 1024
)

// Sandbox is a claimed sandbox handle; all data-plane calls dial its owning
// node directly.
type Sandbox struct {
	ID       string
	Deadline time.Time

	c     *Client
	token string
	owner string // data-plane address (owner node), from the claim
}

// Cmd describes a streaming Run.
type Cmd struct {
	Argv    []string
	Cwd     string
	Env     map[string]string
	User    string
	Session string // when set, runs inside that persistent shell session

	// Stdin is consumed until EOF or until the command exits. A blocking
	// reader (e.g. os.Stdin) whose command exits first keeps its pump
	// goroutine parked in Read until the next bytes arrive — do not share
	// one reader across Runs, the stale pump would swallow them. nil closes
	// the child's stdin immediately.
	Stdin  io.Reader
	Stdout io.Writer // nil discards
	Stderr io.Writer // nil discards
}

// ExitError reports a non-zero exit from Exec.
type ExitError struct {
	Code   int
	Stderr string
}

func (e *ExitError) Error() string {
	if msg := strings.TrimSpace(e.Stderr); msg != "" {
		return fmt.Sprintf("exit status %d: %s", e.Code, msg)
	}
	return fmt.Sprintf("exit status %d", e.Code)
}

// Exec runs argv to completion and returns its stdout; a non-zero exit
// surfaces as *ExitError carrying stderr, alongside the partial stdout.
func (s *Sandbox) Exec(ctx context.Context, argv ...string) (string, error) {
	var stdout, stderr strings.Builder
	code, err := s.Run(ctx, Cmd{Argv: argv, Stdout: &stdout, Stderr: &stderr})
	if err != nil {
		return "", err
	}
	if code != 0 {
		return stdout.String(), &ExitError{Code: code, Stderr: stderr.String()}
	}
	return stdout.String(), nil
}

// dial opens one relayed silkd connection and arms ctx cancellation to close
// it; the returned cleanup must be deferred. One connection carries one RPC.
func (s *Sandbox) dial(ctx context.Context) (*silkd.Conn, func(), error) {
	raw, err := s.c.dialAgent(ctx, s.owner, s.ID, s.token)
	if err != nil {
		return nil, nil, err
	}
	conn := silkd.NewConn(raw)
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	return conn, func() { stop(); _ = conn.Close() }, nil
}

// Run executes cmd in the sandbox, streaming stdio over one relayed silkd
// connection, and returns the exit code.
func (s *Sandbox) Run(ctx context.Context, cmd Cmd) (int, error) {
	if len(cmd.Argv) == 0 {
		return 0, fmt.Errorf("empty argv")
	}
	conn, done, err := s.dial(ctx)
	if err != nil {
		return 0, err
	}
	defer done()

	if err := conn.Send(&silkd.Exec{Argv: cmd.Argv, Cwd: cmd.Cwd, Env: cmd.Env, User: cmd.User, Session: cmd.Session}); err != nil {
		return 0, fmt.Errorf("send exec: %w", err)
	}
	if cmd.Stdin == nil {
		if err := conn.Send(silkd.StdinClose{}); err != nil {
			return 0, fmt.Errorf("close stdin: %w", err)
		}
	} else {
		go pumpStdin(conn, cmd.Stdin)
	}

	stdout, stderr := cmd.Stdout, cmd.Stderr
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	for {
		resp, err := conn.Recv()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return 0, ctxErr
			}
			if errors.Is(err, io.EOF) {
				return 0, fmt.Errorf("connection closed before exit")
			}
			return 0, err
		}
		switch resp := resp.(type) {
		case *silkd.Started:
		case *silkd.Stdout:
			if _, err := stdout.Write(resp.Data); err != nil {
				return 0, err
			}
		case *silkd.Stderr:
			if _, err := stderr.Write(resp.Data); err != nil {
				return 0, err
			}
		case *silkd.Exit:
			return int(resp.Code), nil
		case *silkd.ErrorResp:
			return 0, resp
		default:
			return 0, fmt.Errorf("unexpected frame %q", resp.RespType())
		}
	}
}

// Close releases the sandbox on its node; releasing one already gone is not
// an error. It takes no ctx so it stays defer-friendly — bounded internally.
func (s *Sandbox) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	// Release is owner-scoped: the owning node holds the claim.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+s.owner+"/v1/sandboxes/"+s.ID+"/release", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	resp, err := s.c.hc.Do(req) //nolint:gosec // dialing the caller-configured node is the SDK's purpose
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return apiError("release", resp)
}

// pumpStdin chunks the reader into stdin frames; Send's own locking keeps
// the pump safe against the caller's concurrent frames.
func pumpStdin(conn *silkd.Conn, r io.Reader) {
	buf := make([]byte, stdinChunk)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if sendErr := conn.Send(&silkd.Stdin{Data: buf[:n]}); sendErr != nil {
				return
			}
		}
		if err != nil {
			_ = conn.Send(silkd.StdinClose{})
			return
		}
	}
}
