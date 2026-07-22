package sandbox

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

const (
	releaseTimeout = 30 * time.Second
	stdinChunk     = 32 * 1024
)

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

// Sandbox is a claimed sandbox handle; all data-plane calls dial its owning
// node directly.
type Sandbox struct {
	ID       string
	Deadline time.Time

	// FromCheckpoint names the checkpoint this sandbox branched from; empty
	// for pool and template claims.
	FromCheckpoint string

	c     *Client
	token string
	owner string // data-plane address (owner node), from the claim
}

// Owner returns the data-plane address of the node that owns the sandbox —
// on a cluster the node a redirected claim landed on.
func (s *Sandbox) Owner() string {
	return s.owner
}

// Token is the per-sandbox bearer secret — persist it with ID to reattach
// later via Lookup (treat it like a credential).
func (s *Sandbox) Token() string {
	return s.token
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

// Run executes cmd in the sandbox, streaming stdio over one relayed silkd
// connection, and returns the exit code.
func (s *Sandbox) Run(ctx context.Context, cmd Cmd) (int, error) {
	if len(cmd.Argv) == 0 {
		return 0, fmt.Errorf("empty argv")
	}
	conn, done, err := s.call(ctx, &wire.Exec{Argv: cmd.Argv, Cwd: cmd.Cwd, Env: cmd.Env, User: cmd.User, Session: cmd.Session})
	if err != nil {
		return 0, err
	}
	defer done()

	if cmd.Stdin == nil {
		if err := conn.Send(wire.StdinClose{}); err != nil {
			return 0, fmt.Errorf("close stdin: %w", err)
		}
	} else {
		go pumpStdin(conn, cmd.Stdin)
	}

	stdout := cmp.Or(cmd.Stdout, io.Discard)
	stderr := cmp.Or(cmd.Stderr, io.Discard)
	for {
		resp, err := recv(ctx, conn)
		if err != nil {
			return 0, err
		}
		switch resp := resp.(type) {
		case *wire.Started:
		case *wire.Stdout:
			if _, err := stdout.Write(resp.Data); err != nil {
				return 0, err
			}
		case *wire.Stderr:
			if _, err := stderr.Write(resp.Data); err != nil {
				return 0, err
			}
		case *wire.Exit:
			return int(resp.Code), nil
		case *wire.ErrorResp:
			return 0, resp
		default:
			return 0, unexpected(resp)
		}
	}
}

// Fork clones the sandbox into count children — memory, disk, and guest
// state (sessions, processes, tmpfs) duplicate at the fork point, and each
// child gets a fresh machine identity and its own lease. ttl bounds every
// child's lifetime; zero means the server default. All-or-nothing: on error
// no child survived. Forking a hibernated sandbox reuses its memory image
// without waking it.
func (s *Sandbox) Fork(ctx context.Context, count int, ttl time.Duration) ([]*Sandbox, error) {
	body, err := encodeBody("fork", forkRequest{Token: s.token, Count: count, TTLSeconds: ttlSeconds(ttl)})
	if err != nil {
		return nil, err
	}
	fr, err := doJSON[forkResponse](ctx, s.c, http.MethodPost, s.owner, "/v1/sandboxes/"+s.ID+"/fork", bytes.NewReader(body), s.c.apiToken, "fork")
	if err != nil {
		return nil, err
	}
	children := make([]*Sandbox, len(fr.Children))
	for i, c := range fr.Children {
		children[i] = s.c.handleFrom(s.owner, c)
	}
	return children, nil
}

// Promote publishes the sandbox's current state as a template on its owning
// node: later claims for it (with this sandbox's network lane and size)
// clone from it, provision-on-demand. Re-promoting to the same name replaces
// the template. Like Fork, a hibernated sandbox is promoted from its memory
// image without waking. Templates are node-local — the returned handle is
// bound to the owning node, and its New/Delete always reach it (name-based
// Client calls only see the connected node's templates).
func (s *Sandbox) Promote(ctx context.Context, template string) (*Template, error) {
	body, err := encodeBody("promote", promoteRequest{Token: s.token, Template: template})
	if err != nil {
		return nil, err
	}
	pr, err := doJSON[promoteResponse](ctx, s.c, http.MethodPost, s.owner, "/v1/sandboxes/"+s.ID+"/promote", bytes.NewReader(body), s.c.apiToken, "promote")
	if err != nil {
		return nil, err
	}
	return &Template{Name: pr.Key.Template, c: s.c, addr: s.owner, net: pr.Key.Net, size: pr.Key.Size}, nil
}

// Hibernate atomically snapshots the sandbox and stops its VM, freeing its
// memory; the next call that reaches the guest wakes it transparently with
// sessions, processes, and memory state intact. The TTL keeps running — a
// hibernated sandbox is still reaped at its deadline.
func (s *Sandbox) Hibernate(ctx context.Context) error {
	return doNoContent(ctx, s.c, http.MethodPost, s.owner, "/v1/sandboxes/"+s.ID+"/hibernate", nil, s.token, "hibernate")
}

// Close releases the sandbox on its node; releasing one already gone is not
// an error. It takes no ctx so it stays defer-friendly — bounded internally.
func (s *Sandbox) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	resp, err := s.post(ctx, "release", nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return apiError("release", resp)
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

// call dials one RPC connection and sends its leading request, cleaning up
// itself on a send failure; the returned cleanup must be deferred.
func (s *Sandbox) call(ctx context.Context, req wire.Request) (*silkd.Conn, func(), error) {
	conn, done, err := s.dial(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err := conn.Send(req); err != nil {
		done()
		return nil, nil, err
	}
	return conn, done, nil
}

// post sends a sandbox-scoped verb to the owning node (which holds the
// claim); a non-nil body is JSON.
func (s *Sandbox) post(ctx context.Context, verb string, body io.Reader) (*http.Response, error) {
	return s.c.roundTrip(ctx, http.MethodPost, s.owner, "/v1/sandboxes/"+s.ID+"/"+verb, body, s.token)
}

// pumpStdin chunks the reader into stdin frames; Send's own locking keeps
// the pump safe against the caller's concurrent frames.
func pumpStdin(conn *silkd.Conn, r io.Reader) {
	buf := make([]byte, stdinChunk)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if sendErr := conn.Send(&wire.Stdin{Data: buf[:n]}); sendErr != nil {
				return
			}
		}
		if err != nil {
			_ = conn.Send(wire.StdinClose{})
			return
		}
	}
}
