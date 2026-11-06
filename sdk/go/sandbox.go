package sandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

const (
	releaseTimeout = 30 * time.Second
	killWait       = 5 * time.Second
	stdinChunk     = 32 * 1024
)

// Cmd describes a streaming Run.
type Cmd struct {
	Argv    []string
	Cwd     string
	Env     map[string]string
	User    string
	Session string // when set, runs inside that persistent shell session

	// Stdin is read until EOF or exit; a reader shared across Runs loses bytes to a stale pump, and nil closes the child's stdin.
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

// ownerGoneError tags an owner answer after which nothing reached it, so the call may go to the sandbox's new owner.
type ownerGoneError struct{ err error }

func (e *ownerGoneError) Error() string { return e.err.Error() }

func (e *ownerGoneError) Unwrap() error { return e.err }

// Sandbox is a claimed sandbox handle; all data-plane calls dial its owning
// node directly.
type Sandbox struct {
	ID       string
	Deadline time.Time
	// Volumes lists confirmed applied volumes; only a writable entry echoes a mode.
	Volumes []Volume
	// VolumesWant is the persisted desired set, which may differ after a failed convergence.
	VolumesWant []Volume
	// TemplateDigest is the content identity of the promoted-template export this sandbox was cloned from; empty otherwise.
	TemplateDigest string

	// FromCheckpoint names the checkpoint this sandbox branched from; empty otherwise.
	FromCheckpoint string

	// NetRoute is the guest's network path at claim, as the node reports it.
	NetRoute string

	c     *Client
	token string
	pool  agentPool

	mu    sync.Mutex
	owner string
}

// Owner returns the data-plane address of the node that owns the sandbox —
// on a cluster the node a redirected claim landed on.
func (s *Sandbox) Owner() string {
	s.mu.Lock()
	defer s.mu.Unlock()
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
// connection, and returns the exit code; a ctx that ends first kills the command.
func (s *Sandbox) Run(ctx context.Context, cmd Cmd) (int, error) {
	if len(cmd.Argv) == 0 {
		return 0, fmt.Errorf("empty argv")
	}
	code, err := s.run(ctx, cmd)
	if s.rebind(ctx, err) && cmd.Stdin == nil {
		return s.run(ctx, cmd)
	}
	return code, err
}

// Fork clones the sandbox into count children with their own leases; on error no child survives.
func (s *Sandbox) Fork(ctx context.Context, count int, ttl time.Duration) ([]*Sandbox, error) {
	body, err := encodeBody("fork", forkRequest{Token: s.token, Count: count, TTLSeconds: ttlSeconds(ttl)})
	if err != nil {
		return nil, err
	}
	fr, addr, err := s.ownerJSON[forkResponse](ctx, http.MethodPost, "/fork", body, s.c.apiToken, "fork")
	if err != nil {
		return nil, err
	}
	children := make([]*Sandbox, len(fr.Children))
	for i, c := range fr.Children {
		children[i] = s.c.handleFrom(addr, c)
	}
	return children, nil
}

// Promote publishes the sandbox's current state as a template; the returned handle is bound to the owning node.
func (s *Sandbox) Promote(ctx context.Context, template string) (*Template, error) {
	body, err := encodeBody("promote", promoteRequest{Token: s.token, Template: template})
	if err != nil {
		return nil, err
	}
	pr, addr, err := s.ownerJSON[promoteResponse](ctx, http.MethodPost, "/promote", body, s.c.apiToken, "promote")
	if err != nil {
		return nil, err
	}
	return &Template{
		Name: pr.Key.Template, ContentDigest: pr.ContentDigest,
		c: s.c, addr: addr, net: pr.Key.Net, size: pr.Key.Size,
	}, nil
}

// Hibernate snapshots the sandbox and stops its VM; the next guest call wakes it, and the TTL keeps running.
func (s *Sandbox) Hibernate(ctx context.Context) error {
	s.pool.drain()
	return s.ownerNoContent(ctx, http.MethodPost, "/hibernate", nil, s.token, "hibernate")
}

// Renew resets the lease to ttl from now (zero means the server default) and records the granted deadline in Deadline.
func (s *Sandbox) Renew(ctx context.Context, ttl time.Duration) (time.Time, error) {
	body, err := encodeBody("renew", renewRequest{TTLSeconds: ttlSeconds(ttl)})
	if err != nil {
		return time.Time{}, err
	}
	rr, _, err := s.ownerJSON[renewResponse](ctx, http.MethodPost, "/renew", body, s.token, "renew")
	if err != nil {
		return time.Time{}, err
	}
	s.Deadline = rr.Deadline
	return rr.Deadline, nil
}

// Close releases the sandbox on its node; releasing one already gone is not
// an error. It takes no ctx so it stays defer-friendly — bounded internally.
func (s *Sandbox) Close() error {
	s.pool.drain()
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	found, err := s.releaseAt(ctx, s.Owner())
	if _, refused := errors.AsType[*APIError](err); refused || found {
		return err
	}
	moved, lookupErr := s.c.Lookup(ctx, s.ID, s.token)
	if lookupErr != nil {
		return err
	}
	_, err = s.releaseAt(ctx, moved.owner)
	return err
}

func (s *Sandbox) run(ctx context.Context, cmd Cmd) (int, error) {
	conn, l, err := s.call(ctx, &wire.Exec{Argv: cmd.Argv, Cwd: cmd.Cwd, Env: cmd.Env, User: cmd.User, Session: cmd.Session})
	if err != nil {
		return 0, err
	}
	defer l.close()

	stdinDone := make(chan struct{})
	if cmd.Stdin == nil {
		// A guest that already answered and closed fails this send; the frames it sent still come back below
		_ = conn.Send(wire.StdinClose{})
		close(stdinDone)
	} else {
		go func() {
			defer close(stdinDone)
			pumpStdin(conn, cmd.Stdin)
		}()
	}

	pid, code, exited, err := pumpStdio(ctx, conn, cmd.Stdout, cmd.Stderr)
	// A pump still parked in Read would write into the next RPC, so its connection is not kept
	select {
	case <-stdinDone:
		err = l.done(err)
	default:
	}
	if err != nil {
		if ctx.Err() != nil && pid != 0 {
			kctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), killWait)
			_ = s.Kill(kctx, pid, 0)
			cancel()
		}
		return 0, err
	}
	if !exited {
		return 0, unexpected(&wire.Done{})
	}
	return int(code), nil
}

func (s *Sandbox) releaseAt(ctx context.Context, addr string) (found bool, err error) {
	resp, err := s.c.roundTrip(ctx, http.MethodPost, addr, "/v1/sandboxes/"+s.ID+"/release", nil, s.token)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	}
	return false, apiError("release", resp)
}

// rebind follows a moved sandbox: a tagged owner error asks Lookup, and a different owner replaces it and its parked connections.
func (s *Sandbox) rebind(ctx context.Context, err error) bool {
	if _, gone := errors.AsType[*ownerGoneError](err); !gone {
		return false
	}
	moved, lookupErr := s.c.Lookup(ctx, s.ID, s.token)
	if lookupErr != nil {
		return false
	}
	s.mu.Lock()
	changed := moved.owner != s.owner
	s.owner = moved.owner
	s.mu.Unlock()
	if changed {
		s.pool.drain()
	}
	return changed
}

// retryMoved runs rpc once more on the new owner when nothing of it reached the old one; rpc's request must be its whole input.
func (s *Sandbox) retryMoved(ctx context.Context, rpc func() error) error {
	err := rpc()
	if s.rebind(ctx, err) {
		return rpc()
	}
	return err
}

// ownerJSON returns the reply and the address that gave it, which derived handles bind to.
func (s *Sandbox) ownerJSON[T any](ctx context.Context, method, suffix string, body []byte, bearer, verb string) (T, string, error) {
	var out T
	var addr string
	err := s.retryMoved(ctx, func() (err error) {
		addr = s.Owner()
		out, err = s.c.doJSON[T](ctx, method, addr, "/v1/sandboxes/"+s.ID+suffix, bodyReader(body), bearer, verb)
		return markGone(err)
	})
	return out, addr, err
}

func (s *Sandbox) ownerNoContent(ctx context.Context, method, suffix string, body []byte, bearer, verb string) error {
	return s.retryMoved(ctx, func() error {
		return markGone(s.c.doNoContent(ctx, method, s.Owner(), "/v1/sandboxes/"+s.ID+suffix, bodyReader(body), bearer, verb))
	})
}

// call sends req on a relay connection and hands back the lease that ends the RPC; ctx cancellation closes the connection under it.
func (s *Sandbox) call(ctx context.Context, req wire.Request) (*silkd.Conn, *lease, error) {
	c, err := s.connect(ctx, req)
	if err != nil {
		return nil, nil, err
	}
	return c.Conn, &lease{s: s, conn: c, stop: context.AfterFunc(ctx, func() { _ = c.Close() })}, nil
}

// connect sends req on a parked connection or a fresh dial.
func (s *Sandbox) connect(ctx context.Context, req wire.Request) (*agentConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c := s.pool.take(); c != nil {
		return c.sent(req)
	}
	c, err := s.dial(ctx)
	if err != nil {
		return nil, err
	}
	return c.sent(req)
}

func (s *Sandbox) dial(ctx context.Context) (*agentConn, error) {
	raw, err := s.c.dialAgent(ctx, s.Owner(), s.ID, s.token)
	if err != nil {
		return nil, err
	}
	return newAgentConn(raw), nil
}

// markGone tags an owner's 404 or refused dial, after which nothing reached it; errors from the caller's own writers stay untagged.
func markGone(err error) error {
	ae, ok := errors.AsType[*APIError](err)
	if ok && ae.Status == http.StatusNotFound || !ok && errors.Is(err, syscall.ECONNREFUSED) {
		return &ownerGoneError{err}
	}
	return err
}

// pumpStdin chunks the reader into stdin frames.
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
