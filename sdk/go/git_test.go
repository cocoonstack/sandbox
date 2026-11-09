package sandbox

import (
	"net"
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

func TestGitBranchLifecycle(t *testing.T) {
	sb := fakeSandbox(t)
	ctx := t.Context()

	if err := sb.GitCreateBranch(ctx, "/repo", "feature"); err != nil {
		t.Fatalf("create: %v", err)
	}
	br, err := sb.GitBranches(ctx, "/repo")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if br.Current != "main" || !slices.Contains(br.Branches, "feature") {
		t.Fatalf("branches %+v, want feature alongside current main", br)
	}

	if err = sb.GitCheckout(ctx, "/repo", "feature"); err != nil {
		t.Fatalf("checkout: %v", err)
	}
	if br, err = sb.GitBranches(ctx, "/repo"); err != nil || br.Current != "feature" {
		t.Fatalf("after checkout: current=%q err=%v", br.Current, err)
	}
	if err = sb.GitCheckout(ctx, "/repo", "nope"); err == nil {
		t.Error("checkout of a missing branch succeeded")
	}

	if err = sb.GitDeleteBranch(ctx, "/repo", "main"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if br, err = sb.GitBranches(ctx, "/repo"); err != nil || slices.Contains(br.Branches, "main") {
		t.Errorf("after delete: %+v err=%v", br, err)
	}
}

func TestGitPushPullPlumbing(t *testing.T) {
	reqs := make(chan wire.Request, 2)
	sb := testSandbox(t, newAgentServer(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		sc := wire.NewFrameScanner(conn)
		for sc.Scan() {
			req, err := wire.DecodeRequest(sc.Bytes())
			if err != nil {
				return
			}
			reqs <- req
			line, _ := wire.EncodeResponse(wire.Done{})
			if _, err = conn.Write(append(line, '\n')); err != nil {
				return
			}
		}
	}))
	if err := sb.GitPush(t.Context(), "/repo", ""); err != nil {
		t.Fatalf("push: %v", err)
	}
	if err := sb.GitPull(t.Context(), "/repo", "tok"); err != nil {
		t.Fatalf("pull: %v", err)
	}
	if push, ok := (<-reqs).(*wire.GitPush); !ok || push.Path != "/repo" || push.Auth != "" {
		t.Errorf("guest saw %+v, want a git push of /repo", push)
	}
	if pull, ok := (<-reqs).(*wire.GitPull); !ok || pull.Path != "/repo" || pull.Auth != "tok" {
		t.Errorf("guest saw %+v, want a git pull of /repo with the token", pull)
	}
}
