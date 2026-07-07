// Package e2e drives the full host stack in one process: real pool manager,
// real engine dialing real hybrid-vsock sockets, real HTTP server and relay,
// real SDK — only cocoon and the guest are faked. It doubles as the drift
// guard between the SDK's wire mirrors and sandboxd's types.
package e2e

import (
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/server"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

var testKey = types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall}

func TestEndToEnd(t *testing.T) {
	stack := startStack(t, "node-token", config.PoolSpec{PoolKey: testKey, Warm: 1})

	var sb *sandbox.Sandbox
	t.Run("cold claim before any refill", func(t *testing.T) {
		var err error
		sb, err = stack.client.New(t.Context(), "rt:24.04")
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		out, err := sb.Exec(t.Context(), "echo", "42")
		if err != nil {
			t.Fatalf("Exec: %v", err)
		}
		if out != "42\n" {
			t.Errorf("stdout %q, want 42\\n", out)
		}
	})

	t.Run("refill then warm claim", func(t *testing.T) {
		// Warm-hit = zero VM ops is proven by the pool unit tests; asserting
		// create counts here would race the background refill ticker.
		waitFor(t, func() bool {
			infos, _, _ := stack.mgr.Info()
			return len(infos) == 1 && infos[0].Warm >= 1
		})
		warm, err := stack.client.New(t.Context(), "rt:24.04")
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer warm.Close()
		if out, err := warm.Exec(t.Context(), "echo", "warm"); err != nil || out != "warm\n" {
			t.Errorf("exec on warm claim: %q, %v", out, err)
		}
	})

	t.Run("close releases and revokes access", func(t *testing.T) {
		if sb == nil {
			t.Skip("cold claim subtest failed")
		}
		if err := sb.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, err := sb.Exec(t.Context(), "echo", "zombie"); err == nil ||
			!strings.Contains(err.Error(), "unknown sandbox") {
			t.Errorf("got %v, want unknown sandbox after release", err)
		}
		if err := sb.Close(); err != nil {
			t.Errorf("second Close: %v", err)
		}
	})
}

func TestForkEndToEnd(t *testing.T) {
	stack := startStack(t, "node-token")
	parent, err := stack.client.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer parent.Close()

	children, err := parent.Fork(t.Context(), 2, time.Minute)
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if len(children) != 2 {
		t.Fatalf("got %d children, want 2", len(children))
	}
	for i, child := range children {
		if out, err := child.Exec(t.Context(), "echo", "kid"); err != nil || out != "kid\n" {
			t.Fatalf("child %d exec: %q, %v", i, out, err)
		}
	}
	// Children are independent claims: releasing one leaves the sibling and
	// the parent alive.
	if err := children[0].Close(); err != nil {
		t.Fatalf("close child 0: %v", err)
	}
	if _, err := children[1].Exec(t.Context(), "echo", "alive"); err != nil {
		t.Errorf("sibling died with the released child: %v", err)
	}
	if _, err := parent.Exec(t.Context(), "echo", "alive"); err != nil {
		t.Errorf("parent died with the released child: %v", err)
	}
	_ = children[1].Close()
}

func TestPromoteEndToEnd(t *testing.T) {
	stack := startStack(t, "node-token")
	parent, err := stack.client.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer parent.Close()

	tpl, err := parent.Promote(t.Context(), "e2e-tpl:1")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	// Both claim surfaces must work: the owner-bound handle and the
	// name-based Client call on the (single) node that holds the template.
	child, err := tpl.New(t.Context())
	if err != nil {
		t.Fatalf("claim via template handle: %v", err)
	}
	if out, err := child.Exec(t.Context(), "echo", "tpl"); err != nil || out != "tpl\n" {
		t.Errorf("exec on promoted claim: %q, %v", out, err)
	}
	_ = child.Close()
	byName, nameErr := stack.client.New(t.Context(), "e2e-tpl:1")
	if nameErr != nil {
		t.Fatalf("claim promoted template by name: %v", nameErr)
	}
	_ = byName.Close()

	if err := tpl.Delete(t.Context()); err != nil {
		t.Fatalf("Template.Delete: %v", err)
	}
	if err := stack.client.DeleteTemplate(t.Context(), "e2e-tpl:1"); err == nil ||
		!strings.Contains(err.Error(), "unknown template") {
		t.Errorf("second delete: %v, want unknown template", err)
	}
}

func TestCheckpointEndToEnd(t *testing.T) {
	stack := startStack(t, "node-token")
	src, err := stack.client.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer src.Close()

	ckpt, err := src.Checkpoint(t.Context(), "step-1")
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if ckpt.SandboxID != src.ID || ckpt.Name != "step-1" {
		t.Errorf("record %+v, want bound to %s", ckpt, src.ID)
	}
	if out, execErr := src.Exec(t.Context(), "echo", "still-alive"); execErr != nil || out != "still-alive\n" {
		t.Fatalf("source after checkpoint: %q, %v", out, execErr)
	}

	branch, err := ckpt.New(t.Context())
	if err != nil {
		t.Fatalf("branch: %v", err)
	}
	if branch.ID == src.ID {
		t.Error("branch reused the source id")
	}
	if out, execErr := branch.Exec(t.Context(), "echo", "branched"); execErr != nil || out != "branched\n" {
		t.Errorf("exec on branch: %q, %v", out, execErr)
	}
	_ = branch.Close()

	ckpts, err := stack.client.Checkpoints(t.Context())
	if err != nil || len(ckpts) != 1 || ckpts[0].ID != ckpt.ID {
		t.Fatalf("Checkpoints() = %+v, %v; want the one record", ckpts, err)
	}
	if err := ckpts[0].Delete(t.Context()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := ckpt.New(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "unknown checkpoint") {
		t.Errorf("branch after delete: %v, want unknown checkpoint", err)
	}
}

func TestWrongAPITokenRejected(t *testing.T) {
	stack := startStack(t, "node-token")

	bad, err := sandbox.Connect(stack.addr, sandbox.WithAPIToken("wrong"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := bad.New(t.Context(), "rt:24.04"); err == nil ||
		!strings.Contains(err.Error(), "401") {
		t.Errorf("got %v, want 401", err)
	}
}

type stack struct {
	client *sandbox.Client
	mgr    *pool.Manager
	addr   string
}

func startStack(t *testing.T, apiToken string, pools ...config.PoolSpec) *stack {
	t.Helper()
	// Short prefix: the sockets under it must fit darwin's 104-byte sun_path.
	dir, err := os.MkdirTemp("", "sbx")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	eng := newFakeEngine(dir)
	mgr, err := pool.NewManager(&config.Config{DataDir: dir, Pools: pools}, eng)
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	go mgr.Run(t.Context())

	ts := httptest.NewServer(server.New(apiToken, "", mgr, eng.real, nil, nil).Handler())
	t.Cleanup(ts.Close)
	addr := strings.TrimPrefix(ts.URL, "http://")
	client, err := sandbox.Connect(addr, sandbox.WithAPIToken(apiToken))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return &stack{client: client, mgr: mgr, addr: addr}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met within 10s")
}
