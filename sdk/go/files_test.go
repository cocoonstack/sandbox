package sandbox

import (
	"testing"

	"github.com/cocoonstack/sandbox/sdk/go/silkd/silkdtest"
)

// fakeSandbox wires a Sandbox whose data plane is served by a temp-dir-backed
// silkd Fake behind a hijacking agent endpoint.
func fakeSandbox(t *testing.T) *Sandbox {
	t.Helper()
	fake := silkdtest.NewFake(t.TempDir())
	return testSandbox(t, newAgentServer(t, fake.ServeConn))
}

func TestFilesRoundTrip(t *testing.T) {
	sb := fakeSandbox(t)
	ctx := t.Context()

	if err := sb.Mkdir(ctx, "/work", true); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := sb.WriteFile(ctx, "/work/a.txt", []byte("silk body"), nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := sb.ReadFile(ctx, "/work/a.txt")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "silk body" {
		t.Errorf("read %q, want silk body", got)
	}

	info, err := sb.Stat(ctx, "/work/a.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Kind != "file" || info.Size != 9 {
		t.Errorf("stat %+v", info)
	}

	entries, err := sb.ListDir(ctx, "/work")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "a.txt" {
		t.Errorf("list %+v", entries)
	}

	if err := sb.Rename(ctx, "/work/a.txt", "/work/b.txt"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := sb.Remove(ctx, "/work/b.txt", false); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := sb.ReadFile(ctx, "/work/b.txt"); err == nil {
		t.Error("read of removed file succeeded")
	}
}

func TestReadMissingFileErrors(t *testing.T) {
	sb := fakeSandbox(t)
	if _, err := sb.ReadFile(t.Context(), "/nope"); err == nil {
		t.Error("read of missing file succeeded")
	}
}

func TestSessionLifecycle(t *testing.T) {
	sb := fakeSandbox(t)
	ctx := t.Context()

	sess, err := sb.NewSession(ctx, WithSessionCwd("/work"))
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	if sess.ID == "" {
		t.Fatal("empty session id")
	}
	ids, err := sb.Sessions(ctx)
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(ids) != 1 || ids[0] != sess.ID {
		t.Errorf("sessions %v, want [%s]", ids, sess.ID)
	}
	// exec-in-session runs argv (the fake's echo handler).
	out, err := sess.Exec(ctx, "echo", "hi")
	if err != nil {
		t.Fatalf("session exec: %v", err)
	}
	if out != "hi\n" {
		t.Errorf("exec %q, want hi", out)
	}
	if err := sess.Close(ctx); err != nil {
		t.Fatalf("close session: %v", err)
	}
	if err := sess.Close(ctx); err == nil {
		t.Error("second close succeeded")
	}
}
