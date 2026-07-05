// smoke drives the full SDK v2 surface against one live sandbox: files,
// sessions, find/replace, watch, git, and pty — the bare-metal proof that
// silkd v2 works end to end through the relay. Run by scripts/sandboxd-e2e.sh
// on a real node.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk"
	"github.com/cocoonstack/sandbox/sdk/silkd"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template ref")
	flag.Parse()

	if err := run(*addr, *token, *template); err != nil {
		fmt.Fprintln(os.Stderr, "smoke:", err)
		os.Exit(1)
	}
	fmt.Println("SMOKE PASS")
}

func run(addr, token, template string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var copts []sandbox.ClientOption
	if token != "" {
		copts = append(copts, sandbox.WithAPIToken(token))
	}
	client, err := sandbox.Connect(addr, copts...)
	if err != nil {
		return err
	}
	// Explicitly the no-network lane: the git step asserts its typed error.
	sb, err := client.New(ctx, template, sandbox.WithNetwork(sandbox.NetNone))
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	defer func() { _ = sb.Close() }()

	steps := []struct {
		name string
		fn   func(context.Context, *sandbox.Sandbox) error
	}{
		{"exec", smokeExec},
		{"files", smokeFiles},
		{"session", smokeSession},
		{"find", smokeFind},
		{"replace", smokeReplace},
		{"watch", smokeWatch},
		{"git", smokeGit},
		{"pty", smokePty},
	}
	for _, s := range steps {
		start := time.Now()
		if err := s.fn(ctx, sb); err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
		fmt.Printf("  %-8s ok (%.0fms)\n", s.name, float64(time.Since(start).Microseconds())/1000)
	}
	return nil
}

func smokeExec(ctx context.Context, sb *sandbox.Sandbox) error {
	out, err := sb.Exec(ctx, "echo", "hello")
	if err != nil {
		return err
	}
	return want(out, "hello\n")
}

func smokeFiles(ctx context.Context, sb *sandbox.Sandbox) error {
	if err := sb.Mkdir(ctx, "/work", true); err != nil {
		return err
	}
	if err := sb.WriteFile(ctx, "/work/a.txt", []byte("silk body"), nil); err != nil {
		return err
	}
	got, err := sb.ReadFile(ctx, "/work/a.txt")
	if err != nil {
		return err
	}
	if err = want(string(got), "silk body"); err != nil {
		return err
	}
	entries, err := sb.ListDir(ctx, "/work")
	if err != nil {
		return err
	}
	if len(entries) != 1 || entries[0].Name != "a.txt" {
		return fmt.Errorf("list: %+v", entries)
	}
	return nil
}

func smokeSession(ctx context.Context, sb *sandbox.Sandbox) error {
	sess, err := sb.NewSession(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = sess.Close(ctx) }()
	if _, err = sess.Exec(ctx, "export", "MARK=silk123"); err != nil {
		return err
	}
	out, err := sess.Exec(ctx, "sh", "-c", "echo $MARK")
	if err != nil {
		return err
	}
	return want(strings.TrimSpace(out), "silk123")
}

func smokeFind(ctx context.Context, sb *sandbox.Sandbox) error {
	if err := sb.WriteFile(ctx, "/work/code.rs", []byte("fn main() {\n  // TODO fix\n}\n"), nil); err != nil {
		return err
	}
	if err := sb.WriteFile(ctx, "/work/notes.txt", []byte("TODO decoy\n"), nil); err != nil {
		return err
	}
	matches, err := sb.Find(ctx, "/work", "TODO", ".rs")
	if err != nil {
		return err
	}
	if len(matches) != 1 || !strings.HasSuffix(matches[0].File, "code.rs") || matches[0].Line != 2 {
		return fmt.Errorf("matches %+v, want only code.rs line 2", matches)
	}
	return nil
}

func smokeReplace(ctx context.Context, sb *sandbox.Sandbox) error {
	if err := sb.WriteFile(ctx, "/work/r.txt", []byte("foo foo bar\n"), nil); err != nil {
		return err
	}
	res, err := sb.Replace(ctx, []string{"/work/r.txt"}, "foo", "baz")
	if err != nil {
		return err
	}
	if len(res) != 1 || res[0].Replacements != 2 {
		return fmt.Errorf("replace results %+v, want 2 replacements in one file", res)
	}
	got, err := sb.ReadFile(ctx, "/work/r.txt")
	if err != nil {
		return err
	}
	return want(string(got), "baz baz bar\n")
}

func smokeWatch(ctx context.Context, sb *sandbox.Sandbox) error {
	w, err := sb.Watch(ctx, "/work", true)
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }()
	// fs_watch sends no ready frame (Watch returns before the sandbox
	// confirms), so arming is only observable by its effects — wait it out.
	time.Sleep(300 * time.Millisecond)
	if err := sb.WriteFile(ctx, "/work/w.txt", []byte("x"), nil); err != nil {
		return err
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-w.Events():
			if !ok {
				return fmt.Errorf("watch ended early: %v", w.Err())
			}
			// The atomic write surfaces as temp-file events too; any event
			// whose path carries the name proves the stream.
			if strings.Contains(ev.Path, "w.txt") {
				return nil
			}
		case <-deadline:
			return fmt.Errorf("no event for w.txt within 5s")
		}
	}
}

func smokeGit(ctx context.Context, sb *sandbox.Sandbox) error {
	if _, err := sb.Exec(ctx, "git", "-C", "/work", "init", "-q", "-b", "main"); err != nil {
		return err
	}
	if err := sb.GitAdd(ctx, "/work", "a.txt"); err != nil {
		return err
	}
	hash, err := sb.GitCommit(ctx, "/work", "first", "Dev <dev@example.com>")
	if err != nil {
		return err
	}
	if len(hash) != 40 {
		return fmt.Errorf("commit hash %q", hash)
	}
	st, err := sb.GitStatus(ctx, "/work")
	if err != nil {
		return err
	}
	if st.Branch != "main" {
		return fmt.Errorf("branch %q, want main", st.Branch)
	}
	// quotePath=false: a non-ASCII filename must come back raw, not C-quoted.
	if err = sb.WriteFile(ctx, "/work/café.txt", []byte("x"), nil); err != nil {
		return err
	}
	st2, err := sb.GitStatus(ctx, "/work")
	if err != nil {
		return err
	}
	var found bool
	for _, f := range st2.Files {
		if f.Path == "café.txt" {
			found = true
		}
		if strings.Contains(f.Path, `\`) {
			return fmt.Errorf("path C-quoted (quotePath not disabled): %q", f.Path)
		}
	}
	if !found {
		return fmt.Errorf("non-ASCII file not in status: %+v", st2.Files)
	}

	if err = sb.GitCreateBranch(ctx, "/work", "feature"); err != nil {
		return err
	}
	br, err := sb.GitBranches(ctx, "/work")
	if err != nil {
		return err
	}
	if br.Current != "main" || !slices.Contains(br.Branches, "feature") {
		return fmt.Errorf("branches %+v, want feature alongside current main", br)
	}
	if err = sb.GitCheckout(ctx, "/work", "feature"); err != nil {
		return err
	}
	if br, err = sb.GitBranches(ctx, "/work"); err != nil || br.Current != "feature" {
		return fmt.Errorf("after checkout: current=%q err=%v", br.Current, err)
	}

	// This sandbox is on the no-network lane: push must fail with the typed
	// unimplemented error, not hang on an unreachable remote.
	var e *silkd.ErrorResp
	if err := sb.GitPush(ctx, "/work", ""); !errors.As(err, &e) || e.Kind != silkd.KindUnimplemented {
		return fmt.Errorf("push on none lane: %v, want unimplemented", err)
	}
	return nil
}

func smokePty(ctx context.Context, sb *sandbox.Sandbox) error {
	pty, err := sb.OpenPty(ctx, sandbox.PtyOpts{Cols: 80, Rows: 24})
	if err != nil {
		return err
	}
	defer func() { _ = pty.Close() }()

	if _, err = pty.Write([]byte("echo PTYMARK$((6*7))\nexit\n")); err != nil {
		return err
	}
	out, err := io.ReadAll(pty)
	if err != nil {
		return err
	}
	if !strings.Contains(string(out), "PTYMARK42") {
		return fmt.Errorf("pty output missing marker: %q", out)
	}
	if code, ok := pty.ExitCode(); !ok || code != 0 {
		return fmt.Errorf("pty exit code %d ok=%v, want 0 true", code, ok)
	}
	return nil
}

func want(got, exp string) error {
	if got != exp {
		return fmt.Errorf("got %q, want %q", got, exp)
	}
	return nil
}
