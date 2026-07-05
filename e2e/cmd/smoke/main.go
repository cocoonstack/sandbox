// smoke drives the full SDK v2 surface against one live sandbox: files,
// sessions, find, and git — the bare-metal proof that silkd v2 works end to
// end through the relay. Run by scripts/sandboxd-e2e.sh on a real node.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk"
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
	sb, err := client.New(ctx, template)
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
		{"git", smokeGit},
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
	// find has no SDK helper yet; drive it as a shell fallback via exec grep.
	out, err := sb.Exec(ctx, "grep", "-rn", "TODO", "/work")
	if err != nil {
		return err
	}
	if !strings.Contains(out, "TODO") {
		return fmt.Errorf("grep found no TODO: %q", out)
	}
	return nil
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
	return nil
}

func want(got, exp string) error {
	if got != exp {
		return fmt.Errorf("got %q, want %q", got, exp)
	}
	return nil
}
