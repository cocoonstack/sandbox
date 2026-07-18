// smoke drives the full SDK v2 surface against one live sandbox: files,
// sessions, find/replace, watch, git, and pty — the bare-metal proof that
// silkd v2 works end to end through the relay. Run by scripts/sandboxd-e2e.sh
// on a real node.
package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing/iotest"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template ref")
	egress := flag.Bool("egress", false, "also claim an egress-lane sandbox and check lane detection")
	lspTemplate := flag.String("lsp-template", "", "python-flavor template ref; when set, adds the LSP broker step")
	flag.Parse()

	if err := run(*addr, *token, *template, *lspTemplate, *egress); err != nil {
		fmt.Fprintln(os.Stderr, "smoke:", err)
		os.Exit(1)
	}
	fmt.Println("SMOKE PASS")
}

func run(addr, token, template, lspTemplate string, egress bool) error {
	timeout := 120 * time.Second
	if lspTemplate != "" {
		timeout = 300 * time.Second // the LSP step cold-boots a second sandbox
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := sandbox.Connect(addr, sandbox.WithAPIToken(token))
	if err != nil {
		return err
	}
	// Explicitly the no-network lane: the git step asserts its typed error.
	sb, err := client.New(ctx, template, sandbox.WithNetwork(sandbox.NetNone))
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	defer func() { _ = sb.Close() }()
	// Info is node-local: on a cluster the claim may have been redirected, so
	// hibernated-count assertions must ask the owner, not the entry node.
	ownerClient := client
	if sb.Owner() != addr {
		if ownerClient, err = sandbox.Connect(sb.Owner(), sandbox.WithAPIToken(token)); err != nil {
			return err
		}
	}

	type step struct {
		name string
		fn   func(context.Context, *sandbox.Sandbox) error
	}
	steps := []step{
		{"exec", smokeExec},
		{"files", smokeFiles},
		{"session", smokeSession},
		{"find", smokeFind},
		{"replace", smokeReplace},
		{"watch", smokeWatch},
		{"git", smokeGit},
		{"pty", smokePty},
		{"hibernate", func(ctx context.Context, sb *sandbox.Sandbox) error {
			return smokeHibernate(ctx, ownerClient, sb)
		}},
		{"fork", smokeFork},
		{"promote", func(ctx context.Context, sb *sandbox.Sandbox) error {
			return smokePromote(ctx, client, sb)
		}},
		{"procs", smokeProcs},
		{"checkpoint", smokeCheckpoint},
		{"tree", smokeTree},
		{"port", smokePortForward},
	}
	if egress {
		steps = append(steps, step{"egress", func(ctx context.Context, _ *sandbox.Sandbox) error {
			return smokeEgress(ctx, client, template)
		}})
	}
	if lspTemplate != "" {
		steps = append(steps, step{"lsp", func(ctx context.Context, sb *sandbox.Sandbox) error {
			return smokeLsp(ctx, client, sb, lspTemplate)
		}})
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
	matches, err := sb.Find(ctx, "/work", "TODO", "*.rs")
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
	if err := sb.GitPush(ctx, "/work", ""); !isSilkdKind(err, wire.KindUnimplemented) {
		return fmt.Errorf("push on none lane: %v, want unimplemented", err)
	}
	return nil
}

// smokeEgress claims a second sandbox on the egress lane and pins the
// positive half of silkd's lane detection: git must actually run there, so a
// push with no remote fails inside git — never with the none-lane
// unimplemented guard. Needs no reachable network.
func smokeEgress(ctx context.Context, client *sandbox.Client, template string) error {
	sb, err := client.New(ctx, template, sandbox.WithNetwork(sandbox.NetEgress))
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	defer func() { _ = sb.Close() }()
	if _, err := sb.Exec(ctx, "git", "init", "-q", "-b", "main", "/tmp/r"); err != nil {
		return err
	}
	if err := sb.GitPush(ctx, "/tmp/r", ""); !isSilkdKind(err, wire.KindInternal) {
		return fmt.Errorf("push on egress lane: %v, want internal git failure (lane misdetected?)", err)
	}
	return nil
}

// smokeHibernate proves the full hibernate loop: a session (a live guest
// process) survives hibernate → transparent wake-on-exec, and the node's
// hibernated count tracks the transitions.
func smokeHibernate(ctx context.Context, client *sandbox.Client, sb *sandbox.Sandbox) error {
	sess, err := sb.NewSession(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = sess.Close(context.WithoutCancel(ctx)) }()
	if _, err = sess.Exec(ctx, "export", "HIBMARK=alive"); err != nil {
		return err
	}
	if err = sb.Hibernate(ctx); err != nil {
		return err
	}
	if err = wantHibernated(ctx, client, 1); err != nil {
		return err
	}
	out, err := sess.Exec(ctx, "sh", "-c", "echo $HIBMARK") // first guest access wakes the VM
	if err != nil {
		return fmt.Errorf("exec after hibernate: %w", err)
	}
	if err = want(out, "alive\n"); err != nil {
		return err
	}
	return wantHibernated(ctx, client, 0)
}

// smokeFork proves the fork loop: children inherit the parent's disk at the
// fork point, get fresh distinct machine identities (clone reseed), and are
// fully independent claims whose writes never reach a sibling or the parent.
func smokeFork(ctx context.Context, sb *sandbox.Sandbox) error {
	if err := sb.WriteFile(ctx, "/work/fork-mark.txt", []byte("parent"), nil); err != nil {
		return err
	}
	parentID, err := sb.Exec(ctx, "cat", "/etc/machine-id")
	if err != nil {
		return err
	}
	children, err := sb.Fork(ctx, 2, 2*time.Minute)
	if err != nil {
		return err
	}
	defer func() {
		for _, c := range children {
			_ = c.Close()
		}
	}()
	ids := map[string]bool{strings.TrimSpace(parentID): true}
	for i, c := range children {
		got, err := c.ReadFile(ctx, "/work/fork-mark.txt")
		if err != nil {
			return fmt.Errorf("child %d marker: %w", i, err)
		}
		if string(got) != "parent" {
			return fmt.Errorf("child %d marker %q, want the parent's disk state", i, got)
		}
		id, err := c.Exec(ctx, "cat", "/etc/machine-id")
		if err != nil {
			return err
		}
		if id = strings.TrimSpace(id); id == "" || ids[id] {
			return fmt.Errorf("child %d machine-id %q not distinct", i, id)
		}
		ids[id] = true
		if err := c.WriteFile(ctx, fmt.Sprintf("/work/child-%d.txt", i), []byte("x"), nil); err != nil {
			return err
		}
	}
	for i := range children {
		if _, err := children[1-i].Stat(ctx, fmt.Sprintf("/work/child-%d.txt", i)); err == nil {
			return fmt.Errorf("child %d write visible in sibling — shared disk?", i)
		}
	}
	if _, err := sb.Stat(ctx, "/work/child-0.txt"); err == nil {
		return errors.New("child write visible in parent — shared disk?")
	}
	return nil
}

// smokePromote proves promote-to-template through the owner-bound handle:
// claiming from it must clone the parent's state — on a real node a cold
// boot of this never-registered image ref would fail, so success itself
// proves the golden path — and delete removes it. The name-based
// DeleteTemplate covers the node-local Client surface too.
func smokePromote(ctx context.Context, client *sandbox.Client, sb *sandbox.Sandbox) error {
	tpl, err := sb.Promote(ctx, "smoke-tpl:v1")
	if err != nil {
		return err
	}
	child, err := tpl.New(ctx)
	if err != nil {
		return fmt.Errorf("claim promoted template: %w", err)
	}
	defer func() { _ = child.Close() }()
	if got, err := child.ReadFile(ctx, "/work/fork-mark.txt"); err != nil || string(got) != "parent" {
		return fmt.Errorf("promoted state %q, %v — want the parent's disk", got, err)
	}
	if err := tpl.Delete(ctx); err != nil {
		return err
	}
	if err := client.DeleteTemplate(ctx, tpl.Name); err == nil {
		return errors.New("delete after delete succeeded, want unknown template")
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

// smokeCheckpoint proves the checkpoint tree: a branch carries the exact
// captured state, the source keeps running unaffected, two branches from one
// checkpoint are independent, and a deleted checkpoint stops branching.
func smokeCheckpoint(ctx context.Context, sb *sandbox.Sandbox) error {
	if _, err := sb.Exec(ctx, "sh", "-c", "echo v1 > /root/ck.txt"); err != nil {
		return err
	}
	ckpt, err := sb.Checkpoint(ctx, "step-1")
	if err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	if _, err = sb.Exec(ctx, "sh", "-c", "echo v2 > /root/ck.txt"); err != nil {
		return fmt.Errorf("mutate source after checkpoint: %w", err)
	}

	branch, err := ckpt.New(ctx)
	if err != nil {
		return fmt.Errorf("branch: %w", err)
	}
	defer func() { _ = branch.Close() }()
	out, err := branch.Exec(ctx, "cat", "/root/ck.txt")
	if err != nil {
		return fmt.Errorf("read branch: %w", err)
	}
	if err = want(out, "v1\n"); err != nil {
		return fmt.Errorf("branch state: %w", err)
	}
	out, err = sb.Exec(ctx, "cat", "/root/ck.txt")
	if err != nil {
		return fmt.Errorf("read source: %w", err)
	}
	if err = want(out, "v2\n"); err != nil {
		return fmt.Errorf("source state: %w", err)
	}

	if _, err = branch.Exec(ctx, "sh", "-c", "echo branched > /root/ck.txt"); err != nil {
		return err
	}
	second, err := ckpt.New(ctx)
	if err != nil {
		return fmt.Errorf("second branch: %w", err)
	}
	defer func() { _ = second.Close() }()
	out, err = second.Exec(ctx, "cat", "/root/ck.txt")
	if err != nil {
		return fmt.Errorf("read second branch: %w", err)
	}
	if err = want(out, "v1\n"); err != nil {
		return fmt.Errorf("second branch state: %w", err)
	}

	if err = ckpt.Delete(ctx); err != nil {
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	if _, err = ckpt.New(ctx); err == nil {
		return fmt.Errorf("branch from deleted checkpoint succeeded")
	}
	return nil
}

// smokeTree pushes a tar into the guest, proves a mid-stream failure leaves
// the tree untouched (fs_push atomicity), and pulls it back.
func smokeTree(ctx context.Context, sb *sandbox.Sandbox) error {
	archive := func(content string) []byte {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		_ = tw.WriteHeader(&tar.Header{Name: "t.txt", Mode: 0o644, Size: int64(len(content))})
		_, _ = tw.Write([]byte(content))
		_ = tw.Close()
		return buf.Bytes()
	}
	if err := sb.Push(ctx, "/root/tree", bytes.NewReader(archive("tree-marker"))); err != nil {
		return fmt.Errorf("push: %w", err)
	}
	if out, err := sb.Exec(ctx, "cat", "/root/tree/t.txt"); err != nil || strings.TrimSpace(out) != "tree-marker" {
		return fmt.Errorf("pushed file: %q, %v", out, err)
	}

	full := archive("CLOBBERED")
	half := io.MultiReader(bytes.NewReader(full[:len(full)/2]), iotest.ErrReader(errors.New("cut")))
	if err := sb.Push(ctx, "/root/tree", half); err == nil {
		return fmt.Errorf("truncated push reported success")
	}
	if out, err := sb.Exec(ctx, "cat", "/root/tree/t.txt"); err != nil || strings.TrimSpace(out) != "tree-marker" {
		return fmt.Errorf("tree changed by failed push: %q, %v", out, err)
	}
	// silkd unlinks the staging file when it notices the dead stream — an
	// asynchronous cleanup a fresh RPC can outrun, so settle with a budget.
	var staging string
	var stagingErr error
	for range 20 {
		staging, stagingErr = sb.Exec(ctx, "sh", "-c", "ls -a /root/tree")
		if stagingErr == nil && !strings.Contains(staging, ".silkd-push-") {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if stagingErr != nil || strings.Contains(staging, ".silkd-push-") {
		return fmt.Errorf("staging left behind: %q, %v", staging, stagingErr)
	}

	var pulled bytes.Buffer
	if err := sb.Pull(ctx, "/root/tree", &pulled); err != nil {
		return fmt.Errorf("pull: %w", err)
	}
	if !bytes.Contains(pulled.Bytes(), []byte("tree-marker")) {
		return fmt.Errorf("pulled tar missing pushed content")
	}
	return nil
}

// smokePortForward proves the guest-port relay on the no-network lane: sshd
// (socket-activated in the base image) answers on 22, so its banner must
// arrive through DialPort; a dead port must fail with the typed not_found.
func smokePortForward(ctx context.Context, sb *sandbox.Sandbox) error {
	// The step proves the relay, not sshd's readiness SLA: the guest's
	// socket-activated sshd can transiently refuse, so the positive probe
	// retries briefly; the dead-port negative below stays strict.
	pc, err := sb.DialPort(ctx, 22)
	for attempt := 0; err != nil && attempt < 100; attempt++ {
		time.Sleep(100 * time.Millisecond)
		pc, err = sb.DialPort(ctx, 22)
	}
	if err != nil {
		return err
	}
	defer func() { _ = pc.Close() }()
	banner := make([]byte, 4)
	if _, err := io.ReadFull(pc, banner); err != nil {
		return fmt.Errorf("read ssh banner: %w", err)
	}
	if string(banner) != "SSH-" {
		return fmt.Errorf("banner %q, want an SSH greeting", banner)
	}
	if _, err := sb.DialPort(ctx, 9); !isSilkdKind(err, wire.KindNotFound) {
		return fmt.Errorf("dead port: %v, want not_found", err)
	}
	return nil
}

func wantHibernated(ctx context.Context, client *sandbox.Client, n int) error {
	info, err := client.Info(ctx)
	if err != nil {
		return err
	}
	if info.Hibernated != n {
		return fmt.Errorf("hibernated count %d, want %d", info.Hibernated, n)
	}
	return nil
}

func want(got, exp string) error {
	if got != exp {
		return fmt.Errorf("got %q, want %q", got, exp)
	}
	return nil
}

// smokeLsp proves the LSP broker on hardware: the base sandbox answers a
// typed not_found (no manifests), and a python-flavor sandbox serves a real
// pylsp session — initialize, didOpen, hover — over the relay.
func smokeLsp(ctx context.Context, client *sandbox.Client, base *sandbox.Sandbox, template string) error {
	if _, err := base.StartLsp(ctx, "python", ""); !isSilkdKind(err, wire.KindNotFound) {
		return fmt.Errorf("base StartLsp: %v, want typed not_found", err)
	}

	py, err := client.New(ctx, template, sandbox.WithNetwork(sandbox.NetNone))
	if err != nil {
		return fmt.Errorf("claim %s: %w", template, err)
	}
	defer func() { _ = py.Close() }()
	if err = py.Mkdir(ctx, "/work/lsp", true); err != nil {
		return err
	}
	if err = py.WriteFile(ctx, "/work/lsp/hello.py", []byte("def add(a, b):\n    return a + b\n"), nil); err != nil {
		return err
	}
	lsp, err := py.StartLsp(ctx, "python", "/work/lsp")
	if err != nil {
		return fmt.Errorf("start lsp: %w", err)
	}
	stream, err := lsp.Request(ctx)
	if err != nil {
		return fmt.Errorf("open lsp stream: %w", err)
	}
	defer func() { _ = stream.Close() }()

	r := bufio.NewReader(stream)
	init := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"rootUri":"file:///work/lsp","capabilities":{}}}`
	if err = lspWrite(stream, init); err != nil {
		return err
	}
	if _, err = lspReadResponse(r, 1); err != nil {
		return fmt.Errorf("initialize: %w", err)
	}
	notify := `{"jsonrpc":"2.0","method":"initialized","params":{}}`
	didOpen := `{"jsonrpc":"2.0","method":"textDocument/didOpen","params":{"textDocument":{` +
		`"uri":"file:///work/lsp/hello.py","languageId":"python","version":1,` +
		`"text":"def add(a, b):\n    return a + b\n"}}}`
	hover := `{"jsonrpc":"2.0","id":2,"method":"textDocument/hover","params":{` +
		`"textDocument":{"uri":"file:///work/lsp/hello.py"},"position":{"line":0,"character":4}}}`
	for _, body := range []string{notify, didOpen, hover} {
		if err = lspWrite(stream, body); err != nil {
			return err
		}
	}
	result, err := lspReadResponse(r, 2)
	if err != nil {
		return fmt.Errorf("hover: %w", err)
	}
	if string(result) == "null" || !bytes.Contains(result, []byte("add")) {
		return fmt.Errorf("hover result %s does not describe add", result)
	}
	return nil
}

func isSilkdKind(err error, kind string) bool {
	var er *wire.ErrorResp
	return errors.As(err, &er) && er.Kind == kind
}

// lspWrite frames one JSON-RPC message the LSP way (Content-Length header).
func lspWrite(w io.Writer, body string) error {
	_, err := fmt.Fprintf(w, "Content-Length: %d\r\n\r\n%s", len(body), body)
	return err
}

// lspReadResponse reads framed server messages until the response carrying
// id arrives (server-initiated requests also carry ids but have a method;
// notifications have no id — both are skipped), returning its result.
func lspReadResponse(r *bufio.Reader, id int) (json.RawMessage, error) {
	for {
		length := 0
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return nil, err
			}
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				break
			}
			if v, ok := strings.CutPrefix(line, "Content-Length: "); ok {
				if length, err = strconv.Atoi(v); err != nil {
					return nil, fmt.Errorf("content-length %q: %w", v, err)
				}
			}
		}
		if length <= 0 {
			return nil, fmt.Errorf("lsp frame without content-length")
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
		var msg struct {
			ID     *int            `json:"id"`
			Method string          `json:"method"`
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(body, &msg); err != nil {
			return nil, fmt.Errorf("lsp frame %q: %w", body, err)
		}
		if msg.Method != "" || msg.ID == nil || *msg.ID != id {
			continue
		}
		if msg.Error != nil {
			return nil, fmt.Errorf("lsp error: %s", msg.Error)
		}
		return msg.Result, nil
	}
}

// smokeProcs drives the detached-process surface: spawn, ps, logs replay,
// attach-until-exit, and kill.
func smokeProcs(ctx context.Context, sb *sandbox.Sandbox) error {
	pid, err := sb.Spawn(ctx, sandbox.Cmd{Argv: []string{"sh", "-c", "echo bg-mark; sleep 0.3; echo late"}})
	if err != nil {
		return fmt.Errorf("spawn: %w", err)
	}
	procs, err := sb.Ps(ctx)
	if err != nil {
		return fmt.Errorf("ps: %w", err)
	}
	if !slices.ContainsFunc(procs, func(p wire.ProcInfo) bool { return p.PID == pid && p.Detached }) {
		return fmt.Errorf("ps does not list spawned pid %d: %+v", pid, procs)
	}
	var out bytes.Buffer
	code, exited, err := sb.Attach(ctx, pid, &out, nil)
	if err != nil || !exited || code != 0 {
		return fmt.Errorf("attach: code=%d exited=%v, %v", code, exited, err)
	}
	if !strings.Contains(out.String(), "bg-mark") || !strings.Contains(out.String(), "late") {
		return fmt.Errorf("attach output %q missing marks", out.String())
	}
	out.Reset()
	if code, exited, err = sb.Logs(ctx, pid, &out, nil); err != nil || !exited || code != 0 {
		return fmt.Errorf("logs after exit: code=%d exited=%v, %v", code, exited, err)
	}
	if !strings.Contains(out.String(), "bg-mark") {
		return fmt.Errorf("logs replay %q missing mark", out.String())
	}

	// A long-runner dies to kill; its pid must be gone from the next attach.
	pid, err = sb.Spawn(ctx, sandbox.Cmd{Argv: []string{"sleep", "30"}})
	if err != nil {
		return fmt.Errorf("spawn sleeper: %w", err)
	}
	if err = sb.Kill(ctx, pid, 0); err != nil {
		return fmt.Errorf("kill: %w", err)
	}
	if code, exited, err = sb.Attach(ctx, pid, nil, nil); err != nil || !exited || code == 0 {
		return fmt.Errorf("attach killed: code=%d exited=%v, %v (want non-zero exit)", code, exited, err)
	}
	return nil
}
