// ringsmoke drives two silkd paths the other smokes leave alone: a detached
// process that overflows its output ring one byte at a time, whose logs must
// replay exactly the newest 256 KiB, and exec as a named user.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const (
	ringBytes  = 256 * 1024
	totalBytes = 300000
	stderrTail = "err\n"
	writer     = "i=0; while [ $i -lt 300000 ]; do printf %d $((i%10)); i=$((i+1)); done; echo err 1>&2"
)

func main() {
	var (
		addr     = flag.String("addr", "127.0.0.1:7777", "sandboxd address")
		token    = flag.String("token", "", "node api token")
		template = flag.String("template", "rt:24.04", "template ref")
	)
	flag.Parse()
	if err := run(*addr, *token, *template); err != nil {
		fmt.Fprintln(os.Stderr, "ringsmoke:", err)
		os.Exit(1)
	}
}

func run(addr, token, template string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, sb, err := harness.Claim(ctx, addr, token, template)
	if err != nil {
		return err
	}
	defer func() { _ = sb.Close() }()
	if err = ringTail(ctx, sb); err != nil {
		return err
	}
	return execAsUser(ctx, sb)
}

func ringTail(ctx context.Context, sb *sandbox.Sandbox) error {
	pid, err := sb.Spawn(ctx, sandbox.Cmd{Argv: []string{"sh", "-c", writer}})
	if err != nil {
		return fmt.Errorf("spawn writer: %w", err)
	}
	var stdout, stderr bytes.Buffer
	for {
		stdout.Reset()
		stderr.Reset()
		code, exited, err := sb.Logs(ctx, pid, &stdout, &stderr)
		if err != nil {
			return fmt.Errorf("logs: %w", err)
		}
		if exited {
			if code != 0 {
				return fmt.Errorf("writer exited %d", code)
			}
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if stderr.String() != stderrTail {
		return fmt.Errorf("stderr replay %q, want %q", stderr.String(), stderrTail)
	}
	keep := ringBytes - len(stderrTail)
	if stdout.Len() != keep {
		return fmt.Errorf("stdout replay %d bytes, want exactly %d", stdout.Len(), keep)
	}
	for k, b := range stdout.Bytes() {
		if want := byte('0' + (totalBytes-keep+k)%10); b != want {
			return fmt.Errorf("stdout replay byte %d is %q, want %q", k, b, want)
		}
	}
	fmt.Printf("ring: %d one-byte writes replayed as the newest %d bytes, stderr boundary kept\n", totalBytes, keep)
	return nil
}

func execAsUser(ctx context.Context, sb *sandbox.Sandbox) error {
	var out bytes.Buffer
	code, err := sb.Run(ctx, sandbox.Cmd{Argv: []string{"sh", "-c", "id -u; echo $HOME"}, User: "nobody", Stdout: &out})
	if err != nil || code != 0 {
		return fmt.Errorf("exec as nobody: code %d err %v", code, err)
	}
	if got := out.String(); got != "65534\n/nonexistent\n" {
		return fmt.Errorf("exec as nobody printed %q", got)
	}
	_, err = sb.Run(ctx, sandbox.Cmd{Argv: []string{"true"}, User: "no-such-user-e2e"})
	if err == nil || !strings.Contains(err.Error(), "unknown user") {
		return fmt.Errorf("exec as an unknown user: err %v, want unknown user", err)
	}
	fmt.Println("user: nobody resolved to 65534 with HOME=/nonexistent; an unknown user is refused")
	return nil
}
