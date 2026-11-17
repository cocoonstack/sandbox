// pullbench times n sequential fs_pull transfers of size MiB from the guest; -push times fs_write uploads instead.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template ref")
	size := flag.Int("size", 256, "transfer size, MiB")
	n := flag.Int("n", 5, "transfers to time")
	push := flag.Bool("push", false, "time fs_write uploads to the guest")
	flag.Parse()
	if err := run(*addr, *token, *template, *size, *n, *push); err != nil {
		fmt.Fprintln(os.Stderr, "pullbench:", err)
		os.Exit(1)
	}
}

func run(addr, token, template string, size, n int, push bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	_, sb, err := harness.Claim(ctx, addr, token, template, sandbox.WithNetwork(sandbox.NetNone))
	if err != nil {
		return err
	}
	defer func() { _ = sb.Close() }()

	prepare := fmt.Sprintf("mkdir -p /work/bench && head -c %dM /dev/urandom > /work/bench/blob", size)
	verb, transfer := "pull", func() error { return sb.Pull(ctx, "/work/bench", io.Discard) }
	if push {
		data := make([]byte, size<<20)
		for i := range data {
			data[i] = byte(i)
		}
		prepare = "mkdir -p /work/bench"
		verb, transfer = "push", func() error { return sb.WriteFile(ctx, "/work/bench/blob", data, nil) }
	}
	if _, err := sb.Exec(ctx, "sh", "-c", prepare); err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	for i := range n {
		start := time.Now()
		if err := transfer(); err != nil {
			return fmt.Errorf("%s: %w", verb, err)
		}
		d := time.Since(start)
		fmt.Printf("%s %d: %.2fs  %.1f MiB/s\n", verb, i, d.Seconds(), float64(size)/d.Seconds())
	}
	return nil
}
