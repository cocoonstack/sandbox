// pullbench measures fs_pull throughput end to end — the hardware A/B for
// silkd's chunk-frame render path: writes size MiB of random bytes in the
// guest, then times n sequential Pulls.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template ref")
	size := flag.Int("size", 256, "guest file size, MiB")
	n := flag.Int("n", 5, "pulls to time")
	flag.Parse()
	if err := run(*addr, *token, *template, *size, *n); err != nil {
		fmt.Fprintln(os.Stderr, "pullbench:", err)
		os.Exit(1)
	}
}

func run(addr, token, template string, size, n int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	client, err := sandbox.Connect(addr, sandbox.WithAPIToken(token))
	if err != nil {
		return err
	}
	sb, err := client.New(ctx, template, sandbox.WithNetwork(sandbox.NetNone))
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	defer func() { _ = sb.Close() }()

	if _, err := sb.Exec(ctx, "sh", "-c",
		fmt.Sprintf("mkdir -p /work/bench && head -c %dM /dev/urandom > /work/bench/blob", size)); err != nil {
		return fmt.Errorf("prepare blob: %w", err)
	}
	for i := range n {
		start := time.Now()
		if err := sb.Pull(ctx, "/work/bench", io.Discard); err != nil {
			return fmt.Errorf("pull: %w", err)
		}
		d := time.Since(start)
		fmt.Printf("pull %d: %.2fs  %.1f MiB/s\n", i, d.Seconds(), float64(size)/d.Seconds())
	}
	return nil
}
