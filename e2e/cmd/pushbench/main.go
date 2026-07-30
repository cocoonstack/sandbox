// pushbench measures fs_write throughput end to end — the hardware A/B for
// the SDK's data-frame encode path: writes size MiB to the guest n times.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template ref")
	size := flag.Int("size", 128, "upload size, MiB")
	n := flag.Int("n", 5, "writes to time")
	flag.Parse()
	if err := run(*addr, *token, *template, *size, *n); err != nil {
		fmt.Fprintln(os.Stderr, "pushbench:", err)
		os.Exit(1)
	}
}

func run(addr, token, template string, size, n int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	_, sb, err := harness.Claim(ctx, addr, token, template, sandbox.WithNetwork(sandbox.NetNone))
	if err != nil {
		return err
	}
	defer func() { _ = sb.Close() }()

	if _, err := sb.Exec(ctx, "mkdir", "-p", "/work/bench"); err != nil {
		return fmt.Errorf("prepare dir: %w", err)
	}
	data := make([]byte, size<<20)
	for i := range data {
		data[i] = byte(i)
	}
	for i := range n {
		start := time.Now()
		if err := sb.WriteFile(ctx, "/work/bench/blob", data, nil); err != nil {
			return fmt.Errorf("write: %w", err)
		}
		d := time.Since(start)
		fmt.Printf("push %d: %.2fs  %.1f MiB/s\n", i, d.Seconds(), float64(size)/d.Seconds())
	}
	return nil
}
