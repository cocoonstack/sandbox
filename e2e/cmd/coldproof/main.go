// coldproof times a cold claim and prints the guest uptime and kernel as proof it booted.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func main() {
	var (
		addr     = flag.String("addr", "127.0.0.1:7777", "sandboxd address")
		token    = flag.String("token", "", "node api token")
		template = flag.String("template", "rt:24.04", "template ref; only an unpooled one measures a cold boot")
		n        = flag.Int("n", 1, "iterations")
	)
	flag.Parse()
	if err := run(*addr, *token, *template, *n); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(addr, token, template string, n int) error {
	ctx := context.Background()
	c, err := sandbox.Connect(addr, sandbox.WithAPIToken(token))
	if err != nil {
		return err
	}
	for i := range n {
		start := time.Now()
		sb, err := c.New(ctx, template)
		if err != nil {
			return fmt.Errorf("claim: %w", err)
		}
		claim := time.Since(start)
		uptime, err := sb.Exec(ctx, "cat", "/proc/uptime")
		if err != nil {
			_ = sb.Close()
			return fmt.Errorf("exec uptime: %w", err)
		}
		exec1 := time.Since(start)
		kernel, err := sb.Exec(ctx, "uname", "-r")
		if err != nil {
			_ = sb.Close()
			return fmt.Errorf("exec uname: %w", err)
		}
		fmt.Printf("iter=%d claim=%.1fms first_exec_done=%.1fms guest_uptime=%ss guest_kernel=%s\n",
			i, float64(claim.Microseconds())/1000, float64(exec1.Microseconds())/1000,
			strings.Fields(uptime)[0], strings.TrimSpace(kernel))
		if err := sb.Close(); err != nil {
			return fmt.Errorf("release: %w", err)
		}
	}
	return nil
}
