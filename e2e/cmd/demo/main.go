// demo exercises a live sandboxd through the SDK and prints per-iteration
// latencies — the bare-metal e2e script drives it on a real node.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk"
)

func main() {
	var (
		addr     = flag.String("addr", "127.0.0.1:7777", "sandboxd address")
		token    = flag.String("token", "", "node api token")
		template = flag.String("template", "rt:24.04", "template ref")
		netShape = flag.String("net", "none", "network lane: none|egress")
		n        = flag.Int("n", 3, "iterations")
		ttl      = flag.Int("ttl", 0, "sandbox ttl seconds (0 = server default)")
		leak     = flag.Bool("leak", false, "claim without releasing (reap check)")
	)
	flag.Parse()

	if err := run(*addr, *token, *template, *netShape, *n, *ttl, *leak); err != nil {
		fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(1)
	}
}

func run(addr, token, template, netShape string, n, ttl int, leak bool) error {
	ctx := context.Background()
	var copts []sandbox.ClientOption
	if token != "" {
		copts = append(copts, sandbox.WithAPIToken(token))
	}
	client, err := sandbox.Connect(addr, copts...)
	if err != nil {
		return err
	}
	opts := []sandbox.Option{sandbox.WithNetwork(sandbox.NetShape(netShape))}
	if ttl > 0 {
		opts = append(opts, sandbox.WithTimeout(time.Duration(ttl)*time.Second))
	}

	for i := range n {
		start := time.Now()
		sb, err := client.New(ctx, template, opts...)
		if err != nil {
			return fmt.Errorf("claim %d: %w", i, err)
		}
		claimed := time.Now()
		out, err := sb.Exec(ctx, "echo", "42")
		if err != nil {
			return fmt.Errorf("exec in %s: %w", sb.ID, err)
		}
		if out != "42\n" {
			return fmt.Errorf("exec in %s: got %q, want 42", sb.ID, out)
		}
		execDone := time.Now()
		fmt.Printf("iter=%d id=%s claim=%.1fms exec=%.1fms\n",
			i, sb.ID, claimed.Sub(start).Seconds()*1000, execDone.Sub(claimed).Seconds()*1000)
		if leak {
			continue
		}
		if err := sb.Close(); err != nil {
			return fmt.Errorf("release %s: %w", sb.ID, err)
		}
	}
	return nil
}
