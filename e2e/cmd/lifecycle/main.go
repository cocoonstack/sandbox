// lifecycle exercises sandboxd's auto-archive: a claim idles into hibernation,
// then to a store checkpoint with its local VM dropped, and the next access
// wakes it from the store with the same id and guest state intact. The node
// must run a pool with idle_hibernate_seconds + archive_after_seconds set.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const markerPath = "/root/archive-marker"

func main() {
	var (
		addr     = flag.String("addr", "127.0.0.1:7777", "sandboxd address")
		token    = flag.String("token", "", "node api token")
		template = flag.String("template", "rt:24.04", "template ref")
		netShape = flag.String("net", "none", "network lane: none|egress")
		wait     = flag.Duration("wait", 90*time.Second, "max wait for the archive sweep")
	)
	flag.Parse()
	if err := run(*addr, *token, *template, *netShape, *wait); err != nil {
		fmt.Fprintln(os.Stderr, "lifecycle:", err)
		os.Exit(1)
	}
}

func run(addr, token, template, netShape string, wait time.Duration) error {
	ctx := context.Background()
	var copts []sandbox.ClientOption
	if token != "" {
		copts = append(copts, sandbox.WithAPIToken(token))
	}
	client, err := sandbox.Connect(addr, copts...)
	if err != nil {
		return err
	}

	sb, err := client.New(ctx, template, sandbox.WithNetwork(sandbox.NetShape(netShape)))
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	id := sb.ID
	marker := fmt.Appendf(nil, "archive-marker-%d", time.Now().UnixNano())
	if err = sb.WriteFile(ctx, markerPath, marker, nil); err != nil {
		return fmt.Errorf("write marker: %w", err)
	}
	fmt.Printf("claimed id=%s, marker written\n", id)

	elapsed, err := waitArchived(ctx, client, 1, wait)
	if err != nil {
		return err
	}
	fmt.Printf("idle-hibernated then archived in %.1fs (local VM dropped)\n", elapsed.Seconds())

	// The first access on an archived id is the cold wake: fetch the
	// checkpoint, provision a fresh local VM, keep the id/token.
	wakeStart := time.Now()
	got, err := sb.ReadFile(ctx, markerPath)
	if err != nil {
		return fmt.Errorf("read marker after archive (wake): %w", err)
	}
	if !bytes.Equal(got, marker) {
		return fmt.Errorf("guest state lost across archive/wake: got %q, want %q", got, marker)
	}
	if sb.ID != id {
		return fmt.Errorf("id changed across archive/wake: %s -> %s", id, sb.ID)
	}
	fmt.Printf("woke id=%s in %.0fms, guest state intact\n", sb.ID, time.Since(wakeStart).Seconds()*1000)

	info, err := client.Info(ctx)
	if err != nil {
		return err
	}
	if info.Archived != 0 || info.Claimed != 1 {
		return fmt.Errorf("post-wake info archived=%d claimed=%d, want 0/1", info.Archived, info.Claimed)
	}
	if err = sb.Close(); err != nil {
		return fmt.Errorf("release: %w", err)
	}
	fmt.Println("PASS")
	return nil
}

// waitArchived polls the node until archived reaches n, returning how long the
// reaper's idle→hibernate→archive ladder took.
func waitArchived(ctx context.Context, client *sandbox.Client, n int, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	for {
		info, err := client.Info(ctx)
		if err != nil {
			return 0, err
		}
		if info.Archived == n {
			return time.Since(start), nil
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("archived never reached %d within %s (hibernated=%d archived=%d claimed=%d)",
				n, timeout, info.Hibernated, info.Archived, info.Claimed)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
