// crossnode is the two-node store acceptance: a checkpoint created on node
// A is claimed on node B through the shared checkpoint store; run it with a
// template absent from B's image store so the claim exercises the engine's
// clone --pull path (base blobs resolved by digest). The marker written on
// A proves the memory state crossed nodes.
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
	addrA := flag.String("a", "127.0.0.1:7871", "node A address")
	addrB := flag.String("b", "", "node B address")
	token := flag.String("token", "", "api token (shared by both nodes)")
	template := flag.String("template", "ghcr.io/cocoonstack/sandbox/python:3.12", "template present only on node A")
	flag.Parse()

	if err := run(*addrA, *addrB, *token, *template); err != nil {
		fmt.Fprintln(os.Stderr, "crossnode:", err)
		os.Exit(1)
	}
	fmt.Println("CROSSNODE PASS")
}

func run(addrA, addrB, token, template string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	ca, err := sandbox.Connect(addrA, sandbox.WithAPIToken(token))
	if err != nil {
		return fmt.Errorf("connect A: %w", err)
	}
	t0 := time.Now()
	sbA, err := ca.New(ctx, template, sandbox.WithNetwork(sandbox.NetNone))
	if err != nil {
		return fmt.Errorf("claim on A: %w", err)
	}
	defer func() { _ = sbA.Close() }()
	fmt.Printf("  A: claim %s in %.1fs\n", template, time.Since(t0).Seconds())

	if _, err = sbA.Exec(ctx, "sh", "-c", "echo crossnode-marker > /root/marker"); err != nil {
		return fmt.Errorf("write marker on A: %w", err)
	}
	t0 = time.Now()
	ck, err := sbA.Checkpoint(ctx, "crossnode")
	if err != nil {
		return fmt.Errorf("checkpoint on A: %w", err)
	}
	fmt.Printf("  A: checkpoint %s published to the store in %.1fs\n", ck.ID, time.Since(t0).Seconds())

	cb, err := sandbox.Connect(addrB, sandbox.WithAPIToken(token))
	if err != nil {
		return fmt.Errorf("connect B: %w", err)
	}
	cks, err := cb.Checkpoints(ctx)
	if err != nil {
		return fmt.Errorf("list checkpoints on B: %w", err)
	}
	var ckB *sandbox.Checkpoint
	for _, c := range cks {
		if c.ID == ck.ID {
			ckB = c
			break
		}
	}
	if ckB == nil {
		return fmt.Errorf("checkpoint %s not visible on B (store not shared?)", ck.ID)
	}
	fmt.Printf("  B: sees %s via the shared store\n", ckB.ID)

	t0 = time.Now()
	sbB, err := ckB.New(ctx)
	if err != nil {
		return fmt.Errorf("claim checkpoint on B (clone --pull path): %w", err)
	}
	defer func() { _ = sbB.Close() }()
	fmt.Printf("  B: claim from checkpoint in %.1fs (base image pulled by digest)\n", time.Since(t0).Seconds())

	out, err := sbB.Exec(ctx, "cat", "/root/marker")
	if err != nil {
		return fmt.Errorf("read marker on B: %w", err)
	}
	if got := strings.TrimSpace(out); got != "crossnode-marker" {
		return fmt.Errorf("marker %q, want crossnode-marker", got)
	}
	fmt.Println("  B: marker intact — memory state crossed nodes")

	if err := ckB.Delete(ctx); err != nil {
		return fmt.Errorf("delete checkpoint via B: %w", err)
	}
	fmt.Println("  B: checkpoint deleted from the store")
	return nil
}
