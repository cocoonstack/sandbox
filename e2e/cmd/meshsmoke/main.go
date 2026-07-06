// meshsmoke proves M3.1 cluster template routing on a real two-node mesh:
// a claim at the entry node redirects to the peer that pools the key, a
// promote there is reachable by name from the entry node, and a name-based
// delete follows gossip to the owner. Run against node A of a two-node
// cluster where only node B pools the template.
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
	addr := flag.String("addr", "127.0.0.1:7777", "entry node address (must NOT pool the template)")
	peer := flag.String("peer", "", "peer node address expected to own the redirected claim")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template ref pooled only on the peer")
	flag.Parse()

	if err := run(*addr, *peer, *token, *template); err != nil {
		fmt.Fprintln(os.Stderr, "meshsmoke:", err)
		os.Exit(1)
	}
	fmt.Println("MESHSMOKE PASS")
}

func run(addr, peer, token, template string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	var copts []sandbox.ClientOption
	if token != "" {
		copts = append(copts, sandbox.WithAPIToken(token))
	}
	client, err := sandbox.Connect(addr, copts...)
	if err != nil {
		return err
	}

	sb, err := client.New(ctx, template, sandbox.WithNetwork(sandbox.NetNone))
	if err != nil {
		return fmt.Errorf("claim via %s: %w", addr, err)
	}
	defer func() { _ = sb.Close() }()
	if peer != "" && sb.Owner() != peer {
		return fmt.Errorf("claim owner %s, want redirect to %s", sb.Owner(), peer)
	}
	fmt.Printf("  redirect: claim entered at %s, owned by %s\n", addr, sb.Owner())

	if _, err = sb.Exec(ctx, "sh", "-c", "echo mesh-marker > /root/marker"); err != nil {
		return fmt.Errorf("write marker: %w", err)
	}
	tpl, err := sb.Promote(ctx, "mesh-tpl")
	if err != nil {
		return fmt.Errorf("promote on owner: %w", err)
	}
	fmt.Printf("  promote: mesh-tpl published on %s\n", sb.Owner())

	// Gossip carries the new template to the entry node within about a tick;
	// poll rather than guess the delay (a premature claim cold-fails at A).
	var clone *sandbox.Sandbox
	deadline := time.Now().Add(15 * time.Second)
	for {
		clone, err = client.New(ctx, "mesh-tpl", sandbox.WithNetwork(sandbox.NetNone))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("name-based claim of mesh-tpl via %s: %w", addr, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	defer func() { _ = clone.Close() }()
	out, err := clone.Exec(ctx, "cat", "/root/marker")
	if err != nil {
		return fmt.Errorf("read marker in clone: %w", err)
	}
	if got := strings.TrimSpace(out); got != "mesh-marker" {
		return fmt.Errorf("marker %q, want mesh-marker (clone not from the promoted golden)", got)
	}
	if peer != "" && clone.Owner() != peer {
		return fmt.Errorf("clone owner %s, want %s (template owner)", clone.Owner(), peer)
	}
	fmt.Printf("  name-based claim: clone on %s carries the marker\n", clone.Owner())

	if err := client.DeleteTemplate(ctx, "mesh-tpl", sandbox.WithNetwork(sandbox.NetNone), sandbox.WithSize(sandbox.Small)); err != nil {
		return fmt.Errorf("name-based delete via %s: %w", addr, err)
	}
	if err := client.DeleteTemplate(ctx, "mesh-tpl", sandbox.WithNetwork(sandbox.NetNone), sandbox.WithSize(sandbox.Small)); err == nil {
		return fmt.Errorf("second delete succeeded, want not-found")
	}
	if err := tpl.Delete(ctx); err == nil {
		return fmt.Errorf("owner-handle delete after name-based delete succeeded, want not-found")
	}
	fmt.Println("  delete: followed gossip to the owner; gone everywhere after")
	return nil
}
