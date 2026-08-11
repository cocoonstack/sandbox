// volumesmoke validates shared read-only dataset mounts on a live Cloud Hypervisor node.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template ref")
	volume := flag.String("volume", "", "catalog volume name")
	probe := flag.String("probe", "volume-e2e.txt", "non-empty file inside the volume")
	flag.Parse()

	if err := run(*addr, *token, *template, *volume, *probe); err != nil {
		fmt.Fprintln(os.Stderr, "volumesmoke:", err)
		os.Exit(1)
	}
}

func run(addr, token, template, volume, probe string) error {
	if volume == "" {
		return errors.New("volume is required")
	}
	if probe == "" || probe == "." || path.IsAbs(probe) || path.Clean(probe) != probe || probe == ".." || strings.HasPrefix(probe, "../") {
		return fmt.Errorf("probe %q must be a clean relative file path", probe)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, connectErr := sandbox.Connect(addr, sandbox.WithAPIToken(token))
	if connectErr != nil {
		return fmt.Errorf("connect: %w", connectErr)
	}

	const (
		mountA = "/datasets/e2e-a"
		mountB = "/datasets/e2e-b"
	)
	claim := func(mount string) (*sandbox.Sandbox, time.Duration, error) {
		start := time.Now()
		sb, claimErr := client.New(ctx, template,
			sandbox.WithNetwork(sandbox.NetNone),
			sandbox.WithVolumes(sandbox.Volume{Name: volume, Mount: mount}),
		)
		return sb, time.Since(start), claimErr
	}
	sbA, claimA, err := claim(mountA)
	if err != nil {
		return fmt.Errorf("claim at %s: %w", mountA, err)
	}
	sbB, claimB, err := claim(mountB)
	if err != nil {
		_ = sbA.Close()
		return fmt.Errorf("claim at %s: %w", mountB, err)
	}
	released := false
	defer func() {
		if !released {
			_ = sbA.Close()
			_ = sbB.Close()
		}
	}()
	if want := []sandbox.Volume{{Name: volume, Mount: mountA}}; !slices.Equal(sbA.Volumes, want) {
		return fmt.Errorf("first claim volumes %+v, want %+v", sbA.Volumes, want)
	}
	if want := []sandbox.Volume{{Name: volume, Mount: mountB}}; !slices.Equal(sbB.Volumes, want) {
		return fmt.Errorf("second claim volumes %+v, want %+v", sbB.Volumes, want)
	}
	fmt.Printf("volume=%s claim_a=%.1fms claim_b=%.1fms\n", volume, float64(claimA.Microseconds())/1000, float64(claimB.Microseconds())/1000)

	sandboxes := []*sandbox.Sandbox{sbA, sbB}
	mounts := []string{mountA, mountB}
	var (
		wg      sync.WaitGroup
		outputs [2]string
		errs    [2]error
	)
	startReads := make(chan struct{})
	for i := range sandboxes {
		wg.Go(func() {
			<-startReads
			outputs[i], errs[i] = sandboxes[i].Exec(ctx, "cat", path.Join(mounts[i], probe))
		})
	}
	close(startReads)
	wg.Wait()
	if readErr := errors.Join(errs[:]...); readErr != nil {
		return fmt.Errorf("concurrent volume reads: %w", readErr)
	}
	if outputs[0] == "" || outputs[0] != outputs[1] {
		return fmt.Errorf("concurrent reads differ: first_bytes=%d second_bytes=%d", len(outputs[0]), len(outputs[1]))
	}

	_, err = sbA.Exec(ctx, "touch", path.Join(mountA, ".sandboxd-write-probe"))
	var exitErr *sandbox.ExitError
	if err == nil {
		return errors.New("write to read-only volume succeeded")
	}
	if !errors.As(err, &exitErr) || !strings.Contains(strings.ToLower(exitErr.Stderr), "read-only file system") {
		return fmt.Errorf("write failed without EROFS: %w", err)
	}

	if err := errors.Join(sbA.Close(), sbB.Close()); err != nil {
		return fmt.Errorf("release volume claims: %w", err)
	}
	released = true
	fmt.Printf("VOLUME PASS concurrent_read_bytes=%d read_only=true released=true\n", len(outputs[0]))
	return nil
}
