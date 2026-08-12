// volumesmoke validates shared read-only dataset mounts, and optionally one
// writable dataset mount, on a live Cloud Hypervisor node.
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

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template ref")
	volume := flag.String("volume", "", "catalog volume name")
	rwVolume := flag.String("rw-volume", "", "writable catalog volume name (adds the writable leg)")
	probe := flag.String("probe", "volume-e2e.txt", "non-empty file inside the volume")
	flag.Parse()

	if err := run(*addr, *token, *template, *volume, *rwVolume, *probe); err != nil {
		fmt.Fprintln(os.Stderr, "volumesmoke:", err)
		os.Exit(1)
	}
}

func run(addr, token, template, volume, rwVolume, probe string) error {
	if volume == "" {
		return errors.New("volume is required")
	}
	if probe == "" || probe == "." || path.IsAbs(probe) || path.Clean(probe) != probe || probe == ".." || strings.HasPrefix(probe, "../") {
		return fmt.Errorf("probe %q must be a clean relative file path", probe)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const (
		mountA = "/datasets/e2e-a"
		mountB = "/datasets/e2e-b"
	)
	start := time.Now()
	client, sbA, err := harness.Claim(ctx, addr, token, template,
		sandbox.WithNetwork(sandbox.NetNone),
		sandbox.WithVolumes(sandbox.Volume{Name: volume, Mount: mountA}),
	)
	claimA := time.Since(start)
	if err != nil {
		return err
	}
	start = time.Now()
	sbB, err := client.New(ctx, template,
		sandbox.WithNetwork(sandbox.NetNone),
		sandbox.WithVolumes(sandbox.Volume{Name: volume, Mount: mountB}),
	)
	claimB := time.Since(start)
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
	if rwVolume == "" {
		return nil
	}
	return runWritable(ctx, client, template, rwVolume)
}

// runWritable proves the publish workflow: a writer's bytes survive its own
// release, it excludes concurrent claims, and the next reader sees them.
func runWritable(ctx context.Context, client *sandbox.Client, template, volume string) error {
	const (
		mount = "/datasets/e2e-rw"
		file  = "volume-rw-probe.txt"
	)
	stamp := fmt.Sprintf("rw-%d", time.Now().UnixNano())
	start := time.Now()
	writer, err := client.New(ctx, template, sandbox.WithNetwork(sandbox.NetNone),
		sandbox.WithVolumes(sandbox.Volume{Name: volume, Mount: mount, Mode: "rw"}))
	if err != nil {
		return fmt.Errorf("writable claim: %w", err)
	}
	claimRW := time.Since(start)
	live := writer
	defer func() {
		if live != nil {
			_ = live.Close()
		}
	}()
	if want := []sandbox.Volume{{Name: volume, Mount: mount, Mode: "rw"}}; !slices.Equal(writer.Volumes, want) {
		return fmt.Errorf("writable claim volumes %+v, want %+v", writer.Volumes, want)
	}
	if _, err = writer.Exec(ctx, "sh", "-c", fmt.Sprintf("printf %%s %s > %s", stamp, path.Join(mount, file))); err != nil {
		return fmt.Errorf("write through the writable mount: %w", err)
	}
	// A live writer owns the image: the second claim is refused before attach.
	busy, busyErr := client.New(ctx, template, sandbox.WithNetwork(sandbox.NetNone),
		sandbox.WithVolumes(sandbox.Volume{Name: volume, Mount: mount, Mode: "rw"}))
	if busyErr == nil {
		_ = busy.Close()
		return errors.New("second writable claim succeeded while the first held the image")
	}
	if !strings.Contains(busyErr.Error(), "409") {
		return fmt.Errorf("second writable claim failed without 409: %w", busyErr)
	}
	if err = writer.Close(); err != nil {
		return fmt.Errorf("release writable claim: %w", err)
	}
	live = nil

	reader, err := client.New(ctx, template, sandbox.WithNetwork(sandbox.NetNone),
		sandbox.WithVolumes(sandbox.Volume{Name: volume, Mount: mount}))
	if err != nil {
		return fmt.Errorf("read-only claim after the writer released: %w", err)
	}
	live = reader
	out, err := reader.Exec(ctx, "cat", path.Join(mount, file))
	if err != nil {
		return fmt.Errorf("read back the written file: %w", err)
	}
	if strings.TrimSpace(out) != stamp {
		return fmt.Errorf("read back %q, want %q", strings.TrimSpace(out), stamp)
	}
	if err = reader.Close(); err != nil {
		return fmt.Errorf("release read-only claim: %w", err)
	}
	live = nil
	fmt.Printf("VOLUME RW PASS volume=%s claim=%.1fms durable=true busy_refused=true\n",
		volume, float64(claimRW.Microseconds())/1000)
	return nil
}
