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
	nextVolume := flag.String("next-volume", "", "second catalog dataset for the same-path continuity proof")
	changeHook := flag.String("change-hook", "", "absolute Python hook path to interrupt a change; SANDBOX_ID is provided")
	cancelHook := flag.String("cancel-hook", "", "absolute Python hook path to gate cancellation; SANDBOX_ID and VOLUME_HOOK_STAGE are provided")
	releaseHook := flag.String("release-hook", "", "absolute Python hook path to test failed removal and recovery; uses VOLUME_HOOK_STAGE")
	listener := flag.String("listener", "", "path to the existing guestserver binary for activity checks during faults")
	probe := flag.String("probe", "volume-e2e.txt", "non-empty file inside the volume")
	flag.Parse()

	if err := run(*addr, *token, *template, *volume, *rwVolume, *probe, liveOptions{nextVolume: *nextVolume, changeHook: *changeHook, cancelHook: *cancelHook, releaseHook: *releaseHook, listener: *listener}); err != nil {
		fmt.Fprintln(os.Stderr, "volumesmoke:", err)
		os.Exit(1)
	}
}

func run(addr, token, template, volume, rwVolume, probe string, live liveOptions) error {
	if volume == "" {
		return errors.New("volume is required")
	}
	if (live.changeHook != "" || live.cancelHook != "") && live.listener == "" {
		return errors.New("listener is required with change-hook or cancel-hook")
	}
	if probe == "" || probe == "." || path.IsAbs(probe) || path.Clean(probe) != probe || probe == ".." || strings.HasPrefix(probe, "../") {
		return fmt.Errorf("probe %q must be a clean relative file path", probe)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
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
	if err == nil {
		return errors.New("write to read-only volume succeeded")
	}
	exitErr, ok := errors.AsType[*sandbox.ExitError](err)
	if !ok || !strings.Contains(strings.ToLower(exitErr.Stderr), "read-only file system") {
		return fmt.Errorf("write failed without EROFS: %w", err)
	}

	if err := errors.Join(sbA.Close(), sbB.Close()); err != nil {
		return fmt.Errorf("release volume claims: %w", err)
	}
	released = true
	fmt.Printf("VOLUME PASS concurrent_read_bytes=%d read_only=true released=true\n", len(outputs[0]))
	if err := runLive(ctx, client, template, volume, probe, outputs[0], live); err != nil {
		return err
	}
	if live.releaseHook != "" {
		if err := runRetirement(ctx, client, template, volume, live.releaseHook); err != nil {
			return err
		}
	}
	if rwVolume == "" {
		return nil
	}
	return runWritable(ctx, client, template, rwVolume)
}

// runLive starts without volumes, adds a dataset, then removes it without
// replacing the claim. Run this on the target VMM before deploying live mounts.
func runLive(ctx context.Context, client *sandbox.Client, template, volume, probe, want string, live liveOptions) error {
	const mount = "/datasets/live"
	sb, err := client.New(ctx, template, sandbox.WithNetwork(sandbox.NetNone))
	if err != nil {
		return err
	}
	defer func() { _ = sb.Close() }()
	for range 2 {
		if _, err = sb.SetVolumes(ctx, []sandbox.Volume{{Name: volume, Mount: mount}}); err != nil {
			return fmt.Errorf("live set volumes: %w", err)
		}
	}
	view, err := sb.GetVolumes(ctx)
	if err != nil {
		return fmt.Errorf("live get volumes: %w", err)
	}
	wantVolumes := []sandbox.Volume{{Name: volume, Mount: mount}}
	if !slices.Equal(view.VolumesWant, wantVolumes) || !slices.Equal(view.Volumes, wantVolumes) || len(view.States) != 1 || view.States[0].State != "attached" {
		return fmt.Errorf("live volume view not converged: %+v", view)
	}
	got, err := sb.Exec(ctx, "cat", path.Join(mount, probe))
	if err != nil {
		return fmt.Errorf("live volume read: %w", err)
	}
	if got != want {
		return fmt.Errorf("live volume read: bytes=%d want=%d", len(got), len(want))
	}
	if _, err = sb.Exec(ctx, "touch", path.Join(mount, ".sandboxd-write-probe")); err == nil {
		return errors.New("live read-only mount accepted a write")
	}
	if exitErr, ok := errors.AsType[*sandbox.ExitError](err); !ok || !strings.Contains(strings.ToLower(exitErr.Stderr), "read-only file system") {
		return fmt.Errorf("live write probe failed without EROFS: %w", err)
	}
	for range 2 {
		if _, err = sb.PatchVolumes(ctx, sandbox.VolumePatch{Remove: []string{volume}}); err != nil {
			return fmt.Errorf("live patch remove: %w", err)
		}
	}
	if _, err = sb.Exec(ctx, "test", "!", "-e", path.Join(mount, probe)); err != nil {
		return fmt.Errorf("detached volume probe should be absent: %w", err)
	}
	for range 2 {
		if _, err = sb.PatchVolumes(ctx, sandbox.VolumePatch{Add: wantVolumes}); err != nil {
			return fmt.Errorf("live patch add: %w", err)
		}
	}
	if got, err = sb.Exec(ctx, "cat", path.Join(mount, probe)); err != nil || got != want {
		return fmt.Errorf("live patch add read: bytes=%d want=%d error=%v", len(got), len(want), err)
	}
	for range 2 {
		if _, err = sb.SetVolumes(ctx, []sandbox.Volume{}); err != nil {
			return fmt.Errorf("live set empty volumes: %w", err)
		}
	}
	view, err = sb.GetVolumes(ctx)
	if err != nil {
		return fmt.Errorf("live get empty volumes: %w", err)
	}
	if len(view.VolumesWant) != 0 || len(view.Volumes) != 0 || len(view.States) != 0 {
		return fmt.Errorf("live clear did not converge: %+v", view)
	}
	if _, err = sb.Exec(ctx, "test", "!", "-e", path.Join(mount, probe)); err != nil {
		return fmt.Errorf("cleared volume probe should be absent: %w", err)
	}
	if live.nextVolume != "" {
		if err := runContinuity(ctx, sb, volume, probe, want, live); err != nil {
			return err
		}
	}
	fmt.Printf("LIVE VOLUME PASS read_bytes=%d repeated_put=true repeated_patch=true cleared=true\n", len(got))
	return nil
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
	// a live writer owns the image: the second claim is refused before attach.
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
