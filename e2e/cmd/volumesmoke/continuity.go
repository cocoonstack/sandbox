package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

type (
	liveOptions     struct{ nextVolume, changeHook, cancelHook, releaseHook, listener string }
	processIdentity struct {
		Sandbox, Session, Boot string
		PID                    uint32
		Start                  string
	}
)

func identity(ctx context.Context, sb *sandbox.Sandbox, session *sandbox.Session, pid uint32) (processIdentity, error) {
	boot, err := sb.Exec(ctx, "cat", "/proc/sys/kernel/random/boot_id")
	if err != nil {
		return processIdentity{}, err
	}
	stat, err := sb.Exec(ctx, "cat", fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return processIdentity{}, err
	}
	_, rest, ok := strings.Cut(stat, ") ")
	fields := strings.Fields(rest)
	if !ok || len(fields) < 20 {
		return processIdentity{}, fmt.Errorf("invalid process stat %q", stat)
	}
	return processIdentity{sb.ID, session.ID, strings.TrimSpace(boot), pid, fields[19]}, nil
}

func runContinuity(ctx context.Context, sb *sandbox.Sandbox, volume, probe, want string, opts liveOptions) error {
	const mount = "/datasets/current"
	if opts.changeHook != "" || opts.cancelHook != "" {
		if err := uploadListener(ctx, sb, opts.listener); err != nil {
			return err
		}
	}
	session, err := sb.NewSession(ctx, sandbox.WithSessionCwd("/tmp"), sandbox.WithSessionEnv(map[string]string{"DATASET_SESSION": "same"}))
	if err != nil {
		return err
	}
	defer func() { _ = session.Close(context.Background()) }()
	pid, err := sb.Spawn(ctx, sandbox.Cmd{Argv: []string{"sleep", "3600"}, Cwd: "/tmp"})
	if err != nil {
		return err
	}
	defer func() { _ = sb.Kill(context.Background(), pid, 0) }()
	before, err := identity(ctx, sb, session, pid)
	if err != nil {
		return err
	}
	encoded, _ := json.Marshal(before)
	fmt.Printf("CONTINUITY before=%s\n", encoded)
	v1 := []sandbox.Volume{{Name: volume, Mount: mount}}
	v2 := []sandbox.Volume{{Name: opts.nextVolume, Mount: mount}}
	v2content, err := exerciseMountConflicts(ctx, sb, v1, v2, probe, want)
	if err != nil {
		return err
	}
	if opts.changeHook != "" {
		if err = interruptedChange(ctx, sb, v1, opts.changeHook, false); err != nil {
			return err
		}
		if _, err = sb.SetVolumes(ctx, v2); err != nil {
			return err
		}
	}
	if opts.cancelHook != "" {
		if err = interruptedChange(ctx, sb, v1, opts.cancelHook, true); err != nil {
			return err
		}
	}
	view, err := sb.SetVolumes(ctx, v2)
	if err != nil || !slices.Equal(view.Volumes, v2) || !slices.Equal(view.VolumesWant, v2) {
		return fmt.Errorf("second dataset did not converge: view=%+v error=%v", view, err)
	}
	if content, readErr := sb.Exec(ctx, "cat", path.Join(mount, probe)); readErr != nil || content != v2content {
		return fmt.Errorf("dataset changed after recovery: content=%q error=%v", content, readErr)
	}
	if err = requireSettled(view); err != nil {
		return err
	}
	if err = sb.WriteFile(ctx, "/tmp/continuity", []byte("independent file"), nil); err != nil {
		return err
	}
	data, err := sb.ReadFile(ctx, "/tmp/continuity")
	if err != nil || string(data) != "independent file" {
		return fmt.Errorf("independent file round trip: %q %v", data, err)
	}
	if _, err = sb.SetVolumes(ctx, []sandbox.Volume{}); err != nil {
		return err
	}
	after, err := identity(ctx, sb, session, pid)
	if err != nil || after != before {
		return fmt.Errorf("process identity changed: before=%+v after=%+v error=%v", before, after, err)
	}
	sessions, err := sb.Sessions(ctx)
	if err != nil || !slices.Contains(sessions, session.ID) {
		return fmt.Errorf("session disappeared: %v %v", sessions, err)
	}
	state, err := session.Exec(ctx, "printenv", "DATASET_SESSION")
	if err != nil || strings.TrimSpace(state) != "same" {
		return fmt.Errorf("session state changed: %q %v", state, err)
	}
	encoded, _ = json.Marshal(after)
	fmt.Printf("CONTINUITY PASS after=%s v1=%q v2=%q cwd_409=true bind_409=true foreign_409=true retained_bind=true erofs=true cleared=true\n", encoded, strings.TrimSpace(want), strings.TrimSpace(v2content))
	return nil
}

func refuseBusy(ctx context.Context, sb *sandbox.Sandbox, target []sandbox.Volume, mount, reference string) error {
	view, err := sb.SetVolumes(ctx, target)
	api, ok := errors.AsType[*sandbox.APIError](err)
	if !ok || api.Status != 409 || !strings.Contains(err.Error(), mount) {
		return fmt.Errorf("%s busy removal must return 409 and guest path: view=%+v error=%v", reference, view, err)
	}
	encoded, _ := json.Marshal(view)
	fmt.Printf("BUSY PASS reference=%s view=%s\n", reference, encoded)
	return nil
}

func interruptedChange(ctx context.Context, sb *sandbox.Sandbox, target []sandbox.Volume, hook string, cancelRequest bool) error {
	mode := "restart"
	if cancelRequest {
		mode = "cancel"
	}
	if err := runHook(ctx, sb.ID, hook, mode, "arm"); err != nil {
		return err
	}
	server, err := sb.Spawn(ctx, sandbox.Cmd{Argv: []string{"/tmp/volume-guestserver", "-addr", "127.0.0.1:18080"}, Cwd: "/tmp"})
	if err != nil {
		return err
	}
	defer func() { _ = sb.Kill(context.Background(), server, 0) }()
	for deadline := time.Now().Add(5 * time.Second); ; {
		if _, err := harness.HTTPOverPort(ctx, sb, 18080, "GET", "/", nil); err == nil {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("independent HTTP server did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := sb.SetVolumes(requestCtx, target); done <- err }()
	if err := runHook(ctx, sb.ID, hook, mode, "wait"); err != nil {
		return err
	}
	if _, err := sb.Exec(ctx, "true"); err != nil {
		return err
	}
	if err := sb.WriteFile(ctx, "/tmp/during-change", []byte("still running"), nil); err != nil {
		return err
	}
	if content, readErr := sb.ReadFile(ctx, "/tmp/during-change"); readErr != nil || string(content) != "still running" {
		return fmt.Errorf("file read during change: %q %v", content, readErr)
	}
	if _, err := harness.HTTPOverPort(ctx, sb, 18080, "GET", "/during-change", nil); err != nil {
		return err
	}
	if cancelRequest {
		cancel()
	}
	if err := runHook(ctx, sb.ID, hook, mode, "interrupt"); err != nil {
		return err
	}
	fmt.Println("ACTIVITY PASS exec=true file=true port=true")
	requestErr := <-done
	fmt.Printf("INTERRUPTION request_error=%v\n", requestErr)
	if requestErr == nil {
		return errors.New("change hook did not interrupt the request")
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		view, err := sb.GetVolumes(ctx)
		if err == nil {
			encoded, _ := json.Marshal(view)
			fmt.Printf("INTERRUPTION recovered_view=%s\n", encoded)
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("daemon did not recover: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := sb.SetVolumes(ctx, target); err != nil {
		return fmt.Errorf("retry after interruption: %w", err)
	}
	fmt.Printf("INTERRUPTION PASS sandbox=%s\n", sb.ID)
	return nil
}

func runHook(ctx context.Context, id, hook, mode, stage string) error {
	if !filepath.IsAbs(hook) || filepath.Clean(hook) != hook {
		return errors.New("fault hook must be a clean absolute Python script path")
	}
	command := exec.CommandContext(ctx, "python3")
	command.Args = append(command.Args, hook, mode)
	command.Env = append(os.Environ(), "SANDBOX_ID="+id, "VOLUME_HOOK_STAGE="+stage)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("change hook %s: %w", stage, err)
	}
	return nil
}

func requireSettled(view sandbox.SandboxVolumesResponse) error {
	encoded, err := json.Marshal(view)
	if err != nil {
		return err
	}
	var progress struct {
		Converging *bool `json:"converging"`
	}
	if err := json.Unmarshal(encoded, &progress); err != nil {
		return err
	}
	if progress.Converging == nil || *progress.Converging {
		return fmt.Errorf("settled view must explicitly report converging=false: %s", encoded)
	}
	return nil
}

func exerciseMountConflicts(ctx context.Context, sb *sandbox.Sandbox, v1, v2 []sandbox.Volume, probe, want string) (string, error) {
	const mount = "/datasets/current"
	var err error
	if _, err = sb.SetVolumes(ctx, v1); err != nil {
		return "", err
	}
	busy, err := sb.Spawn(ctx, sandbox.Cmd{Argv: []string{"sleep", "3600"}, Cwd: mount})
	if err != nil {
		return "", err
	}
	defer func() { _ = sb.Kill(context.Background(), busy, 0) }()
	if err = refuseBusy(ctx, sb, v2, mount, "cwd"); err != nil {
		return "", err
	}
	got, err := sb.Exec(ctx, "cat", path.Join(mount, probe))
	if err != nil || got != want {
		return "", fmt.Errorf("busy cwd changed old dataset: content=%q error=%v", got, err)
	}
	if err = sb.Kill(ctx, busy, 0); err != nil {
		return "", err
	}
	if _, err = sb.SetVolumes(ctx, v2); err != nil {
		return "", fmt.Errorf("cwd retry: %w", err)
	}
	if _, err = sb.SetVolumes(ctx, v1); err != nil {
		return "", err
	}
	if _, err = sb.Exec(ctx, "sh", "-c", "mkdir -p /tmp/volume-bind; mount --bind /datasets/current /tmp/volume-bind"); err != nil {
		return "", err
	}
	defer func() { _, _ = sb.Exec(context.Background(), "umount", "/tmp/volume-bind") }()
	fmt.Printf("RETAINED begin=%d\n", time.Now().UnixNano())
	if _, err = sb.SetVolumes(ctx, v1); err != nil {
		return "", fmt.Errorf("retained volume with bind was reapplied: %w", err)
	}
	fmt.Printf("RETAINED end=%d\n", time.Now().UnixNano())
	if err = refuseBusy(ctx, sb, v2, "/tmp/volume-bind", "bind"); err != nil {
		return "", err
	}
	if got, err = sb.Exec(ctx, "cat", path.Join("/tmp/volume-bind", probe)); err != nil || got != want {
		return "", fmt.Errorf("busy bind was changed: content=%q error=%v", got, err)
	}
	if _, err = sb.Exec(ctx, "umount", "/tmp/volume-bind"); err != nil {
		return "", err
	}
	if _, err = sb.SetVolumes(ctx, v2); err != nil {
		return "", fmt.Errorf("same-path retry: %w", err)
	}
	v2content, err := sb.Exec(ctx, "cat", path.Join(mount, probe))
	if err != nil || v2content == "" || v2content == want {
		return "", fmt.Errorf("second dataset marker must differ: content=%q error=%v", v2content, err)
	}
	if _, err = sb.Exec(ctx, "touch", path.Join(mount, ".write-probe")); err == nil {
		return "", errors.New("second dataset accepted a write")
	}
	if exit, ok := errors.AsType[*sandbox.ExitError](err); !ok || !strings.Contains(strings.ToLower(exit.Stderr), "read-only file system") {
		return "", fmt.Errorf("second dataset write did not fail with EROFS: %w", err)
	}
	if _, err = sb.Exec(ctx, "sh", "-c", "awk '$2 == \"/datasets/current\" && $4 ~ /(^|,)ro(,|$)/ { found=1; print } END { exit !found }' /proc/mounts"); err != nil {
		return "", fmt.Errorf("guest mount is not read-only: %w", err)
	}
	if _, err = sb.Exec(ctx, "mount", "-t", "tmpfs", "tmpfs", mount); err != nil {
		return "", err
	}
	if err = refuseBusy(ctx, sb, v1, mount, "foreign"); err != nil {
		return "", err
	}
	if _, err = sb.Exec(ctx, "test", "!", "-e", path.Join(mount, probe)); err != nil {
		return "", fmt.Errorf("foreign mount was removed: %w", err)
	}
	if _, err = sb.Exec(ctx, "umount", mount); err != nil {
		return "", err
	}
	if _, err = sb.SetVolumes(ctx, v1); err != nil {
		return "", err
	}
	if _, err = sb.SetVolumes(ctx, v2); err != nil {
		return "", err
	}
	return v2content, nil
}

func runRetirement(ctx context.Context, client *sandbox.Client, template, volume, hook string) error {
	reader, err := client.New(ctx, template, sandbox.WithNetwork(sandbox.NetNone), sandbox.WithVolumes(sandbox.Volume{Name: volume, Mount: "/datasets/retiring"}))
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()
	if err := runHook(ctx, reader.ID, hook, "retire", "arm"); err != nil {
		return err
	}
	fmt.Printf("RETIREMENT release_error=%v\n", reader.Close())
	writerOptions := []sandbox.Option{sandbox.WithNetwork(sandbox.NetNone), sandbox.WithVolumes(sandbox.Volume{Name: volume, Mount: "/datasets/reused", Mode: "rw"})}
	refuseWriter := func() error {
		writer, err := client.New(ctx, template, writerOptions...)
		if err == nil {
			_ = writer.Close()
			return errors.New("writer admitted while retired VM still owns backing")
		}
		api, ok := errors.AsType[*sandbox.APIError](err)
		if !ok || api.Status != 409 {
			return fmt.Errorf("retired backing must return 409: %w", err)
		}
		fmt.Printf("RETIREMENT writer_refused=%v\n", err)
		return nil
	}
	if err := refuseWriter(); err != nil {
		return err
	}
	if err := runHook(ctx, reader.ID, hook, "retire", "wait"); err != nil {
		return err
	}
	if err := refuseWriter(); err != nil {
		return err
	}
	if err := runHook(ctx, reader.ID, hook, "retire", "interrupt"); err != nil {
		return err
	}
	deadline := time.Now().Add(45 * time.Second)
	for {
		writer, err := client.New(ctx, template, writerOptions...)
		if err == nil {
			defer func() { _ = writer.Close() }()
			if _, err = writer.Exec(ctx, "touch", "/datasets/reused/.retirement-reuse"); err != nil {
				return err
			}
			if err = writer.Close(); err != nil {
				return err
			}
			fmt.Println("RETIREMENT PASS writer_blocked_before_restart=true writer_blocked_after_restart=true backing_reused=true")
			return nil
		}
		api, ok := errors.AsType[*sandbox.APIError](err)
		if !ok || api.Status != 409 || time.Now().After(deadline) {
			return fmt.Errorf("backing not reusable after cleanup: %w", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func uploadListener(ctx context.Context, sb *sandbox.Sandbox, listener string) error {
	root, err := os.OpenRoot(filepath.Dir(listener))
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	binary, err := root.ReadFile(filepath.Base(listener))
	if err != nil {
		return err
	}
	return sb.WriteFile(ctx, "/tmp/volume-guestserver", binary, new(uint32(0o755)))
}
