package engine

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

const (
	readerPort         = 2051
	readerRoot         = "/run/reader"
	readerView         = "/view"
	readerSilkd        = "/usr/local/bin/silkd"
	readerStartTimeout = 30 * time.Second
	readerProbeTimeout = 10 * time.Second
	layersParam        = "cocoon.layers="
)

// StartReader mounts the attached copy-on-write disk over the layers the VM booted with as a read-only overlay of the claim's filesystem, and starts a silkd chrooted into it.
func (e *Engine) StartReader(ctx context.Context, vsockSocket, cowDisk string) error {
	ctx, cancel := context.WithTimeout(ctx, readerStartTimeout)
	defer cancel()
	cmdline, err := e.silkdReadFile(ctx, vsockSocket, "/proc/cmdline", silkdReadMax)
	if err != nil {
		return fmt.Errorf("read guest cmdline: %w", err)
	}
	layers := cmdlineLayers(string(cmdline))
	if len(layers) == 0 {
		return fmt.Errorf("guest cmdline names no %s", layersParam)
	}
	cow := readerRoot + "/cow"
	if err = e.MountVolume(ctx, vsockSocket, cowDisk, cow, true); err != nil {
		return fmt.Errorf("mount claim disk: %w", err)
	}
	lower := []string{cow + "/upper"}
	for i, layer := range layers {
		mount := fmt.Sprintf("%s/layer%d", readerRoot, i)
		if err = e.MountVolume(ctx, vsockSocket, layer, mount, false); err != nil {
			return fmt.Errorf("mount layer %d: %w", i, err)
		}
		lower = append(lower, mount)
	}
	if err = e.silkdExec(ctx, vsockSocket, "mkdir", "-p", "--", readerView); err != nil {
		return fmt.Errorf("create view mount point: %w", err)
	}
	opts := "ro,metacopy=on,redirect_dir=follow,lowerdir=" + strings.Join(lower, ":")
	if err = e.silkdExec(ctx, vsockSocket, "mount", "-t", "overlay", "-o", opts, "--", "overlay", readerView); err != nil {
		return fmt.Errorf("mount view: %w", err)
	}
	if err = e.silkdExec(ctx, vsockSocket, "test", "-x", readerView+readerSilkd); err != nil {
		return fmt.Errorf("claim image has no silkd at %s: %w", readerSilkd, err)
	}
	if err = e.silkdSpawn(ctx, vsockSocket, "env", fmt.Sprintf("SILKD_PORT=%d", readerPort), "chroot", readerView, readerSilkd); err != nil {
		return fmt.Errorf("start view silkd: %w", err)
	}
	return e.probe(ctx, vsockSocket, readerPort, readerProbeTimeout)
}

// DialReader connects to the silkd StartReader started.
func (e *Engine) DialReader(ctx context.Context, vsockSocket string) (net.Conn, error) {
	return e.dialVsock(ctx, vsockSocket, readerPort)
}

func (e *Engine) silkdSpawn(ctx context.Context, vsockSocket string, argv ...string) error {
	s, err := e.dialSilkdSession(ctx, vsockSocket)
	if err != nil {
		return err
	}
	defer s.close()
	if err = s.send(wire.Exec{Argv: argv, Env: map[string]string{"PATH": guestExecPATH}, Detach: true}); err != nil {
		return err
	}
	frame, err := s.recv()
	if err != nil {
		return err
	}
	switch resp := frame.(type) {
	case *wire.Started:
		return nil
	case *wire.ErrorResp:
		return fmt.Errorf("silkd %w", resp)
	default:
		return fmt.Errorf("unexpected silkd frame %q", frame.RespType())
	}
}

func cmdlineLayers(cmdline string) []string {
	var layers []string
	for field := range strings.FieldsSeq(cmdline) {
		if val, ok := strings.CutPrefix(field, layersParam); ok {
			layers = slices.DeleteFunc(strings.Split(val, ","), func(s string) bool { return s == "" })
		}
	}
	return layers
}
