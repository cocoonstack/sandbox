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
)

// ReaderDisks names the disks attached to a reader VM: the claim's layers, bottom layer first, and its writable copy-on-write disk.
type ReaderDisks struct {
	Layers []string
	Cow    string
}

// StartReader mounts the attached disks as a read-only overlay of the claim's filesystem and starts a silkd chrooted into it.
func (e *Engine) StartReader(ctx context.Context, vsockSocket string, d ReaderDisks) error {
	ctx, cancel := context.WithTimeout(ctx, readerStartTimeout)
	defer cancel()
	cow := readerRoot + "/cow"
	if err := e.MountVolume(ctx, vsockSocket, d.Cow, cow, true); err != nil {
		return fmt.Errorf("mount claim disk: %w", err)
	}
	lower := []string{cow + "/upper"}
	for i, layer := range slices.Backward(d.Layers) {
		mount := fmt.Sprintf("%s/layer%d", readerRoot, i)
		if err := e.MountVolume(ctx, vsockSocket, layer, mount, false); err != nil {
			return fmt.Errorf("mount layer %d: %w", i, err)
		}
		lower = append(lower, mount)
	}
	if err := e.silkdExec(ctx, vsockSocket, "mkdir", "-p", "--", readerView); err != nil {
		return fmt.Errorf("create view mount point: %w", err)
	}
	opts := "ro,metacopy=on,redirect_dir=follow,lowerdir=" + strings.Join(lower, ":")
	if err := e.silkdExec(ctx, vsockSocket, "mount", "-t", "overlay", "-o", opts, "--", "overlay", readerView); err != nil {
		return fmt.Errorf("mount view: %w", err)
	}
	if err := e.silkdExec(ctx, vsockSocket, "test", "-x", readerView+readerSilkd); err != nil {
		return fmt.Errorf("claim image has no silkd at %s: %w", readerSilkd, err)
	}
	if err := e.silkdSpawn(ctx, vsockSocket, "env", fmt.Sprintf("SILKD_PORT=%d", readerPort), "chroot", readerView, readerSilkd); err != nil {
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
