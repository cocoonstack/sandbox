package filecache

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// Share drives VM-level vhost-user-fs hotplug for the uncached workspace mode;
// the engine implements it over cocoon's fs attach/detach verbs. Separate from
// Guest (in-guest silkd ops) because attach/detach act on the VM, and separate
// from Disk because a share carries no local storage at all.
type Share interface {
	// Attach hot-attaches the vhost-user-fs share served on socket to vmName
	// under the guest mount tag.
	Attach(ctx context.Context, vmName, socket, tag string) error
	// Detach removes the share tagged tag from vmName.
	Detach(ctx context.Context, vmName, tag string) error
}

const (
	// shareTag is the guest mount tag and the detach key. One share per
	// sandbox, so a constant is fine.
	shareTag = "fcws"
	// socketWait bounds how long we wait for virtiofsd to bind its socket
	// before giving up on the share.
	socketWait = 10 * time.Second
	// tagWait bounds how long the guest gets to probe the hot-attached device
	// and register its mount tag.
	tagWait = 15 * time.Second
)

// shareProvisioner runs one virtiofsd per uncached sandbox over that
// sandbox's workspace directory on the node's NAS mount, hot-attaches it, and
// mounts it in the guest. There is no local copy and no sync loop: the guest's
// writes land on the NAS as they happen, which is the point of the mode.
//
// virtiofsd is a separate binary by design (the vhost-user backend runs out of
// process), so this is one of the few places sandboxd owns a child process
// rather than shelling out and waiting. The child is tied to the session: it
// starts in Arm and is killed in Barrier.
type shareProvisioner struct {
	share  Share
	guest  Guest
	binary string // virtiofsd path
	runDir string // per-sandbox socket directory
}

// shareHandle is the live state of one sandbox's share, held by its Session so
// the barrier can tear the child down.
type shareHandle struct {
	cmd  *exec.Cmd
	sock string
}

func (s *shareProvisioner) sockPath(id string) string {
	return filepath.Join(s.runDir, id+".sock")
}

// serveAndMount starts virtiofsd over ws, attaches it to vmName, and mounts it
// at mount in the guest. On any failure it unwinds what it created, so a
// failed arm leaves neither a stray child nor a half-attached device.
func (s *shareProvisioner) serveAndMount(ctx context.Context, id, vmName, vsockSocket, ws, mount string) (*shareHandle, error) {
	if err := os.MkdirAll(s.runDir, 0o750); err != nil {
		return nil, fmt.Errorf("share run dir: %w", err)
	}
	sock := s.sockPath(id)
	// Leftovers from a previous incarnation would make virtiofsd fail to bind;
	// nothing is listening on them, since its child died with us.
	_ = os.Remove(sock)
	_ = os.Remove(sock + ".pid")

	// --sandbox=none: the workspace is a network mount the node already
	// exports to this sandbox, and virtiofsd's own sandbox cannot pivot_root
	// into one. --cache=auto keeps the guest page cache coherent enough for
	// the close-to-open contract the cached mode also promises.
	cmd := exec.Command(s.binary, //nolint:gosec // binary from node config, args built here
		"--socket-path="+sock,
		"--shared-dir="+ws,
		"--cache=auto",
		"--sandbox=none",
	)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start virtiofsd: %w", err)
	}
	h := &shareHandle{cmd: cmd, sock: sock}
	if err := waitForSocket(ctx, sock); err != nil {
		h.stop()
		return nil, err
	}
	if err := s.share.Attach(ctx, vmName, sock, shareTag); err != nil {
		h.stop()
		return nil, fmt.Errorf("attach workspace share: %w", err)
	}
	if err := s.mountInGuest(ctx, vsockSocket, mount); err != nil {
		_ = s.share.Detach(ctx, vmName, shareTag)
		h.stop()
		return nil, err
	}
	return h, nil
}

// mountInGuest mounts the attached share at mount. An existing mount there is
// dropped first: a re-arm after a daemon restart finds the guest holding a
// mount whose backend died with the old process.
//
// The mount is retried, because attach only tells the VMM about the device:
// the guest still has to probe it and register the tag, and a mount issued
// before that fails with "wrong fs type" on a tag that is about to exist.
func (s *shareProvisioner) mountInGuest(ctx context.Context, vsockSocket, mount string) error {
	if _, err := s.guest.Run(ctx, vsockSocket, "mkdir", "-p", "--", mount); err != nil {
		return fmt.Errorf("create workspace mount point %s: %w", mount, err)
	}
	_, _ = s.guest.Run(ctx, vsockSocket, "umount", "-l", "--", mount)
	ctx, cancel := context.WithTimeout(ctx, tagWait)
	defer cancel()
	var lastErr error
	for {
		if _, lastErr = s.guest.Run(ctx, vsockSocket, "mount", "-t", "virtiofs", shareTag, mount); lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("mount workspace share at %s: %w (last: %v)", mount, ctx.Err(), lastErr)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// unmountAndStop reverses serveAndMount. Best-effort per step so one failure
// does not strand the rest; unlike the cached mode there is nothing to drain
// first, since every write already reached the NAS.
func (s *shareProvisioner) unmountAndStop(ctx context.Context, h *shareHandle, vmName, vsockSocket, mount string) {
	if _, err := s.guest.Run(ctx, vsockSocket, "umount", "--", mount); err != nil {
		// A lazy unmount still detaches the tree; the VM is going away anyway.
		_, _ = s.guest.Run(ctx, vsockSocket, "umount", "-l", "--", mount)
	}
	_ = s.share.Detach(ctx, vmName, shareTag)
	h.stop()
}

// stop kills virtiofsd and reaps it, then removes the socket and the pid file
// virtiofsd writes beside it.
func (h *shareHandle) stop() {
	if h == nil || h.cmd == nil {
		return
	}
	if h.cmd.Process != nil {
		_ = h.cmd.Process.Kill()
		_ = h.cmd.Wait()
	}
	_ = os.Remove(h.sock)
	_ = os.Remove(h.sock + ".pid")
}

// waitForSocket blocks until virtiofsd has bound path, or the wait budget runs
// out. virtiofsd binds within milliseconds; a timeout here means it died on
// startup (bad shared dir, missing permissions) and attach would fail anyway.
func waitForSocket(ctx context.Context, path string) error {
	ctx, cancel := context.WithTimeout(ctx, socketWait)
	defer cancel()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("virtiofsd socket %s: %w", path, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}
