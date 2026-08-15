package filecache

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// recordingGuest records every argv so a test can assert what the guest was
// told to do, and reports whether the sync loops ever touched it.
type recordingGuest struct {
	mu   sync.Mutex
	runs [][]string
}

func (g *recordingGuest) Run(_ context.Context, _ string, argv ...string) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.runs = append(g.runs, argv)
	return "", nil
}
func (g *recordingGuest) WriteFile(context.Context, string, string, uint32, []byte) error { return nil }
func (g *recordingGuest) ReadFile(context.Context, string, string) ([]byte, error) {
	return nil, nil
}
func (g *recordingGuest) PushTar(context.Context, string, string, io.Reader) error { return nil }
func (g *recordingGuest) Remove(context.Context, string, string, bool) error       { return nil }

func (g *recordingGuest) ran(verb string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, argv := range g.runs {
		if len(argv) > 0 && argv[0] == verb {
			return true
		}
	}
	return false
}

// flakyMountGuest fails the first n mount attempts, standing in for a guest
// that has not finished probing the hot-attached device yet.
type flakyMountGuest struct {
	recordingGuest
	failMounts int
}

func (g *flakyMountGuest) Run(ctx context.Context, sock string, argv ...string) (string, error) {
	if len(argv) > 0 && argv[0] == "mount" && g.failMounts > 0 {
		g.failMounts--
		_, _ = g.recordingGuest.Run(ctx, sock, argv...)
		return "", errors.New("exit code 32: wrong fs type, bad option, bad superblock on fcws")
	}
	return g.recordingGuest.Run(ctx, sock, argv...)
}

// TestMountRetriesUntilTheTagAppears: attach only tells the VMM about the
// device, so the first mounts lose a race with the guest's probe. Failing the
// arm there would leave the sandbox with no workspace at all.
func TestMountRetriesUntilTheTagAppears(t *testing.T) {
	g := &flakyMountGuest{failMounts: 3}
	s := &shareProvisioner{share: &fakeShare{}, guest: g, binary: stubVirtiofsd(t), runDir: filepath.Join(t.TempDir(), "shares")}
	if _, err := s.serveAndMount(t.Context(), "sb_a", "sbx-1", "/vsock/a", t.TempDir(), "/workspace"); err != nil {
		t.Fatalf("serveAndMount gave up on a racing mount: %v", err)
	}
	if g.failMounts != 0 {
		t.Fatalf("%d mount failures left unconsumed", g.failMounts)
	}
}

// fakeShare stands in for cocoon's fs attach/detach verbs.
type fakeShare struct {
	attached  int
	detached  int
	lastVM    string
	lastSock  string
	lastTag   string
	attachErr error
}

func (s *fakeShare) Attach(_ context.Context, vmName, socket, tag string) error {
	if s.attachErr != nil {
		return s.attachErr
	}
	s.attached++
	s.lastVM, s.lastSock, s.lastTag = vmName, socket, tag
	return nil
}

func (s *fakeShare) Detach(_ context.Context, vmName, tag string) error {
	s.detached++
	s.lastVM, s.lastTag = vmName, tag
	return nil
}

// stubVirtiofsd writes a script that binds nothing but creates the socket path
// and blocks, which is all serveAndMount waits for.
func stubVirtiofsd(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "virtiofsd")
	script := "#!/bin/sh\nfor a in \"$@\"; do case $a in --socket-path=*) : > \"${a#--socket-path=}\";; esac; done\nsleep 300\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil { //nolint:gosec // test fixture
		t.Fatal(err)
	}
	return path
}

// TestArmNoCacheSharesInsteadOfSyncing is the switch itself: an uncached claim
// must mount the shared workspace in the guest and start no sync loops, so
// nothing it writes waits on a push to become visible to the other writers.
func TestArmNoCacheSharesInsteadOfSyncing(t *testing.T) {
	ws := t.TempDir()
	g := &recordingGuest{}
	sh := &fakeShare{}
	m := NewManager(g)
	m.EnableShare(sh, stubVirtiofsd(t), filepath.Join(t.TempDir(), "shares"))

	cfg := Config{VMName: "sbx-1", NoCache: true}
	if err := m.Arm(t.Context(), "sb_a", "/vsock/a", ws, "sb_a", cfg); err != nil {
		t.Fatalf("Arm: %v", err)
	}
	if !m.Has("sb_a") {
		t.Fatal("uncached session not registered")
	}
	if sh.attached != 1 || sh.lastVM != "sbx-1" || sh.lastTag != shareTag {
		t.Fatalf("attach: n=%d vm=%q tag=%q", sh.attached, sh.lastVM, sh.lastTag)
	}
	if !g.ran("mount") {
		t.Fatal("guest was never told to mount the share")
	}
	// The cache's fingerprint is a .filecache tree on the NAS; an uncached
	// session must not create one, and must not hydrate the guest.
	if _, err := os.Stat(filepath.Join(ws, fcDir)); !os.IsNotExist(err) {
		t.Fatalf("uncached session created the journal tree: %v", err)
	}
	if g.ran("find") {
		t.Fatal("uncached session ran a sync cycle")
	}

	m.Barrier(t.Context(), "sb_a")
	if m.Has("sb_a") {
		t.Fatal("session survived the barrier")
	}
	if sh.detached != 1 {
		t.Fatalf("share not detached: n=%d", sh.detached)
	}
	if !g.ran("umount") {
		t.Fatal("guest mount was never dropped")
	}
}

// TestArmNoCacheFallsBackWithoutShareDriver: a node that does not serve the
// uncached mode must still honor the claim, just with the cache — dropping
// the workspace entirely would silently lose the sandbox's writes.
func TestArmNoCacheFallsBackWithoutShareDriver(t *testing.T) {
	ws := t.TempDir()
	g := &recordingGuest{}
	m := NewManager(g)

	cfg := Config{VMName: "sbx-1", NoCache: true}
	if err := m.Arm(t.Context(), "sb_a", "/vsock/a", ws, "sb_a", cfg); err != nil {
		t.Fatalf("Arm: %v", err)
	}
	if _, err := os.Stat(filepath.Join(ws, fcDir)); err != nil {
		t.Fatalf("fell back to no workspace at all: %v", err)
	}
	m.Barrier(t.Context(), "sb_a")
}

// TestServeAndMountUnwindsOnAttachFailure: a failed attach must leave no
// virtiofsd behind, since the claim stays alive and may be armed again.
func TestServeAndMountUnwindsOnAttachFailure(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), "shares")
	s := &shareProvisioner{
		share:  &fakeShare{attachErr: os.ErrPermission},
		guest:  &recordingGuest{},
		binary: stubVirtiofsd(t),
		runDir: runDir,
	}
	h, err := s.serveAndMount(t.Context(), "sb_a", "sbx-1", "/vsock/a", t.TempDir(), "/workspace")
	if err == nil {
		t.Fatal("attach failure not reported")
	}
	if h != nil {
		t.Fatal("handle returned for a failed arm")
	}
	if _, statErr := os.Stat(s.sockPath("sb_a")); !os.IsNotExist(statErr) {
		t.Fatalf("socket left behind: %v", statErr)
	}
	if !strings.Contains(err.Error(), "attach workspace share") {
		t.Fatalf("unhelpful error: %v", err)
	}
}
