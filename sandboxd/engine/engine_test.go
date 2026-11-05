package engine

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	infoFrame = `{"type":"info","version":"test","uptime_secs":0,"procs":0,"sessions":0}` + "\n"
	errFrame  = `{"type":"error","kind":"internal","message":"boom"}` + "\n"
)

func TestDialSilkdConsumesOnlyHandshake(t *testing.T) {
	path := sockPath(t)

	listenMuxer(t, path, "OK 2048\nX")

	conn, err := newTestEngine().DialSilkd(t.Context(), path)
	if err != nil {
		t.Fatalf("DialSilkd: %v", err)
	}
	defer conn.Close()
	buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	if buf[0] != 'X' {
		t.Errorf("got %q, want %q", buf[0], byte('X'))
	}
}

func TestDialSilkdRejectedHandshake(t *testing.T) {
	path := sockPath(t)
	listenMuxer(t, path, "ERR no guest listener\n")

	_, err := newTestEngine().DialSilkd(t.Context(), path)
	if err == nil || !strings.Contains(err.Error(), "no guest listener") {
		t.Errorf("got %v, want handshake rejection", err)
	}
}

func TestDialSilkdReturnsRawConnWithoutOverRead(t *testing.T) {
	path := sockPath(t)
	listenMuxer(t, path, "OK 2048\n")

	conn, err := newTestEngine().DialSilkd(t.Context(), path)
	if err != nil {
		t.Fatalf("DialSilkd: %v", err)
	}
	defer conn.Close()
	if _, ok := conn.(*net.UnixConn); !ok {
		t.Errorf("got %T, want *net.UnixConn", conn)
	}
}

func TestDialSilkdRejectsOverlongReply(t *testing.T) {
	path := sockPath(t)
	listenMuxer(t, path, "OK "+strings.Repeat("0", connectMax)+"\n")

	if _, err := newTestEngine().DialSilkd(t.Context(), path); !errors.Is(err, bufio.ErrBufferFull) {
		t.Errorf("got %v, want bufio.ErrBufferFull", err)
	}
}

func TestProbeSucceedsOnAnInfoFrame(t *testing.T) {
	path := sockPath(t)
	listenMuxer(t, path, "OK 2048\n", infoFrame)

	if err := newTestEngine().Probe(t.Context(), path, 2*time.Second); err != nil {
		t.Errorf("Probe: %v", err)
	}
}

func TestInfoRoundTripRejectsErrorFrame(t *testing.T) {
	path := sockPath(t)
	listenMuxer(t, path, "OK 2048\n", errFrame)

	err := newTestEngine().infoRoundTrip(t.Context(), path, silkdPort)
	if err == nil || !strings.Contains(err.Error(), `info reply type "error"`) {
		t.Errorf("got %v, want error-frame rejection", err)
	}
}

func TestProbeRetriesPastFailures(t *testing.T) {
	path := sockPath(t)
	listenMuxer(t, path, "OK 2048\n", errFrame, errFrame, infoFrame)

	if err := newTestEngine().Probe(t.Context(), path, 2*time.Second); err != nil {
		t.Errorf("Probe: %v", err)
	}
}

func TestProbeRetriesUntilListenerAppears(t *testing.T) {
	path := sockPath(t)
	done := make(chan error, 1)
	go func() {
		done <- newTestEngine().Probe(t.Context(), path, 2*time.Second)
	}()

	time.Sleep(60 * time.Millisecond)
	listenMuxer(t, path, "OK 2048\n", infoFrame)
	if err := <-done; err != nil {
		t.Errorf("Probe: %v", err)
	}
}

func TestDialGuestPortCtxCancel(t *testing.T) {
	path := sockPath(t)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("setup listener: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		if _, err := r.ReadString('\n'); err != nil {
			return
		}
		if _, err := io.WriteString(conn, "OK 2048\n"); err != nil {
			return
		}
		_, _ = r.ReadString('\n')
		<-hold
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	if _, err := newTestEngine().DialGuestPort(ctx, path, 8080); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want context.DeadlineExceeded", err)
	}
}

func TestSnapshotListReadsAnEmptyStoreAndRejectsProse(t *testing.T) {
	fakeCocoon(t, "#!/bin/sh\nif [ \"$COCOON_TEST_PROSE\" = 1 ]; then echo 'No snapshots found.'; else echo '[]'; fi\n")
	e := newTestEngine()

	t.Setenv("COCOON_TEST_PROSE", "0")
	if snaps, err := e.SnapshotList(t.Context()); err != nil || len(snaps) != 0 {
		t.Fatalf("empty store: snaps=%v err=%v, want none and nil", snaps, err)
	}
	t.Setenv("COCOON_TEST_PROSE", "1")
	if _, err := e.SnapshotList(t.Context()); err == nil || !strings.Contains(err.Error(), "parse snapshot list") {
		t.Fatalf("prose banner: err=%v, want a parse error", err)
	}
}

func TestListPassesNoPositionalArgsToCocoon(t *testing.T) {
	fakeCocoon(t, "#!/bin/sh\n[ \"$#\" = 4 ] || { echo \"unknown command $4 for cocoon vm list\" >&2; exit 1; }\necho '[{\"config\":{\"name\":\"sbx-1\"},\"state\":\"running\"}]'\n")
	vms, err := newTestEngine().List(t.Context())
	if err != nil || len(vms) != 1 || vms[0].Config.Name != "sbx-1" {
		t.Fatalf("List = %+v, %v; want the one VM cocoon printed", vms, err)
	}
}

func TestImageIDsMapsNameToID(t *testing.T) {
	fakeCocoon(t, "#!/bin/sh\n[ \"$*\" = \"image list --format json\" ] || { echo \"unexpected args $*\" >&2; exit 1; }\n"+
		"echo '[{\"id\":\"sha256:afe6\",\"name\":\"ghcr.io/cocoonstack/sandbox/rt:24.04\",\"type\":\"oci\",\"size\":1}]'\n")
	ids, err := newTestEngine().ImageIDs(t.Context())
	if err != nil || len(ids) != 1 || ids["ghcr.io/cocoonstack/sandbox/rt:24.04"] != "sha256:afe6" {
		t.Fatalf("ImageIDs = %v, %v; want the one image cocoon printed", ids, err)
	}
}

func TestRemoveStopsAVMThatRefusesDeletionThenRemovesItAgain(t *testing.T) {
	fakeCocoon(t, "#!/bin/sh\necho \"$*\" >> \"$COCOON_TEST_TRACE\"\n"+
		"case \"$1 $2\" in\n"+
		"\"vm rm\") [ -z \"$COCOON_TEST_MISSING\" ] || { echo 'Error: vm not found' >&2; exit 1; }\n"+
		"  [ -z \"$COCOON_TEST_STUCK\" ] && [ -e \"$COCOON_TEST_TRACE.stopped\" ] || { echo 'refuse delete: api socket still responsive' >&2; exit 1; } ;;\n"+
		"\"vm stop\") : > \"$COCOON_TEST_TRACE.stopped\" ;;\n"+
		"esac\n")
	const rm, stop = "vm rm --force sbx-1", "vm stop --force sbx-1"
	for _, tt := range []struct {
		name, env string
		calls     []string
		fails     bool
	}{
		{"refused then removed", "", []string{rm, stop, rm}, false},
		{"still refused after the stop", "COCOON_TEST_STUCK", []string{rm, stop, rm}, true},
		{"already gone", "COCOON_TEST_MISSING", []string{rm}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			trace := filepath.Join(t.TempDir(), "trace")
			t.Setenv("COCOON_TEST_TRACE", trace)
			t.Setenv("COCOON_TEST_STUCK", "")
			t.Setenv("COCOON_TEST_MISSING", "")
			if tt.env != "" {
				t.Setenv(tt.env, "1")
			}
			err := newTestEngine().Remove(t.Context(), "sbx-1")
			data, _ := os.ReadFile(trace)
			if calls := strings.Split(strings.TrimSpace(string(data)), "\n"); !slices.Equal(calls, tt.calls) || (err != nil) != tt.fails {
				t.Errorf("Remove = %v with cocoon calls %q, want %q and failure %v", err, calls, tt.calls, tt.fails)
			}
		})
	}
}

func TestACanceledCommandGetsSIGTERMSoCocoonCanResumeTheVM(t *testing.T) {
	trace := filepath.Join(t.TempDir(), "trace")
	t.Setenv("COCOON_TEST_TRACE", trace)
	fakeCocoon(t, "#!/bin/sh\ntrap 'echo term >> \"$COCOON_TEST_TRACE\"; kill $!; exit 3' TERM\n"+
		"sleep 30 >/dev/null 2>&1 &\necho ready >> \"$COCOON_TEST_TRACE\"\nwait\n")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- newTestEngine().Hibernate(ctx, "vm", "snap") }()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if data, _ := os.ReadFile(trace); strings.Contains(string(data), "ready") {
			break
		}
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("a canceled hibernate reported success")
	}
	if data, _ := os.ReadFile(trace); !strings.Contains(string(data), "term") {
		t.Errorf("trace %q, want the command to have received SIGTERM", data)
	}
}

func TestAMemoryCommandGetsItsLongerTermGraceAndAColdBootTheCommandOne(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		run  func(ctx context.Context, e *Engine) error
		want string
	}{
		{"restore", func(ctx context.Context, e *Engine) error {
			_, err := e.Restore(ctx, "vm", "snap")
			return err
		}, "exit status 3"},
		{"clone", func(ctx context.Context, e *Engine) error {
			_, err := e.Clone(ctx, "dir", "vm", types.PoolKey{})
			return err
		}, "exit status 3"},
		{"hibernate", func(ctx context.Context, e *Engine) error {
			return e.Hibernate(ctx, "vm", "snap")
		}, "exit status 3"},
		{"snapshot save", func(ctx context.Context, e *Engine) error {
			return e.SnapshotSave(ctx, "vm", "snap")
		}, "exit status 3"},
		{"migrate capture", func(ctx context.Context, e *Engine) error {
			_, err := e.MigrateCapture(ctx, "vm", "s1", true)
			return err
		}, "exit status 3"},
		{"migrate receive", func(ctx context.Context, e *Engine) error {
			_, err := e.MigrateReceive(ctx, "dir", "vm", types.PoolKey{})
			return err
		}, "exit status 3"},
		{"cold boot", func(ctx context.Context, e *Engine) error {
			_, err := e.RunCold(ctx, "vm", types.PoolKey{}, "")
			return err
		}, "signal: killed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			bin, ready := filepath.Join(dir, "cocoon"), filepath.Join(dir, "ready")
			script := "#!/bin/sh\ntrap 'kill $!; sleep " + strconv.Itoa(int(cmdTermGrace/time.Second)+2) + " >/dev/null 2>&1; exit 3' TERM\n" +
				"sleep 300 >/dev/null 2>&1 &\n: > " + ready + "\nwait\n"
			installScript(t, bin, script)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- tt.run(ctx, New(bin, nil, nil, false, false, "")) }()
			waitFile(t, ready)
			cancel()
			if err := <-done; err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("got %v, want %q", err, tt.want)
			}
		})
	}
}

func TestCloseWaitsForARunningCommandAndRefusesNewOnes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bin, ready, release := filepath.Join(dir, "cocoon"), filepath.Join(dir, "ready"), filepath.Join(dir, "release")
	installScript(t, bin, "#!/bin/sh\n: > "+ready+"\nwhile [ ! -e "+release+" ]; do sleep 0.05; done\n")
	e := New(bin, nil, nil, false, false, "")
	done := make(chan error, 1)
	go func() { done <- e.Hibernate(t.Context(), "vm", "snap") }()
	waitFile(t, ready)
	closed := make(chan struct{})
	go func() {
		e.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while a cocoon command was running")
	case <-time.After(200 * time.Millisecond):
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("the running hibernate: %v", err)
	}
	<-closed
	if _, err := e.List(t.Context()); !errors.Is(err, ErrStopping) {
		t.Errorf("List after Close = %v, want %v", err, ErrStopping)
	}
}

func TestWaitCaptureBlocksOnAnExactArgument(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reads /proc")
	}
	snap := "sbx-hib-" + strings.ReplaceAll(t.Name(), "/", "-")
	cmd := exec.Command("sh", "-c", "sleep 30; :", "--name", snap)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	done := make(chan error, 1)
	go func() { done <- newTestEngine().WaitCapture(t.Context(), snap) }()
	select {
	case err := <-done:
		t.Fatalf("WaitCapture returned %v with the process alive", err)
	case <-time.After(2 * captureExitPoll):
	}
	if err := newTestEngine().WaitCapture(t.Context(), snap[:len(snap)-1]); err != nil {
		t.Errorf("a prefix of the name: %v", err)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	if err := <-done; err != nil {
		t.Errorf("WaitCapture after exit: %v", err)
	}
}

func TestADialOfAGuestThatNeverAnswersFails(t *testing.T) {
	t.Parallel()
	path := sockPath(t)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("setup listener: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	start := time.Now()
	if _, err := newTestEngine().DialSilkd(t.Context(), path); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("dial: %v, want a deadline error", err)
	}
	if elapsed := time.Since(start); elapsed > 2*connectTimeout {
		t.Errorf("dial took %v, want about %v", elapsed, connectTimeout)
	}
}

func TestMemoryDumpsRunAtMostEightAtOnce(t *testing.T) {
	trace := filepath.Join(t.TempDir(), "trace")
	t.Setenv("COCOON_TEST_TRACE", trace)
	fakeCocoon(t, "#!/bin/sh\necho + >> \"$COCOON_TEST_TRACE\"\nsleep 0.1\necho - >> \"$COCOON_TEST_TRACE\"\n")
	e := newTestEngine()
	var wg sync.WaitGroup
	for i := range 40 {
		wg.Go(func() {
			switch i % 3 {
			case 0:
				_ = e.Hibernate(t.Context(), "vm", "snap")
			case 1:
				_ = e.SnapshotSave(t.Context(), "vm", "snap")
			default:
				_, _ = e.MigrateCapture(t.Context(), "vm", "", false)
			}
		})
	}
	wg.Wait()
	data, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	running, peak := 0, 0
	for line := range strings.FieldsSeq(string(data)) {
		if line == "+" {
			running++
		} else {
			running--
		}
		peak = max(peak, running)
	}
	if peak > memoryDumps || peak < 2 {
		t.Errorf("peak concurrent dumps = %d, want 2..%d", peak, memoryDumps)
	}
}

func TestProbeOfAMissingSocketTimesOut(t *testing.T) {
	path := sockPath(t)

	err := newTestEngine().Probe(t.Context(), path, 150*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "silkd probe") {
		t.Errorf("got %v, want probe timeout", err)
	}
}

func listenMuxer(t *testing.T, path, handshake string, frames ...string) {
	t.Helper()
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("setup listener: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for i := 0; ; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			frame := ""
			if len(frames) > 0 {
				frame = frames[min(i, len(frames)-1)]
			}
			go serveConn(conn, handshake, frame)
		}
	}()
}

func serveConn(conn net.Conn, handshake, frame string) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	if _, err := r.ReadString('\n'); err != nil {
		return
	}
	if _, err := io.WriteString(conn, handshake); err != nil || frame == "" {
		return
	}
	if _, err := r.ReadString('\n'); err != nil {
		return
	}
	_, _ = io.WriteString(conn, frame)
}

func newTestEngine() *Engine {
	return New("cocoon", nil, nil, false, false, "")
}

func sockPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sbx")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "mux.sock")
}

func waitFile(t *testing.T, path string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(path); err == nil {
			return
		}
	}
	t.Fatalf("%s not created within 5s", path)
}

func fakeCocoon(t *testing.T, script string) {
	t.Helper()
	dir := t.TempDir()
	installScript(t, filepath.Join(dir, "cocoon"), script)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func installScript(t *testing.T, path, script string) {
	t.Helper()
	src := path + ".src"
	if err := os.WriteFile(src, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("install", "-m", "0700", src, path).CombinedOutput(); err != nil {
		t.Fatalf("install %s: %v: %s", path, err, out)
	}
}
