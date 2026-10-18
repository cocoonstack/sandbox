package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/projecteru2/core/log"
	"github.com/rs/zerolog"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
)

var (
	infoFrame = line(wire.Info{})
	readFrame = line(wire.FsRead{Path: "/work/out.txt"})
)

func TestSleepingReadIsServedByTheReaderWithoutWaking(t *testing.T) {
	env := newSleepingEnv(t)
	conn, r := env.upgrade(t)

	send(t, conn, statFrame)
	expectLine(t, r, "reader")
	if wakes, dials := env.reader.counts(); wakes != 0 || dials != 1 {
		t.Errorf("wakes=%d reader dials=%d, want the read served by one reader and no wake", wakes, dials)
	}
	if guests := env.guestDials.Load(); guests != 0 {
		t.Errorf("%d guest dials, want none", guests)
	}
	if got := env.frames.all(); !slices.Equal(got, []string{"reader:" + statFrame}) {
		t.Errorf("frames %q, want the stat at the reader only", got)
	}
}

func TestSleepingFirstDialPipelinesInfoBehindTheRead(t *testing.T) {
	env := newSleepingEnv(t)
	conn, r := env.upgrade(t)

	send(t, conn, infoFrame)
	time.Sleep(10 * time.Millisecond)
	send(t, conn, readFrame)
	expectLine(t, r, "reader")
	expectLine(t, r, "reader")
	if wakes, _ := env.reader.counts(); wakes != 0 {
		t.Errorf("wakes=%d, want the info and the read served by the reader", wakes)
	}
	if got := env.frames.all(); !slices.Equal(got, []string{"reader:" + infoFrame, "reader:" + readFrame}) {
		t.Errorf("frames %q, want info then read at the reader in order", got)
	}
}

func TestSleepingLoneInfoWakes(t *testing.T) {
	env := newSleepingEnv(t)
	conn, r := env.upgrade(t)

	send(t, conn, infoFrame)
	expectLine(t, r, "guest")
	if wakes, dials := env.reader.counts(); wakes != 1 || dials != 0 {
		t.Errorf("wakes=%d reader dials=%d, want a wake and no reader", wakes, dials)
	}
	if got := env.frames.all(); !slices.Equal(got, []string{"guest:" + infoFrame}) {
		t.Errorf("frames %q, want the info replayed to the guest", got)
	}
}

func TestSleepingFirstWriteWakesAsBefore(t *testing.T) {
	for name, first := range map[string]string{"exec": execFrame, "write": line(wire.FsWrite{Path: "/x"}), "port": line(wire.PortForward{Port: 80})} {
		t.Run(name, func(t *testing.T) {
			env := newSleepingEnv(t)
			conn, r := env.upgrade(t)

			send(t, conn, first)
			expectLine(t, r, "guest")
			if wakes, dials := env.reader.counts(); wakes != 1 || dials != 0 {
				t.Errorf("wakes=%d reader dials=%d, want a wake and no reader", wakes, dials)
			}
			if got := env.frames.all(); !slices.Equal(got, []string{"guest:" + first}) {
				t.Errorf("frames %q, want the first call at the guest", got)
			}
		})
	}
}

func TestSleepingLaterWriteMovesTheConnectionToTheGuest(t *testing.T) {
	env := newSleepingEnv(t)
	conn, r := env.upgrade(t)
	send(t, conn, statFrame)
	expectLine(t, r, "reader")

	send(t, conn, execFrame)
	expectLine(t, r, "guest")
	select {
	case <-env.readerClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("the reader connection stayed open after the claim woke")
	}
	send(t, conn, statFrame)
	expectLine(t, r, "guest")
	if wakes, dials := env.reader.counts(); wakes != 1 || dials != 1 {
		t.Errorf("wakes=%d reader dials=%d, want one reader read then one wake", wakes, dials)
	}
	want := []string{"reader:" + statFrame, "guest:" + execFrame, "guest:" + statFrame}
	if got := env.frames.all(); !slices.Equal(got, want) {
		t.Errorf("frames %q, want %q", got, want)
	}
}

func TestSleepingReaderStopsServingOnceTheClaimWoke(t *testing.T) {
	env := newSleepingEnv(t)
	conn, r := env.upgrade(t)
	send(t, conn, statFrame)
	expectLine(t, r, "reader")

	env.reader.setSource("")
	send(t, conn, statFrame)
	expectLine(t, r, "guest")
	select {
	case <-env.readerClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("the stale reader connection stayed open")
	}
}

func TestSleepingConnectionMovesToAFreshReaderWhenTheClaimSleepsAgain(t *testing.T) {
	env := newSleepingEnv(t)
	conn, r := env.upgrade(t)
	send(t, conn, statFrame)
	expectLine(t, r, "reader")

	env.reader.setSource("snap-2")
	send(t, conn, readFrame)
	expectLine(t, r, "reader")
	select {
	case <-env.readerClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("the stale reader connection stayed open")
	}
	if wakes, dials := env.reader.counts(); wakes != 0 || dials != 2 {
		t.Errorf("wakes=%d reader dials=%d, want a fresh reader for the new sleep and no wake", wakes, dials)
	}
	want := []string{"reader:" + statFrame, "reader:" + readFrame}
	if got := env.frames.all(); !slices.Equal(got, want) {
		t.Errorf("frames %q, want %q", got, want)
	}
}

func TestSleepingUnavailableReaderWakes(t *testing.T) {
	logs := captureLogs(t)
	env := newSleepingEnv(t)
	env.reader.setDialErr(fmt.Errorf("%w: attach layer 0: boom", pool.ErrNoReader))
	conn, r := env.upgrade(t)

	send(t, conn, statFrame)
	expectLine(t, r, "guest")
	if wakes, _ := env.reader.counts(); wakes != 1 {
		t.Errorf("wakes=%d, want the read to fall back to a wake", wakes)
	}
	got := logs.String()
	for _, want := range []string{`"level":"warn"`, "sb_1", "attach layer 0: boom"} {
		if !strings.Contains(got, want) {
			t.Errorf("log %q lacks %q: the fallback to a wake must be visible", got, want)
		}
	}
	if n := strings.Count(got, "waking it"); n != 1 {
		t.Errorf("%d warnings for one failed setup, want 1", n)
	}
}

func TestSleepingFailuresAnswerWithAnErrorFrame(t *testing.T) {
	for _, tt := range []struct {
		name      string
		dialErr   error
		wakeErr   error
		firstCall string
		kind      string
		message   string
	}{
		{"unknown sandbox", pool.ErrUnknownSandbox, nil, statFrame, wire.KindNotFound, "unknown sandbox"},
		{"lease lost", pool.ErrLeaseLost, nil, statFrame, wire.KindInternal, "lease lost; retry"},
		{"wake refused", pool.ErrNoReader, pool.ErrTenantRemoved, statFrame, wire.KindInternal, pool.ErrTenantRemoved.Error()},
		{"wake fails", nil, errors.New("restore broke"), execFrame, wire.KindInternal, "sandbox lookup failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			env := newSleepingEnv(t)
			env.reader.setDialErr(tt.dialErr)
			env.manager.socket = func(string, string) (string, error) { return "", tt.wakeErr }
			conn, r := env.upgrade(t)

			send(t, conn, tt.firstCall)
			reply, err := r.ReadString('\n')
			if err != nil {
				t.Fatalf("read reply: %v", err)
			}
			resp, err := wire.DecodeResponse([]byte(reply))
			if err != nil {
				t.Fatalf("decode %q: %v", reply, err)
			}
			if got, ok := resp.(*wire.ErrorResp); !ok || got.Kind != tt.kind || got.Message != tt.message {
				t.Errorf("reply %+v, want an error frame of kind %s saying %q", resp, tt.kind, tt.message)
			}
			if _, err = r.ReadString('\n'); !errors.Is(err, io.EOF) {
				t.Errorf("connection after the error frame: %v, want EOF", err)
			}
		})
	}
}

func TestSleepingHandoverEndsWhenItsClaimIsFenced(t *testing.T) {
	env := newSleepingEnv(t)
	fenced := make(chan struct{})
	env.manager.fenced = fenced
	conn, r := env.upgrade(t)
	send(t, conn, execFrame)
	expectLine(t, r, "guest")

	close(fenced)
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Errorf("handed-over relay after the fence: %v, want EOF", err)
	}
}

func TestSleepingFramesAreAuditedOnceOnEitherPath(t *testing.T) {
	env := newSleepingEnv(t)
	var mu sync.Mutex
	var audited []string
	env.manager.audited = func(_ string, frame []byte) {
		mu.Lock()
		audited = append(audited, string(frame))
		mu.Unlock()
	}
	conn, r := env.upgrade(t)
	send(t, conn, infoFrame)
	time.Sleep(10 * time.Millisecond)
	send(t, conn, statFrame)
	expectLine(t, r, "reader")
	expectLine(t, r, "reader")
	send(t, conn, execFrame)
	expectLine(t, r, "guest")

	mu.Lock()
	defer mu.Unlock()
	want := []string{strings.TrimSpace(infoFrame), strings.TrimSpace(statFrame), strings.TrimSpace(execFrame)}
	if !slices.Equal(audited, want) {
		t.Errorf("audited %q, want each opening frame once: %q", audited, want)
	}
}

func TestSleepingLongReadFrameReachesTheReaderIntact(t *testing.T) {
	env := newSleepingEnv(t)
	conn, r := env.upgrade(t)
	long := line(wire.FsRead{Path: "/" + strings.Repeat("a", 3*routeBuf)})

	send(t, conn, long)
	expectLine(t, r, "reader")
	if got := env.frames.all(); !slices.Equal(got, []string{"reader:" + long}) {
		t.Errorf("the reader received %d frames, want the long frame whole", len(got))
	}
}

func TestSleepingAwakeClaimRelaysToItsGuest(t *testing.T) {
	env := newSleepingEnv(t)
	env.reader.setSource("")
	conn, r := env.upgrade(t)

	send(t, conn, statFrame)
	expectLine(t, r, "guest")
	if _, dials := env.reader.counts(); dials != 0 {
		t.Errorf("reader dials=%d for a running claim, want none", dials)
	}
}

func TestSleepingCloseRelaysEndsReaderConnections(t *testing.T) {
	env := newSleepingEnv(t)
	conn, r := env.upgrade(t)
	send(t, conn, statFrame)
	expectLine(t, r, "reader")

	done := make(chan struct{})
	go func() {
		env.srv.CloseRelays()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("CloseRelays hung on a reader connection")
	}
	if _, err := r.ReadString('\n'); !errors.Is(err, io.EOF) {
		t.Errorf("client after CloseRelays: %v, want EOF", err)
	}
}

func captureLogs(t *testing.T) *logSink {
	t.Helper()
	sink := &logSink{}
	logger := log.GetGlobalLogger()
	prev := *logger
	*logger = zerolog.New(sink)
	t.Cleanup(func() { *logger = prev })
	return sink
}

type logSink struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *logSink) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logSink) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

type sleepingEnv struct {
	ts           *httptest.Server
	srv          *Server
	manager      *fakeManager
	reader       *fakeReader
	frames       *frameLog
	guestDials   *atomic.Int32
	readerClosed chan struct{}
}

func newSleepingEnv(t *testing.T) *sleepingEnv {
	t.Helper()
	env := &sleepingEnv{frames: &frameLog{}, guestDials: &atomic.Int32{}, readerClosed: make(chan struct{}, 4)}
	env.reader = &fakeReader{src: "snap-1", serve: tagServer("reader", env.frames, env.readerClosed)}
	env.manager = &fakeManager{reader: env.reader}
	dialer := &fakeDialer{dial: func(context.Context, string) (net.Conn, error) {
		env.guestDials.Add(1)
		relayEnd, guestEnd := net.Pipe()
		go tagServer("guest", env.frames, nil)(guestEnd)
		return relayEnd, nil
	}}
	env.srv = New("", "node:7777", env.manager, dialer, nil, nil, nil, nil)
	env.ts = httptest.NewServer(env.srv.Handler())
	t.Cleanup(func() {
		env.srv.CloseRelays()
		env.ts.Close()
	})
	return env
}

func (e *sleepingEnv) upgrade(t *testing.T) (net.Conn, *bufio.Reader) {
	t.Helper()
	return upgradeConn(t, e.ts)
}

func send(t *testing.T, conn net.Conn, frame string) {
	t.Helper()
	if _, err := io.WriteString(conn, frame); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func expectLine(t *testing.T, r *bufio.Reader, want string) {
	t.Helper()
	got, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply (want %q): %v", want, err)
	}
	if strings.TrimSpace(got) != want {
		t.Fatalf("reply %q, want %q", got, want)
	}
}

func tagServer(tag string, frames *frameLog, closed chan<- struct{}) func(net.Conn) {
	return func(c net.Conn) {
		defer func() {
			_ = c.Close()
			if closed != nil {
				select {
				case closed <- struct{}{}:
				default:
				}
			}
		}()
		r := bufio.NewReaderSize(c, 1<<20)
		for {
			got, err := r.ReadString('\n')
			if err != nil {
				return
			}
			frames.add(tag + ":" + got)
			if _, err = io.WriteString(c, tag+"\n"); err != nil {
				return
			}
		}
	}
}

type frameLog struct {
	mu      sync.Mutex
	entries []string
}

func (l *frameLog) add(entry string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entry)
}

func (l *frameLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.entries)
}

type fakeReader struct {
	mu      sync.Mutex
	src     string
	wakes   int
	dials   int
	dialErr error
	serve   func(net.Conn)
}

func (f *fakeReader) source() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.src
}

func (f *fakeReader) setSource(src string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.src = src
}

func (f *fakeReader) setDialErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dialErr = err
}

func (f *fakeReader) wake() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.src = ""
	f.wakes++
}

func (f *fakeReader) counts() (wakes, dials int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.wakes, f.dials
}

func (f *fakeReader) dial() (net.Conn, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dialErr != nil {
		return nil, "", f.dialErr
	}
	if f.src == "" {
		return nil, "", pool.ErrNoReader
	}
	f.dials++
	relayEnd, readerEnd := net.Pipe()
	go f.serve(readerEnd)
	return relayEnd, f.src, nil
}
