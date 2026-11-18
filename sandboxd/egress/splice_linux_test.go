package egress

import (
	"crypto/sha256"
	"io"
	"net"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"sync"
	"testing"
	"time"
)

func TestSpliceCopiesThroughADoorConnInTheKernel(t *testing.T) {
	for _, tt := range []struct {
		name     string
		noSplice bool
	}{
		{"splice", false},
		{"no rx splice", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const total = 16 << 20
			buffered := make(chan struct{}, 2)
			guestDone := make(chan struct{})
			door, upstream := spliceEnds(t, func(c net.Conn) {
				pump(c, total, buffered)
				_ = c.(*net.TCPConn).CloseWrite()
			}, func(c net.Conn) {
				defer close(guestDone)
				defer func() { _ = c.Close() }()
				buf := make([]byte, 64<<10)
				buffered <- struct{}{}
				for range total / len(buf) {
					_, _ = c.Write(buf)
				}
				_ = c.(*net.UnixConn).CloseWrite()
				for {
					if _, readErr := c.Read(buf); readErr != nil {
						return
					}
				}
			})

			p := New(Policy{}, nil, nil, fixedDial(""), nil, nil)
			if tt.noSplice {
				p.NoRxSplice()
			}
			tr := p.begin(Event{}, door, upstream)
			<-buffered
			<-buffered
			before := bufferAllocs()
			p.splice(tr)
			allocs := bufferAllocs() - before
			<-guestDone
			up, down := tr.sent.Load(), tr.received.Load()
			if up != total || down != total {
				t.Fatalf("moved %d up and %d down, want %d each", up, down, total)
			}
			if !tt.noSplice && allocs > 0 {
				t.Errorf("splice made %d buffer-sized allocations: a door conn fell back to buffered copies", allocs)
			}
		})
	}
}

func TestKickResumesASpliceWithoutLosingAByte(t *testing.T) {
	const total = 32 << 20
	upHash, downHash := make(chan [32]byte, 1), make(chan [32]byte, 1)
	door, upstream := spliceEnds(t,
		func(c net.Conn) { upHash <- echoHash(c, total) },
		func(c net.Conn) { downHash <- echoHash(c, total) })

	p := New(Policy{}, nil, nil, fixedDial(""), nil, nil)
	tr := p.begin(Event{}, door, upstream)
	done := make(chan struct{})
	go func() {
		p.splice(tr)
		close(done)
	}()
	kicks := 0
	for kicking := true; kicking; kicks++ {
		select {
		case <-done:
			kicking = false
		case <-time.After(200 * time.Microsecond):
			p.kick()
		}
	}
	h := sha256.New()
	_, _ = io.Copy(h, io.LimitReader(new(pattern), total))
	want := [32]byte(h.Sum(nil))
	if up, down := <-upHash, <-downHash; up != want || down != want {
		t.Fatalf("a stream changed across %d kicks: up intact %v, down intact %v", kicks, up == want, down == want)
	}
	if tr.sent.Load() != total || tr.received.Load() != total || kicks < 10 {
		t.Errorf("counted %d up and %d down across %d kicks, want %d each", tr.sent.Load(), tr.received.Load(), kicks, total)
	}
}

type doorShaped struct{ *net.UnixConn }

func (c doorShaped) NetConn() net.Conn { return c.UnixConn }

func spliceEnds(t *testing.T, serve, guest func(net.Conn)) (doorShaped, net.Conn) {
	t.Helper()
	uln, err := net.Listen("unix", filepath.Join(t.TempDir(), "door"))
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	t.Cleanup(func() { _ = uln.Close() })
	origin := acceptOnce(t, serve)
	go func() {
		c, dialErr := net.Dial("unix", uln.Addr().String())
		if dialErr != nil {
			t.Errorf("guest dial: %v", dialErr)
			_ = uln.Close()
			return
		}
		guest(c)
	}()
	accepted, err := uln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	upstream, err := net.Dial("tcp", origin)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	door := doorShaped{accepted.(*net.UnixConn)}
	t.Cleanup(func() { _ = door.Close(); _ = upstream.Close() })
	return door, upstream
}

func echoHash(c net.Conn, total int64) [32]byte {
	defer func() { _ = c.Close() }()
	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = io.Copy(c, io.LimitReader(new(pattern), total))
		_ = c.(interface{ CloseWrite() error }).CloseWrite()
	})
	h := sha256.New()
	_, _ = io.Copy(h, c)
	wg.Wait()
	return [32]byte(h.Sum(nil))
}

type pattern struct{ off int }

func (p *pattern) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = byte((p.off + i) % 251)
	}
	p.off += len(b)
	return len(b), nil
}

func pump(c net.Conn, total int, buffered chan<- struct{}) {
	buf := make([]byte, 64<<10)
	buffered <- struct{}{}
	for n := 0; n < total; {
		m, err := c.Read(buf)
		n += m
		if err != nil {
			return
		}
	}
	for range total / len(buf) {
		_, _ = c.Write(buf)
	}
}

func bufferAllocs() uint64 {
	runtime.GC()
	s := []metrics.Sample{{Name: "/gc/heap/allocs-by-size:bytes"}}
	metrics.Read(s)
	h := s[0].Value.Float64Histogram()
	var n uint64
	for i, c := range h.Counts {
		if h.Buckets[i] >= 16<<10 {
			n += c
		}
	}
	return n
}
