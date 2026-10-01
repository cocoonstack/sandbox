//go:build linux

package egress

import (
	"net"
	"path/filepath"
	"runtime"
	"testing"
)

func TestSpliceCopiesThroughADoorConnInTheKernel(t *testing.T) {
	const total = 16 << 20
	uln, err := net.Listen("unix", filepath.Join(t.TempDir(), "door"))
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	t.Cleanup(func() { _ = uln.Close() })
	tln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	t.Cleanup(func() { _ = tln.Close() })
	go func() {
		c, acceptErr := tln.Accept()
		if acceptErr != nil {
			return
		}
		pump(c, total)
		_ = c.(*net.TCPConn).CloseWrite()
	}()
	guestDone := make(chan struct{})
	go func() {
		defer close(guestDone)
		c, dialErr := net.Dial("unix", uln.Addr().String())
		if dialErr != nil {
			return
		}
		defer func() { _ = c.Close() }()
		buf := make([]byte, 64<<10)
		for n := 0; n < total; n += len(buf) {
			_, _ = c.Write(buf)
		}
		_ = c.(*net.UnixConn).CloseWrite()
		for {
			if _, readErr := c.Read(buf); readErr != nil {
				return
			}
		}
	}()
	guest, err := uln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	upstream, err := net.Dial("tcp", tln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	door := doorShaped{guest.(*net.UnixConn)}
	defer func() { _ = door.Close(); _ = upstream.Close() }()

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	up, down := splice(door, upstream)
	runtime.ReadMemStats(&after)
	<-guestDone
	if up != total || down != total {
		t.Fatalf("moved %d up and %d down, want %d each", up, down, total)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc >= 32<<10 {
		t.Errorf("splice allocated %d bytes: a door conn fell back to buffered copies", alloc)
	}
}

type doorShaped struct{ *net.UnixConn }

func (c doorShaped) NetConn() net.Conn { return c.UnixConn }

func pump(c net.Conn, total int) {
	buf := make([]byte, 64<<10)
	for n := 0; n < total; {
		m, err := c.Read(buf)
		n += m
		if err != nil {
			return
		}
	}
	for n := 0; n < total; n += len(buf) {
		_, _ = c.Write(buf)
	}
}
