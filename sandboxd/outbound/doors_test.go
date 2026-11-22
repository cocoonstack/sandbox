package outbound

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestEgressDoorHalfCloseReachesTheGuest(t *testing.T) {
	origin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("origin: %v", err)
	}
	t.Cleanup(func() { _ = origin.Close() })
	go func() {
		conn, acceptErr := origin.Accept()
		if acceptErr != nil {
			return
		}
		_, _ = io.WriteString(conn, "hello")
		_ = conn.Close()
	}()
	_, path := armDialingDoor(t, "sb_halfclose", &egress.Policy{Allow: []egress.Rule{{Host: "127.0.0.1"}}})
	_, tunnel := outboundtest.ConnectDoor(t, path, origin.Addr().String())
	body, bodyErr := io.ReadAll(tunnel)
	if bodyErr != nil {
		t.Fatalf("tunnel did not end with EOF after the origin closed: %v", bodyErr)
	}
	if string(body) != "hello" {
		t.Errorf("tunnel body %q, want hello", body)
	}
}

func TestDoorConnHandsSpliceItsUnixConn(t *testing.T) {
	ln, err := listenUnix(filepath.Join(outboundtest.SockRoot(t), "d"))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	door := limitDoor(ln)
	t.Cleanup(func() { _ = door.Close() })
	peer, err := net.Dial("unix", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	conn, err := door.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	inner, ok := conn.(interface{ NetConn() net.Conn })
	if !ok {
		t.Fatalf("door conn %T hides its socket from splice", conn)
	}
	if _, ok := inner.NetConn().(*net.UnixConn); !ok {
		t.Fatalf("NetConn is %T, want the *net.UnixConn the kernel copy needs", inner.NetConn())
	}
	_ = conn.Close()
	if n := len(door.slots); n != 0 {
		t.Errorf("%d slots held after close", n)
	}
}

func TestEgressDoorClosesAConnectionPastItsCap(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(origin.Close)
	h, path := armDialingDoor(t, "sb_cap", &egress.Policy{Allow: []egress.Rule{{Host: outboundtest.Hostname(t, origin.URL)}}})

	idle := make([]net.Conn, 0, doorConns)
	deadline := time.Now().Add(10 * time.Second)
	for len(idle) < doorConns {
		conn, err := net.Dial("unix", path)
		if (errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EAGAIN)) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("idle dial %d: %v", len(idle), err)
		}
		idle = append(idle, conn)
	}
	t.Cleanup(func() {
		for _, conn := range idle {
			_ = conn.Close()
		}
	})

	waitHeld(t, h, "sb_cap", doorConns)

	for i := range 2 * doorConns {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("dial %d past the cap: %v, want the door to accept it", i, err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Read(make([]byte, 1))
		_ = conn.Close()
		if !errors.Is(err, io.EOF) {
			t.Fatalf("dial %d past the cap: read %v, want it closed on accept", i, err)
		}
	}

	_ = idle[0].Close()
	client := outboundtest.EgressClient(path)
	client.Timeout = 5 * time.Second
	deadline = time.Now().Add(5 * time.Second)
	resp, err := client.Get(origin.URL + "/")
	for err != nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		resp, err = client.Get(origin.URL + "/")
	}
	if err != nil {
		t.Fatalf("request after a slot freed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d after a slot freed, want 200", resp.StatusCode)
	}
}

func TestARearmedClaimKeepsADoorItsGuestCanDial(t *testing.T) {
	pol := &egress.Policy{Socks5: true, Allow: []egress.Rule{{Host: "example.com"}}}
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, pol), nil)
	sb := outboundtest.VsockSandbox(t, "sb_rearm", testKey)
	t.Cleanup(func() { h.Disarm(sb.ID, true) })
	for range 2 {
		if err := h.ArmProxy(t.Context(), sb); err != nil {
			t.Fatalf("arm proxy: %v", err)
		}
	}
	for _, path := range []string{engine.EgressSocketPath(sb.VsockSocket), engine.SocksSocketPath(sb.VsockSocket)} {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("dial after the second arm: %v", err)
		}
		_ = conn.Close()
	}
}

func TestARearmWithNoPolicyLeftClosesTheDoor(t *testing.T) {
	var cur atomic.Pointer[View]
	cur.Store(NewView(poolConfig(testKey, &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}), nil))
	h := newHost(t, &fakeEngine{}, &config.Config{}, func(o *Options) { o.View = cur.Load })
	sb := outboundtest.VsockSandbox(t, "sb_rearm_none", testKey)
	if err := h.ArmProxy(t.Context(), sb); err != nil {
		t.Fatalf("arm proxy: %v", err)
	}
	cur.Store(NewView(&config.Config{}, nil))
	if err := h.ArmProxy(t.Context(), sb); err != nil {
		t.Fatalf("second arm: %v", err)
	}
	if route := h.Route(sb); route != types.NetRouteNone {
		t.Errorf("route %q, want none", route)
	}
	if _, err := net.Dial("unix", engine.EgressSocketPath(sb.VsockSocket)); err == nil {
		t.Error("the door still accepts with no policy left")
	}
}

func TestAClaimDisarmedWhileItsDoorsBindGetsNoDoors(t *testing.T) {
	for _, tt := range []struct {
		name     string
		inBoot   bool
		inEgress bool
	}{
		{name: "during the boot bind", inBoot: true},
		{name: "after the boot door published", inEgress: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var h *Host
			var disarmed atomic.Bool
			id := "sb_doors_race"
			disarm := func() {
				if disarmed.CompareAndSwap(false, true) {
					h.Disarm(id, true)
				}
			}
			h = newHost(t, &fakeEngine{}, poolConfig(testKey, egPolicy), func(o *Options) {
				view := o.View
				o.View = func() *View {
					if h != nil && tt.inEgress {
						disarm()
					}
					return view()
				}
			})
			sb := outboundtest.VsockSandbox(t, id, testKey)

			err := h.armProxy(t.Context(), sb, func(path string) (*net.UnixListener, error) {
				ln, err := listenUnix(path)
				if tt.inBoot {
					disarm()
				}
				return ln, err
			})
			if err != nil {
				t.Fatalf("arm proxy: %v", err)
			}

			for _, path := range []string{engine.BootSocketPath(sb.VsockSocket), engine.EgressSocketPath(sb.VsockSocket)} {
				if conn, err := net.Dial("unix", path); err == nil {
					_ = conn.Close()
					t.Errorf("%s accepts for a claim disarmed mid-arm", path)
				}
			}
			h.mu.Lock()
			_, keptDoors := h.doors[id]
			_, keptBoot := h.boots[id]
			h.mu.Unlock()
			if keptDoors || keptBoot {
				t.Errorf("entries left: doors %v, boot %v, want none", keptDoors, keptBoot)
			}
		})
	}
}

func TestAReloadDuringAnArmSkipsTheReservedSlot(t *testing.T) {
	var h *Host
	var reloaded atomic.Bool
	h = newHost(t, &fakeEngine{}, poolConfig(testKey, egPolicy), func(o *Options) {
		view := o.View
		o.View = func() *View {
			if h != nil && reloaded.CompareAndSwap(false, true) {
				h.Reloaded()
			}
			return view()
		}
	})
	sb := outboundtest.VsockSandbox(t, "sb_reload_arm", testKey)
	t.Cleanup(func() { h.Disarm(sb.ID, true) })

	if err := h.ArmProxy(t.Context(), sb); err != nil {
		t.Fatalf("arm proxy: %v", err)
	}

	if conn, err := net.Dial("unix", engine.EgressSocketPath(sb.VsockSocket)); err != nil {
		t.Errorf("dial after an arm a reload crossed: %v", err)
	} else {
		_ = conn.Close()
	}
}

func TestAPolicylessArmLeavesNoDoorsEntry(t *testing.T) {
	h := newHost(t, &fakeEngine{}, &config.Config{}, nil)
	sb := outboundtest.VsockSandbox(t, "sb_no_policy", testKey)
	t.Cleanup(func() { h.Disarm(sb.ID, true) })

	if err := h.ArmProxy(t.Context(), sb); err != nil {
		t.Fatalf("arm proxy: %v", err)
	}

	h.mu.Lock()
	_, kept := h.doors[sb.ID]
	h.mu.Unlock()
	if kept {
		t.Error("a claim with no policy kept a doors entry")
	}
}

func TestTheProxyDoorBoundsAnIdleKeepAlive(t *testing.T) {
	h, _ := armDialingDoor(t, "sb_idle", &egress.Policy{Allow: []egress.Rule{{Host: "127.0.0.1"}}})
	h.mu.Lock()
	idle := h.doors["sb_idle"].srv.IdleTimeout
	h.mu.Unlock()
	if idle <= 0 {
		t.Errorf("door idle timeout %v, want a bound so an idle keep-alive frees its slot", idle)
	}
}

func armDialingDoor(t *testing.T, id string, pol *egress.Policy) (*Host, string) {
	t.Helper()
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, pol), func(o *Options) { o.Dial = (&net.Dialer{}).DialContext })
	sb := outboundtest.VsockSandbox(t, id, testKey)
	if err := h.Arm(t.Context(), sb); err != nil {
		t.Fatalf("arm egress: %v", err)
	}
	t.Cleanup(func() { h.Disarm(sb.ID, true) })
	return h, engine.EgressSocketPath(sb.VsockSocket)
}

func waitHeld(t *testing.T, h *Host, id string, want int) {
	t.Helper()
	h.mu.Lock()
	door := h.doors[id].ln.(*doorListener)
	h.mu.Unlock()
	for range 500 {
		if len(door.slots) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("door holds %d connections, want %d", len(door.slots), want)
}
