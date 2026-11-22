package outbound

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestTheBootDoorOfAPolicylessClaimHandsANoticeOnAndAcks(t *testing.T) {
	notices := make(chan string, 4)
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, nil), func(o *Options) {
		o.Rebooted = func(_ context.Context, _ *types.Sandbox, bootID string) { notices <- bootID }
	})
	sb := outboundtest.VsockSandbox(t, "sb_boot", testKey)
	sb.VMName = "sbx-boot"
	path := engine.BootSocketPath(sb.VsockSocket)

	h.Prebind(t.Context(), sb)
	if _, err := os.Stat(path); err != nil || prebootCount(h) != 1 {
		t.Fatalf("prebound boot doors %d, stat %v, want the door bound at refill", prebootCount(h), err)
	}
	if conn, err := net.Dial("unix", path); err == nil {
		_ = conn.Close()
		t.Fatal("the prebound boot door accepts before the claim arms it")
	}
	if err := h.ArmProxy(t.Context(), sb); err != nil {
		t.Fatalf("arm proxy: %v", err)
	}
	if n := prebootCount(h); n != 0 {
		t.Errorf("%d prebound boot doors after the arm, want the claim to take its own", n)
	}

	if ack := sendBootNotice(t, path, "5f1c0e2a-77b1-4c43-9d2e-0a1b2c3d4e5f\n"); ack != "\n" {
		t.Errorf("ack %q, want one byte", ack)
	}
	if got := <-notices; got != "5f1c0e2a-77b1-4c43-9d2e-0a1b2c3d4e5f" {
		t.Errorf("notice %q, want the boot id", got)
	}
	for _, bad := range []string{"NOT A BOOT ID\n", "\n", "5f1c0e2a-77b1-4c43-9d2e-0a1b2c3d4e5f", "5f1c0e2a-77b1\n"} {
		if ack := sendBootNotice(t, path, bad); ack != "" {
			t.Errorf("malformed notice %q got ack %q, want a close", bad, ack)
		}
	}
	select {
	case got := <-notices:
		t.Errorf("a malformed notice reached Rebooted as %q", got)
	default:
	}

	h.Disarm(sb.ID, true)
	if conn, err := net.Dial("unix", path); err == nil {
		_ = conn.Close()
		t.Error("the boot door still accepts after disarm")
	}
}

func TestARearmReplacesTheBootDoorAndClosePreboundDropsAnUnclaimedOne(t *testing.T) {
	notices := make(chan string, 4)
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, egPolicy), func(o *Options) {
		o.Rebooted = func(_ context.Context, _ *types.Sandbox, bootID string) { notices <- bootID }
	})
	sb := outboundtest.VsockSandbox(t, "sb_boot_rearm", testKey)
	t.Cleanup(func() { h.Disarm(sb.ID, true) })
	for range 2 {
		if err := h.ArmProxy(t.Context(), sb); err != nil {
			t.Fatalf("arm proxy: %v", err)
		}
	}
	if ack := sendBootNotice(t, engine.BootSocketPath(sb.VsockSocket), "0f6a3c1e-2b4d-4e8f-9a7c-5d3e1b2a4c6f\n"); ack != "\n" || <-notices != "0f6a3c1e-2b4d-4e8f-9a7c-5d3e1b2a4c6f" {
		t.Errorf("ack %q after a re-arm, want the notice taken", ack)
	}

	warm := outboundtest.VsockSandbox(t, "", testKey)
	warm.VMName = "sbx-warm"
	h.Prebind(t.Context(), warm)
	h.ClosePrebound(warm.VMName)
	if _, err := os.Stat(engine.BootSocketPath(warm.VsockSocket)); !errors.Is(err, fs.ErrNotExist) || prebootCount(h) != 0 {
		t.Errorf("prebound boot doors %d, stat %v, want the unclaimed door closed and unlinked", prebootCount(h), err)
	}
}

func TestTheBootDoorPacesTheNoticesItHandsOn(t *testing.T) {
	handed := make(chan time.Time, 4)
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, nil), func(o *Options) {
		o.Rebooted = func(context.Context, *types.Sandbox, string) { handed <- time.Now() }
	})
	sb := outboundtest.VsockSandbox(t, "sb_boot_pace", testKey)
	t.Cleanup(func() { h.Disarm(sb.ID, true) })
	if err := h.ArmProxy(t.Context(), sb); err != nil {
		t.Fatalf("arm proxy: %v", err)
	}
	path := engine.BootSocketPath(sb.VsockSocket)

	sendBootNotice(t, path, "5f1c0e2a-77b1-4c43-9d2e-0a1b2c3d4e5f\n")
	sendBootNotice(t, path, "0f6a3c1e-2b4d-4e8f-9a7c-5d3e1b2a4c6f\n")

	first, second := <-handed, <-handed
	if gap := second.Sub(first); gap < bootNoticeGap*9/10 {
		t.Errorf("Rebooted ran %v apart, want the door to hold the second notice about %v", gap, bootNoticeGap)
	}
}

func TestABootDoorDisarmedWhileItBindsIsClosedNotPublished(t *testing.T) {
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, nil), nil)
	sb := outboundtest.VsockSandbox(t, "sb_boot_race", testKey)
	path := engine.BootSocketPath(sb.VsockSocket)

	if err := h.armProxy(t.Context(), sb, func(path string) (*net.UnixListener, error) {
		ln, err := listenUnix(path)
		h.Disarm(sb.ID, true)
		return ln, err
	}); err != nil {
		t.Fatalf("arm proxy: %v", err)
	}

	if conn, err := net.Dial("unix", path); err == nil {
		_ = conn.Close()
		t.Error("the boot door of a claim disarmed while it bound accepts")
	}
	if n := bootEntries(h, sb.ID); n != 0 {
		t.Errorf("%d boot door entries left, want none", n)
	}
}

func TestABootDoorThatCannotBindLeavesNoEntry(t *testing.T) {
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, nil), nil)
	sb := outboundtest.VsockSandbox(t, "sb_boot_nobind", testKey)

	if err := h.armProxy(t.Context(), sb, func(string) (*net.UnixListener, error) { return nil, errors.New("bind refused") }); err != nil {
		t.Fatalf("arm proxy: %v", err)
	}

	if n := bootEntries(h, sb.ID); n != 0 {
		t.Errorf("%d boot door entries left after a failed bind, want none", n)
	}
}

func bootEntries(h *Host, id string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.boots[id]; ok {
		return 1
	}
	return 0
}

func prebootCount(h *Host) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.preboots)
}

func sendBootNotice(t *testing.T, path, line string) string {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial the boot door: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err = io.WriteString(conn, line); err != nil {
		t.Fatalf("write the notice: %v", err)
	}
	if !strings.HasSuffix(line, "\n") {
		_ = conn.(*net.UnixConn).CloseWrite()
	}
	ack, _ := io.ReadAll(conn)
	return string(ack)
}
