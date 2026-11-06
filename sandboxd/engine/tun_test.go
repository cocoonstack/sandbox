package engine

import (
	"slices"
	"strings"
	"testing"
)

func TestTunUpAndDownSendTheirVerbs(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	e := newTestEngine()
	if err := e.TunUp(t.Context(), path); err != nil {
		t.Fatalf("TunUp: %v", err)
	}
	if err := e.TunDown(t.Context(), path); err != nil {
		t.Fatalf("TunDown: %v", err)
	}
	fake.mu.Lock()
	ops := slices.Clone(fake.tunOps)
	fake.tunErr = "no /dev/net/tun"
	fake.mu.Unlock()
	if !slices.Equal(ops, []string{"tun_up", "tun_down"}) {
		t.Errorf("silkd saw %q, want tun_up then tun_down", ops)
	}
	if err := e.TunUp(t.Context(), path); err == nil || !strings.Contains(err.Error(), "no /dev/net/tun") {
		t.Errorf("TunUp on an error frame: %v, want the guest's error", err)
	}
}
