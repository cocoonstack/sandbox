package tunnet

import (
	"bytes"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
)

func TestHostAtSkipsNetworkAndBroadcastAndWraps(t *testing.T) {
	p := netip.MustParsePrefix("10.1.2.0/24")
	for n, want := range map[uint64]string{0: "10.1.2.1", 1: "10.1.2.2", 253: "10.1.2.254", 254: "10.1.2.1", 1<<40 + 5: "10.1.2.38"} {
		if got := HostAt(p, n); got != netip.MustParseAddr(want) {
			t.Errorf("HostAt(%d) = %s, want %s", n, got, want)
		}
	}
	if got := HostAt(netip.MustParsePrefix("240.0.0.0/4"), 0); got != netip.MustParseAddr("240.0.0.1") {
		t.Errorf("HostAt on a /4 = %s, want 240.0.0.1", got)
	}
}

func TestLocalAssignsInOrderAndIdempotently(t *testing.T) {
	dir := t.TempDir()
	l := newLocal(t, dir, time.Hour)
	a, b := assign(t, l, "a"), assign(t, l, "b")
	if a != netip.MustParseAddr("10.1.2.1") || b != netip.MustParseAddr("10.1.2.2") {
		t.Fatalf("assigned %s %s, want .1 .2", a, b)
	}
	if again := assign(t, l, "a"); again != a {
		t.Errorf("a second assign for a = %s, want %s", again, a)
	}
	next := newLocal(t, dir, time.Hour)
	next.Seed(map[string]netip.Addr{"a": a, "b": b})
	if c := assign(t, next, "c"); c != netip.MustParseAddr("10.1.2.3") {
		t.Errorf("after a restart c = %s, want .3 past the seeded claims", c)
	}
	if again := assign(t, next, "b"); again != b {
		t.Errorf("a seeded claim assigned %s, want %s", again, b)
	}
}

func TestLocalSkipsHeldAddressesAndHoldsFreedOnesForTheReuseWindow(t *testing.T) {
	dir := t.TempDir()
	l := newLocal(t, dir, time.Hour)
	l.Seed(map[string]netip.Addr{"x": netip.MustParseAddr("10.1.2.1")})
	a := assign(t, l, "a")
	if a != netip.MustParseAddr("10.1.2.2") {
		t.Fatalf("assigned %s, want .2 past the held .1", a)
	}
	if err := l.Release(t.Context(), "y", netip.MustParseAddr("10.1.2.1")); err != nil {
		t.Fatalf("release of x's address by y: %v", err)
	}
	if err := l.Release(t.Context(), "a", a); err != nil {
		t.Fatalf("release: %v", err)
	}
	l.next = 0
	if b := assign(t, l, "b"); b != netip.MustParseAddr("10.1.2.3") {
		t.Errorf("assigned %s, want .3: x still holds .1 and .2 waits out its window", b)
	}
	held := newLocal(t, dir, time.Hour)
	held.Seed(map[string]netip.Addr{"x": netip.MustParseAddr("10.1.2.1"), "b": netip.MustParseAddr("10.1.2.3")})
	if c := assign(t, held, "c"); c != netip.MustParseAddr("10.1.2.4") {
		t.Errorf("after a restart c = %s, want .4: the freed .2 survives in the log", c)
	}
	expired := newLocal(t, dir, 0)
	expired.next = 1
	if d := assign(t, expired, "d"); d != a {
		t.Errorf("past the window d = %s, want the freed %s", d, a)
	}
}

func TestLocalCompactsTheLogToTheLiveHolds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		l, err := NewLocal(dir, netip.MustParsePrefix("10.1.0.0/16"), 20*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		churn := func(from, to int) {
			for i := from; i < to; i++ {
				id := fmt.Sprint(i)
				if relErr := l.Release(t.Context(), id, assign(t, l, id)); relErr != nil {
					t.Fatalf("release %d: %v", i, relErr)
				}
			}
		}
		churn(0, compactAfter-24)
		time.Sleep(30 * time.Millisecond)
		churn(compactAfter-24, compactAfter+100)
		raw, err := os.ReadFile(filepath.Join(dir, freedFile))
		if err != nil {
			t.Fatal(err)
		}
		if n := bytes.Count(raw, []byte("\n")); n > 200 {
			t.Errorf("log holds %d lines after its expired holds were compacted away, want at most 200", n)
		}
	})
}

func TestLocalRefusesWhenTheSubnetIsFull(t *testing.T) {
	l := newLocal(t, t.TempDir(), time.Hour)
	p := netip.MustParsePrefix("10.1.2.0/24")
	held := map[string]netip.Addr{}
	for n := range uint64(253) {
		held[HostAt(p, n).String()] = HostAt(p, n)
	}
	l.Seed(held)
	last := assign(t, l, "last")
	if last != netip.MustParseAddr("10.1.2.254") {
		t.Fatalf("assigned %s, want the one free .254", last)
	}
	if _, err := l.Assign(t.Context(), "over"); !errors.Is(err, ErrNoAddress) {
		t.Errorf("assign on a full subnet: %v, want ErrNoAddress", err)
	}
}

func newLocal(t *testing.T, dir string, reuse time.Duration) *Local {
	t.Helper()
	l, err := NewLocal(dir, netip.MustParsePrefix("10.1.2.0/24"), reuse)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	return l
}

func assign(t *testing.T, l *Local, id string) netip.Addr {
	t.Helper()
	addr, err := l.Assign(t.Context(), id)
	if err != nil {
		t.Fatalf("assign %s: %v", id, err)
	}
	return addr
}
