package tunnet

import (
	"net/netip"
	"testing"
	"time"
)

func TestOnlyAdmittedTrafficTouchesClaim(t *testing.T) {
	u := &fakeUnder{out: make(chan sent, 64)}
	r := New(u, testOverlay(1450))
	touches := 0
	r.Arm(Claim{
		ID: "c1", Addr: claimAddr,
		Allow: func(_ netip.Addr, port uint16) bool { return port == 443 },
		Touch: func(time.Time) { touches++ },
	})
	a := r.claims[claimAddr]
	l := &link{r: r, a: a, mtu: 1450}
	if !a.attach(l) {
		t.Fatal("attach failed")
	}
	if touches != 0 {
		t.Fatal("arming or attaching touched the claim")
	}
	if drop := l.forward(frame(ProtoTCP, GuestAddr, remote, false)); drop != DropNone || touches != 1 {
		t.Fatalf("outbound drop %v, touches %d; want allowed and one touch", drop, touches)
	}
	if _, drop := r.admit(append(make([]byte, VnetHdrLen), reply(remote)[Overhead:]...)); drop != DropNone || touches != 2 {
		t.Fatalf("inbound drop %v, touches %d; want allowed and two touches", drop, touches)
	}
	if drop := l.forward(frame(ProtoTCP, GuestAddr, claimAddr, false)); drop != DropPolicy {
		t.Fatalf("outbound drop %v, want policy", drop)
	}
	if _, drop := r.admit(nil); drop != DropMalformed {
		t.Fatalf("inbound drop %v, want malformed", drop)
	}
	if touches != 2 {
		t.Fatalf("denied packets touched the claim: %d", touches)
	}
}
