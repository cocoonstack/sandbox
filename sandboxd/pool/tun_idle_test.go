package pool

import (
	"encoding/binary"
	"net/netip"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

func TestTunTrafficRestartsIdleClockButAnOpenStreamDoesNotHold(t *testing.T) {
	m := tunManager(t, newFakeEngine(), &fakeAddrs{}, config.PoolSpec{
		PoolKey: testKey, Tun: true, Egress: anyPolicy, IdleHibernateSeconds: 60,
	})
	sb := mustClaim(t, m, testKey)
	stream := openStream(t, sb)
	for i := range 3 {
		backdate(m, sb, 2*time.Minute)
		pkt := udpFrame(netip.MustParseAddr("93.184.216.34"))
		b := binary.LittleEndian.AppendUint32(nil, uint32(len(pkt)))
		if _, err := stream.Write(append(b, pkt...)); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return m.tunRouter.Stats().DgramsOut == uint64(i+1) })
		sweepIdle(t, m)
		if hibernated(m) != 0 {
			t.Fatal("active TUN claim was idle-hibernated")
		}
	}
	backdate(m, sb, 2*time.Minute)
	sweepIdle(t, m)
	if hibernated(m) != 1 {
		t.Fatal("silent TUN stream prevented idle hibernation")
	}
}
