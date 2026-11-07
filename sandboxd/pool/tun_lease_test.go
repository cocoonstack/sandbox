package pool

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/archives/archivestest"
	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestLeaseExpiryStopsReplicatedTunUntilOwnershipIsConfirmed(t *testing.T) {
	for _, name := range []string{"kept", "taken", "unreplicated"} {
		t.Run(name, func(t *testing.T) {
			spec := config.PoolSpec{PoolKey: testKey, Tun: true, Egress: anyPolicy}
			if name != "unreplicated" {
				spec.ReplicateSeconds = 60
			}
			m := tunManager(t, newFakeEngine(), &fakeAddrs{}, spec)
			reg := archivestest.NewMem()
			cut := &cutRegistry{Registry: reg}
			m.archives, m.nodeID = cut, "a"
			sb := mustClaim(t, m, testKey)
			if sb.Replicated {
				replicateNow(t, m, sb)
			}
			synctest.Test(t, func(t *testing.T) {
				startLease(t, m)
				synctest.Wait()
				stream := leaseTunStream(t, m, sb)
				probeLeaseTun(t, m, stream, true)
				cut.cut.Store(true)
				time.Sleep(leaseWindow + time.Second)
				synctest.Wait()
				if !m.isFenced() {
					t.Fatal("the expired lease did not fence the node")
				}
				for range 2 {
					m.trip(t.Context())
					probeLeaseTun(t, m, stream, !sb.Replicated)
				}
				_ = stream.Close()
				stream = leaseTunStream(t, m, sb)
				probeLeaseTun(t, m, stream, !sb.Replicated)
				if name == "taken" {
					reg.Expire("a")
					row, _, err := reg.Get(t.Context(), sb.ID)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = reg.Take(t.Context(), sb.ID, row.Epoch, "b"); err != nil {
						t.Fatal(err)
					}
				}
				cut.cut.Store(false)
				time.Sleep(leaseRenew)
				synctest.Wait()
				if m.isFenced() {
					t.Fatal("the renewed and confirmed lease stayed fenced")
				}
				if name != "taken" {
					probeLeaseTun(t, m, stream, true)
					m.unfence(t.Context(), time.Now())
					probeLeaseTun(t, m, stream, true)
					return
				}
				if _, held := m.byID(sb.ID); held {
					t.Fatal("the former owner retained the taken claim")
				}
				if _, err := stream.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
					t.Errorf("the taken claim's old stream read %v, want EOF", err)
				}
				before := m.tunRouter.Stats().ARPSent
				m.tunRouter.Reload(m.cfg.Tun.Overlay())
				if got := m.tunRouter.Stats().ARPSent; got != before {
					t.Errorf("the former owner resumed ARP: %d -> %d", before, got)
				}
			})
		})
	}
}

func leaseTunStream(t *testing.T, m *Manager, sb *types.Sandbox) net.Conn {
	t.Helper()
	host, guest := net.Pipe()
	t.Cleanup(func() { _ = guest.Close() })
	go func() { _ = m.tunRouter.Serve(host, sb.TunAddr) }()
	if !helloOn(t, guest) {
		t.Fatal("the packet stream is off")
	}
	return guest
}

func probeLeaseTun(t *testing.T, m *Manager, stream net.Conn, allowed bool) {
	t.Helper()
	before := m.tunRouter.Stats()
	pkt := udpFrame(netip.MustParseAddr("93.184.216.34"))
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(pkt)))
	if _, err := stream.Write(append(b, pkt...)); err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
	m.tunRouter.Reload(m.cfg.Tun.Overlay())
	want := uint64(0)
	if allowed {
		want = 1
	}
	got := m.tunRouter.Stats()
	if got.DgramsOut-before.DgramsOut != want || got.ARPSent-before.ARPSent != want {
		t.Errorf("allowed %v: sent %d packets and %d ARPs, want %d each", allowed, got.DgramsOut-before.DgramsOut, got.ARPSent-before.ARPSent, want)
	}
}
