package tunnet

import (
	"net/netip"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestFenceStopsPacketsAndARPAndResumesWithoutResettingQuota(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		u, r := newRouter(t)
		var fenced atomic.Bool
		used := new(atomic.Int64)
		r.Arm(Claim{
			ID: "c1", Addr: claimAddr, Fenced: fenced.Load, Used: used,
			Allow: func(netip.Addr, uint16) bool { return true },
		})
		u.sent(t)
		guest := serve(t, r, claimAddr)
		guestHello(t, guest)
		pkt := frame(ProtoTCP, GuestAddr, remote, false)
		sendFrame(t, guest, pkt)
		u.sent(t)
		u.inject(t, reply(remote))
		readFrame(t, guest)
		synctest.Wait()
		before, charged := r.Stats(), used.Load()
		arp := appendARP(appendHeader(nil, 7, broadcast, exitMAC, etherARP), arpReq, exitMAC, vtep, [6]byte{}, claimAddr)

		for range 2 {
			fenced.Store(true)
			sendFrame(t, guest, pkt)
			u.inject(t, reply(remote))
			u.inject(t, arp)
			r.Reload(testOverlay(1450))
			time.Sleep(refreshEvery)
			synctest.Wait()
			got := r.Stats()
			if got.DgramsOut != before.DgramsOut || got.FramesOut != before.FramesOut || got.ARPSent != before.ARPSent || got.ARPAnswered != before.ARPAnswered {
				t.Errorf("fenced traffic: before %+v, after %+v", before, got)
			}
			if used.Load() != charged || len(u.out) != 0 {
				t.Errorf("fenced claim charged %d bytes and sent %d datagrams", used.Load()-charged, len(u.out))
			}
		}

		_ = guest.Close()
		guest = serve(t, r, claimAddr)
		guestHello(t, guest)
		sendFrame(t, guest, pkt)
		synctest.Wait()
		if got := r.Stats().DgramsOut; got != before.DgramsOut {
			t.Errorf("reconnect escaped the fence: sent %d datagrams, want %d", got, before.DgramsOut)
		}
		fenced.Store(false)
		sendFrame(t, guest, pkt)
		u.sent(t)
		u.inject(t, reply(remote))
		readFrame(t, guest)
		u.inject(t, arp)
		if _, etype, _ := u.sent(t); etype != etherARP {
			t.Fatal("unfenced claim did not answer ARP")
		}
		time.Sleep(refreshEvery)
		synctest.Wait()
		if got := r.Stats(); got.ARPSent != before.ARPSent+1 || got.ARPAnswered != before.ARPAnswered+1 || got.DgramsOut != before.DgramsOut+1 || got.FramesOut != before.FramesOut+1 {
			t.Errorf("unfenced traffic: before %+v, after %+v", before, got)
		}
		if got := used.Load(); got != charged*2 {
			t.Errorf("quota after recovery %d, want %d", got, charged*2)
		}
	})
}
