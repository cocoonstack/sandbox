package pool

import (
	"context"
	"errors"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
)

func TestLeaseRenewLoopKeepsTheNodeLeasedUntilItStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newReplicaCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		replicateNow(t, a, sb)
		row, _, _ := c.reg.Get(t.Context(), sb.ID)
		c.expire("a")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		go a.keepLease(ctx)
		time.Sleep(10 * leaseTTL)
		if _, err := c.reg.Take(ctx, sb.ID, row.Epoch, "b"); !errors.Is(err, archives.ErrLost) {
			t.Fatalf("take while the loop renews: %v, want ErrLost", err)
		}
		cancel()
		synctest.Wait()
		time.Sleep(leaseTTL + time.Second)
		if _, err := c.reg.Take(t.Context(), sb.ID, row.Epoch, "b"); err != nil {
			t.Errorf("take after the loop stopped and the lease ran out: %v, want it taken", err)
		}
	})
}

func TestLeaseOverdueRenewalFencesReplicatedClaimsOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newReplicaCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		cut := &cutRegistry{Registry: c.reg}
		a.archives = cut
		replicated, plain := mustClaim(t, a, testKey), mustClaim(t, a, testKey)
		replicateNow(t, a, replicated)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		go a.keepLease(ctx)
		synctest.Wait()
		time.Sleep(4*leaseRenew + 2*time.Second)
		synctest.Wait()
		assertServed(t, a, replicated.ID, replicated.Token)

		cut.cut.Store(true)
		time.Sleep(leaseTTL - leaseMargin - 3*time.Second)
		synctest.Wait()
		assertServed(t, a, replicated.ID, replicated.Token)
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if _, _, _, err := a.WakeAgentSocket(ctx, replicated.ID, replicated.Token); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("relay or exec of a replicated claim past the fence: %v, want ErrLeaseLost", err)
		}
		if _, err := a.DialPort(ctx, replicated.ID, Cred{Token: replicated.Token}, 80); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("port dial past the fence: %v, want ErrLeaseLost", err)
		}
		if _, err := a.DialPort(ctx, replicated.ID, Cred{Operator: true}, 80); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("preview dial past the fence: %v, want ErrLeaseLost", err)
		}
		assertServed(t, a, plain.ID, plain.Token)
		if _, held := a.byID(replicated.ID); !held || a.eng.(*fakeEngine).removed(lockedVMName(replicated)) {
			t.Error("the fence dropped the claim or removed its VM")
		}

		cut.cut.Store(false)
		time.Sleep(leaseRenew)
		synctest.Wait()
		assertServed(t, a, replicated.ID, replicated.Token)
	})
}

func TestLeaseFenceClosesOpenStreamsOfReplicatedClaimsOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newReplicaCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		cut := &cutRegistry{Registry: c.reg}
		a.archives = cut
		replicated, plain := mustClaim(t, a, testKey), mustClaim(t, a, testKey)
		replicateNow(t, a, replicated)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go a.keepLease(ctx)
		synctest.Wait()

		replicatedCred, plainCred := Cred{Token: replicated.Token}, Cred{Token: plain.Token}
		a.eng.(*fakeEngine).guestDial = func() (net.Conn, error) {
			host, _ := net.Pipe()
			return host, nil
		}
		streams := map[string]net.Conn{}
		defer func() {
			for _, conn := range streams {
				_ = conn.Close()
			}
		}()
		for name, dial := range map[string]func() (net.Conn, error){
			"port":    func() (net.Conn, error) { return a.DialPort(ctx, replicated.ID, replicatedCred, 80) },
			"passive": func() (net.Conn, error) { return a.DialPort(ctx, replicated.ID, Cred{Operator: true}, 80) },
			"preview": func() (net.Conn, error) { return a.PreviewDial(ctx, replicated.ID, 80) },
			"plain":   func() (net.Conn, error) { return a.DialPort(ctx, plain.ID, plainCred, 80) },
		} {
			conn, err := dial()
			if err != nil {
				t.Fatalf("open %s: %v", name, err)
			}
			streams[name] = conn
		}
		_, relayFenced, relayDone, err := a.WakeAgentSocket(ctx, replicated.ID, replicated.Token)
		if err != nil {
			t.Fatalf("relay of the replicated claim: %v", err)
		}
		defer relayDone()
		_, plainFenced, plainDone, err := a.WakeAgentSocket(ctx, plain.ID, plain.Token)
		if err != nil || plainFenced != nil {
			t.Fatalf("relay of the plain claim: fence channel %v, %v, want none", plainFenced, err)
		}
		defer plainDone()

		cut.cut.Store(true)
		time.Sleep(leaseTTL - leaseMargin + time.Second)
		synctest.Wait()
		select {
		case <-relayFenced:
		default:
			t.Error("the replicated claim's relay was not told to end")
		}
		for name, conn := range streams {
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			_, readErr := conn.Read(make([]byte, 1))
			closed := !errors.Is(readErr, os.ErrDeadlineExceeded)
			if closed != (name != "plain") {
				t.Errorf("%s stream closed=%v (%v) after the fence, want closed=%v", name, closed, readErr, name != "plain")
			}
		}

		cut.cut.Store(false)
		time.Sleep(leaseRenew)
		synctest.Wait()
		conn, err := a.DialPort(ctx, replicated.ID, replicatedCred, 80)
		if err != nil {
			t.Fatalf("a new stream after the fence lifted: %v", err)
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		if _, readErr := conn.Read(make([]byte, 1)); !errors.Is(readErr, os.ErrDeadlineExceeded) {
			t.Errorf("the new stream after unfence: %v, want it open", readErr)
		}
		_, fresh, done, err := a.WakeAgentSocket(ctx, replicated.ID, replicated.Token)
		if err != nil {
			t.Fatalf("relay after the fence lifted: %v", err)
		}
		defer done()
		select {
		case <-fresh:
			t.Error("the fence channel after unfence is already closed")
		default:
		}
	})
}

func TestLeaseAdoptionWaitsOutTheFenceAndTheOldOwnerDropsItsCopy(t *testing.T) {
	for _, tt := range []struct {
		name       string
		wakeBefore bool
	}{
		{"recovers before the adopter wakes", false},
		{"recovers after the adopter woke", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newReplicaCluster(t)
				a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
				cut := &cutRegistry{Registry: c.reg}
				a.archives = cut
				sb := mustClaim(t, a, testKey)
				vm := lockedVMName(sb)
				replicateNow(t, a, sb)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				go a.keepLease(ctx)
				synctest.Wait()

				cut.cut.Store(true)
				time.Sleep(leaseTTL - leaseMargin + time.Second)
				synctest.Wait()
				if _, _, _, err := a.WakeAgentSocket(ctx, sb.ID, sb.Token); !errors.Is(err, ErrLeaseLost) {
					t.Fatalf("the cut-off owner past its fence: %v, want ErrLeaseLost", err)
				}
				if adopted, err := b.AdoptArchived(ctx, sb.ID, sb.Token); err != nil || adopted {
					t.Fatalf("adoption while the fenced owner's lease is still live: %v %v, want refused", adopted, err)
				}

				time.Sleep(leaseMargin)
				if adopted, err := b.AdoptArchived(ctx, sb.ID, sb.Token); err != nil || !adopted {
					t.Fatalf("adoption after the lease expired: %v %v, want adopted", adopted, err)
				}
				if tt.wakeBefore {
					if _, _, _, err := b.WakeAgentSocket(ctx, sb.ID, sb.Token); err != nil {
						t.Fatalf("adopter's wake: %v", err)
					}
				}

				cut.cut.Store(false)
				time.Sleep(leaseRenew)
				synctest.Wait()
				if _, held := a.byID(sb.ID); held || !a.eng.(*fakeEngine).removed(vm) {
					t.Errorf("old owner holds the claim %v, VM removed %v, want its copy dropped and its VM removed", held, a.eng.(*fakeEngine).removed(vm))
				}
				if a.fenced.Load() {
					t.Error("the old owner stays fenced after its lease renewed")
				}
				if _, _, _, err := b.WakeAgentSocket(ctx, sb.ID, sb.Token); err != nil {
					t.Errorf("adopter's wake after the old owner dropped its copy: %v", err)
				}
			})
		})
	}
}

type cutRegistry struct {
	archives.Registry
	cut atomic.Bool
}

func (r *cutRegistry) Renew(ctx context.Context, node string, ttl time.Duration) error {
	if r.cut.Load() {
		return archives.ErrUnavailable
	}
	return r.Registry.Renew(ctx, node, ttl)
}

func (r *cutRegistry) Rows(ctx context.Context, ids []string) (map[string]archives.Record, error) {
	if r.cut.Load() {
		return nil, archives.ErrUnavailable
	}
	return r.Registry.Rows(ctx, ids)
}

func assertServed(t *testing.T, m *Manager, id, token string) {
	t.Helper()
	_, _, done, err := m.WakeAgentSocket(t.Context(), id, token)
	if err != nil {
		t.Fatalf("serving %s: %v", id, err)
	}
	done()
}
