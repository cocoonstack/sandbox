package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestLeaseRenewLoopKeepsTheNodeLeasedUntilItStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newReplicaCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		replicateNow(t, a, sb)
		c.expire("a")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		go a.keepLease(ctx)
		time.Sleep(10 * leaseTTL)
		row, _, _ := c.reg.Get(ctx, sb.ID)
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

func TestAnUnfenceOnALiftedNodeKeepsTheOpenFenceChannel(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	lifted := m.fence.Load()
	m.unfence(t.Context(), time.Now())
	if m.fence.Load() != lifted {
		t.Error("an unfence replaced a lifted state, so open streams would miss the next trip")
	}
}

func TestLeaseOverdueRenewalFencesReplicatedClaimsOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newReplicaCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		cut := &cutRegistry{Registry: c.reg}
		a.archives = cut
		replicated, plain := mustClaim(t, a, testKey), mustClaim(t, a, plainKey)
		replicateNow(t, a, replicated)
		ctx := startLease(t, a)
		synctest.Wait()
		time.Sleep(4*leaseRenew + 2*time.Second)
		synctest.Wait()
		assertServed(t, a, replicated.ID, replicated.Token)

		cut.cut.Store(true)
		time.Sleep(leaseWindow - 3*time.Second)
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
		if _, held := a.byID(replicated.ID); !held || fakeOf(a).removed(lockedVMName(replicated)) {
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
		replicated, plain, early := mustClaim(t, a, testKey), mustClaim(t, a, plainKey), mustClaim(t, a, testKey)
		replicateNow(t, a, replicated)
		ctx := startLease(t, a)
		synctest.Wait()

		replicatedCred, plainCred := Cred{Token: replicated.Token}, Cred{Token: plain.Token}
		fakeOf(a).guestDial = func() (net.Conn, error) {
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
			"early":   func() (net.Conn, error) { return a.DialPort(ctx, early.ID, Cred{Token: early.Token}, 80) },
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
		time.Sleep(leaseWindow + time.Second)
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

func TestLeaseSlowUnfenceStaysFencedAndKeepsRenewing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newReplicaCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		cut := &cutRegistry{Registry: c.reg}
		a.archives = cut
		sb := mustClaim(t, a, testKey)
		replicateNow(t, a, sb)
		ctx := startLease(t, a)
		synctest.Wait()

		cut.cut.Store(true)
		time.Sleep(leaseTTL)
		synctest.Wait()
		cut.cut.Store(false)
		cut.rowsDelay.Store(int64(leaseTTL))
		time.Sleep(leaseRenew)
		time.Sleep(leaseTTL + time.Second)
		synctest.Wait()
		if _, _, _, err := a.WakeAgentSocket(ctx, sb.ID, sb.Token); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("serving after a confirmation that outlasted the lease window: %v, want ErrLeaseLost", err)
		}
		row, _, _ := c.reg.Get(ctx, sb.ID)
		if _, err := c.reg.Take(ctx, sb.ID, row.Epoch, "b"); !errors.Is(err, archives.ErrLost) {
			t.Errorf("take while the slow unfence is renewing: %v, want ErrLost: renewals must go on", err)
		}

		cut.rowsDelay.Store(0)
		time.Sleep(leaseTTL + 2*leaseRenew)
		synctest.Wait()
		assertServed(t, a, sb.ID, sb.Token)
	})
}

func TestLeaseFailedConfirmKeepsTheTeardownOfAForgottenClaim(t *testing.T) {
	for range 64 {
		exercised := false
		synctest.Test(t, func(t *testing.T) {
			c := newReplicaCluster(t)
			a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
			moved, kept := mustClaim(t, a, testKey), mustClaim(t, a, testKey)
			vm := lockedVMName(moved)
			cut := &cutRegistry{Registry: c.reg, takeErr: func(id string) error {
				a.mu.Lock()
				_, known := a.claimed[moved.ID]
				a.mu.Unlock()
				if id != kept.ID || known {
					return nil
				}
				exercised = true
				return archives.ErrUnavailable
			}}
			a.archives = cut
			replicateNow(t, a, moved)
			replicateNow(t, a, kept)
			startLease(t, a)
			synctest.Wait()

			cut.cut.Store(true)
			time.Sleep(leaseTTL)
			synctest.Wait()
			c.expire("a")
			mustAdopt(t, b, moved, "adopt while the owner is fenced")
			cut.cut.Store(false)
			time.Sleep(leaseRenew)
			synctest.Wait()
			if exercised && !fakeOf(a).removed(vm) {
				t.Error("a failed confirmation dropped the teardown of a claim the owner had already forgotten")
			}
		})
		if exercised {
			return
		}
	}
	t.Fatal("the failing confirmation never followed the forgotten claim")
}

func TestLeaseAdoptionDuringUnfenceDropsTheClaimWithoutServingIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newReplicaCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		cut := &cutRegistry{Registry: c.reg}
		a.archives = cut
		sb := mustClaim(t, a, testKey)
		vm := lockedVMName(sb)
		replicateNow(t, a, sb)
		ctx := startLease(t, a)
		synctest.Wait()

		cut.cut.Store(true)
		time.Sleep(leaseTTL)
		synctest.Wait()
		fencedAtTake := false
		cut.beforeTake = func() {
			fencedAtTake = a.isFenced()
			c.expire("a")
			if adopted, err := b.AdoptArchived(ctx, sb.ID, sb.Token); err != nil || !adopted {
				t.Errorf("adopt during the owner's unfence: %v %v", adopted, err)
			}
		}
		cut.cut.Store(false)
		time.Sleep(leaseRenew)
		synctest.Wait()

		if !fencedAtTake {
			t.Error("the owner served again before it confirmed the row")
		}
		if _, _, _, err := a.WakeAgentSocket(ctx, sb.ID, sb.Token); !errors.Is(err, ErrUnknownSandbox) {
			t.Errorf("the owner after losing the row: %v, want ErrUnknownSandbox", err)
		}
		if !fakeOf(a).removed(vm) {
			t.Error("the owner kept its VM of the adopted claim")
		}
		if _, _, _, err := b.WakeAgentSocket(ctx, sb.ID, sb.Token); err != nil {
			t.Errorf("the adopter's wake: %v", err)
		}
	})
}

func TestLeaseANodeLiftedAfterAnOutageAdoptsOnlyOnceEveryOwnerCouldRenew(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newReplicaCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		vm := lockedVMName(sb)
		replicateNow(t, a, sb)
		a.tripFence()
		b.tripFence()
		c.expire("a")
		c.expire("b")

		mustNotAdopt(t, b, sb, "adoption by a fenced node")
		c.renew(t, "b")
		b.unfence(t.Context(), time.Now())
		if b.isFenced() {
			t.Fatal("the node stayed fenced after its renewal")
		}
		mustNotAdopt(t, b, sb, "adoption right after the lift")
		c.renew(t, "a")
		a.unfence(t.Context(), time.Now())
		synctest.Wait()
		if _, held := a.byID(sb.ID); !held || fakeOf(a).removed(vm) {
			t.Fatalf("the healthy owner lost its claim: held=%v VM removed=%v", held, fakeOf(a).removed(vm))
		}

		c.expire("a")
		time.Sleep(adoptGrace - time.Millisecond)
		mustNotAdopt(t, b, sb, "adoption inside the grace")
		time.Sleep(time.Millisecond)
		mustAdopt(t, b, sb, "adoption once the grace passed")
	})
}

func TestLeaseFencedNodeDoesNotAnswerAsOwnerOfReplicatedClaims(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	replicated, plain := mustClaim(t, a, testKey), mustClaim(t, a, plainKey)
	replicateNow(t, a, replicated)
	a.tripFence()

	if _, err := a.AgentSocket(replicated.ID, replicated.Token); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("owner lookup of a replicated claim while fenced: %v, want ErrLeaseLost", err)
	}
	if _, err := a.AgentSocket(plain.ID, plain.Token); err != nil {
		t.Errorf("owner lookup of an unreplicated claim while fenced: %v", err)
	}
	a.liftFence()
	if _, err := a.AgentSocket(replicated.ID, replicated.Token); err != nil {
		t.Errorf("owner lookup once the fence lifted: %v", err)
	}
}

func TestLeaseFencedNodeLeavesAReplicatedClaimsExitedVMMAlone(t *testing.T) {
	c := newReplicaCluster(t)
	eng := newFakeEngine()
	a := c.nodeOn(t, eng, "a", t.TempDir(), keepAnHour)
	a.liftFence()
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	a.tripFence()
	before := usageEventsOf(t, a, sb.ID)
	killVMM(eng, sb.VMName)
	a.recoverVMM(t.Context(), sb)
	if got := usageEventsOf(t, a, sb.ID); got != before {
		t.Errorf("events %q after %q, want nothing journaled for a claim a peer may have adopted", got, before)
	}
}

func TestLeaseLiftAfterTheWindowClosedFencesAgain(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	a.tripFence()
	late := time.Now().Add(-(leaseWindow + time.Second))

	a.lift(t.Context(), a.fence.Load(), late)
	if !a.isFenced() {
		t.Error("a lift that landed after the lease window left the node serving")
	}

	a.tripFence()
	a.lift(t.Context(), a.fence.Load(), time.Now())
	if a.isFenced() {
		t.Error("a lift inside the window left the node fenced")
	}
}

func TestLeaseLiftOfAReplacedStateChangesNothing(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	a.tripFence()
	seen := a.fence.Load()
	a.tripFence()

	a.lift(t.Context(), seen, time.Now())
	if !a.isFenced() {
		t.Error("a lift of a state that was replaced unfenced the node")
	}
}

func TestLeaseConfirmThatKeepsLosingOnItsOwnRowStaysFencedAndKeepsTheClaim(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	vm := lockedVMName(sb)
	replicateNow(t, a, sb)
	a.archives = &lostTakeRegistry{Registry: c.reg}
	row, _, _ := c.reg.Get(t.Context(), sb.ID)

	if keep, err := a.confirmOwnership(t.Context(), sb, row); err == nil || keep {
		t.Fatalf("confirm on a row that keeps refusing: %v %v, want an error", keep, err)
	}
	a.tripFence()
	a.unfence(t.Context(), time.Now())
	if !a.isFenced() {
		t.Error("the node lifted its fence without confirming a claim it owns")
	}
	if _, held := a.byID(sb.ID); !held || fakeOf(a).removed(vm) {
		t.Errorf("claim held %v, VM removed %v, want the claim and VM kept: its row is still this node's", held, fakeOf(a).removed(vm))
	}
}

func TestLeaseConfirmRetriesOnceOnAStaleEpoch(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	stale, _, _ := c.reg.Get(t.Context(), sb.ID)
	replicateNow(t, a, sb)

	if keep, err := a.confirmOwnership(t.Context(), sb, stale); err != nil || !keep {
		t.Fatalf("confirm on a row read before a newer round: %v %v, want kept", keep, err)
	}
	current, _, _ := c.reg.Get(t.Context(), sb.ID)
	if a.epochOf(sb) != current.Epoch {
		t.Errorf("claim epoch %d, want the row's %d", a.epochOf(sb), current.Epoch)
	}
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
				ctx := startLease(t, a)
				synctest.Wait()

				cut.cut.Store(true)
				time.Sleep(leaseWindow + time.Second)
				synctest.Wait()
				if _, _, _, err := a.WakeAgentSocket(ctx, sb.ID, sb.Token); !errors.Is(err, ErrLeaseLost) {
					t.Fatalf("the cut-off owner past its fence: %v, want ErrLeaseLost", err)
				}
				mustNotAdopt(t, b, sb, "adoption while the fenced owner's lease is still live")

				time.Sleep(leaseMargin)
				mustAdopt(t, b, sb, "adoption after the lease expired")
				if tt.wakeBefore {
					if _, _, _, err := b.WakeAgentSocket(ctx, sb.ID, sb.Token); err != nil {
						t.Fatalf("adopter's wake: %v", err)
					}
				}

				cut.cut.Store(false)
				time.Sleep(leaseRenew)
				synctest.Wait()
				if _, held := a.byID(sb.ID); held || !fakeOf(a).removed(vm) {
					t.Errorf("old owner holds the claim %v, VM removed %v, want its copy dropped and its VM removed", held, fakeOf(a).removed(vm))
				}
				if a.isFenced() {
					t.Error("the old owner stays fenced after its lease renewed")
				}
				if _, _, _, err := b.WakeAgentSocket(ctx, sb.ID, sb.Token); err != nil {
					t.Errorf("adopter's wake after the old owner dropped its copy: %v", err)
				}
			})
		})
	}
}

func TestLeaseUnfenceOfManyClaimsLiftsInsideTheWindowOverASlowRegistry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newReplicaCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		sbs := make([]*types.Sandbox, 200)
		for i := range sbs {
			sb := &types.Sandbox{ID: fmt.Sprintf("sb_%016x", i), Token: fmt.Sprintf("%032x", i), Key: testKey, Tenant: "t", ReplicaCk: fmt.Sprintf("ck_%016x", i)}
			epoch, err := c.reg.Put(t.Context(), archives.Record{ID: sb.ID, Owner: "a", TokenSHA256: "x", Checkpoint: sb.ReplicaCk, Claim: []byte(`{}`)})
			if err != nil {
				t.Fatalf("put: %v", err)
			}
			sb.ArchiveEpoch = epoch
			a.mu.Lock()
			a.addClaimLocked(sb)
			a.mu.Unlock()
			sbs[i] = sb
		}
		a.archives = &slowRegistry{Registry: c.reg, rtt: 100 * time.Millisecond}
		a.tripFence()

		a.unfence(t.Context(), time.Now())
		if a.isFenced() {
			t.Fatalf("%d claims at a 100 ms round trip outlasted the %s window", len(sbs), leaseWindow)
		}
		for _, sb := range sbs {
			if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "a" || rec.Epoch != a.epochOf(sb) {
				t.Fatalf("row %s at %s@%d, claim epoch %d, want this node's row at the claim's epoch", sb.ID, rec.Owner, rec.Epoch, a.epochOf(sb))
			}
		}
	})
}

func TestLeaseConsumeDoesNotRaceAnUnfenceRewritingTheEpoch(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	rows, err := c.reg.Rows(t.Context(), []string{sb.ID})
	if err != nil {
		t.Fatalf("rows: %v", err)
	}
	var unfence sync.WaitGroup
	unfence.Go(func() {
		if _, takeErr := a.takeOwnRows(t.Context(), map[string]*types.Sandbox{sb.ID: sb}, rows); takeErr != nil {
			t.Errorf("take own rows: %v", takeErr)
		}
	})
	time.Sleep(20 * time.Millisecond)
	sb.Transition.Lock()
	consumed, err := a.consumeArchive(t.Context(), sb)
	sb.Transition.Unlock()
	unfence.Wait()
	if err != nil || consumed == 0 {
		t.Errorf("consume after the unfence took the row: epoch %d, %v", consumed, err)
	}
}

type lostTakeRegistry struct {
	archives.Registry
}

func (r *lostTakeRegistry) Take(context.Context, string, int64, string) (int64, error) {
	return 0, archives.ErrLost
}

func (r *lostTakeRegistry) TakeOwned(context.Context, string, map[string]int64) (map[string]int64, error) {
	return nil, nil
}

type slowRegistry struct {
	archives.Registry
	rtt time.Duration
}

func (r *slowRegistry) Take(ctx context.Context, id string, expect int64, owner string) (int64, error) {
	time.Sleep(r.rtt)
	return r.Registry.Take(ctx, id, expect, owner)
}

func (r *slowRegistry) TakeOwned(ctx context.Context, owner string, expect map[string]int64) (map[string]int64, error) {
	time.Sleep(r.rtt)
	return r.Registry.TakeOwned(ctx, owner, expect)
}

type cutRegistry struct {
	archives.Registry
	cut        atomic.Bool
	rowsDelay  atomic.Int64
	beforeTake func()
	takeErr    func(id string) error
}

func (r *cutRegistry) Take(ctx context.Context, id string, expect int64, owner string) (int64, error) {
	if hook := r.beforeTake; hook != nil {
		r.beforeTake = nil
		hook()
	}
	if r.takeErr != nil {
		if err := r.takeErr(id); err != nil {
			return 0, err
		}
	}
	return r.Registry.Take(ctx, id, expect, owner)
}

func (r *cutRegistry) TakeOwned(ctx context.Context, owner string, expect map[string]int64) (map[string]int64, error) {
	if hook := r.beforeTake; hook != nil {
		r.beforeTake = nil
		hook()
	}
	if r.takeErr != nil {
		return nil, nil
	}
	return r.Registry.TakeOwned(ctx, owner, expect)
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
	time.Sleep(time.Duration(r.rowsDelay.Load()))
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
