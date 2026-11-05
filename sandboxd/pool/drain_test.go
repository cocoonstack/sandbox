package pool

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestDrainRefusesClaimsTrimsWarmAndUndrainRefills(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		seedGolden(t, m, testKey, "")
		if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: testKey, Warm: 2}}); err != nil {
			t.Fatalf("SetPools: %v", err)
		}
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return len(infos) == 1 && infos[0].Warm == 2
		})

		m.Drain(t.Context())

		if _, err := m.ClaimWarm(t.Context(), testKey, ClaimOptions{}); !errors.Is(err, ErrQuota) {
			t.Fatalf("ClaimWarm during drain: %v, want ErrQuota", err)
		}
		if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); !errors.Is(err, ErrQuota) {
			t.Fatalf("ClaimProvision during drain: %v, want ErrQuota", err)
		}
		infos, g := m.Info()
		if !g.Draining || infos[0].Warm != 0 {
			t.Fatalf("draining=%v warm=%d, want true/0", g.Draining, infos[0].Warm)
		}
		if removed := eng.removedNames(); len(removed) != 2 {
			t.Fatalf("removed=%v, want both warm VMs destroyed", removed)
		}
		m.refillOnce(t.Context())
		if infos, _ = m.Info(); infos[0].Warm != 0 || infos[0].Refilling != 0 {
			t.Fatalf("refill ran during drain: %+v", infos)
		}

		m.Undrain(t.Context())
		waitFor(t, func() bool {
			now, gauges := m.Info()
			return !gauges.Draining && len(now) == 1 && now[0].Warm == 2
		})
		if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{}); err != nil {
			t.Fatalf("claim after undrain: %v", err)
		}
	})
}

func TestEvacuateHandsClaimsToAPeerWhileItsLeaseIsLive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		running, asleep, archived := mustClaim(t, a, testKey), mustClaim(t, a, testKey), mustClaim(t, a, testKey)
		hibernateClaim(t, a, asleep)
		mustArchive(t, a, archived)

		a.Evacuate(t.Context())
		synctest.Wait()

		assertEvacuation(t, a, 0, 0)
		for _, sb := range []*types.Sandbox{running, asleep, archived} {
			if _, held := a.byID(sb.ID); held {
				t.Errorf("a kept %s", sb.ID)
			}
			rec, ok, _ := c.reg.Get(t.Context(), sb.ID)
			if !ok || rec.Owner != "b" || rec.Consumed || !ckExists(t, b, rec.Checkpoint) {
				t.Fatalf("row of %s %+v, want a live row b owns on a stored checkpoint", sb.ID, rec)
			}
			if _, err := a.putArchive(t.Context(), sb, rec.Checkpoint); !errors.Is(err, archives.ErrLost) {
				t.Errorf("a's late put over the row of %s: %v, want ErrLost", sb.ID, err)
			}
			if row, held := b.Sandbox(sb.ID); !held || row.Token != sb.Token {
				t.Errorf("root read of %s on b: held %v, want it with its token", sb.ID, held)
			}
			if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
				t.Errorf("adopter's wake of %s: %v", sb.ID, err)
			}
		}
	})
}

func TestEvacuateHandOffRefusedByAConcurrentOwnerWriteKeepsTheClaim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		mustArchive(t, a, sb)
		a.archives = &hookRegistry{Registry: c.reg, beforeRelease: func() {
			rec, _, _ := c.reg.Get(t.Context(), sb.ID)
			if _, err := c.reg.Replace(t.Context(), rec, rec.Epoch); err != nil {
				t.Errorf("owner write: %v", err)
			}
		}}

		a.Evacuate(t.Context())
		synctest.Wait()

		assertEvacuation(t, a, 0, 1)
		if _, held := a.byID(sb.ID); !held {
			t.Fatal("a refused hand-off dropped the claim")
		}
		if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "a" {
			t.Errorf("row owner %q after a refused hand-off, want a", rec.Owner)
		}
		assertNotAdopted(t, b, sb, "adopt a row the hand-off did not release")

		a.Evacuate(t.Context())
		synctest.Wait()
		assertEvacuation(t, a, 0, 0)
		mustAdopt(t, b, sb, "adopt after the retried hand-off")
	})
}

func TestEvacuateHandOffOfARowAPeerTookDropsTheClaim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		mustArchive(t, a, sb)
		a.archives = &hookRegistry{Registry: c.reg, beforeRelease: func() {
			c.expire("a")
			mustAdopt(t, b, sb, "adopt once a's lease lapsed")
			c.renew(t, "a")
		}}

		a.Evacuate(t.Context())
		synctest.Wait()

		assertEvacuation(t, a, 0, 0)
		if _, held := a.byID(sb.ID); held {
			t.Error("a kept a claim b adopted")
		}
		if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "b" {
			t.Errorf("row owner %q, want b", rec.Owner)
		}
	})
}

func TestEvacuateKeepsClaimsThatCannotMove(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		c.node(t, "b", t.TempDir(), keepAnHour)
		failed, withVolume, moved := mustClaim(t, a, testKey), mustClaim(t, a, testKey), mustClaim(t, a, testKey)
		a.mu.Lock()
		failed.Failed = "vmm down"
		withVolume.Volumes = []types.Volume{{Name: "data"}}
		a.store.set(failed, withVolume)
		a.mu.Unlock()

		a.Evacuate(t.Context())
		synctest.Wait()

		assertEvacuation(t, a, 0, 2)
		for _, sb := range []*types.Sandbox{failed, withVolume} {
			if held, ok := a.byID(sb.ID); !ok || held.HibernateSnap != "" || held.ArchiveCk != "" {
				t.Errorf("skipped %s: held %v, want it kept as it was", sb.ID, ok)
			}
		}
		if _, held := a.byID(moved.ID); held {
			t.Error("a kept the claim it could move")
		}
		if _, _, _, err := a.WakeAgentSocket(t.Context(), withVolume.ID, withVolume.Token); err != nil {
			t.Errorf("a skipped claim's data plane: %v", err)
		}
	})
}

func TestEvacuateWithoutArchiveFailoverSkipsEveryClaim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng, archivePool(keepAnHour))
		sb := mustClaim(t, m, testKey)

		m.Evacuate(t.Context())
		synctest.Wait()

		assertEvacuation(t, m, 0, 1)
		if held, ok := m.byID(sb.ID); !ok || held.HibernateSnap != "" || len(eng.hibernates) != 0 {
			t.Errorf("held %v hibernates %v, want the claim left running", ok, eng.hibernates)
		}
	})
}

func TestEvacuateWaitsOutAnArchiveInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		mustArchive(t, a, sb)
		a.mu.Lock()
		a.archiving[sb.ID] = struct{}{}
		a.mu.Unlock()

		a.Evacuate(t.Context())
		synctest.Wait()
		assertEvacuation(t, a, 1, 0)

		a.untrack(a.archiving, sb.ID)
		time.Sleep(reapInterval)
		synctest.Wait()
		assertEvacuation(t, a, 0, 0)
		mustAdopt(t, b, sb, "adopt once the archive in flight ended")
	})
}

func TestUndrainStopsAnEvacuationBeforeItsHandOff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		stall := stallExports(a)
		sb := mustClaim(t, a, testKey)

		a.Evacuate(t.Context())
		synctest.Wait()
		assertEvacuation(t, a, 1, 0)

		a.Undrain(t.Context())
		close(stall)
		synctest.Wait()

		assertEvacuation(t, a, 0, 0)
		if _, g := a.Info(); g.Draining {
			t.Error("still draining after undrain")
		}
		held, ok := a.byID(sb.ID)
		if !ok || held.ArchiveCk == "" {
			t.Fatalf("held %v, want the claim kept archived on a", ok)
		}
		if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "a" {
			t.Errorf("row owner %q after undrain, want a", rec.Owner)
		}
		assertNotAdopted(t, b, sb, "adopt a claim whose evacuation stopped")
		if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
			t.Errorf("wake after undrain: %v", err)
		}
	})
}

func TestAnUndrainDuringAnEvacuationsDrainStopsIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		seedGolden(t, a, testKey, "")
		if err := a.SetPools(t.Context(), []config.PoolSpec{{PoolKey: testKey, Warm: 1}}); err != nil {
			t.Fatalf("SetPools: %v", err)
		}
		waitFor(t, func() bool {
			infos, _ := a.Info()
			return len(infos) == 1 && infos[0].Warm == 1
		})
		release := holdMaint(a)
		done := make(chan struct{})
		go func() {
			defer close(done)
			a.Evacuate(t.Context())
		}()
		synctest.Wait()

		a.Undrain(t.Context())
		release()
		<-done
		synctest.Wait()

		assertEvacuation(t, a, 0, 0)
		held, ok := a.byID(sb.ID)
		if !ok || held.HibernateSnap != "" || held.ArchiveCk != "" {
			t.Errorf("held %v, want the claim still running after the undrain", ok)
		}
	})
}

func TestEvacuateHoldsTheWakeOfAQueuedClaimUntilUndrain(t *testing.T) {
	tests := []struct {
		name string
		park func(*testing.T, *Manager, *types.Sandbox)
	}{
		{"hibernated", hibernateClaim},
		{"archived", mustArchive},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newFailoverCluster(t)
				a := c.node(t, "a", t.TempDir(), keepAnHour)
				sb := mustClaim(t, a, testKey)
				tt.park(t, a, sb)
				snap, ck := sb.HibernateSnap, sb.ArchiveCk
				release := holdMaint(a)

				a.Evacuate(t.Context())
				synctest.Wait()
				if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrEvacuating) {
					t.Fatalf("wake of a queued claim: %v, want ErrEvacuating", err)
				}
				assertEvacuation(t, a, 1, 0)

				a.Undrain(t.Context())
				release()
				synctest.Wait()

				if held, ok := a.byID(sb.ID); !ok || held.HibernateSnap != snap || held.ArchiveCk != ck {
					t.Fatalf("held %v, want the claim left as it was parked", ok)
				}
				if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
					t.Errorf("wake after undrain: %v", err)
				}
			})
		})
	}
}

func TestAQueuedFailedClaimStillRestarts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		editView(a, func(v *configView) { v.vmmRestart = types.VMMRestartNone })
		sb := mustClaim(t, a, testKey)
		killVMM(fakeOf(a), sb.VMName)
		a.recoverVMM(t.Context(), sb)
		release := holdMaint(a)

		a.Evacuate(t.Context())
		synctest.Wait()
		if err := a.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
			t.Errorf("wake of a queued failed claim: %v", err)
		}

		a.Undrain(t.Context())
		release()
		synctest.Wait()
	})
}

func TestEvacuateRequeuesAClaimAWakeUnarchived(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		release := holdMaint(a)

		a.Evacuate(t.Context())
		synctest.Wait()
		sb.Transition.Lock()
		err := a.handOff(t.Context(), sb)
		sb.Transition.Unlock()
		if !errors.Is(err, errArchiveBusy) {
			t.Errorf("hand-off of a running claim: %v, want errArchiveBusy", err)
		}

		a.Undrain(t.Context())
		release()
		synctest.Wait()
	})
}

func TestEvacuateKeepsAClaimOfARemovedTenant(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := tenantManager(t, t.TempDir(), tenants.TenantSpec{Name: "acme", Token: "acme-tok"})
		m.archives = newFailoverCluster(t).reg
		sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour, Tenant: "acme", OnExpire: types.ExpireArchive})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if err = m.DeleteTenant(t.Context(), "acme"); err != nil {
			t.Fatalf("DeleteTenant: %v", err)
		}

		m.Evacuate(t.Context())
		synctest.Wait()

		assertEvacuation(t, m, 0, 1)
		if held, ok := m.byID(sb.ID); !ok || held.HibernateSnap != "" || held.ArchiveCk != "" {
			t.Errorf("held %v, want the claim left running", ok)
		}
	})
}

func TestAnEnvWriteThatLostTheClaimAnswersUnknown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		mustArchive(t, a, sb)
		sb.Transition.Lock()
		a.dropAdopted(t.Context(), sb)
		sb.Transition.Unlock()

		if err := a.rewriteArchiveRow(t.Context(), sb, false); !errors.Is(err, ErrUnknownSandbox) {
			t.Errorf("row rewrite of a dropped claim: %v, want ErrUnknownSandbox", err)
		}
	})
}

func TestAnEnvWriteDuringTheArchiveReachesItsRow(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	hibernateClaim(t, a, sb)
	written := make(chan error, 1)
	a.archives = &hookRegistry{Registry: c.reg, beforePut: func() {
		go func() { written <- a.SetEnv(t.Context(), sb.ID, types.Env{"H": {Value: "new", Guest: hostOnly}}, "") }()
		waitFor(t, func() bool {
			env, err := a.Env(sb.ID, "")
			_, stored := env["H"]
			return err == nil && stored
		})
	}}

	if err := a.archive(t.Context(), sb); err != nil {
		t.Fatalf("archive: %v", err)
	}
	if err := <-written; err != nil {
		t.Fatalf("env write: %v", err)
	}
	rec, _, _ := c.reg.Get(t.Context(), sb.ID)
	if _, err := c.reg.Release(t.Context(), sb.ID, rec.Epoch, "a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	mustAdopt(t, b, sb, "adopt the released row")
	if held, ok := b.byID(sb.ID); !ok || held.Env["H"].Value != "new" {
		t.Errorf("adopter env %v, want the write the archive raced", held.Env)
	}
}

func TestASecondEvacuateWhileOneRunsChangesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		release := holdMaint(a)

		a.Evacuate(t.Context())
		synctest.Wait()
		first := a.evac
		a.Evacuate(t.Context())
		if a.evac != first {
			t.Error("a second evacuate replaced the run")
		}

		a.Undrain(t.Context())
		release()
		synctest.Wait()
		if held, ok := a.byID(sb.ID); !ok || held.HibernateSnap != "" || held.ArchiveCk != "" {
			t.Errorf("held %v, want the claim left running after undrain", ok)
		}
	})
}

func TestAStoppedEvacuationDoesNotPauseAClaimItHadQueued(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		release := holdMaint(a)

		a.Evacuate(t.Context())
		synctest.Wait()
		a.Undrain(t.Context())
		if err := a.evacuateClaim(t.Context(), sb); err != nil {
			t.Errorf("evacuateClaim after undrain: %v, want nil", err)
		}
		if held, ok := a.byID(sb.ID); !ok || held.HibernateSnap != "" || held.ArchiveCk != "" {
			t.Errorf("held %v, want the claim left running", ok)
		}

		release()
		synctest.Wait()
	})
}

func TestEvacuateKeepsAClaimArchivedWithoutARow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		mustArchive(t, a, sb)
		a.mu.Lock()
		epoch := sb.ArchiveEpoch
		sb.ArchiveEpoch = 0
		a.mu.Unlock()
		if err := c.reg.Delete(t.Context(), sb.ID, epoch); err != nil {
			t.Fatalf("delete row: %v", err)
		}

		a.Evacuate(t.Context())
		synctest.Wait()

		assertEvacuation(t, a, 0, 1)
		if held, ok := a.byID(sb.ID); !ok || held.ArchiveCk == "" {
			t.Errorf("held %v, want the rowless archive kept", ok)
		}
	})
}

func TestEvacuateMovesTheCheapestClaimsFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		running, hibernated, archived := mustClaim(t, a, testKey), mustClaim(t, a, testKey), mustClaim(t, a, testKey)
		hibernateClaim(t, a, hibernated)
		mustArchive(t, a, archived)
		a.maintSem = make(chan struct{}, 1)
		var moved []string
		a.WithPeerAdopt(func(ctx context.Context, id, token string) bool {
			moved = append(moved, id)
			adopted, err := b.AdoptArchived(ctx, id, token)
			return err == nil && adopted
		})

		a.Evacuate(t.Context())
		synctest.Wait()

		if want := []string{archived.ID, hibernated.ID, running.ID}; !slices.Equal(moved, want) {
			t.Errorf("moved %v, want %v", moved, want)
		}
	})
}

func TestAnEvacuationLeavesOtherMaintenanceItsTurn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		c.node(t, "b", t.TempDir(), keepAnHour)
		stall := stallExports(a)
		for range 3 {
			mustClaim(t, a, testKey)
		}
		a.maintSem = make(chan struct{}, 1)

		a.Evacuate(t.Context())
		synctest.Wait()
		left := -1
		a.runBounded(t.Context(), 1, func(context.Context, int) {
			_, g := a.Info()
			left = g.Evacuating
		})
		synctest.Wait()
		close(stall)
		synctest.Wait()

		if left != 2 {
			t.Errorf("maintenance ran with %d claims left, want its turn after the first of 3", left)
		}
	})
}

func TestEvacuateWithNoPeerToAdoptKeepsTheClaim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)

		a.Evacuate(t.Context())
		synctest.Wait()

		assertEvacuation(t, a, 0, 1)
		rec, _, _ := c.reg.Get(t.Context(), sb.ID)
		held, ok := a.byID(sb.ID)
		if !ok || rec.Owner != "a" || held.ArchiveEpoch != rec.Epoch {
			t.Fatalf("held %v row %s@%d, want the claim kept on the row it took back", ok, rec.Owner, rec.Epoch)
		}
		if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
			t.Errorf("wake of the kept claim: %v", err)
		}
	})
}

func TestEvacuateConvergesWhenAPeersAnswerIsLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		a.WithPeerAdopt(func(ctx context.Context, id, token string) bool {
			if _, err := b.AdoptArchived(ctx, id, token); err != nil {
				t.Errorf("adopt: %v", err)
			}
			return false
		})

		a.Evacuate(t.Context())
		synctest.Wait()

		assertEvacuation(t, a, 0, 0)
		if _, held := a.byID(sb.ID); held {
			t.Error("a kept a claim b adopted")
		}
		if _, held := b.byID(sb.ID); !held {
			t.Error("b lost the claim")
		}
	})
}

func TestEvacuateConvergesWhenTheReleaseReplyIsLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		a.archives = &hookRegistry{Registry: c.reg, loseReleaseReply: true}

		a.Evacuate(t.Context())
		synctest.Wait()

		assertEvacuation(t, a, 0, 1)
		if err := a.checkArchiveRow(t.Context(), sb.ID); err != nil {
			t.Errorf("a released row read as lost: %v", err)
		}
		if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrUnknownSandbox) {
			t.Errorf("old owner's wake: %v, want ErrUnknownSandbox", err)
		}
		mustAdopt(t, b, sb, "adopt the released row")
	})
}

func TestARepeatEvacuateResumesAHandOffAnOutageCutShort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		a.archives = &hookRegistry{Registry: c.reg, loseReleaseReply: true}

		a.Evacuate(t.Context())
		synctest.Wait()
		assertEvacuation(t, a, 0, 1)
		a.Evacuate(t.Context())
		synctest.Wait()

		assertEvacuation(t, a, 0, 0)
		if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "b" {
			t.Errorf("row owner %q, want the peer the hand-off resumed at", rec.Owner)
		}
		if _, held := b.byID(sb.ID); !held {
			t.Error("b does not list the claim")
		}
	})
}

func TestARepeatEvacuateWithNoAdopterLeavesAReleasedRowReleased(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		a.archives = &hookRegistry{Registry: c.reg, loseReleaseReply: true}
		a.Evacuate(t.Context())
		synctest.Wait()
		a.WithPeerAdopt(func(context.Context, string, string) bool { return false })

		a.Evacuate(t.Context())
		synctest.Wait()

		assertEvacuation(t, a, 0, 1)
		if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "" {
			t.Errorf("row owner %q, want it left released", rec.Owner)
		}
		if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrUnknownSandbox) {
			t.Errorf("wake of the stale claim: %v, want ErrUnknownSandbox", err)
		}
		mustAdopt(t, b, sb, "adopt from the row")
	})
}

func TestACrashBeforeThePeersAnswerLeavesARowAnyNodeSettles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		adopted, purged := mustClaim(t, a, testKey), mustClaim(t, a, testKey)
		for _, sb := range []*types.Sandbox{adopted, purged} {
			mustArchive(t, a, sb)
			rec, _, _ := c.reg.Get(t.Context(), sb.ID)
			if _, err := c.reg.Release(t.Context(), sb.ID, rec.Epoch, "a"); err != nil {
				t.Fatalf("release: %v", err)
			}
		}

		restarted := c.restart(t, a)

		if _, held := restarted.byID(adopted.ID); held {
			t.Error("the restarted node kept a released claim")
		}
		mustAdopt(t, b, adopted, "adopt after the crash")
		rec, _, _ := c.reg.Get(t.Context(), purged.ID)
		time.Sleep(2 * time.Hour)
		b.sweepOrphanArchives(t.Context())
		synctest.Wait()
		if _, ok, _ := c.reg.Get(t.Context(), purged.ID); ok || ckExists(t, b, rec.Checkpoint) {
			t.Errorf("row %v checkpoint %v after the retention, want both gone", ok, ckExists(t, b, rec.Checkpoint))
		}
	})
}

func TestEvacuationDeadlines(t *testing.T) {
	tests := []struct {
		name      string
		key       types.PoolKey
		onExpire  types.ExpireAction
		ttl       time.Duration
		retention int
		want      time.Duration
	}{
		{"a claim that never chose archiving keeps its lease", plainKey, "", time.Minute, keepAnHour, time.Minute},
		{"a claim that never chose archiving is not kept forever", plainKey, "", time.Minute, 0, time.Minute},
		{"a long lease outlives a short retention", testKey, types.ExpireArchive, 24 * time.Hour, keepAnHour, 24 * time.Hour},
		{"a long retention outlives a short lease", testKey, types.ExpireArchive, time.Minute, keepAnHour, time.Hour},
		{"no retention keeps a chosen archive forever", testKey, types.ExpireArchive, time.Minute, 0, 0},
		{"a pool archive tier counts as the choice", testKey, "", time.Minute, keepAnHour, time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newFailoverCluster(t)
				a := c.node(t, "a", t.TempDir(), tt.retention)
				sb, err := claimAny(t.Context(), a, tt.key, ClaimOptions{TTL: tt.ttl, OnExpire: tt.onExpire})
				if err != nil {
					t.Fatalf("claim: %v", err)
				}
				now := time.Now()

				a.Evacuate(t.Context())
				synctest.Wait()

				rec, ok, _ := c.reg.Get(t.Context(), sb.ID)
				if !ok {
					t.Fatal("no row after the evacuation")
				}
				var want time.Time
				if tt.want != 0 {
					want = now.Add(tt.want)
				}
				if !rec.Deadline.Equal(want) {
					t.Errorf("row deadline %v, want %v", rec.Deadline, want)
				}
			})
		})
	}
}

func assertEvacuation(t *testing.T, m *Manager, left, kept int) {
	t.Helper()
	if _, g := m.Info(); g.Evacuating != left || g.Draining && g.Claimed-g.Evacuating != kept {
		t.Errorf("evacuation left %d of %d claims, want %d left and %d kept", g.Evacuating, g.Claimed, left, kept)
	}
}

func holdMaint(m *Manager) (release func()) {
	for range cap(m.maintSem) {
		m.maintSem <- struct{}{}
	}
	return func() {
		for range cap(m.maintSem) {
			<-m.maintSem
		}
	}
}

func stallExports(m *Manager) chan struct{} {
	eng := fakeOf(m)
	stall := make(chan struct{})
	eng.mu.Lock()
	eng.exportStall = stall
	eng.mu.Unlock()
	return stall
}
