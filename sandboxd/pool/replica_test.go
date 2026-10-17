package pool

import (
	"encoding/json/v2"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const replicateEvery = 60

func TestReplicaRoundPublishesMovesTheRowAndDropsThePrevious(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	vm := lockedVMName(sb)

	replicateNow(t, a, sb)
	first := a.replicaOf(sb)
	rec, ok, err := c.reg.Get(t.Context(), sb.ID)
	if err != nil || !ok {
		t.Fatalf("row: %v %v, want one", ok, err)
	}
	if first == "" || rec.Owner != "a" || rec.Checkpoint != first || rec.Consumed || rec.TokenSHA256 != tenants.TokenSHA256(sb.Token) || rec.Deadline.IsZero() {
		t.Fatalf("row %+v with replica %q, want a live row of a on the replica with the token hash and a deadline", rec, first)
	}
	if stored, loadErr := a.store.load(); loadErr != nil || stored[sb.ID].ReplicaCk != first || stored[sb.ID].ArchiveEpoch != rec.Epoch {
		t.Errorf("claims.json replica %q epoch %d (%v), want %q at %d", stored[sb.ID].ReplicaCk, stored[sb.ID].ArchiveEpoch, loadErr, first, rec.Epoch)
	}
	meta, err := a.loadCheckpoint(t.Context(), first)
	if err != nil || !meta.Archive {
		t.Errorf("replica record %+v %v, want an archive record: not branchable and not listed", meta, err)
	}
	if listed, listErr := a.Checkpoints(t.Context(), ""); listErr != nil || len(listed) != 0 {
		t.Errorf("listing shows %d checkpoints %v, want the replica hidden", len(listed), listErr)
	}
	if delErr := a.DeleteCheckpoint(t.Context(), first, "", DeleteFleet); !errors.Is(delErr, ErrUnknownCheckpoint) {
		t.Errorf("delete of the replica: %v, want ErrUnknownCheckpoint", delErr)
	}

	replicateNow(t, a, sb)
	second := a.replicaOf(sb)
	next, _, _ := c.reg.Get(t.Context(), sb.ID)
	if second == "" || second == first || next.Checkpoint != second || next.Epoch <= rec.Epoch {
		t.Errorf("second round replica %q row %s@%d, want a new replica at a new epoch above %d", second, next.Checkpoint, next.Epoch, rec.Epoch)
	}
	if ckExists(t, a, first) || !ckExists(t, a, second) {
		t.Errorf("previous replica kept %v, new replica kept %v, want only the new one", ckExists(t, a, first), ckExists(t, a, second))
	}
	claim := &types.Sandbox{}
	if err = json.Unmarshal(next.Claim, claim); err != nil || claim.ID != sb.ID || claim.Token != "" || claim.VMName != "" || claim.ReplicaCk != "" {
		t.Errorf("row claim %+v %v, want the claim without token, VM or replica", claim, err)
	}
	if a.eng.(*fakeEngine).removed(vm) {
		t.Error("a replication round removed the running VM")
	}
}

func TestReplicaRefusedPutDropsTheCopyAndItsExport(t *testing.T) {
	c := newReplicaCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	vm := lockedVMName(sb)
	replicateNow(t, a, sb)
	a.archives = &hookRegistry{Registry: c.reg, beforePut: func() {
		c.expire("a")
		if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
			t.Errorf("adopt: %v %v", adopted, err)
		}
	}}

	if err := a.replicate(t.Context(), sb); !errors.Is(err, archives.ErrLost) {
		t.Fatalf("round whose row another node took: %v, want ErrLost", err)
	}
	if _, held := a.byID(sb.ID); held || !a.eng.(*fakeEngine).removed(vm) {
		t.Errorf("old owner holds the claim %v, VM removed %v, want its copy dropped", held, a.eng.(*fakeEngine).removed(vm))
	}
	if metas, err := a.ckpts.Metas(t.Context()); err != nil || len(metas) != 1 {
		t.Errorf("store holds %d checkpoints %v, want only the adopter's replica", len(metas), err)
	}
}

func TestReplicaFailedCaptureKeepsTheOldReplicaAndRow(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	first := a.replicaOf(sb)
	before, _, _ := c.reg.Get(t.Context(), sb.ID)

	a.eng.(*fakeEngine).snapSaveErr = errors.New("pause failed")
	if err := a.replicate(t.Context(), sb); err == nil {
		t.Fatal("a round whose capture failed reported success")
	}
	after, _, _ := c.reg.Get(t.Context(), sb.ID)
	if after.Checkpoint != before.Checkpoint || after.Epoch != before.Epoch {
		t.Errorf("row %s@%d after a failed round, want %s@%d", after.Checkpoint, after.Epoch, before.Checkpoint, before.Epoch)
	}
	if a.replicaOf(sb) != first || !ckExists(t, a, first) {
		t.Errorf("replica %q kept %v, want %q kept", a.replicaOf(sb), ckExists(t, a, first), first)
	}
	if metas, err := a.ckpts.Metas(t.Context()); err != nil || len(metas) != 1 {
		t.Errorf("store holds %d checkpoints %v, want only the old replica", len(metas), err)
	}
}

func TestReplicaRegistryOutagePausesRoundsWithoutACapture(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	eng := a.eng.(*fakeEngine)

	c.reg.SetDown(true)
	if err := a.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("round with the registry down: %v, want ErrArchiveStoreDown", err)
	}
	if len(eng.snapSaves) != 0 || a.replicaOf(sb) != "" {
		t.Errorf("%d captures and replica %q during the outage, want none", len(eng.snapSaves), a.replicaOf(sb))
	}
	c.reg.SetDown(false)
	replicateNow(t, a, sb)
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); !ok || rec.Checkpoint != a.replicaOf(sb) {
		t.Errorf("row %+v present %v after recovery, want it on the new replica", rec, ok)
	}
}

func TestReplicaPassSnapshotsOnlyDueRunningClaims(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newReplicaCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		a.fenced.Store(false)
		running := mustClaim(t, a, testKey)
		paused := mustClaim(t, a, testKey)
		withVolume := mustClaim(t, a, testKey)
		if err := a.Hibernate(t.Context(), paused.ID, Cred{Token: paused.Token}); err != nil {
			t.Fatalf("hibernate: %v", err)
		}
		a.mu.Lock()
		withVolume.Volumes = []types.Volume{{Name: "data"}}
		a.mu.Unlock()
		eng := a.eng.(*fakeEngine)
		base := len(eng.snapSaves)

		a.replicateOnce(t.Context())
		synctest.Wait()
		time.Sleep((replicateEvery - 1) * time.Second)
		a.replicateOnce(t.Context())
		synctest.Wait()
		if got := len(eng.snapSaves) - base; got != 0 || a.replicaOf(running) != "" {
			t.Fatalf("%d captures before the interval, want none", got)
		}

		time.Sleep(time.Second)
		a.replicateOnce(t.Context())
		synctest.Wait()
		if got := len(eng.snapSaves) - base; got != 1 || a.replicaOf(running) == "" {
			t.Errorf("%d captures at the interval, replica %q, want exactly the running claim's", got, a.replicaOf(running))
		}
		if a.replicaOf(paused) != "" || a.replicaOf(withVolume) != "" {
			t.Error("a hibernated claim or one with volumes was replicated")
		}
		if !a.replicas[withVolume.ID].skipLogged {
			t.Error("a claim with volumes was not skipped and logged")
		}
		if _, ok, _ := c.reg.Get(t.Context(), running.ID); !ok {
			t.Error("the due claim has no row")
		}
	})
}

func TestReplicaPassSkipsWhileFenced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newReplicaCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		a.replicateOnce(t.Context())
		time.Sleep(replicateEvery * time.Second)
		a.replicateOnce(t.Context())
		synctest.Wait()
		if a.replicaOf(sb) != "" || len(a.eng.(*fakeEngine).snapSaves) != 0 {
			t.Error("a fenced node replicated")
		}
	})
}

func TestReplicaPassRetiresAReplicaWhenThePoolTurnsItOff(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	a.fenced.Store(false)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)

	view := *a.view.Load()
	view.poolReplicate = nil
	a.view.Store(&view)
	a.replicateOnce(t.Context())
	waitFor(t, func() bool { return !a.replicating.Load() })
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || a.replicaOf(sb) != "" || ckExists(t, a, ck) {
		t.Errorf("row present %v, replica %q, ck kept %v, want all gone", ok, a.replicaOf(sb), ckExists(t, a, ck))
	}
	if _, held := a.byID(sb.ID); !held {
		t.Error("turning replication off dropped the claim")
	}
}

func TestReplicaReleaseDropsTheRowTheReplicaAndTheVM(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	vm := lockedVMName(sb)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)

	c.reg.SetDown(true)
	if err := a.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("release with the registry down: %v, want ErrArchiveStoreDown", err)
	}
	if _, held := a.byID(sb.ID); !held || !ckExists(t, a, ck) {
		t.Fatal("a failed release dropped the claim or its replica")
	}
	c.reg.SetDown(false)
	if err := a.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || ckExists(t, a, ck) || !a.eng.(*fakeEngine).removed(vm) {
		t.Errorf("row present %v, replica kept %v, VM removed %v, want the row and replica gone and the VM removed", ok, ckExists(t, a, ck), a.eng.(*fakeEngine).removed(vm))
	}
	assertReleasedForGood(t, c, sb)
}

func TestReplicaHibernateKeepsTheRowOnTheLastReplica(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	before, _, _ := c.reg.Get(t.Context(), sb.ID)

	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	after, ok, _ := c.reg.Get(t.Context(), sb.ID)
	if !ok || after.Epoch != before.Epoch || after.Checkpoint != before.Checkpoint || !ckExists(t, a, before.Checkpoint) {
		t.Errorf("row %+v present %v after the hibernate, want it untouched on %s", after, ok, before.Checkpoint)
	}
}

func TestReplicaArchiveReplacesTheRowAndDropsTheReplica(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	replica := a.replicaOf(sb)
	before, _, _ := c.reg.Get(t.Context(), sb.ID)

	mustArchive(t, a, sb)
	rec, ok, _ := c.reg.Get(t.Context(), sb.ID)
	a.mu.Lock()
	archiveCk, left, epoch := sb.ArchiveCk, sb.ReplicaCk, sb.ArchiveEpoch
	a.mu.Unlock()
	if !ok || rec.Checkpoint != archiveCk || rec.Epoch <= before.Epoch || epoch != rec.Epoch || left != "" {
		t.Errorf("row %s@%d, claim ck %q epoch %d replica %q, want the archive's row at a new epoch and no replica", rec.Checkpoint, rec.Epoch, archiveCk, epoch, left)
	}
	if ckExists(t, a, replica) || !ckExists(t, a, archiveCk) {
		t.Errorf("replica kept %v, archive ck kept %v, want only the archive's", ckExists(t, a, replica), ckExists(t, a, archiveCk))
	}
}

func TestReplicaExpiryDropsTheRowThenTheClaim(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	vm := lockedVMName(sb)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)
	a.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Second)
	a.mu.Unlock()

	a.reapOnce(t.Context())
	waitFor(t, func() bool { return a.replicaOf(sb) == "" && !ckExists(t, a, ck) })
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok {
		t.Error("the row outlived the first reap")
	}
	if _, held := a.byID(sb.ID); !held {
		t.Fatal("the first reap destroyed the claim before its row was dropped")
	}
	a.reapOnce(t.Context())
	waitFor(t, func() bool { return a.eng.(*fakeEngine).removed(vm) })
	if _, held := a.byID(sb.ID); held {
		t.Error("the second reap kept the claim")
	}
}

func TestReplicaRestartKeepsItsRowAndStaysFencedUntilALeaseRenews(t *testing.T) {
	c := newReplicaCluster(t)
	dirA := t.TempDir()
	a := c.node(t, "a", dirA, keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	before, _, _ := c.reg.Get(t.Context(), sb.ID)

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	held, ok := restarted.byID(sb.ID)
	if !ok {
		t.Fatal("the restart dropped a replicated claim whose row is its own")
	}
	rec, rowOK, _ := c.reg.Get(t.Context(), sb.ID)
	if !rowOK || rec.Epoch != before.Epoch || restarted.replicaOf(held) != ck || !ckExists(t, restarted, ck) {
		t.Errorf("row %+v present %v, replica %q, ck kept %v, want the row and replica kept", rec, rowOK, restarted.replicaOf(held), ckExists(t, restarted, ck))
	}
	if _, _, err := restarted.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("serving before the first renewal: %v, want ErrLeaseLost", err)
	}
}

func TestReplicaRestartAdoptsTheRowsCheckpointOfAnInterruptedRound(t *testing.T) {
	c := newReplicaCluster(t)
	dirA := t.TempDir()
	a := c.node(t, "a", dirA, keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	first := a.replicaOf(sb)
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	second := interruptReplica(t, a, sb)

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	held, ok := restarted.byID(sb.ID)
	if !ok {
		t.Fatal("the restart dropped the claim")
	}
	if restarted.replicaOf(held) != second || !ckExists(t, restarted, second) || ckExists(t, restarted, first) {
		t.Errorf("replica %q, new kept %v, old kept %v, want the row's checkpoint %s adopted and the old one reclaimed", restarted.replicaOf(held), ckExists(t, restarted, second), ckExists(t, restarted, first), second)
	}
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Checkpoint != second || rec.Owner != "a" {
		t.Errorf("row %s on %s, want a's on %s", rec.Owner, rec.Checkpoint, second)
	}
}

func TestReplicaRestartDropsAClaimAdoptedElsewhere(t *testing.T) {
	c := newReplicaCluster(t)
	dirA := t.TempDir()
	a, b := c.node(t, "a", dirA, keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}

	c.renew(t, "a")
	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); held {
		t.Error("the restarted node kept a claim another node adopted")
	}
	if !ckExists(t, b, ck) {
		t.Error("the restarted node deleted the adopter's replica")
	}
}

func TestReplicaRestartDropsAClaimTheAdopterWokeAndFinished(t *testing.T) {
	c := newReplicaCluster(t)
	dirA := t.TempDir()
	a, b := c.node(t, "a", dirA, keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}
	if _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("adopter's wake: %v", err)
	}

	c.renew(t, "a")
	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); held {
		t.Error("the restarted node kept a claim whose adopter woke it and consumed the row")
	}
	if _, held := b.byID(sb.ID); !held {
		t.Error("the adopter lost the claim it woke")
	}
}

func TestReplicaRestartKeepsAClaimWhoseUnconfirmedRowIsGone(t *testing.T) {
	c := newReplicaCluster(t)
	dirA := t.TempDir()
	a := c.node(t, "a", dirA, keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)
	rec, _, _ := c.reg.Get(t.Context(), sb.ID)
	if err := c.reg.Delete(t.Context(), sb.ID, rec.Epoch); err != nil {
		t.Fatalf("delete row: %v", err)
	}
	a.mu.Lock()
	sb.ArchiveEpoch = 0
	js := a.store.set(sb)
	a.mu.Unlock()
	if err := a.store.commit(js); err != nil {
		t.Fatalf("commit: %v", err)
	}

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	held, ok := restarted.byID(sb.ID)
	if !ok || restarted.replicaOf(held) != ck || !ckExists(t, restarted, ck) {
		t.Errorf("claim held %v, replica %q, ck kept %v, want the claim and its replica kept", ok, restarted.replicaOf(held), ckExists(t, restarted, ck))
	}
}

func TestReplicaSweepKeepsTheRowOfAReplicatedClaim(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)
	before, _, _ := c.reg.Get(t.Context(), sb.ID)

	a.sweepOrphanArchives(t.Context())
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); !ok || rec.Epoch != before.Epoch || !ckExists(t, a, ck) {
		t.Errorf("row %+v present %v, replica kept %v, want the sweep to leave a live replicated claim alone", rec, ok, ckExists(t, a, ck))
	}
}

func TestReplicaSweepRearmsATombstoneItsClaimStillNames(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)
	row, _, _ := c.reg.Get(t.Context(), sb.ID)
	if _, err := c.reg.Consume(t.Context(), sb.ID, row.Epoch); err != nil {
		t.Fatalf("consume: %v", err)
	}

	a.sweepOrphanArchives(t.Context())
	rec, ok, _ := c.reg.Get(t.Context(), sb.ID)
	if !ok || rec.Consumed || rec.Checkpoint != ck || rec.Owner != "a" || !ckExists(t, a, ck) {
		t.Errorf("row %+v present %v, replica kept %v, want a live row of a re-armed on %s", rec, ok, ckExists(t, a, ck), ck)
	}
}

func TestReplicaALeftoverDeleteMarkerSparesTheLiveReplica(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)
	if err := a.markArchiveCk(ck); err != nil {
		t.Fatalf("mark: %v", err)
	}

	a.retryArchiveDeletes(t.Context())
	if !ckExists(t, a, ck) {
		t.Error("the delete retry removed a replica its claim still names")
	}
}

func TestReplicaAdoptionWakesTheClaimFromTheLastReplica(t *testing.T) {
	c := newReplicaCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)

	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || adopted {
		t.Fatalf("adopt while a's lease is live: %v %v, want refused", adopted, err)
	}
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt after the lease expired: %v %v, want adopted", adopted, err)
	}
	moved, _ := b.byID(sb.ID)
	if moved.ArchiveCk != ck {
		t.Fatalf("adopted claim names %q, want the replica %q", moved.ArchiveCk, ck)
	}
	if _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("adopter's wake: %v", err)
	}
	if got := b.eng.(*fakeEngine).cloneCount(); got != 1 {
		t.Errorf("the wake cloned %d VMs, want the one restored from the replica", got)
	}
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || ckExists(t, b, ck) {
		t.Error("the adopter's wake left the row or the replica")
	}
}

func newReplicaCluster(t *testing.T) *failoverCluster {
	t.Helper()
	c := newFailoverCluster(t)
	c.replicate = replicateEvery
	return c
}

func replicateNow(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	if err := m.replicate(t.Context(), sb); err != nil {
		t.Fatalf("replicate %s: %v", sb.ID, err)
	}
}

func interruptReplica(t *testing.T, m *Manager, sb *types.Sandbox) string {
	t.Helper()
	ck, _, err := m.publishCheckpoint(t.Context(), sb, store.CheckpointID(randHex(8)), "replica", sb.Tenant, true)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err = m.putArchive(t.Context(), sb, ck.ID); err != nil {
		t.Fatalf("put: %v", err)
	}
	return ck.ID
}
