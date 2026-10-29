package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/archives/archivestest"
	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const keepAnHour = 3600

func TestFailoverArchiveWritesTheRow(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)

	rec, ok, err := c.reg.Get(t.Context(), sb.ID)
	if err != nil || !ok {
		t.Fatalf("row: %v %v, want one", ok, err)
	}
	if rec.Owner != "a" || rec.Epoch != 1 || rec.Checkpoint != sb.ArchiveCk || rec.TokenSHA256 != tenants.TokenSHA256(sb.Token) || rec.Deadline.IsZero() {
		t.Errorf("row %+v, want a@1 on %s with the token hash and a deadline", rec, sb.ArchiveCk)
	}
	claim := &types.Sandbox{}
	if err = json.Unmarshal(rec.Claim, claim); err != nil || claim.ID != sb.ID || claim.Token != "" || claim.VMName != "" || claim.ArchiveCk != sb.ArchiveCk {
		t.Errorf("row claim %+v %v, want the archived record without token or VM", claim, err)
	}
	stored, err := newClaimStore(a.dataDir, false).load()
	if err != nil || stored[sb.ID].ArchiveEpoch != 1 {
		t.Errorf("claims.json epoch %v %v, want 1", stored[sb.ID], err)
	}
}

func TestFailoverRowWriteFailureKeepsTheClaimHibernated(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	c.reg.SetDown(true)
	if err := a.archive(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("archive with the registry down: %v, want ErrArchiveStoreDown", err)
	}
	a.mu.Lock()
	ck, snap := sb.ArchiveCk, sb.HibernateSnap
	a.mu.Unlock()
	if ck != "" || snap == "" {
		t.Errorf("claim ck=%q snap=%q, want still hibernated", ck, snap)
	}
	if metas, err := a.ckpts.Metas(t.Context()); err != nil || len(metas) != 0 {
		t.Errorf("store holds %d checkpoints %v, want no export with the registry down", len(metas), err)
	}
	c.reg.SetDown(false)
	if err := a.archive(t.Context(), sb); err != nil {
		t.Fatalf("archive once the registry is back: %v", err)
	}
}

func TestFailoverOwnerWakeTakesThenDeletesTheRow(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	ck := sb.ArchiveCk

	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake: %v", err)
	}
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || c.reg.Deletes() != 1 {
		t.Errorf("row present %v after %d deletes, want gone after one", ok, c.reg.Deletes())
	}
	if ckExists(t, a, ck) {
		t.Error("the consumed checkpoint survived the wake")
	}
	mustArchive(t, a, sb)
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Epoch <= 3 {
		t.Errorf("re-archive epoch %d, want above the wake's tombstone at 3", rec.Epoch)
	}
}

func TestFailoverOwnerWakeWithTheRegistryDownFailsClosed(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	c.reg.SetDown(true)
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("wake: %v, want ErrArchiveStoreDown", err)
	}
	a.mu.Lock()
	ck, epoch := sb.ArchiveCk, sb.ArchiveEpoch
	a.mu.Unlock()
	if ck == "" || epoch != 1 || !ckExists(t, a, ck) {
		t.Errorf("claim ck=%q epoch=%d, want untouched", ck, epoch)
	}
}

func TestFailoverLegacyVolumeTransferPreservesOtherReader(t *testing.T) {
	c := newFailoverCluster(t)
	b := c.node(t, "b", t.TempDir(), keepAnHour)
	d := c.node(t, "d", t.TempDir(), keepAnHour)
	backing := writeVolumeImage(t, "data", "volume bytes")
	for _, m := range []*Manager{b, d} {
		if err := m.RegisterVolume(t.Context(), "data", types.VolumeRegistration{Path: backing, Writable: true}); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile("testdata/legacy-volume-replica.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Token  string
		Record archives.Record
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if _, err := c.reg.Put(t.Context(), fixture.Record); err != nil {
		t.Fatal(err)
	}
	c.expire(fixture.Record.Owner)
	for range 2 {
		if adopted, err := b.AdoptArchived(t.Context(), fixture.Record.ID, fixture.Token); err != nil || !adopted {
			t.Fatalf("adopt recorded legacy row: adopted=%v err=%v", adopted, err)
		}
	}
	writer := ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}
	if _, err := b.ClaimProvision(t.Context(), plainKey, writer); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("adopted reader failed to exclude writer: %v", err)
	}
	other, err := b.ClaimProvision(t.Context(), plainKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}})
	if err != nil {
		t.Fatal(err)
	}
	c.expire("b")
	if adopted, err := d.AdoptArchived(t.Context(), fixture.Record.ID, fixture.Token); err != nil || !adopted {
		t.Fatalf("transfer recorded row: adopted=%v err=%v", adopted, err)
	}
	for range 2 {
		if err := b.Release(t.Context(), fixture.Record.ID, Cred{Token: fixture.Token}); !errors.Is(err, ErrUnknownSandbox) {
			t.Fatalf("former owner release: %v", err)
		}
		if _, err := b.ClaimProvision(t.Context(), plainKey, writer); !errors.Is(err, ErrVolumeBusy) {
			t.Fatalf("former owner released independent reader: %v", err)
		}
	}
	if err := b.Release(t.Context(), other.ID, Cred{Token: other.Token}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ClaimProvision(t.Context(), plainKey, writer); err != nil {
		t.Fatalf("former owner leaked reader hold: %v", err)
	}
	if _, err := d.ClaimProvision(t.Context(), plainKey, writer); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("new owner failed to exclude writer: %v", err)
	}
}

func TestFailoverAdoptionMovesTheClaimAndTheOldOwnerLetsGo(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	ck := sb.ArchiveCk
	c.expire("a")

	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v, want adopted", adopted, err)
	}
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "b" || rec.Epoch != 2 {
		t.Errorf("row %s@%d, want b@2", rec.Owner, rec.Epoch)
	}
	c.renew(t, "a")
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("old owner's wake: %v, want ErrUnknownSandbox", err)
	}
	if _, held := a.byID(sb.ID); held {
		t.Error("the old owner kept the claim")
	}
	if !ckExists(t, b, ck) {
		t.Fatal("the old owner deleted the adopter's checkpoint")
	}
	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("adopter's wake: %v", err)
	}
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || ckExists(t, b, ck) {
		t.Error("the adopter's wake left the row or the checkpoint")
	}
}

func TestFailoverNoAdoptionWhileTheOwnerIsAlive(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)

	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || adopted {
		t.Errorf("adopt from a live owner: %v %v, want refused", adopted, err)
	}
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "a" || rec.Epoch != 1 {
		t.Errorf("row %s@%d, want a@1", rec.Owner, rec.Epoch)
	}
}

func TestFailoverADrainingNodeDoesNotAdopt(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	c.expire("a")
	b.Drain(t.Context())

	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || adopted {
		t.Errorf("adopt on a draining node: %v %v, want refused", adopted, err)
	}
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "a" || rec.Epoch != 1 {
		t.Errorf("row %s@%d, want a@1 left for another node", rec.Owner, rec.Epoch)
	}
	b.mu.Lock()
	b.draining = false
	b.mu.Unlock()
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Errorf("adopt once the drain lifts: %v %v, want adopted", adopted, err)
	}
}

func TestFailoverAdoptionNeedsTheToken(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	c.expire("a")

	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, "wrong"); err != nil || adopted {
		t.Errorf("adopt with a wrong token: %v %v, want refused", adopted, err)
	}
	if adopted, err := b.AdoptArchived(t.Context(), "sb_unknown", sb.Token); err != nil || adopted {
		t.Errorf("adopt of an id with no row: %v %v, want refused", adopted, err)
	}
	if _, held := b.byID(sb.ID); held {
		t.Error("a refused adoption materialized the claim")
	}
}

func TestFailoverConcurrentAdoptionsLeaveOneOwner(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	b, d := c.node(t, "b", t.TempDir(), keepAnHour), c.node(t, "d", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	c.expire("a")

	adopters := []*Manager{b, d}
	won := make([]bool, len(adopters))
	var wg sync.WaitGroup
	for i, m := range adopters {
		wg.Go(func() {
			adopted, err := m.AdoptArchived(t.Context(), sb.ID, sb.Token)
			if err != nil {
				t.Errorf("adopt on %s: %v", m.nodeID, err)
			}
			won[i] = adopted
		})
	}
	wg.Wait()
	if won[0] == won[1] {
		t.Fatalf("adoptions won %v, want exactly one", won)
	}
	winner, loser := b, d
	if won[1] {
		winner, loser = d, b
	}
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != winner.nodeID {
		t.Errorf("row owner %s, want %s", rec.Owner, winner.nodeID)
	}
	if _, held := loser.byID(sb.ID); held {
		t.Error("the losing node materialized the claim")
	}
}

func TestFailoverRestartDropsAClaimAdoptedElsewhere(t *testing.T) {
	c := newFailoverCluster(t)
	dirA := t.TempDir()
	a, b := c.node(t, "a", dirA, keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}
	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("adopter's wake: %v", err)
	}
	moved, _ := b.byID(sb.ID)
	mustArchive(t, b, moved)
	adopterCk := moved.ArchiveCk

	c.renew(t, "a")
	restarted := c.node(t, "a", dirA, keepAnHour)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); held {
		t.Error("the restarted node kept a claim another node holds")
	}
	if !ckExists(t, b, adopterCk) {
		t.Fatal("the restarted node deleted the adopter's checkpoint")
	}
	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Errorf("adopter's wake after the restart: %v", err)
	}
}

func TestFailoverRestartReclaimsAnInterruptedArchive(t *testing.T) {
	c := newFailoverCluster(t)
	dirA := t.TempDir()
	claims := map[string]*types.Sandbox{
		"sb_x": {ID: "sb_x", VMName: "sbx-gone-1", Key: testKey, Token: "tok", HibernateSnap: "sbx-hib-x"},
	}
	if err := newClaimStore(dirA, false).save(claims); err != nil {
		t.Fatalf("setup: %v", err)
	}
	a := c.node(t, "a", dirA, keepAnHour)
	const ck = "ck_00000000000000aa"
	seedArchiveCk(t, a, ck, "sb_x")
	if _, err := c.reg.Put(t.Context(), archives.Record{ID: "sb_x", Owner: "a", Epoch: 1, Checkpoint: ck, Claim: []byte(`{}`)}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := a.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, ok, _ := c.reg.Get(t.Context(), "sb_x"); ok || ckExists(t, a, ck) {
		t.Error("reconcile kept the row or the checkpoint of an archive that never committed")
	}
}

func TestFailoverRestartReclaimSparesACheckpointAdoptedMidReconcile(t *testing.T) {
	c := newFailoverCluster(t)
	dirA := t.TempDir()
	claims := map[string]*types.Sandbox{
		"sb_x": {ID: "sb_x", VMName: "sbx-gone-1", Key: testKey, Token: "tok", HibernateSnap: "sbx-hib-x"},
	}
	if err := newClaimStore(dirA, false).save(claims); err != nil {
		t.Fatalf("setup: %v", err)
	}
	a := c.node(t, "a", dirA, keepAnHour)
	const ck = "ck_00000000000000cc"
	seedArchiveCk(t, a, ck, "sb_x")
	a.archives = &hookRegistry{Registry: c.reg, afterRows: func() {
		if _, err := c.reg.Put(t.Context(), archives.Record{ID: "sb_x", Owner: "b", Epoch: 1, Checkpoint: ck, Claim: []byte(`{}`)}); err != nil {
			t.Errorf("adopter's put: %v", err)
		}
	}}
	if err := a.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !ckExists(t, a, ck) {
		t.Error("the restarted node deleted a checkpoint the adopter committed during its reconcile")
	}
}

func TestFailoverRestartReclaimsOnlyTheCheckpointsItPublished(t *testing.T) {
	c := newFailoverCluster(t)
	dirA := t.TempDir()
	claims := map[string]*types.Sandbox{
		"sb_x": {ID: "sb_x", VMName: "sbx-gone-1", Key: testKey, Token: "tok", HibernateSnap: "sbx-hib-x"},
	}
	if err := newClaimStore(dirA, false).save(claims); err != nil {
		t.Fatalf("setup: %v", err)
	}
	a := c.node(t, "a", dirA, keepAnHour)
	const own, foreign = "ck_00000000000000e1", "ck_00000000000000e2"
	seedArchiveCkBy(t, a, "a", own, "sb_x")
	seedArchiveCkBy(t, a, "b", foreign, "sb_x")
	if err := a.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if ckExists(t, a, own) {
		t.Error("reconcile kept its own orphan archive checkpoint")
	}
	if !ckExists(t, a, foreign) {
		t.Error("reconcile deleted a checkpoint another node published")
	}
}

func TestFailoverRestartDropsAClaimWhoseInterruptedArchiveWasAdopted(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	a.mu.Lock()
	vm, snap := sb.VMName, sb.HibernateSnap
	a.mu.Unlock()
	const ck = "ck_00000000000000bb"
	seedArchiveCk(t, a, ck, sb.ID)
	if _, err := c.reg.Put(t.Context(), archives.Record{ID: sb.ID, Owner: "b", Epoch: 2, Checkpoint: ck, Claim: []byte(`{}`)}); err != nil {
		t.Fatalf("put: %v", err)
	}

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); held {
		t.Error("the restarted node kept a claim whose archive another node adopted")
	}
	eng := a.eng.(*fakeEngine)
	eng.mu.Lock()
	snapDropped := slices.Contains(eng.snapRemoves, snap)
	eng.mu.Unlock()
	if !eng.removed(vm) || !snapDropped {
		t.Errorf("vm removed %v, snapshot dropped %v, want both swept", eng.removed(vm), snapDropped)
	}
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "b" || !ckExists(t, restarted, ck) {
		t.Error("the restarted node touched the adopter's row or checkpoint")
	}
}

func TestFailoverRestartWithTheRegistryDownKeepsEverything(t *testing.T) {
	c := newFailoverCluster(t)
	dirA := t.TempDir()
	a := c.node(t, "a", dirA, keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	c.reg.SetDown(true)
	restarted := c.node(t, "a", dirA, keepAnHour)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); !held || !ckExists(t, restarted, sb.ArchiveCk) {
		t.Errorf("claim held %v, checkpoint kept %v, want both", held, ckExists(t, restarted, sb.ArchiveCk))
	}
}

func TestFailoverReleaseDropsTheRowThenTheCheckpoint(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	ck := sb.ArchiveCk

	c.reg.SetDown(true)
	if err := a.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("release with the registry down: %v, want ErrArchiveStoreDown", err)
	}
	if _, held := a.byID(sb.ID); !held || !ckExists(t, a, ck) {
		t.Fatal("a failed release dropped the claim or its checkpoint")
	}
	c.reg.SetDown(false)
	if err := a.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || ckExists(t, a, ck) {
		t.Error("release left the row or the checkpoint")
	}
}

func TestFailoverReleaseAfterAdoptionKeepsTheCheckpoint(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}
	if err := a.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("old owner's release: %v, want ErrUnknownSandbox", err)
	}
	if _, held := a.byID(sb.ID); held || !ckExists(t, b, sb.ArchiveCk) {
		t.Errorf("old owner held %v, checkpoint kept %v, want dropped and kept", held, ckExists(t, b, sb.ArchiveCk))
	}
}

func TestFailoverRetentionPurgeConsumesTheRowFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a := c.node(t, "a", t.TempDir(), 1)
		sb := mustClaim(t, a, testKey)
		mustArchive(t, a, sb)
		ck := sb.ArchiveCk
		time.Sleep(2 * time.Second)

		a.sweepOrphanArchives(t.Context())
		if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); !ok || rec.Consumed || !ckExists(t, a, ck) {
			t.Fatalf("row %+v present %v after a sweep, want the local claim's archive left to its own reap", rec, ok)
		}
		a.reapOnce(t.Context())
		synctest.Wait()
		if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); !ok || !rec.Consumed {
			t.Fatalf("row after the first reap %+v present %v, want a tombstone", rec, ok)
		}
		a.reapOnce(t.Context())
		synctest.Wait()
		if _, held := a.byID(sb.ID); held || ckExists(t, a, ck) {
			t.Error("the second reap kept the claim or its checkpoint")
		}
		a.sweepOrphanArchives(t.Context())
		if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok {
			t.Error("the sweep kept the tombstone of a purged archive")
		}
	})
}

func TestFailoverSweepPurgesADepartedOwnersArchiveOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a := c.node(t, "a", t.TempDir(), 1)
		b, d := c.node(t, "b", t.TempDir(), 1), c.node(t, "d", t.TempDir(), 1)
		sb := mustClaim(t, a, testKey)
		mustArchive(t, a, sb)
		ck := sb.ArchiveCk
		time.Sleep(2 * time.Second)

		b.sweepOrphanArchives(t.Context())
		if _, ok, _ := c.reg.Get(t.Context(), sb.ID); !ok {
			t.Fatal("a sweep purged the archive of a live owner")
		}
		c.expire("a")
		var wg sync.WaitGroup
		for _, m := range []*Manager{b, d} {
			wg.Go(func() { m.sweepOrphanArchives(t.Context()) })
		}
		wg.Wait()
		if c.reg.Deletes() != 1 || ckExists(t, b, ck) {
			t.Errorf("%d row deletes, checkpoint kept %v, want one delete and the checkpoint gone", c.reg.Deletes(), ckExists(t, b, ck))
		}
		if purged := b.counters.archiveDeletes.Load() + d.counters.archiveDeletes.Load(); purged != 1 {
			t.Errorf("%d purges counted, want 1", purged)
		}
	})
}

func TestFailoverAdoptionMidWakeFencesTheOwnerOut(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	ck := sb.ArchiveCk
	a.archives = &hookRegistry{Registry: c.reg, beforeConsume: func() {
		c.expire("a")
		if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
			t.Errorf("adopt mid-wake: %v %v, want adopted", adopted, err)
		}
	}}

	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrUnknownSandbox) {
		t.Fatalf("owner's wake after a mid-wake adoption: %v, want ErrUnknownSandbox", err)
	}
	eng := a.eng.(*fakeEngine)
	eng.mu.Lock()
	built := eng.clones[len(eng.clones)-1]
	eng.mu.Unlock()
	if !eng.removed(built) {
		t.Errorf("the owner kept its built VM %s", built)
	}
	_, heldA := a.byID(sb.ID)
	_, heldB := b.byID(sb.ID)
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); heldA || !heldB || rec.Owner != "b" {
		t.Errorf("a holds %v, b holds %v, row owner %s, want b alone", heldA, heldB, rec.Owner)
	}
	if !ckExists(t, b, ck) || archiveCkMarked(a, ck) {
		t.Error("the fenced owner deleted or marked the adopter's checkpoint")
	}
}

func TestFailoverWakeRollbackRecoversOnTheNextWake(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	breakStore(t, a)
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err == nil {
		t.Fatal("wake with claims.json unwritable succeeded")
	}
	a.mu.Lock()
	ck := sb.ArchiveCk
	a.mu.Unlock()
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); !ok || rec.Owner != "a" || rec.Checkpoint != ck || ck == "" {
		t.Fatalf("row %+v present %v, want this node's row for the rolled-back archive", rec, ok)
	}
	if a.epochOf(sb) == 0 {
		t.Error("the rolled-back wake dropped the claim's archive epoch")
	}
	healStore(t, a)
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake after the store heals: %v", err)
	}
}

func TestFailoverSweepRunsOnTheHourlyStoreTicker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		cfg := &config.Config{DataDir: t.TempDir(), CheckpointDir: c.ckDir}
		m, err := NewManager(t.Context(), cfg, newFakeEngine(), testSecrets(t), testBackends(t, cfg))
		if err != nil {
			t.Fatalf("manager: %v", err)
		}
		counted := &countingRegistry{Registry: c.reg}
		m.archives, m.nodeID = counted, "b"
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			m.Run(ctx)
		}()
		time.Sleep(time.Hour - time.Minute)
		synctest.Wait()
		if n := counted.sweepable.Load(); n != 0 {
			t.Errorf("%d sweeps in the first hour's minutes, want none before the hourly tick", n)
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if n := counted.sweepable.Load(); n != 1 {
			t.Errorf("%d sweeps after the hourly tick, want 1", n)
		}
		cancel()
		<-done
	})
}

func TestFailoverRestartFencesWithOneQuery(t *testing.T) {
	c := newFailoverCluster(t)
	dirA := t.TempDir()
	a := c.node(t, "a", dirA, keepAnHour)
	var ids []string
	for range 3 {
		sb := mustClaim(t, a, testKey)
		mustArchive(t, a, sb)
		ids = append(ids, sb.ID)
	}
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	restarted := c.node(t, "a", dirA, keepAnHour)
	counted := &countingRegistry{Registry: c.reg}
	restarted.archives = counted
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rows, gets := counted.rows.Load(), counted.gets.Load(); rows != 1 || gets != 0 {
		t.Errorf("reconcile ran %d rows and %d get queries, want one rows query", rows, gets)
	}
	for _, id := range ids {
		if _, held := restarted.byID(id); !held {
			t.Errorf("reconcile dropped %s, which this node still owns", id)
		}
	}
}

func TestFailoverArchivePutsTheRowUnderTransition(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	wakeable := false
	a.archives = &hookRegistry{Registry: c.reg, beforePut: func() {
		if sb.Transition.TryLock() {
			wakeable = true
			sb.Transition.Unlock()
		}
	}}
	if err := a.archive(t.Context(), sb); err != nil {
		t.Fatalf("archive: %v", err)
	}
	if wakeable {
		t.Error("the row was put while a wake could still commit the claim as running")
	}
}

func TestFailoverWakeAfterALostConsumeReplyKeepsTheClaim(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	ck := sb.ArchiveCk
	a.archives = &hookRegistry{Registry: c.reg, loseConsumeReply: true}

	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("wake whose consume reply was lost: %v, want ErrArchiveStoreDown", err)
	}
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); !rec.Consumed {
		t.Fatal("setup: the consume did not land")
	}
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("retried wake: %v, want the claim re-armed from its own tombstone", err)
	}
	if _, held := a.byID(sb.ID); !held || ckExists(t, a, ck) {
		t.Error("the retried wake lost the claim or left the consumed checkpoint")
	}
}

func TestFailoverRestartAfterAConsumeWithoutCommitKeepsTheClaim(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	epoch, err := c.reg.Take(t.Context(), sb.ID, 1, "a")
	if err != nil {
		t.Fatalf("take: %v", err)
	}
	_, _ = a.commitIfLive(t.Context(), sb, func() { sb.ArchiveEpoch = epoch })
	if _, err = c.reg.Consume(t.Context(), sb.ID, epoch); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err = a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	restarted := c.restart(t, a)
	if err = restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	held, _ := restarted.byID(sb.ID)
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); !ok || rec.Owner != "a" || rec.Consumed || held == nil || held.ArchiveEpoch != rec.Epoch {
		t.Fatalf("row %+v present %v, held %v, want the tombstone re-armed at the claim's epoch", rec, ok, held != nil)
	}
	if _, _, _, err = restarted.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Errorf("wake after the restart: %v", err)
	}
}

func TestFailoverAnArchiveWokenElsewhereIsDropped(t *testing.T) {
	c := newFailoverCluster(t)
	dirA := t.TempDir()
	a, b := c.node(t, "a", dirA, keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}
	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("adopter's wake: %v", err)
	}
	c.renew(t, "a")

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); held {
		t.Error("the restart kept a claim whose checkpoint an adopter consumed")
	}
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("old owner's wake: %v, want ErrUnknownSandbox", err)
	}
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok {
		t.Error("a consumed archive got its row back")
	}
}

func TestFailoverALeftoverDeleteMarkerSparesTheAdoptersCheckpoint(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	ck := sb.ArchiveCk
	if err := a.markArchiveCk(ck); err != nil {
		t.Fatalf("mark: %v", err)
	}
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}
	c.renew(t, "a")
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrUnknownSandbox) {
		t.Fatalf("old owner's wake: %v, want ErrUnknownSandbox", err)
	}
	a.retryArchiveDeletes(t.Context())
	if !ckExists(t, b, ck) {
		t.Error("the old owner's leftover marker deleted the adopter's checkpoint")
	}
}

func TestFailoverRestartClearsTheMarkerOfAnAdoptedClaim(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	ck := sb.ArchiveCk
	if err := a.markArchiveCk(ck); err != nil {
		t.Fatalf("mark: %v", err)
	}
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
	if !ckExists(t, b, ck) || archiveCkMarked(restarted, ck) {
		t.Error("the restart deleted the adopter's checkpoint or kept its marker")
	}
}

func TestFailoverARepeatedAdoptionOnOneNodeKeepsOneClaim(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	for _, m := range []*Manager{a, b} {
		if err := m.PutTenant(t.Context(), tenants.TenantSpec{Name: "acme", Token: "tok-acme"}); err != nil {
			t.Fatalf("put tenant: %v", err)
		}
	}
	sb, err := claimAnyAs(t, a, "acme")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	mustArchive(t, a, sb)
	c.expire("a")

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			if _, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil {
				t.Errorf("racing adopt: %v", err)
			}
		})
	}
	wg.Wait()
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("repeated adopt: %v %v, want adopted", adopted, err)
	}
	if n := b.TenantClaims()["acme"]; n != 1 {
		t.Errorf("tenant acme counts %d claims on b, want 1", n)
	}
	held, _ := b.byID(sb.ID)
	rec, _, _ := c.reg.Get(t.Context(), sb.ID)
	b.mu.Lock()
	epoch := held.ArchiveEpoch
	b.mu.Unlock()
	if rec.Owner != "b" || epoch != rec.Epoch {
		t.Errorf("row %s@%d, local epoch %d, want the claim at the row's epoch", rec.Owner, rec.Epoch, epoch)
	}
	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Errorf("wake after repeated adoptions: %v", err)
	}
}

func TestFailoverRestartInsideAnAdoptersWakeDropsTheClaim(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	ck := sb.ArchiveCk
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}
	b.ckpts = &failingDeleteStore{Store: b.ckpts, attempts: make(chan string, 8)}
	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("adopter's wake: %v", err)
	}
	if !ckExists(t, b, ck) {
		t.Fatal("setup: the adopter's checkpoint delete went through")
	}
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); !ok || !rec.Consumed || rec.Owner != "b" {
		t.Errorf("row %+v present %v, want b's tombstone kept while the checkpoint remains", rec, ok)
	}
	c.renew(t, "a")

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); held {
		t.Error("the restarted node kept a claim the adopter woke")
	}
	moved, _ := b.byID(sb.ID)
	mustArchive(t, b, moved)
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "b" || rec.Consumed || rec.Checkpoint != moved.ArchiveCk {
		t.Errorf("row after the adopter re-archives: %+v, want b's live row", rec)
	}
}

func TestFailoverRestartAfterAnAdopterReleasedDropsTheClaim(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}
	b.ckpts = &failingDeleteStore{Store: b.ckpts, attempts: make(chan string, 8)}
	if err := b.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("adopter's release: %v", err)
	}
	if !ckExists(t, b, sb.ArchiveCk) {
		t.Fatal("setup: the adopter's checkpoint delete went through")
	}
	c.renew(t, "a")

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); held {
		t.Error("the restarted node kept a claim the adopter released")
	}
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); ok && !rec.Consumed {
		t.Errorf("a live row %+v outlived the adopter's release", rec)
	}
}

func TestFailoverReleaseDuringAnArchiveLeavesNoRow(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	released := make(chan error, 1)
	a.archives = &hookRegistry{Registry: c.reg, beforePut: func() {
		go func() { released <- a.Release(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
		select {
		case err := <-released:
			released <- err
		case <-time.After(200 * time.Millisecond):
		}
	}}
	if err := a.archive(t.Context(), sb); err != nil {
		t.Fatalf("archive: %v, want it to commit before the release proceeds", err)
	}
	if err := <-released; err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, held := a.byID(sb.ID); held {
		t.Error("the release left the claim")
	}
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); ok {
		t.Errorf("row %+v outlived the released claim", rec)
	}
	if ckExists(t, a, sb.ArchiveCk) {
		t.Error("the release left the archive's checkpoint")
	}
}

func TestFailoverRestartDropsAClaimAdoptedWhileItsInterruptedArchiveSettles(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	const ck = "ck_00000000000000cc"
	seedArchiveCk(t, a, ck, sb.ID)
	if _, err := a.putArchive(t.Context(), sb, ck); err != nil {
		t.Fatalf("put: %v", err)
	}

	restarted := c.restart(t, a)
	restarted.archives = &hookRegistry{Registry: c.reg, beforeDelete: func() {
		c.expire("a")
		if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
			t.Errorf("adopt during the restart: %v %v, want adopted", adopted, err)
		}
	}}
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); held {
		t.Error("the restarted node kept a claim adopted while it settled the interrupted archive")
	}
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "b" || !ckExists(t, b, ck) {
		t.Error("the restart touched the adopter's row or checkpoint")
	}
}

func TestFailoverRestartKeepsAClaimWhoseInterruptedArchiveRowVanished(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	const ck = "ck_00000000000000dd"
	seedArchiveCk(t, a, ck, sb.ID)
	epoch, err := a.putArchive(t.Context(), sb, ck)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	restarted := c.restart(t, a)
	restarted.archives = &hookRegistry{Registry: c.reg, beforeDelete: func() {
		if delErr := c.reg.Delete(t.Context(), sb.ID, epoch); delErr != nil {
			t.Errorf("delete under the restart: %v", delErr)
		}
	}}
	if err = restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); !held {
		t.Error("the restart dropped a claim whose interrupted archive row was already gone")
	}
	if ckExists(t, restarted, ck) {
		t.Error("the restart kept the interrupted archive's checkpoint")
	}
}

func TestFailoverRestartLeavesItsOwnTombstoneToTheSweep(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	a.ckpts = &failingDeleteStore{Store: a.ckpts, attempts: make(chan string, 8)}
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake: %v", err)
	}
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	before, _, _ := c.reg.Get(t.Context(), sb.ID)

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); !held {
		t.Error("the restart dropped a claim whose wake committed")
	}
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); !ok || !rec.Consumed || rec.Epoch != before.Epoch {
		t.Errorf("row %+v present %v, want the tombstone at %d left for the sweep", rec, ok, before.Epoch)
	}
}

func TestFailoverSweepLeavesADepartedOwnersTombstoneUntilItReturns(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	d := c.node(t, "d", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	ck := sb.ArchiveCk
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}
	healthy := b.ckpts
	b.ckpts = &failingDeleteStore{Store: healthy, attempts: make(chan string, 8)}
	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("adopter's wake: %v", err)
	}
	before, _, _ := c.reg.Get(t.Context(), sb.ID)
	c.expire("b")

	d.sweepOrphanArchives(t.Context())
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); !ok || rec.Epoch != before.Epoch || !ckExists(t, d, ck) {
		t.Fatalf("row %+v present %v after a peer's sweep, want the departed owner's tombstone and checkpoint untouched", rec, ok)
	}
	c.renew(t, "b")
	b.ckpts = healthy
	b.sweepOrphanArchives(t.Context())
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || ckExists(t, b, ck) {
		t.Error("the returning owner's sweep kept its tombstone or its checkpoint")
	}
}

func TestFailoverSweepPurgesItsOwnTombstoneNoClaimHolds(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	ck := sb.ArchiveCk
	healthy := a.ckpts
	a.ckpts = &failingDeleteStore{Store: healthy, attempts: make(chan string, 8)}
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake: %v", err)
	}
	a.ckpts = healthy

	a.sweepOrphanArchives(t.Context())
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || ckExists(t, a, ck) || archiveCkMarked(a, ck) {
		t.Error("the sweep kept the tombstone, its checkpoint or its marker after a committed wake")
	}
}

func TestFailoverARetriedReleaseAfterALostConsumeReplyCompletes(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	ck := sb.ArchiveCk
	a.archives = &hookRegistry{Registry: c.reg, loseConsumeReply: true}
	if err := a.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("release whose consume reply was lost: %v, want ErrArchiveStoreDown", err)
	}
	if err := a.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("retried release: %v, want it to settle on its own tombstone", err)
	}

	a.sweepOrphanArchives(t.Context())
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || ckExists(t, a, ck) {
		t.Error("a release whose consume reply was lost left the row or the checkpoint")
	}
}

func TestFailoverSweepSparesATombstoneItsClaimStillHolds(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	a.archives = &hookRegistry{Registry: c.reg, loseConsumeReply: true}
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("wake whose consume reply was lost: %v, want ErrArchiveStoreDown", err)
	}

	a.sweepOrphanArchives(t.Context())
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); !ok || !ckExists(t, a, sb.ArchiveCk) {
		t.Fatal("the sweep purged a tombstone whose archived claim still holds the checkpoint")
	}
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Errorf("retried wake after the sweep: %v", err)
	}
}

func TestFailoverATombstoneIsNotAdopted(t *testing.T) {
	c := newFailoverCluster(t)
	a, d := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "d", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	a.ckpts = &failingDeleteStore{Store: a.ckpts, attempts: make(chan string, 8)}
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake: %v", err)
	}
	c.expire("a")
	counted := &countingRegistry{Registry: c.reg}
	d.archives = counted

	if adopted, err := d.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || adopted {
		t.Errorf("adopt of a tombstone: %v %v, want refused", adopted, err)
	}
	if n := counted.takes.Load(); n != 0 {
		t.Errorf("adoption tried %d takes on a tombstone, want none", n)
	}
}

func TestFailoverADropClearsTheMarkerBeforeTheCheckpointIsUnpinned(t *testing.T) {
	for range 20 {
		c := newFailoverCluster(t)
		a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
		sb := mustClaim(t, a, testKey)
		mustArchive(t, a, sb)
		ck := sb.ArchiveCk
		if err := a.markArchiveCk(ck); err != nil {
			t.Fatalf("mark: %v", err)
		}
		c.expire("a")
		if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
			t.Fatalf("adopt: %v %v", adopted, err)
		}
		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
					a.retryArchiveDeletes(t.Context())
				}
			}
		})
		_, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
		close(done)
		wg.Wait()
		if !errors.Is(err, ErrUnknownSandbox) {
			t.Fatalf("old owner's wake: %v, want ErrUnknownSandbox", err)
		}
		if !ckExists(t, b, ck) {
			t.Fatal("a delete retry racing the drop deleted the adopter's checkpoint")
		}
	}
}

func TestFailoverReleaseAfterALostTakeReplyLeavesNoLiveRow(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	a.archives = &hookRegistry{Registry: c.reg, loseTakeReply: true}
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("wake whose take reply was lost: %v, want ErrArchiveStoreDown", err)
	}
	if err := a.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("release: %v", err)
	}
	assertReleasedForGood(t, c, sb, a, b)
}

func TestFailoverReleaseAfterALostRearmReplyLeavesNoLiveRow(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	a.archives = &hookRegistry{Registry: c.reg, loseConsumeReply: true}
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("wake whose consume reply was lost: %v, want ErrArchiveStoreDown", err)
	}
	a.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("wake whose re-arm reply was lost: %v, want ErrArchiveStoreDown", err)
	}
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Consumed {
		t.Fatal("setup: the re-arm put did not land")
	}
	if err := a.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("release: %v", err)
	}
	assertReleasedForGood(t, c, sb, a, b)
}

func TestFailoverARepeatedAdoptionOfAReleasedClaimIsRefused(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}
	cur, _ := b.byID(sb.ID)
	before, _, _ := c.reg.Get(t.Context(), sb.ID)

	cur.Transition.Lock()
	adopted := make(chan bool, 1)
	go func() {
		ok, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token)
		if err != nil {
			t.Errorf("repeated adopt: %v", err)
		}
		adopted <- ok
	}()
	waitFor(t, func() bool { rec, _, _ := c.reg.Get(t.Context(), sb.ID); return rec.Epoch != before.Epoch })
	time.Sleep(50 * time.Millisecond)
	b.forgetClaim(t.Context(), cur)
	cur.Transition.Unlock()

	if <-adopted {
		t.Error("a repeated adoption answered for a claim released under it")
	}
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); ok {
		t.Errorf("row %+v outlived the released claim", rec)
	}
	if ckExists(t, b, cur.ArchiveCk) {
		t.Error("the retired row's checkpoint outlived it")
	}
}

func TestFailoverSweepDropsTheRowOfAnArchiveThatNeverCommitted(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	a.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
	if err := a.archive(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("archive whose put reply was lost: %v, want ErrArchiveStoreDown", err)
	}
	stale, _, _ := c.reg.Get(t.Context(), sb.ID)

	a.sweepOrphanArchives(t.Context())
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); ok {
		t.Errorf("row %+v of an archive that never committed survived the sweep", rec)
	}
	if ckExists(t, a, stale.Checkpoint) {
		t.Error("the sweep kept the export of an archive that never committed")
	}
	if _, held := a.byID(sb.ID); !held {
		t.Error("the sweep dropped the hibernated claim")
	}
}

func TestFailoverSweepRearmsATombstoneItsArchivedClaimHolds(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	a.archives = &hookRegistry{Registry: c.reg, loseConsumeReply: true}
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("wake whose consume reply was lost: %v, want ErrArchiveStoreDown", err)
	}

	a.sweepOrphanArchives(t.Context())
	rec, _, _ := c.reg.Get(t.Context(), sb.ID)
	a.mu.Lock()
	epoch := sb.ArchiveEpoch
	a.mu.Unlock()
	if rec.Consumed || rec.Owner != "a" || rec.Epoch != epoch {
		t.Fatalf("row %+v, local epoch %d, want the tombstone re-armed at the claim's epoch", rec, epoch)
	}
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Errorf("adopt of the re-armed archive: %v %v, want adopted", adopted, err)
	}
}

func TestFailoverSweepLeavesADepartedOwnersWakeTombstone(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	epoch, err := c.reg.Take(t.Context(), sb.ID, 1, "a")
	if err != nil {
		t.Fatalf("take: %v", err)
	}
	if _, err = c.reg.Consume(t.Context(), sb.ID, epoch); err != nil {
		t.Fatalf("consume: %v", err)
	}
	c.expire("a")

	b.sweepOrphanArchives(t.Context())
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); !ok || !rec.Consumed || !ckExists(t, b, sb.ArchiveCk) {
		t.Error("a sweep touched a departed owner's wake tombstone")
	}
}

func TestFailoverRestartAfterAConsumeWhileAPeerSweptKeepsTheClaim(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	epoch, err := c.reg.Take(t.Context(), sb.ID, 1, "a")
	if err != nil {
		t.Fatalf("take: %v", err)
	}
	_, _ = a.commitIfLive(t.Context(), sb, func() { sb.ArchiveEpoch = epoch })
	if _, err = c.reg.Consume(t.Context(), sb.ID, epoch); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err = a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	c.expire("a")
	b.sweepOrphanArchives(t.Context())
	c.renew(t, "a")

	restarted := c.restart(t, a)
	if err = restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); !held {
		t.Fatal("the restart dropped a claim whose only copy a peer swept")
	}
	if _, _, _, err = restarted.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Errorf("wake after the restart: %v", err)
	}
}

func TestFailoverArchiveSweepExportsNothingWhileTheRegistryIsDown(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	c.reg.SetDown(true)
	for range 3 {
		backdate(a, sb, time.Minute)
		a.archiveOnce(t.Context())
		waitFor(t, func() bool { return !a.archiveSweep.Load() })
	}
	if metas, err := a.ckpts.Metas(t.Context()); err != nil || len(metas) != 0 {
		t.Errorf("three archive ticks with the registry down left %d exports %v, want none", len(metas), err)
	}
	if hibernated(a) != 1 {
		t.Error("the claim left hibernation while the registry was down")
	}
}

func TestFailoverArchiveExportsNothingForARowAnotherNodeHolds(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	if _, err := c.reg.Put(t.Context(), archives.Record{ID: sb.ID, Owner: "b", Checkpoint: "ck_00000000000000ee", Claim: []byte(`{}`)}); err != nil {
		t.Fatalf("put: %v", err)
	}
	published := &publishCountingStore{Store: a.ckpts}
	a.ckpts = published
	if err := a.archive(t.Context(), sb); !errors.Is(err, archives.ErrLost) {
		t.Fatalf("archive of a claim another node holds: %v, want ErrLost", err)
	}
	if n := published.n.Load(); n != 0 {
		t.Errorf("archive published %d exports, want none", n)
	}
}

func TestFailoverAWakeKeepsItsFenceAgainstThisNodesOwnAdoption(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	ck := sb.ArchiveCk
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}
	second := make(chan bool, 1)
	b.archives = &hookRegistry{Registry: c.reg, beforeConsume: func() {
		taken, _, _ := c.reg.Get(t.Context(), sb.ID)
		go func() {
			ok, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token)
			if err != nil {
				t.Errorf("second adopt: %v", err)
			}
			second <- ok
		}()
		waitFor(t, func() bool { rec, _, _ := c.reg.Get(t.Context(), sb.ID); return rec.Epoch != taken.Epoch })
	}}

	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake raced by this node's own adoption: %v, want it to commit", err)
	}
	if !<-second {
		t.Error("the racing adoption answered that this node does not hold the claim")
	}
	held, ok := b.byID(sb.ID)
	if !ok || held.ArchiveCk != "" {
		t.Fatalf("claim held %v, want it woken on b", ok)
	}
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); ok && !rec.Consumed {
		t.Errorf("a live row %+v survived the wake", rec)
	}
	if ckExists(t, b, ck) {
		t.Error("the wake left its consumed checkpoint")
	}
}

func TestFailoverConcurrentAdoptionsOnOneNodeAllAnswerForIt(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	for _, m := range []*Manager{a, b} {
		if err := m.PutTenant(t.Context(), tenants.TenantSpec{Name: "acme", Token: "tok-acme"}); err != nil {
			t.Fatalf("put tenant: %v", err)
		}
	}
	sb, err := claimAnyAs(t, a, "acme")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	mustArchive(t, a, sb)
	c.expire("a")

	const callers = 10
	start := make(chan struct{})
	var refused atomic.Int32
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			<-start
			adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token)
			if err != nil || !adopted {
				refused.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()
	if n := refused.Load(); n != 0 {
		t.Errorf("%d of %d concurrent adoptions on one node answered no, want none", n, callers)
	}
	if n := b.TenantClaims()["acme"]; n != 1 {
		t.Errorf("tenant acme counts %d claims on b, want 1", n)
	}
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "b" {
		t.Errorf("row owner %s, want b", rec.Owner)
	}
}

func TestFailoverACoalescedAdoptionStillNeedsTheToken(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	c.expire("a")
	wrong := make(chan bool, 1)
	b.archives = &hookRegistry{Registry: c.reg, beforeTake: func() {
		go func() {
			ok, _ := b.AdoptArchived(t.Context(), sb.ID, "wrong")
			wrong <- ok
		}()
		select {
		case ok := <-wrong:
			wrong <- ok
		case <-time.After(200 * time.Millisecond):
		}
	}}
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}
	if <-wrong {
		t.Error("an adoption with a wrong token answered for the claim")
	}
}

func TestFailoverArchivePersistsItsIntentBeforeThePut(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	var intent string
	a.archives = &hookRegistry{Registry: c.reg, beforePut: func() {
		stored, err := newClaimStore(a.dataDir, false).load()
		if err != nil {
			t.Errorf("load: %v", err)
			return
		}
		intent = stored[sb.ID].ArchivePending
	}}
	if err := a.archive(t.Context(), sb); err != nil {
		t.Fatalf("archive: %v", err)
	}
	stored, err := newClaimStore(a.dataDir, false).load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if intent == "" || intent != sb.ArchiveCk || stored[sb.ID].ArchivePending != "" {
		t.Errorf("intent %q before the put and %q after the commit, want %s then none", intent, stored[sb.ID].ArchivePending, sb.ArchiveCk)
	}
}

func TestFailoverRestartDropsAClaimWhoseInterruptedArchiveWasConsumedElsewhere(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	a.mu.Lock()
	vm := sb.VMName
	a.mu.Unlock()
	ck := interruptArchive(t, a, sb, true)
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}
	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("adopter's wake: %v", err)
	}
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || ckExists(t, b, ck) {
		t.Fatal("setup: the adopter's wake left the row or the checkpoint")
	}
	c.renew(t, "a")

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); held {
		t.Error("the restarted node kept a claim another node adopted and woke")
	}
	if eng := a.eng.(*fakeEngine); !eng.removed(vm) {
		t.Errorf("the dropped claim's VM %s was not swept", vm)
	}
}

func TestFailoverRestartKeepsAClaimWhoseArchivePutNeverCommitted(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	ck := interruptArchive(t, a, sb, false)

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	held, ok := restarted.byID(sb.ID)
	if !ok || held.ArchivePending != "" {
		t.Fatalf("claim held %v, want it kept with its intent cleared", ok)
	}
	if ckExists(t, restarted, ck) {
		t.Error("the restart kept the export of an archive whose put never committed")
	}
}

func TestFailoverRestartKeepsAnIntentWhoseCheckpointItCannotRead(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	ck := interruptArchive(t, a, sb, false)

	restarted := c.restart(t, a)
	restarted.ckpts = &unreadableStore{Store: restarted.ckpts}
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	held, ok := restarted.byID(sb.ID)
	if !ok || held.ArchivePending != ck {
		t.Fatalf("claim held %v, want it kept with its intent for the next restart", ok)
	}
	if !ckExists(t, restarted, ck) {
		t.Error("the restart reclaimed the export its kept intent names")
	}
}

func TestFailoverRestartAfterAnAbandonClearedItsIntentReclaimsTheExport(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	ck := interruptArchive(t, a, sb, false)
	if !a.clearArchiveIntent(t.Context(), sb, ck) {
		t.Fatal("setup: clear intent")
	}

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, held := restarted.byID(sb.ID); !held || ckExists(t, restarted, ck) {
		t.Error("the restart dropped the claim or kept the export an abandon left")
	}
}

func TestFailoverAnAbandonKeepsTheExportWhileItsIntentIsDurable(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	ck := interruptArchive(t, a, sb, true)
	rec, _, _ := c.reg.Get(t.Context(), sb.ID)
	breakStore(t, a)
	a.abandonArchive(t.Context(), sb, rec.Epoch, ck)
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok {
		t.Error("the abandon kept the row")
	}
	if !ckExists(t, a, ck) {
		t.Error("the abandon deleted the export while the journal still names it")
	}
	healStore(t, a)
}

func TestFailoverAnOwnerReturningWithItsWakeCommittedPurgesItsTombstone(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	ck := sb.ArchiveCk
	a.ckpts = &failingDeleteStore{Store: a.ckpts, attempts: make(chan string, 8)}
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake: %v", err)
	}
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	c.expire("a")
	b.sweepOrphanArchives(t.Context())
	c.renew(t, "a")

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	restarted.sweepOrphanArchives(t.Context())
	if held, ok := restarted.byID(sb.ID); !ok || held.ArchiveCk != "" {
		t.Fatal("the returning owner lost the claim its wake committed")
	}
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || ckExists(t, restarted, ck) {
		t.Error("the returning owner's sweep kept its wake tombstone or the consumed checkpoint")
	}
}

func TestFailoverRestartDropsAClaimConsumedWhileItSettlesItsInterruptedArchive(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	ck := interruptArchive(t, a, sb, true)

	restarted := c.restart(t, a)
	restarted.archives = &hookRegistry{Registry: c.reg, beforeDelete: func() {
		c.expire("a")
		if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
			t.Errorf("adopt during the restart: %v %v, want adopted", adopted, err)
		}
		if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
			t.Errorf("adopter's wake during the restart: %v", err)
		}
	}}
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || ckExists(t, b, ck) {
		t.Fatal("setup: the adopter's wake left the row or the checkpoint")
	}
	if _, held := restarted.byID(sb.ID); held {
		t.Error("the restarted node kept a claim another node adopted and consumed while it settled")
	}
	if held, ok := b.byID(sb.ID); !ok || held.ArchiveCk != "" {
		t.Error("the adopter lost the claim it woke")
	}
}

func TestFailoverRestartUndoesItsOwnInterruptedArchiveAndItsIntent(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	ck := interruptArchive(t, a, sb, true)

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	held, ok := restarted.byID(sb.ID)
	if !ok || held.ArchivePending != "" {
		t.Fatalf("claim held %v, want it kept with its intent cleared", ok)
	}
	if _, rowed, _ := c.reg.Get(t.Context(), sb.ID); rowed || ckExists(t, restarted, ck) {
		t.Error("the restart kept the row or the export of its own interrupted archive")
	}
}

func TestFailoverReleaseAfterALostArchivePutReplyLeavesNothingAdoptable(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	a.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
	if err := a.archive(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("archive whose put reply was lost: %v, want ErrArchiveStoreDown", err)
	}
	if err := a.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if metas, err := a.ckpts.Metas(t.Context()); err != nil || len(metas) != 0 {
		t.Errorf("store holds %d exports %v after the release, want none", len(metas), err)
	}
	assertReleasedForGood(t, c, sb, a, b)
}

func TestFailoverTheNextArchiveSettlesALostPutReplyFirst(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	a.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
	if err := a.archive(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("archive whose put reply was lost: %v, want ErrArchiveStoreDown", err)
	}
	a.archives = c.reg
	if err := a.archive(t.Context(), sb); err != nil {
		t.Fatalf("next archive: %v", err)
	}
	rec, _, _ := c.reg.Get(t.Context(), sb.ID)
	metas, err := a.ckpts.Metas(t.Context())
	if err != nil || len(metas) != 1 || rec.Checkpoint != sb.ArchiveCk || sb.ArchivePending != "" {
		t.Errorf("%d exports %v, row names %s, claim %s pending %q, want one export the row and claim share", len(metas), err, rec.Checkpoint, sb.ArchiveCk, sb.ArchivePending)
	}
}

func TestFailoverSweepSettlesALostArchivePutReply(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	a.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
	if err := a.archive(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("archive whose put reply was lost: %v, want ErrArchiveStoreDown", err)
	}
	a.mu.Lock()
	pending := sb.ArchivePending
	a.mu.Unlock()
	if pending == "" || !ckExists(t, a, pending) {
		t.Fatal("setup: the lost put dropped its intent or its export")
	}
	a.sweepOrphanArchives(t.Context())
	a.mu.Lock()
	pending = sb.ArchivePending
	a.mu.Unlock()
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || pending != "" {
		t.Errorf("row present %v, intent %q after the sweep, want both settled", ok, pending)
	}
	if metas, err := a.ckpts.Metas(t.Context()); err != nil || len(metas) != 0 {
		t.Errorf("store holds %d exports %v after the sweep, want none", len(metas), err)
	}
	if _, held := a.byID(sb.ID); !held || hibernated(a) != 1 {
		t.Error("the sweep lost the hibernated claim")
	}
}

func TestFailoverAWakeSettlesALostArchivePutReplyFirst(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	a.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
	if err := a.archive(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("archive whose put reply was lost: %v, want ErrArchiveStoreDown", err)
	}
	a.archives = c.reg
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake: %v", err)
	}
	a.mu.Lock()
	pending := sb.ArchivePending
	a.mu.Unlock()
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || pending != "" {
		t.Errorf("row present %v, intent %q after the wake, want both settled", ok, pending)
	}
	if metas, err := a.ckpts.Metas(t.Context()); err != nil || len(metas) != 0 {
		t.Errorf("store holds %d exports %v after the wake, want none", len(metas), err)
	}
	if hibernated(a) != 0 {
		t.Error("the claim stayed hibernated after the wake")
	}
}

func TestFailoverAWakeWithoutARegistryKeepsItsPendingIntent(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	a.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
	if err := a.archive(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("archive whose put reply was lost: %v, want ErrArchiveStoreDown", err)
	}
	a.archives = nil
	if _, _, _, err := a.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake with failover off: %v", err)
	}
	if hibernated(a) != 0 {
		t.Error("the claim stayed hibernated after the wake")
	}
}

func TestFailoverAReleaseFindsItsUnsettledArchiveTakenElsewhere(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	a.mu.Lock()
	vm := sb.VMName
	a.mu.Unlock()
	a.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
	if err := a.archive(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("archive whose put reply was lost: %v, want ErrArchiveStoreDown", err)
	}
	c.expire("a")
	if adopted, err := b.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %v %v", adopted, err)
	}
	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("adopter's wake: %v", err)
	}
	c.renew(t, "a")

	if err := a.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrUnknownSandbox) {
		t.Fatalf("old owner's release: %v, want ErrUnknownSandbox", err)
	}
	if _, held := a.byID(sb.ID); held || !a.eng.(*fakeEngine).removed(vm) {
		t.Error("the old owner kept the claim or its VM")
	}
	if held, ok := b.byID(sb.ID); !ok || held.ArchiveCk != "" {
		t.Error("the adopter lost the claim it woke")
	}
}

type failoverCluster struct {
	reg       *archivestest.Mem
	ckDir     string
	replicate int
}

func newFailoverCluster(t *testing.T) *failoverCluster {
	return &failoverCluster{reg: archivestest.NewMem(), ckDir: t.TempDir()}
}

func (c *failoverCluster) expire(node string) {
	c.reg.Expire(node)
}

func (c *failoverCluster) renew(t *testing.T, node string) {
	t.Helper()
	if err := c.reg.Renew(t.Context(), node, time.Hour); err != nil {
		t.Fatalf("renew %s: %v", node, err)
	}
}

func (c *failoverCluster) node(t *testing.T, name, dataDir string, deleteAfter int) *Manager {
	t.Helper()
	m := c.nodeOn(t, newFakeEngine(), name, dataDir, deleteAfter)
	m.liftFence()
	return m
}

func (c *failoverCluster) restart(t *testing.T, old *Manager) *Manager {
	t.Helper()
	return c.nodeOn(t, old.eng.(*fakeEngine), old.nodeID, old.dataDir, keepAnHour)
}

func (c *failoverCluster) nodeOn(t *testing.T, eng *fakeEngine, name, dataDir string, deleteAfter int) *Manager {
	t.Helper()
	spec := archivePool(deleteAfter)
	spec.ReplicateSeconds = c.replicate
	cfg := &config.Config{
		APIToken: "root", DataDir: dataDir, CheckpointDir: c.ckDir, Pools: []config.PoolSpec{spec, {PoolKey: plainKey, Warm: 1}},
		Mesh: &config.MeshConfig{NodeID: name},
	}
	b := testBackends(t, cfg)
	b.Archives = c.reg
	m, err := NewManager(t.Context(), cfg, eng, testSecrets(t), b)
	if err != nil {
		t.Fatalf("manager %s: %v", name, err)
	}
	_ = c.reg.Renew(t.Context(), name, time.Hour)
	return m
}

func assertReleasedForGood(t *testing.T, c *failoverCluster, sb *types.Sandbox, nodes ...*Manager) {
	t.Helper()
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); ok && !rec.Consumed {
		t.Errorf("a live row %+v outlived the release", rec)
	}
	c.expire("a")
	for _, m := range nodes {
		if adopted, err := m.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || adopted {
			t.Errorf("node %s adopts the released claim: %v %v, want a 404", m.nodeID, adopted, err)
		}
	}
}

func claimAnyAs(t *testing.T, m *Manager, tenant string) (*types.Sandbox, error) {
	t.Helper()
	sb, err := m.ClaimWarm(t.Context(), testKey, ClaimOptions{Tenant: tenant})
	if errors.Is(err, ErrNoWarm) {
		return m.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: tenant})
	}
	return sb, err
}

func interruptArchive(t *testing.T, m *Manager, sb *types.Sandbox, putCommitted bool) string {
	t.Helper()
	ck, _, err := m.publishCheckpoint(t.Context(), sb, store.CheckpointID(randHex(8)), "archive", sb.Tenant, true)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	m.mu.Lock()
	sb.ArchivePending = ck.ID
	js := m.store.set(sb)
	m.mu.Unlock()
	if err = m.store.commit(js); err != nil {
		t.Fatalf("commit intent: %v", err)
	}
	if putCommitted {
		if _, err = m.putArchive(t.Context(), sb, ck.ID); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	return ck.ID
}

func seedArchiveCk(t *testing.T, m *Manager, id, sandboxID string) {
	t.Helper()
	seedArchiveCkBy(t, m, m.nodeID, id, sandboxID)
}

func seedArchiveCkBy(t *testing.T, m *Manager, node, id, sandboxID string) {
	t.Helper()
	staging, err := m.ckpts.Stage(id)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(staging, store.ExportDir), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	meta := `{"id":"` + id + `","sandbox_id":"` + sandboxID + `","node":"` + node + `","archive":true}`
	if err := os.WriteFile(filepath.Join(staging, store.MetaFile), []byte(meta), 0o600); err != nil {
		t.Fatalf("meta: %v", err)
	}
	if err := m.ckpts.Publish(t.Context(), staging, id, ""); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

type hookRegistry struct {
	archives.Registry
	beforePut        func()
	beforeTake       func()
	beforeConsume    func()
	beforeDelete     func()
	afterRows        func()
	loseConsumeReply bool
	loseTakeReply    bool
	losePutReply     bool
	failPut          bool
}

func (r *hookRegistry) Put(ctx context.Context, rec archives.Record) (int64, error) {
	if r.failPut {
		return 0, archives.ErrUnavailable
	}
	if hook := r.beforePut; hook != nil {
		r.beforePut = nil
		hook()
	}
	epoch, err := r.Registry.Put(ctx, rec)
	if r.losePutReply && err == nil {
		r.losePutReply = false
		return 0, archives.ErrUnavailable
	}
	return epoch, err
}

func (r *hookRegistry) Replace(ctx context.Context, rec archives.Record, expect int64) (int64, error) {
	if r.failPut {
		return 0, archives.ErrUnavailable
	}
	if hook := r.beforePut; hook != nil {
		r.beforePut = nil
		hook()
	}
	epoch, err := r.Registry.Replace(ctx, rec, expect)
	if r.losePutReply && err == nil {
		r.losePutReply = false
		return 0, archives.ErrUnavailable
	}
	return epoch, err
}

func (r *hookRegistry) Take(ctx context.Context, id string, expect int64, owner string) (int64, error) {
	if hook := r.beforeTake; hook != nil {
		r.beforeTake = nil
		hook()
	}
	epoch, err := r.Registry.Take(ctx, id, expect, owner)
	if r.loseTakeReply && err == nil {
		r.loseTakeReply = false
		return 0, archives.ErrUnavailable
	}
	return epoch, err
}

func (r *hookRegistry) Delete(ctx context.Context, id string, epoch int64) error {
	if hook := r.beforeDelete; hook != nil {
		r.beforeDelete = nil
		hook()
	}
	return r.Registry.Delete(ctx, id, epoch)
}

func (r *hookRegistry) Rows(ctx context.Context, ids []string) (map[string]archives.Record, error) {
	rows, err := r.Registry.Rows(ctx, ids)
	if hook := r.afterRows; hook != nil {
		r.afterRows = nil
		hook()
	}
	return rows, err
}

func (r *hookRegistry) Consume(ctx context.Context, id string, expect int64) (int64, error) {
	if hook := r.beforeConsume; hook != nil {
		r.beforeConsume = nil
		hook()
	}
	epoch, err := r.Registry.Consume(ctx, id, expect)
	if r.loseConsumeReply && err == nil {
		r.loseConsumeReply = false
		return 0, archives.ErrUnavailable
	}
	return epoch, err
}

type publishCountingStore struct {
	store.Store
	n atomic.Int32
}

func (s *publishCountingStore) Publish(ctx context.Context, staging, id, lineage string) error {
	s.n.Add(1)
	return s.Store.Publish(ctx, staging, id, lineage)
}

type unreadableStore struct {
	store.Store
}

func (s *unreadableStore) ReadMeta(context.Context, string) ([]byte, error) {
	return nil, errors.New("store unreachable")
}

type countingRegistry struct {
	archives.Registry
	sweepable atomic.Int32
	rows      atomic.Int32
	gets      atomic.Int32
	takes     atomic.Int32
}

func (r *countingRegistry) Take(ctx context.Context, id string, expect int64, owner string) (int64, error) {
	r.takes.Add(1)
	return r.Registry.Take(ctx, id, expect, owner)
}

func (r *countingRegistry) Get(ctx context.Context, id string) (archives.Record, bool, error) {
	r.gets.Add(1)
	return r.Registry.Get(ctx, id)
}

func (r *countingRegistry) Rows(ctx context.Context, ids []string) (map[string]archives.Record, error) {
	r.rows.Add(1)
	return r.Registry.Rows(ctx, ids)
}

func (r *countingRegistry) Sweepable(ctx context.Context, now time.Time, owner string) ([]archives.Record, error) {
	r.sweepable.Add(1)
	return r.Registry.Sweepable(ctx, now, owner)
}
