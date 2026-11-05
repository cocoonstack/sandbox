package pool

import (
	"encoding/json/v2"
	"errors"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
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
	if first == "" || rec.Owner != "a" || rec.Checkpoint != first || rec.Consumed || rec.TokenSHA256 != tenants.TokenSHA256(sb.Token) || !rec.Deadline.IsZero() {
		t.Fatalf("row %+v with replica %q, want a live row of a on the replica with the token hash and no deadline", rec, first)
	}
	if stored, loadErr := newClaimStore(a.dataDir, false).load(); loadErr != nil || stored[sb.ID].ReplicaCk != first || stored[sb.ID].ArchiveEpoch != rec.Epoch {
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
	if fakeOf(a).removed(vm) {
		t.Error("a replication round removed the running VM")
	}
}

func TestReplicaIntervalSurvivesATickTheClaimCannotReplicate(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	last := time.Now().Add(-2 * replicateEvery * time.Second)
	a.mu.Lock()
	a.replicas = map[string]time.Time{sb.ID: last}
	sb.ArchivePending = "ck_pending"
	a.mu.Unlock()
	a.replicateOnce(t.Context())
	a.mu.Lock()
	defer a.mu.Unlock()
	if got := a.replicas[sb.ID]; !got.Equal(last) {
		t.Errorf("last attempt %v after a tick the claim could not replicate, want %v kept so a woken claim is due at once", got, last)
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
	if _, held := a.byID(sb.ID); held || !fakeOf(a).removed(vm) {
		t.Errorf("old owner holds the claim %v, VM removed %v, want its copy dropped", held, fakeOf(a).removed(vm))
	}
	if metas, err := a.ckpts.Metas(t.Context()); err != nil || len(metas) != 1 {
		t.Errorf("store holds %d checkpoints %v, want only the adopter's replica", len(metas), err)
	}
}

func TestReplicaLostPutReplyKeepsTheCheckpointTheRowNames(t *testing.T) {
	c := newReplicaCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	vm := lockedVMName(sb)
	a.archives = &hookRegistry{Registry: c.reg, losePutReply: true}

	if err := a.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("round whose put reply was lost: %v, want ErrArchiveStoreDown", err)
	}
	row, ok, _ := c.reg.Get(t.Context(), sb.ID)
	a.mu.Lock()
	pending := sb.ReplicaPending
	a.mu.Unlock()
	if !ok || pending == "" || row.Checkpoint != pending || !ckExists(t, a, pending) || !sb.HasReplica() {
		t.Fatalf("row %+v present %v, pending %q kept %v replicated %v, want the row's checkpoint kept and the claim fenced",
			row, ok, pending, ckExists(t, a, pending), sb.HasReplica())
	}

	c.expire("a")
	mustAdopt(t, b, sb, "adopt")
	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("the adopter's wake from the checkpoint: %v", err)
	}
	if got := fakeOf(b).cloneCount(); got != 1 {
		t.Errorf("the wake cloned %d VMs, want the one restored from the checkpoint", got)
	}

	c.renew(t, "a")
	sb.Transition.Lock()
	err := a.resolveIntent(t.Context(), sb)
	sb.Transition.Unlock()
	if !errors.Is(err, ErrUnknownSandbox) {
		t.Fatalf("the returning owner's settle: %v, want ErrUnknownSandbox", err)
	}
	if _, held := a.byID(sb.ID); held || !fakeOf(a).removed(vm) {
		t.Errorf("old owner holds the claim %v, VM removed %v, want its copy dropped", held, fakeOf(a).removed(vm))
	}
}

func TestReplicaUnavailablePutThatNeverLandedIsSettledAtTheNextRound(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	old := a.replicaOf(sb)
	row, _, _ := c.reg.Get(t.Context(), sb.ID)
	a.archives = &hookRegistry{Registry: c.reg, failPut: true}

	if err := a.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("round whose put failed: %v, want ErrArchiveStoreDown", err)
	}
	a.mu.Lock()
	pending := sb.ReplicaPending
	a.mu.Unlock()
	if pending == "" || !ckExists(t, a, pending) || !ckExists(t, a, old) {
		t.Fatalf("pending %q kept %v, old kept %v, want both checkpoints kept until settled", pending, ckExists(t, a, pending), ckExists(t, a, old))
	}

	a.archives = c.reg
	replicateNow(t, a, sb)
	if ckExists(t, a, pending) || ckExists(t, a, old) {
		t.Errorf("failed round's checkpoint kept %v, old replica kept %v, want both gone", ckExists(t, a, pending), ckExists(t, a, old))
	}
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Checkpoint != a.replicaOf(sb) || rec.Epoch <= row.Epoch {
		t.Errorf("row %s@%d, want it on the new replica %s", rec.Checkpoint, rec.Epoch, a.replicaOf(sb))
	}
}

func TestReplicaRestartSettlesALostPutReply(t *testing.T) {
	c := newReplicaCluster(t)
	dirA := t.TempDir()
	a := c.node(t, "a", dirA, keepAnHour)
	sb := mustClaim(t, a, testKey)
	a.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
	if err := a.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("round whose put reply was lost: %v", err)
	}
	a.mu.Lock()
	pending := sb.ReplicaPending
	a.mu.Unlock()
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	held, ok := restarted.byID(sb.ID)
	if !ok || restarted.replicaOf(held) != pending || !ckExists(t, restarted, pending) {
		t.Fatalf("claim held %v, replica %q, ck kept %v, want the row's checkpoint %s adopted", ok, restarted.replicaOf(held), ckExists(t, restarted, pending), pending)
	}
	if held.ReplicaPending != "" {
		t.Errorf("intent %q left after the restart settled it", held.ReplicaPending)
	}
}

func TestReplicaRowOfAHibernatedClaimHasNoRetentionDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newReplicaCluster(t)
		a, b := c.node(t, "a", t.TempDir(), 1), c.node(t, "b", t.TempDir(), 1)
		sb := mustClaim(t, a, testKey)
		replicateNow(t, a, sb)
		ck := a.replicaOf(sb)
		hibernateClaim(t, a, sb)
		time.Sleep(2 * time.Second)
		c.expire("a")

		b.sweepOrphanArchives(t.Context())
		rec, ok, _ := c.reg.Get(t.Context(), sb.ID)
		if !ok || !rec.Deadline.IsZero() || !ckExists(t, b, ck) {
			t.Fatalf("row %+v present %v, replica kept %v after a sweep past the retention window, want the row kept with no deadline", rec, ok, ckExists(t, b, ck))
		}
		mustAdopt(t, b, sb, "adopt")
		moved, _ := b.byID(sb.ID)
		if moved.Deadline.IsZero() {
			t.Error("the adopted claim has no retention deadline, want the pool's window from the adoption")
		}
	})
}

func TestReplicaRowIsNeverReaderSafe(t *testing.T) {
	c := newReplicaCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	a.mu.Lock()
	sb.ReaderSafe = true
	a.mu.Unlock()

	replicateNow(t, a, sb)
	row, ok, err := c.reg.Get(t.Context(), sb.ID)
	if err != nil || !ok {
		t.Fatalf("row: %v %v, want one", ok, err)
	}
	claim := &types.Sandbox{}
	if err = json.Unmarshal(row.Claim, claim); err != nil || claim.ReaderSafe {
		t.Errorf("replica row claim reader_safe %v (%v), want false", claim.ReaderSafe, err)
	}
	if !sb.ReaderSafe {
		t.Error("writing the row cleared the live claim's reader_safe")
	}

	c.expire("a")
	mustAdopt(t, b, sb, "adopt")
	sock, _, done, err := b.WakeAgentSocketOrRead(t.Context(), sb.ID, sb.Token)
	if err != nil || sock == "" {
		t.Fatalf("read of the adopted replica = %q, %v, want a wake", sock, err)
	}
	done()
}

func TestArchiveRowKeepsReaderSafe(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	hibernateClaim(t, a, sb)
	a.mu.Lock()
	sb.ReaderSafe = true
	a.mu.Unlock()
	if err := a.archive(t.Context(), sb); err != nil {
		t.Fatalf("archive: %v", err)
	}

	row, _, _ := c.reg.Get(t.Context(), sb.ID)
	claim := &types.Sandbox{}
	if err := json.Unmarshal(row.Claim, claim); err != nil || !claim.ReaderSafe {
		t.Errorf("archive row claim reader_safe %v (%v), want true", claim.ReaderSafe, err)
	}
	c.expire("a")
	mustAdopt(t, b, sb, "adopt")
	if _, _, _, err := b.WakeAgentSocketOrRead(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrSleeping) {
		t.Errorf("read of the adopted archive = %v, want ErrSleeping", err)
	}
}

func TestReplicaRetireAfterAnArchiveLeavesTheArchiveRowAlone(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	mustArchive(t, a, sb)
	row, _, _ := c.reg.Get(t.Context(), sb.ID)
	ck := sb.ArchiveCk

	a.retireReplica(t.Context(), sb)
	rec, ok, _ := c.reg.Get(t.Context(), sb.ID)
	if !ok || rec.Consumed || rec.Epoch != row.Epoch || rec.Checkpoint != ck || !ckExists(t, a, ck) {
		t.Errorf("row %+v present %v, archive ck kept %v, want the archive's row and checkpoint untouched by a stale retire", rec, ok, ckExists(t, a, ck))
	}
}

func TestReplicaClaimThatGainsVolumesStopsBeingReplicated(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)
	a.mu.Lock()
	sb.Volumes = []types.Volume{{Name: "data"}}
	a.mu.Unlock()

	a.replicateOnce(t.Context())
	waitFor(t, func() bool { return !a.replicating.Load() })
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || a.replicaOf(sb) != "" || ckExists(t, a, ck) || sb.HasReplica() {
		t.Errorf("row present %v, replica %q, ck kept %v, has replica %v, want the stale row and replica retired", ok, a.replicaOf(sb), ckExists(t, a, ck), sb.HasReplica())
	}
	if _, held := a.byID(sb.ID); !held {
		t.Error("retiring the replica dropped the claim")
	}
}

func TestReplicaFollowsThePolicyPoolOfAPromotedTemplate(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	a.mu.Lock()
	a.dropClaimLocked(sb)
	sb.Key, sb.PolicySource = tplKey("promoted:v1"), testKey
	a.addClaimLocked(sb)
	a.mu.Unlock()
	if !sb.Replicated {
		t.Error("a claim of a template promoted out of a replicating pool is not marked replicated")
	}

	a.replicateOnce(t.Context())
	waitFor(t, func() bool { return !a.replicating.Load() })
	a.mu.Lock()
	a.replicas[sb.ID] = time.Now().Add(-time.Hour)
	a.mu.Unlock()
	a.replicateOnce(t.Context())
	waitFor(t, func() bool { return !a.replicating.Load() })
	if a.replicaOf(sb) == "" {
		t.Error("a claim of a template promoted out of a replicating pool was not replicated")
	}
}

func TestReplicaInFlightRoundCannotRecreateARowTheAdopterFinished(t *testing.T) {
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
		if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
			t.Errorf("the adopter's wake: %v", err)
		}
	}}

	if err := a.replicate(t.Context(), sb); !errors.Is(err, archives.ErrLost) {
		t.Fatalf("round that outlived the adoption: %v, want ErrLost", err)
	}
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); ok {
		t.Errorf("row %+v recreated by the old owner's round", rec)
	}
	if _, held := a.byID(sb.ID); held || !fakeOf(a).removed(vm) {
		t.Errorf("old owner holds the claim %v, VM removed %v, want its copy dropped", held, fakeOf(a).removed(vm))
	}
	if _, held := b.byID(sb.ID); !held {
		t.Error("the adopter lost the claim it woke")
	}
	if metas, err := a.ckpts.Metas(t.Context()); err != nil || len(metas) != 0 {
		t.Errorf("store holds %d checkpoints %v, want the lost round's export deleted", len(metas), err)
	}
}

func TestReplicaRoundWithAStaleEpochOfItsOwnRowKeepsTheClaim(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	row, _, _ := c.reg.Get(t.Context(), sb.ID)
	if _, err := c.reg.Take(t.Context(), sb.ID, row.Epoch, "a"); err != nil {
		t.Fatalf("take: %v", err)
	}

	if err := a.replicate(t.Context(), sb); !errors.Is(err, archives.ErrLost) {
		t.Fatalf("round at a stale epoch: %v, want ErrLost", err)
	}
	if _, held := a.byID(sb.ID); !held {
		t.Fatal("a stale epoch of the node's own row dropped the claim")
	}
	replicateNow(t, a, sb)
}

func TestReplicaArchiveCannotRecreateARowTheAdopterFinished(t *testing.T) {
	c := newReplicaCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	hibernateClaim(t, a, sb)
	c.expire("a")
	mustAdopt(t, b, sb, "adopt")
	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("the adopter's wake: %v", err)
	}

	if err := a.archive(t.Context(), sb); err == nil {
		t.Fatal("an archive over a row the adopter finished succeeded")
	}
	if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); ok {
		t.Errorf("row %+v recreated by the old owner's archive", rec)
	}
	if _, held := a.byID(sb.ID); held {
		t.Error("the old owner kept a claim the adopter runs")
	}
}

func TestReplicaLeftoverJournalWithFailoverOffStillServesStreams(t *testing.T) {
	eng := newFakeEngine()
	dir := t.TempDir()
	m := newTestManagerAt(t, eng, dir, config.PoolSpec{PoolKey: testKey, Warm: 1})
	sb := mustClaim(t, m, testKey)
	m.mu.Lock()
	sb.ReplicaCk = "ck_leftover"
	js := m.store.set(sb)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		t.Fatalf("commit: %v", err)
	}

	restarted := newTestManagerAt(t, eng, dir, config.PoolSpec{PoolKey: testKey, Warm: 1})
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	held, ok := restarted.byID(sb.ID)
	if !ok || held.ReplicaCk != "ck_leftover" || held.Replicated {
		t.Fatalf("claim held %v replica %q replicated %v, want the leftover kept and not replicated", ok, held.ReplicaCk, held.Replicated)
	}
	_, fenced, done, err := restarted.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if err != nil || fenced != nil {
		t.Fatalf("stream to a claim with a leftover replica and failover off: %v, fence %v, want served with no fence", err, fenced)
	}
	done()
	guest, host := net.Pipe()
	t.Cleanup(func() { _ = host.Close() })
	eng.guestDial = func() (net.Conn, error) { return guest, nil }
	conn, err := restarted.DialPort(t.Context(), sb.ID, Cred{Token: sb.Token}, 80)
	if err != nil {
		t.Fatalf("port dial: %v", err)
	}
	_ = conn.Close()
}

func TestReplicaRoundAfterAWakeKeepsTheClaimAndWritesAFreshRow(t *testing.T) {
	for _, tt := range []struct {
		name           string
		adopt          bool
		leaveTombstone bool
	}{
		{"owner wakes its archive", false, false},
		{"adopter wakes the adopted archive", true, false},
		{"owner wakes and its tombstone delete fails", false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newReplicaCluster(t)
			a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
			sb := mustClaim(t, a, testKey)
			mustArchive(t, a, sb)
			waker, holder := a, sb
			if tt.adopt {
				c.expire("a")
				mustAdopt(t, b, sb, "adopt")
				waker = b
				holder, _ = b.byID(sb.ID)
			}
			if tt.leaveTombstone {
				waker.ckpts = &failingDeleteStore{Store: waker.ckpts, attempts: make(chan string, 8)}
			}
			if _, _, _, err := waker.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
				t.Fatalf("wake: %v", err)
			}
			if epoch := waker.epochOf(holder); epoch != 0 {
				t.Errorf("woken claim holds epoch %d of the consumed row, want 0", epoch)
			}

			replicateNow(t, waker, holder)
			if _, held := waker.byID(sb.ID); !held {
				t.Fatal("the round after the wake dropped the running claim")
			}
			rec, ok, _ := c.reg.Get(t.Context(), sb.ID)
			if !ok || rec.Consumed || rec.Owner != waker.nodeID || rec.Checkpoint != waker.replicaOf(holder) {
				t.Errorf("row %+v present %v, want a live row of %s on the new replica %q", rec, ok, waker.nodeID, waker.replicaOf(holder))
			}
		})
	}
}

func TestReplicaFailedCaptureKeepsTheOldReplicaAndRow(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	first := a.replicaOf(sb)
	before, _, _ := c.reg.Get(t.Context(), sb.ID)

	fakeOf(a).snapSaveErr = errors.New("pause failed")
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
	eng := fakeOf(a)

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
		running := mustClaim(t, a, testKey)
		paused := mustClaim(t, a, testKey)
		withVolume := mustClaim(t, a, testKey)
		hibernateClaim(t, a, paused)
		a.mu.Lock()
		withVolume.Volumes = []types.Volume{{Name: "data"}}
		a.mu.Unlock()
		eng := fakeOf(a)
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
		if _, ok, _ := c.reg.Get(t.Context(), running.ID); !ok {
			t.Error("the due claim has no row")
		}
	})
}

func TestAFencedNodeNeitherHibernatesNorWakesAReplicatedClaim(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb, asleep := mustClaim(t, a, testKey), mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	hibernateClaim(t, a, asleep)
	a.tripFence()
	if err := a.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("hibernate while fenced: %v, want ErrLeaseLost", err)
	}
	if err := a.Wake(t.Context(), asleep.ID, Cred{Token: asleep.Token}); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("wake while fenced: %v, want ErrLeaseLost", err)
	}
	if got, want := usageEventsOf(t, a, sb.ID), "claim"; got != want {
		t.Errorf("events of the running claim %s, want %s", got, want)
	}
	if got, want := usageEventsOf(t, a, asleep.ID), "claim,hibernate"; got != want {
		t.Errorf("events of the hibernated claim %s, want %s", got, want)
	}
}

func TestAFencedNodeCapturesNoReplicatedClaim(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb, plain := mustClaim(t, a, testKey), mustClaim(t, a, plainKey)
	replicateNow(t, a, sb)
	a.tripFence()
	cred := Cred{Token: sb.Token}
	if _, err := a.Fork(t.Context(), sb.ID, cred, 1, time.Hour, "", ""); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("fork while fenced: %v, want ErrLeaseLost", err)
	}
	if _, err := a.Checkpoint(t.Context(), sb.ID, cred, "", ""); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("checkpoint while fenced: %v, want ErrLeaseLost", err)
	}
	if _, _, err := a.Promote(t.Context(), sb.ID, cred, "tpl", ""); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("promote while fenced: %v, want ErrLeaseLost", err)
	}
	if err := a.replicate(t.Context(), sb); !errors.Is(err, ErrLeaseLost) || !benignSweepErr(err) {
		t.Errorf("replicate while fenced: %v, want a benign ErrLeaseLost", err)
	}
	if _, err := a.Checkpoint(t.Context(), plain.ID, Cred{Token: plain.Token}, "", ""); err != nil {
		t.Errorf("checkpoint of an unreplicated claim while fenced: %v", err)
	}
	a.liftFence()
	if _, err := a.Fork(t.Context(), sb.ID, cred, 1, time.Hour, "", ""); err != nil {
		t.Errorf("fork after the fence lifted: %v", err)
	}
}

func TestTheSweepsLeaveAReplicatedClaimAloneWhileFenced(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	backdate(a, sb, time.Hour)
	a.tripFence()
	sweepIdle(t, a)
	if hibernated(a) != 0 {
		t.Error("the idle sweep hibernated a replicated claim while fenced")
	}
	a.mu.Lock()
	a.archiving[sb.ID] = struct{}{}
	a.mu.Unlock()
	if err := a.pauseArchive(t.Context(), sb); !errors.Is(err, ErrLeaseLost) || !benignSweepErr(err) {
		t.Errorf("pause of an expired claim while fenced: %v, want a benign ErrLeaseLost", err)
	}
	a.mu.Lock()
	_, tracked := a.archiving[sb.ID]
	a.mu.Unlock()
	if tracked {
		t.Error("the refused pause left the claim marked as archiving")
	}
	a.liftFence()
	sweepIdle(t, a)
	if hibernated(a) != 1 {
		t.Error("the idle sweep did not hibernate the claim once the fence lifted")
	}
}

func TestReplicaPassSkipsWhileFenced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newReplicaCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		a.tripFence()
		sb := mustClaim(t, a, testKey)
		a.replicateOnce(t.Context())
		time.Sleep(replicateEvery * time.Second)
		a.replicateOnce(t.Context())
		synctest.Wait()
		if a.replicaOf(sb) != "" || len(fakeOf(a).snapSaves) != 0 {
			t.Error("a fenced node replicated")
		}
	})
}

func TestReplicaPassRetiresAReplicaWhenThePoolTurnsItOff(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)

	disableReplication(a)
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
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || ckExists(t, a, ck) || !fakeOf(a).removed(vm) {
		t.Errorf("row present %v, replica kept %v, VM removed %v, want the row and replica gone and the VM removed", ok, ckExists(t, a, ck), fakeOf(a).removed(vm))
	}
	assertReleasedForGood(t, c, sb)
}

func TestReplicaHibernateKeepsTheRowOnTheLastReplica(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	before, _, _ := c.reg.Get(t.Context(), sb.ID)

	hibernateClaim(t, a, sb)
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
	checkpointBeforeDelete := make(chan bool, 1)
	a.archives = &hookRegistry{Registry: c.reg, beforeDelete: func() {
		checkpointBeforeDelete <- ckExists(t, a, ck)
	}}
	a.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Second)
	a.mu.Unlock()

	a.reapOnce(t.Context())
	if !<-checkpointBeforeDelete {
		t.Fatal("checkpoint disappeared before its registry tombstone was deleted")
	}
	waitFor(t, func() bool { return a.replicaOf(sb) == "" && !ckExists(t, a, ck) })
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok {
		t.Error("the row outlived the first reap")
	}
	if _, held := a.byID(sb.ID); !held {
		t.Fatal("the first reap destroyed the claim before its row was dropped")
	}
	a.reapOnce(t.Context())
	waitFor(t, func() bool { return fakeOf(a).removed(vm) })
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
	if _, _, _, err := restarted.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrLeaseLost) {
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
	interrupted, _, publishErr := a.publishCheckpoint(t.Context(), sb, store.CheckpointID(randHex(8)), "replica", sb.Tenant, "", true)
	if publishErr != nil {
		t.Fatalf("publish: %v", publishErr)
	}
	if _, putErr := a.putReplica(t.Context(), sb, interrupted.ID); putErr != nil {
		t.Fatalf("put: %v", putErr)
	}
	second := interrupted.ID

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

func TestAReplicaRowWithNoClaimLeftGainsRetentionAndIsPurged(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)
	restarted := restartWithoutVM(t, c, a, sb, keepAnHour)

	restarted.sweepOrphanArchives(t.Context())
	rec, ok, err := c.reg.Get(t.Context(), sb.ID)
	if err != nil || !ok || rec.Deadline.IsZero() || !ckExists(t, restarted, ck) {
		t.Fatalf("row of the dropped claim after a sweep: %+v %t %v, want it kept with a deadline", rec, ok, err)
	}

	rec.Deadline = time.Now().Add(-time.Second)
	if _, err = c.reg.Replace(t.Context(), rec, rec.Epoch); err != nil {
		t.Fatalf("age the row: %v", err)
	}
	restarted.sweepOrphanArchives(t.Context())
	if _, ok, err = c.reg.Get(t.Context(), sb.ID); err != nil || ok || ckExists(t, restarted, ck) {
		t.Errorf("row %t %v after retention, want the row and its checkpoint gone", ok, err)
	}
	if got, want := usageEventsOf(t, restarted, sb.ID), "claim,reap,archive_delete"; got != want {
		t.Errorf("events %s, want %s", got, want)
	}
}

func TestAReplicaRowWithNoClaimLeftStaysAdoptable(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	restarted := restartWithoutVM(t, c, a, sb, keepAnHour)
	restarted.liftFence()
	restarted.sweepOrphanArchives(t.Context())
	mustAdopt(t, restarted, sb, "adopt inside retention")
}

func TestAReplicaRowOfAKeepForeverPoolKeepsNoDeadline(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	restarted := restartWithoutVM(t, c, a, sb, 0)
	before, _, err := c.reg.Get(t.Context(), sb.ID)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	restarted.sweepOrphanArchives(t.Context())
	rec, ok, err := c.reg.Get(t.Context(), sb.ID)
	if err != nil || !ok || !rec.Deadline.IsZero() || rec.Epoch != before.Epoch {
		t.Errorf("row %+v %t %v, want it untouched at epoch %d", rec, ok, err, before.Epoch)
	}
}

func TestReconcileWithTheRegistryDownRecordsOnlyTheEndNoOtherNodeCanHold(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	replicated, plain := mustClaim(t, a, testKey), mustClaim(t, a, plainKey)
	replicateNow(t, a, replicated)
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	eng := fakeOf(a)
	delete(eng.vms, replicated.VMName)
	delete(eng.vms, plain.VMName)
	c.reg.SetDown(true)
	restarted := c.restart(t, a)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c.reg.SetDown(false)
	if got, want := usageEventsOf(t, restarted, replicated.ID), "claim"; got != want {
		t.Errorf("events of the replicated claim %s, want %s", got, want)
	}
	if got, want := usageEventsOf(t, restarted, plain.ID), "claim,reap"; got != want {
		t.Errorf("events of the unreplicated claim %s, want %s", got, want)
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
	mustAdopt(t, b, sb, "adopt")

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
	mustAdopt(t, b, sb, "adopt")
	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
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
	if !ok || rec.Consumed || rec.Checkpoint != ck || rec.Owner != "a" || !rec.Deadline.IsZero() || !ckExists(t, a, ck) {
		t.Errorf("row %+v present %v, replica kept %v, want a live row of a with no deadline re-armed on %s", rec, ok, ckExists(t, a, ck), ck)
	}
}

func TestReplicaSweepSparesTheRowAndCheckpointOfAnUnsettledRound(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	a.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
	if err := a.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("round whose put reply was lost: %v", err)
	}
	a.mu.Lock()
	pending := sb.ReplicaPending
	a.mu.Unlock()
	rec, ok, _ := c.reg.Get(t.Context(), sb.ID)
	if !ok || rec.Checkpoint != pending {
		t.Fatalf("row %+v present %v, want it on the pending checkpoint %s", rec, ok, pending)
	}

	a.reconcileOwnRow(t.Context(), rec, time.Now())
	if _, held := a.byID(sb.ID); !held {
		t.Fatal("the sweep dropped the claim")
	}
	if again, ok, _ := c.reg.Get(t.Context(), sb.ID); !ok || again.Epoch != rec.Epoch || !ckExists(t, a, pending) {
		t.Errorf("row %+v present %v, checkpoint kept %v, want both intact until the intent settles", again, ok, ckExists(t, a, pending))
	}
	if _, pinned := a.pinnedArchiveCks()[pending]; !pinned {
		t.Error("the unsettled round's checkpoint is not pinned")
	}
	sb.Transition.Lock()
	err := a.resolveIntent(t.Context(), sb)
	sb.Transition.Unlock()
	if err != nil || a.replicaOf(sb) != pending {
		t.Errorf("settle: %v, replica %q, want the row's checkpoint %s adopted", err, a.replicaOf(sb), pending)
	}
}

func TestReplicaRestartKeepsTheCheckpointOfARoundItCannotSettleYet(t *testing.T) {
	c := newReplicaCluster(t)
	dirA := t.TempDir()
	a := c.node(t, "a", dirA, keepAnHour)
	sb := mustClaim(t, a, testKey)
	a.archives = &hookRegistry{Registry: c.reg, failPut: true}
	if err := a.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("round whose put failed: %v", err)
	}
	a.mu.Lock()
	pending := sb.ReplicaPending
	a.mu.Unlock()
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	restarted := c.restart(t, a)
	restarted.ckpts = &unreadableStore{Store: restarted.ckpts}
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	held, ok := restarted.byID(sb.ID)
	if !ok || held.ReplicaPending != pending || !ckExists(t, restarted, pending) {
		t.Errorf("claim held %v, intent %q, ck kept %v, want the intent and its checkpoint kept for the next settle", ok, held.ReplicaPending, ckExists(t, restarted, pending))
	}
}

func TestReplicaALeftoverDeleteMarkerSparesTheLiveReplica(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	ck := a.replicaOf(sb)
	if err := a.ckMarks.mark(ck); err != nil {
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

	mustNotAdopt(t, b, sb, "adopt while a's lease is live")
	c.expire("a")
	mustAdopt(t, b, sb, "adopt after the lease expired")
	moved, _ := b.byID(sb.ID)
	if moved.ArchiveCk != ck {
		t.Fatalf("adopted claim names %q, want the replica %q", moved.ArchiveCk, ck)
	}
	evs := usageJournal(t, b)
	last := evs[len(evs)-1]
	if want := b.deadlineOf(moved); last.Event != "adopt" || last.ID != sb.ID || last.Reference != ck || want.IsZero() || !last.Deadline.Equal(want) {
		t.Errorf("last event %+v, want an adopt of %s naming %s with deadline %v", last, sb.ID, ck, want)
	}
	if _, _, _, err := b.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("adopter's wake: %v", err)
	}
	if got, want := usageEventsOf(t, b, sb.ID), "adopt,unarchive"; got != want {
		t.Errorf("events on the adopter %s, want %s", got, want)
	}
	if got := fakeOf(b).cloneCount(); got != 1 {
		t.Errorf("the wake cloned %d VMs, want the one restored from the replica", got)
	}
	if _, ok, _ := c.reg.Get(t.Context(), sb.ID); ok || ckExists(t, b, ck) {
		t.Error("the adopter's wake left the row or the replica")
	}
}

func TestReplicaEnvRotationOfAHibernatedClaimReachesTheAdopter(t *testing.T) {
	c := newReplicaCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	patchHostOnly(t, a, sb, "old")
	replicateNow(t, a, sb)
	hibernateClaim(t, a, sb)
	replica := sb.ReplicaCk

	patchHostOnly(t, a, sb, "new")
	if got := rowEnv(t, c, sb); got != "new" {
		t.Errorf("row env %q after a rotation, want new", got)
	}
	rec, _, err := c.reg.Get(t.Context(), sb.ID)
	if err != nil {
		t.Fatalf("row: %v", err)
	}
	if rec.Epoch != a.epochOf(sb) || rec.Checkpoint != replica || !rec.Deadline.IsZero() {
		t.Errorf("row %+v claim epoch %d, want it on the replica %s at the claim's epoch with no deadline", rec, a.epochOf(sb), replica)
	}

	c.expire("a")
	mustAdopt(t, b, sb, "adopt")
	held, ok := b.byID(sb.ID)
	if !ok {
		t.Fatal("adopter has no claim")
	}
	if got, _ := held.HiddenEnv("GH_TOKEN"); got != "new" {
		t.Errorf("adopter holds %q, want the rotated value", got)
	}
}

func TestReplicaEnvWriteOfARunningClaimLeavesTheRowToTheRound(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	reg := &hookRegistry{Registry: c.reg, failPut: true}
	a.archives = reg
	patchHostOnly(t, a, sb, "new")
	if reg.replaces != 0 {
		t.Errorf("%d registry replaces for a running replicated claim, want 0", reg.replaces)
	}
}

func TestReplicaRenewOfAHibernatedClaimReachesTheAdopter(t *testing.T) {
	c := newReplicaCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	hibernateClaim(t, a, sb)

	if _, err := a.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, 2*time.Hour, types.ExpireArchive); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if got := rowClaim(t, c, sb); got.OnExpire != types.ExpireArchive || got.LeaseSeconds != 7200 {
		t.Errorf("row claim on_expire %q lease %d s after a renew, want archive and 7200", got.OnExpire, got.LeaseSeconds)
	}

	c.expire("a")
	mustAdopt(t, b, sb, "adopt")
	held, ok := b.byID(sb.ID)
	if !ok {
		t.Fatal("adopter has no claim")
	}
	if held.OnExpire != types.ExpireArchive || held.LeaseSeconds != 7200 {
		t.Errorf("adopter holds on_expire %q lease %d s, want the renewed archive and 7200", held.OnExpire, held.LeaseSeconds)
	}
}

func TestReplicaRenewOfAHibernatedClaimWithTheRegistryDownIsLoudAndARetryRepairs(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	hibernateClaim(t, a, sb)
	reg := &hookRegistry{Registry: c.reg, failPut: true}
	a.archives = reg

	if _, err := a.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, 2*time.Hour, types.ExpireArchive); !errors.Is(err, archives.ErrUnavailable) {
		t.Fatalf("renew with the registry down: %v, want ErrUnavailable", err)
	}
	if got := rowClaim(t, c, sb); got.OnExpire == types.ExpireArchive {
		t.Error("row took the renew while the registry was down")
	}
	reg.failPut = false
	if _, err := a.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, 2*time.Hour, types.ExpireArchive); err != nil {
		t.Fatalf("resend: %v", err)
	}
	if got := rowClaim(t, c, sb); got.OnExpire != types.ExpireArchive || got.LeaseSeconds != 7200 {
		t.Errorf("row claim on_expire %q lease %d s after the resend, want archive and 7200", got.OnExpire, got.LeaseSeconds)
	}
}

func TestReplicaRenewOfARunningClaimLeavesTheRowToTheRound(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	reg := &hookRegistry{Registry: c.reg, failPut: true}
	a.archives = reg
	if _, err := a.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, 2*time.Hour, types.ExpireArchive); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if reg.replaces != 0 {
		t.Errorf("%d registry replaces for a running replicated claim, want 0", reg.replaces)
	}
}

func TestReplicaArchiveRetriedAfterARegistryFaultKeepsTheClaim(t *testing.T) {
	for _, tt := range []struct {
		name string
		reg  func(*failoverCluster) *hookRegistry
	}{
		{"the replace failed", func(c *failoverCluster) *hookRegistry { return &hookRegistry{Registry: c.reg, failPut: true} }},
		{"the replace landed and its reply was lost", func(c *failoverCluster) *hookRegistry {
			return &hookRegistry{Registry: c.reg, losePutReply: true}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newReplicaCluster(t)
			a := c.node(t, "a", t.TempDir(), keepAnHour)
			sb := mustClaim(t, a, testKey)
			replicateNow(t, a, sb)
			hibernateClaim(t, a, sb)
			a.archives = tt.reg(c)
			if err := a.archive(t.Context(), sb); err == nil {
				t.Fatal("archive under the fault succeeded")
			}
			a.archives = c.reg
			if err := a.archive(t.Context(), sb); err != nil {
				t.Fatalf("retried archive: %v", err)
			}
			if _, held := a.byID(sb.ID); !held || sb.ArchiveCk == "" {
				t.Fatalf("claim held=%v archive ck %q, want it archived", held, sb.ArchiveCk)
			}
			rec, ok, err := c.reg.Get(t.Context(), sb.ID)
			if err != nil || !ok || rec.Consumed || rec.Checkpoint != sb.ArchiveCk || rec.Epoch != sb.ArchiveEpoch {
				t.Errorf("row %+v ok=%v err=%v, want it live on %s at epoch %d", rec, ok, err, sb.ArchiveCk, sb.ArchiveEpoch)
			}
		})
	}
}

func TestReplicaRestartMidArchiveKeepsTheClaim(t *testing.T) {
	for _, tt := range []struct {
		name      string
		interrupt func(t *testing.T, c *failoverCluster, a *Manager, sb *types.Sandbox)
	}{
		{"the archive row never landed", func(t *testing.T, _ *failoverCluster, a *Manager, sb *types.Sandbox) {
			interruptArchive(t, a, sb, false)
		}},
		{"the archive row landed", func(t *testing.T, _ *failoverCluster, a *Manager, sb *types.Sandbox) {
			interruptArchive(t, a, sb, true)
		}},
		{"the journal write failed", func(t *testing.T, c *failoverCluster, a *Manager, sb *types.Sandbox) {
			a.archives = &hookRegistry{Registry: c.reg, beforePut: func() { breakStore(t, a) }}
			if err := a.archive(t.Context(), sb); err == nil {
				t.Fatal("archive with an unwritable journal succeeded")
			}
			if rec, ok, err := c.reg.Get(t.Context(), sb.ID); err != nil || !ok || rec.Consumed || rec.Owner != "a" {
				t.Errorf("row %+v ok=%v err=%v, want this node's live row", rec, ok, err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newReplicaCluster(t)
			dir := t.TempDir()
			a := c.node(t, "a", dir, keepAnHour)
			sb := mustClaim(t, a, testKey)
			vm := lockedVMName(sb)
			replicateNow(t, a, sb)
			replica := sb.ReplicaCk
			hibernateClaim(t, a, sb)
			tt.interrupt(t, c, a, sb)
			eng := fakeOf(a)

			restarted := c.nodeOn(t, eng, "a", dir, keepAnHour)
			if err := restarted.Reconcile(t.Context()); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			rec, ok, err := c.reg.Get(t.Context(), sb.ID)
			if err != nil || !ok || rec.Consumed || rec.Checkpoint != replica {
				t.Errorf("row %+v ok=%v err=%v, want it live on the replica %s", rec, ok, err, replica)
			}
			c.renew(t, "a")
			restarted.unfence(t.Context(), time.Now())
			held, kept := restarted.byID(sb.ID)
			if !kept || eng.removed(vm) {
				t.Fatalf("restart dropped a claim nobody adopted: held=%v VM removed=%v", kept, eng.removed(vm))
			}
			if err = restarted.archive(t.Context(), held); err != nil {
				t.Fatalf("archive after the restart: %v", err)
			}
			if rec, ok, err = c.reg.Get(t.Context(), sb.ID); err != nil || !ok || rec.Checkpoint != held.ArchiveCk {
				t.Errorf("row %+v ok=%v err=%v, want it on %s", rec, ok, err, held.ArchiveCk)
			}
		})
	}
}

func TestARunningClaimThatMovedClosesItsDoors(t *testing.T) {
	c := newReplicaCluster(t)
	c.egressPol = &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}
	ea, eb := newFakeEngine(), newFakeEngine()
	ea.sockRoot, eb.sockRoot = outboundtest.SockRoot(t), outboundtest.SockRoot(t)
	a := c.nodeOn(t, ea, "a", t.TempDir(), keepAnHour)
	a.liftFence()
	b := c.nodeOn(t, eb, "b", t.TempDir(), keepAnHour)
	b.liftFence()
	sb := mustClaim(t, a, testKey)
	if got := a.out.Route(sb); got != types.NetRouteRelay {
		t.Fatalf("route after claim %q, want relay", got)
	}
	replicateNow(t, a, sb)
	c.expire("a")
	mustAdopt(t, b, sb, "adoption after the lease expired")
	c.renew(t, "a")
	a.trip(t.Context())
	a.unfence(t.Context(), time.Now())
	waitFor(t, func() bool { return ea.removed(sb.VMName) })
	if got := a.out.Route(sb); got != types.NetRouteNone {
		t.Errorf("route after the claim moved %q, want none", got)
	}
	if conn, err := net.Dial("unix", engine.EgressSocketPath(sb.VsockSocket)); err == nil {
		_ = conn.Close()
		t.Error("the door of the removed VM still accepts")
	}
}

func restartWithoutVM(t *testing.T, c *failoverCluster, a *Manager, sb *types.Sandbox, deleteAfter int) *Manager {
	t.Helper()
	if err := a.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	eng := fakeOf(a)
	delete(eng.vms, sb.VMName)
	restarted := c.nodeOn(t, eng, a.nodeID, a.dataDir, deleteAfter)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return restarted
}
