package pool

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/peer"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// TestClaimCheckpointNeverPullsOnMiss: ClaimCheckpoint answers from the local
// store alone — a redirect exists precisely so this path never pays a peer
// transfer; only ClaimCheckpointHeal may.
func TestClaimCheckpointNeverPullsOnMiss(t *testing.T) {
	ckpt := types.Checkpoint{ID: "ck_00000000000000bb", Key: testKey, CreatedAt: time.Now()}
	m, puller := newHealManager(t, ckpt, []string{"peer-a:7777"})

	if _, err := m.ClaimCheckpoint(t.Context(), "ck_00000000000000cc", time.Hour, ""); !errors.Is(err, ErrUnknownCheckpoint) {
		t.Errorf("claim: %v, want ErrUnknownCheckpoint", err)
	}
	if got := puller.count(); got != 0 {
		t.Errorf("puller called %d times, want 0: ClaimCheckpoint must never pull", got)
	}
}

// TestClaimCheckpointHealPullsOnce: a heal pays the transfer once and leaves
// the record served from the local store from then on.
func TestClaimCheckpointHealPullsOnce(t *testing.T) {
	id := "ck_00000000000000bb"
	ckpt := types.Checkpoint{ID: id, Key: testKey, CreatedAt: time.Now()}
	m, puller := newHealManager(t, ckpt, []string{"peer-a:7777"})

	sb, err := m.ClaimCheckpointHeal(t.Context(), id, time.Hour, "")
	if err != nil {
		t.Fatalf("ClaimCheckpointHeal: %v", err)
	}
	if sb.FromCheckpoint != id {
		t.Errorf("FromCheckpoint = %q, want %q", sb.FromCheckpoint, id)
	}
	if got := puller.count(); got != 1 {
		t.Errorf("puller called %d times, want 1", got)
	}
	if !m.HasCheckpoint(t.Context(), id) {
		t.Errorf("HasCheckpoint(%s) = false, want true after heal", id)
	}
}

// TestClaimCheckpointAfterHealStaysLocal: once healed, the record is served
// from the local store — a later branch must not pull again.
func TestClaimCheckpointAfterHealStaysLocal(t *testing.T) {
	id := "ck_00000000000000bb"
	ckpt := types.Checkpoint{ID: id, Key: testKey, CreatedAt: time.Now()}
	m, puller := newHealManager(t, ckpt, []string{"peer-a:7777"})

	if _, err := m.ClaimCheckpointHeal(t.Context(), id, time.Hour, ""); err != nil {
		t.Fatalf("ClaimCheckpointHeal: %v", err)
	}
	if _, err := m.ClaimCheckpoint(t.Context(), id, time.Hour, ""); err != nil {
		t.Fatalf("ClaimCheckpoint after heal: %v", err)
	}
	if got := puller.count(); got != 1 {
		t.Errorf("puller called %d times after a local claim, want still 1 (pay once)", got)
	}
}

// TestClaimCheckpointHealDedupsConcurrentPulls: two branches racing to heal
// the same missing checkpoint must share one transfer, not each pay for it —
// healFlights (not the record lock, which no longer spans the transfer)
// dedups them onto one Manager-owned staging dir, and each branch must
// still get a genuinely valid, non-empty checkpoint out of it (the exact
// case a caller-owned-staging-dir singleflight used to get wrong).
func TestClaimCheckpointHealDedupsConcurrentPulls(t *testing.T) {
	id := "ck_00000000000000bb"
	ckpt := types.Checkpoint{ID: id, Key: testKey, CreatedAt: time.Now()}
	m, puller := newHealManager(t, ckpt, []string{"peer-a:7777"})
	puller.release = make(chan struct{})

	var wg sync.WaitGroup
	sbs := make([]*types.Sandbox, 2)
	errs := make([]error, 2)
	for i := range 2 {
		wg.Go(func() {
			sbs[i], errs[i] = m.ClaimCheckpointHeal(t.Context(), id, time.Hour, "")
		})
	}
	waitFor(t, func() bool { return puller.count() >= 1 })
	time.Sleep(50 * time.Millisecond) // let the second goroutine join the same flight
	close(puller.release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
			continue
		}
		if sbs[i] == nil || sbs[i].FromCheckpoint != id {
			t.Errorf("goroutine %d: sandbox = %+v, want a valid branch of %s", i, sbs[i], id)
		}
	}
	if got := puller.count(); got != 1 {
		t.Errorf("puller called %d times for two concurrent heals of one id, want 1", got)
	}
}

// TestDeleteVetoesInFlightHeal covers the Codex r2 blocker: a heal's transfer
// runs unlocked (holding the lock across a 30-minute budget would pin every
// other operation on the id behind it), so a delete arriving while it stages
// must not block on that transfer — but it must also not let the heal
// resurrect the checkpoint moments after the delete answered "not here" for
// it. The delete returns immediately (nothing local to delete yet) and
// vetoes the pending heal, so the heal's own locked decide phase abandons
// its publish instead of racing the delete to a stale conclusion.
func TestDeleteVetoesInFlightHeal(t *testing.T) {
	id := "ck_00000000000000bb"
	ckpt := types.Checkpoint{ID: id, Key: testKey, CreatedAt: time.Now()}
	m, puller := newHealManager(t, ckpt, []string{"peer-a:7777"})
	puller.release = make(chan struct{})

	healDone := make(chan error, 1)
	go func() {
		_, err := m.ClaimCheckpointHeal(t.Context(), id, time.Hour, "")
		healDone <- err
	}()
	waitFor(t, func() bool { return puller.count() >= 1 }) // heal is staging, unpublished

	if err := m.DeleteCheckpoint(t.Context(), id, "", DeleteLocal); !errors.Is(err, ErrUnknownCheckpoint) {
		t.Fatalf("delete while heal is staging: %v, want ErrUnknownCheckpoint (nothing local yet, and it must not block)", err)
	}

	close(puller.release)
	if err := <-healDone; !errors.Is(err, ErrUnknownCheckpoint) {
		t.Errorf("heal after a concurrent delete: %v, want ErrUnknownCheckpoint (vetoed, must not publish)", err)
	}
	if m.HasCheckpoint(t.Context(), id) {
		t.Error("checkpoint present after a delete vetoed the pending heal — resurrection")
	}
}

// TestClaimCheckpointHealCtxCancelReturnsPromptly: a caller that gives up
// waiting must not be stuck behind the whole heal budget — the flight runs
// detached (context.Background() in runHeal), so this call returns
// ctx.Err() as soon as its own ctx is canceled, while the transfer keeps
// running and still lands.
func TestClaimCheckpointHealCtxCancelReturnsPromptly(t *testing.T) {
	id := "ck_00000000000000bb"
	ckpt := types.Checkpoint{ID: id, Key: testKey, CreatedAt: time.Now()}
	m, puller := newHealManager(t, ckpt, []string{"peer-a:7777"})
	puller.release = make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := m.ClaimCheckpointHeal(ctx, id, time.Hour, "")
		done <- err
	}()
	waitFor(t, func() bool { return puller.count() >= 1 }) // transfer started, unlocked

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ClaimCheckpointHeal after cancel: %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ClaimCheckpointHeal did not return promptly after ctx cancellation")
	}

	// Detached from the canceled caller, the transfer must still complete
	// and publish.
	close(puller.release)
	waitFor(t, func() bool { return m.HasCheckpoint(t.Context(), id) })
}

// TestRecLockEvictionWaitsForAllHolders covers the lock-identity-split half
// of the same Codex r2 blocker: evicting an id's entry while another holder
// still references it would let that holder's (or a new caller's) next
// recLock for the same id return a different mutex, splitting mutual
// exclusion between them. Eviction must wait until every holder has
// released, not just the one that happened to delete the record.
func TestRecLockEvictionWaitsForAllHolders(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	const id = "ck_00000000000000aa"

	l1 := m.recLock(id)
	l1.Lock()
	l2 := m.recLock(id) // a second, concurrent holder registers interest first
	if l2 != l1 {
		t.Fatalf("recLock returned a different mutex while the first holder is still outstanding")
	}
	l1.Unlock()
	m.recDoneEvict(id) // first holder's release must not evict: l2 is still outstanding
	if _, ok := m.recLocks.Load(id); !ok {
		t.Fatal("entry evicted while a second holder still references it")
	}

	l3 := m.recLock(id) // a third caller arriving now must still find the SAME mutex
	if l3 != l1 {
		t.Fatal("a concurrent recLock diverged onto a different mutex for the same id — the lock-identity split")
	}

	m.recDone(id)      // l2's release
	m.recDoneEvict(id) // l3's release, last one out
	if _, ok := m.recLocks.Load(id); ok {
		t.Error("entry not evicted once every holder released")
	}
}

// TestRecLockEvictDeferredToLastHolder is the production sequence the test
// above sidesteps: a delete (recDoneEvict) releases while an ordinary holder
// (recDone) still references the id, so the ordinary holder is the last one
// out. The eviction the delete asked for must not be lost.
func TestRecLockEvictDeferredToLastHolder(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	const id = "ck_00000000000000aa"

	l1 := m.recLock(id) // the delete
	l2 := m.recLock(id) // a concurrent claim/fetch
	if l2 != l1 {
		t.Fatal("recLock diverged for the same id")
	}
	m.recDoneEvict(id) // delete releases first, wants eviction, but l2 still holds
	if _, ok := m.recLocks.Load(id); !ok {
		t.Fatal("entry evicted while an ordinary holder still references it")
	}
	m.recDone(id) // the ordinary holder, last one out
	if _, ok := m.recLocks.Load(id); ok {
		t.Error("recLocks entry leaked: a delete's deferred eviction was lost when a plain recDone dropped the last reference")
	}
}

// TestFetchCheckpointNeverPulls: the peer-transfer blob endpoint must never
// trigger a recursive pull, even with a healer installed that could serve it.
func TestFetchCheckpointNeverPulls(t *testing.T) {
	id := "ck_00000000000000bb"
	ckpt := types.Checkpoint{ID: id, Key: testKey, CreatedAt: time.Now()}
	m, puller := newHealManager(t, ckpt, []string{"peer-a:7777"})

	if _, _, _, err := m.FetchCheckpoint(t.Context(), id); !errors.Is(err, ErrUnknownCheckpoint) {
		t.Errorf("FetchCheckpoint: %v, want ErrUnknownCheckpoint", err)
	}
	if got := puller.count(); got != 0 {
		t.Errorf("puller called %d times, want 0: the blob endpoint must never pull", got)
	}
}

// TestHasCheckpoint covers the probe answer across a present, missing, and
// archive record: only a live branchable checkpoint reports true.
func TestHasCheckpoint(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	src := mustClaim(t, m, testKey)

	ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "", "")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if !m.HasCheckpoint(t.Context(), ckpt.ID) {
		t.Errorf("HasCheckpoint(%s) = false, want true for a published record", ckpt.ID)
	}
	if m.HasCheckpoint(t.Context(), "ck_00000000000000ff") {
		t.Error("HasCheckpoint = true for a missing id, want false")
	}

	sb := mustClaim(t, m, testKey)
	mustArchive(t, m, sb)
	if m.HasCheckpoint(t.Context(), sb.ArchiveCk) {
		t.Errorf("HasCheckpoint(%s) = true for an archive wake image, want false", sb.ArchiveCk)
	}
}

// TestClaimCheckpointQuotaDoesNotMaskMiss: a full node must still answer
// "not here" for a checkpoint it does not hold, or the handler's
// redirect/heal tiers behind ErrUnknownCheckpoint never run; a full node
// that DOES hold the record still answers the quota error.
func TestClaimCheckpointQuotaDoesNotMaskMiss(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	src := mustClaim(t, m, testKey)
	ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "", "")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	m.draining = true

	if _, err := m.ClaimCheckpoint(t.Context(), "ck_00000000000000ff", time.Hour, ""); !errors.Is(err, ErrUnknownCheckpoint) {
		t.Errorf("claim missing id while draining: %v, want ErrUnknownCheckpoint (so the handler can redirect)", err)
	}
	if _, err := m.ClaimCheckpoint(t.Context(), ckpt.ID, time.Hour, ""); !errors.Is(err, ErrQuota) {
		t.Errorf("claim present id while draining: %v, want ErrQuota", err)
	}
}

// TestClaimCheckpointHealQuotaBeforePull: unlike ClaimCheckpoint's cheap
// local read, resolving a heal means a peer transfer — quota must reject a
// full node before that cost, so the puller must never be called.
func TestClaimCheckpointHealQuotaBeforePull(t *testing.T) {
	id := "ck_00000000000000bb"
	ckpt := types.Checkpoint{ID: id, Key: testKey, CreatedAt: time.Now()}
	m, puller := newHealManager(t, ckpt, []string{"peer-a:7777"})
	m.draining = true

	if _, err := m.ClaimCheckpointHeal(t.Context(), id, time.Hour, ""); !errors.Is(err, ErrQuota) {
		t.Errorf("heal while draining: %v, want ErrQuota", err)
	}
	if got := puller.count(); got != 0 {
		t.Errorf("puller called %d times, want 0: quota must reject before the pull", got)
	}
}

// TestClaimCheckpointHealConcurrencyCapRejectsExtra: heals of DIFFERENT ids
// (so the recLock does not serialize them) must still be capped node-wide —
// each pulls a whole guest memory image, so distinct ids are not free just
// because they don't contend on the same lock.
func TestClaimCheckpointHealConcurrencyCapRejectsExtra(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	m.healSem = make(chan struct{}, 1)
	puller := &blockingPuller{started: make(chan struct{}), release: make(chan struct{})}
	m.healer = peer.NewHealer(func(string) []string { return []string{"peer-a:7777"} }, puller)

	done := make(chan error, 1)
	go func() {
		_, err := m.ClaimCheckpointHeal(t.Context(), "ck_00000000000000aa", time.Hour, "")
		done <- err
	}()
	<-puller.started

	if _, err := m.ClaimCheckpointHeal(t.Context(), "ck_00000000000000bb", time.Hour, ""); !errors.Is(err, ErrHealBusy) {
		t.Errorf("second heal while one is in flight: %v, want ErrHealBusy", err)
	}
	close(puller.release)
	if err := <-done; err != nil {
		t.Errorf("first heal: %v", err)
	}
}

// TestValidateHealedCheckpointAcceptsValid: a well-formed staged record must
// pass every check.
func TestValidateHealedCheckpointAcceptsValid(t *testing.T) {
	dir := t.TempDir()
	plantHealedRecord(t, dir, types.Checkpoint{ID: "ck_00000000000000aa"})
	if err := validateHealedCheckpoint(dir, "ck_00000000000000aa"); err != nil {
		t.Errorf("validate: %v, want a valid record accepted", err)
	}
}

// TestValidateHealedCheckpointRejectsMismatchedID: a peer's meta.json must
// name the id that was actually requested, or a hostile peer could serve a
// crafted record under someone else's id.
func TestValidateHealedCheckpointRejectsMismatchedID(t *testing.T) {
	dir := t.TempDir()
	plantHealedRecord(t, dir, types.Checkpoint{ID: "ck_00000000000000bb"})
	if err := validateHealedCheckpoint(dir, "ck_00000000000000aa"); err == nil {
		t.Error("validate accepted a mismatched id")
	}
}

// TestValidateHealedCheckpointRejectsMissingMeta: no meta.json means nothing
// to trust.
func TestValidateHealedCheckpointRejectsMissingMeta(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, store.ExportDir), 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := validateHealedCheckpoint(dir, "ck_00000000000000aa"); err == nil {
		t.Error("validate accepted a record with no meta.json")
	}
}

// TestValidateHealedCheckpointRejectsMissingExport: a meta with no export
// dir cannot back a branch.
func TestValidateHealedCheckpointRejectsMissingExport(t *testing.T) {
	dir := t.TempDir()
	meta, err := json.Marshal(types.Checkpoint{ID: "ck_00000000000000aa"})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, store.MetaFile), meta, 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := validateHealedCheckpoint(dir, "ck_00000000000000aa"); err == nil {
		t.Error("validate accepted a record with no export dir")
	}
}

// TestValidateHealedCheckpointRejectsArchive: a wake image must never be
// healed in as a branchable checkpoint.
func TestValidateHealedCheckpointRejectsArchive(t *testing.T) {
	dir := t.TempDir()
	plantHealedRecord(t, dir, types.Checkpoint{ID: "ck_00000000000000aa", Archive: true})
	if err := validateHealedCheckpoint(dir, "ck_00000000000000aa"); err == nil {
		t.Error("validate accepted an archive-flagged record")
	}
}

// TestValidateHealedCheckpointRejectsEmptyExport: an export directory that is
// present but empty clones to nothing — publishing it would suppress a good
// owner and fail every later local claim, so it must be rejected and the next
// owner tried.
func TestValidateHealedCheckpointRejectsEmptyExport(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, store.ExportDir), 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	meta, err := json.Marshal(types.Checkpoint{ID: "ck_00000000000000aa", Key: testKey})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, store.MetaFile), meta, 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := validateHealedCheckpoint(dir, "ck_00000000000000aa"); err == nil {
		t.Error("validate accepted a record with an empty export")
	}
}

// TestValidateHealedCheckpointRejectsInvalidKey: a record whose key does not
// name a branchable pool must not be published.
func TestValidateHealedCheckpointRejectsInvalidKey(t *testing.T) {
	dir := t.TempDir()
	plantHealedRecord(t, dir, types.Checkpoint{ID: "ck_00000000000000aa", Key: types.PoolKey{Net: "bogus"}})
	if err := validateHealedCheckpoint(dir, "ck_00000000000000aa"); err == nil {
		t.Error("validate accepted a record with an invalid key")
	}
}

// TestDeleteCheckpointBroadcasts: a local delete calls the peer-delete hook
// once so a copy healed onto another node is removed too.
func TestDeleteCheckpointBroadcasts(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	src := mustClaim(t, m, testKey)
	ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "", "")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	var calls []string
	m.peerDelete = func(_ context.Context, id string) { calls = append(calls, id) }

	if err := m.DeleteCheckpoint(t.Context(), ckpt.ID, "", DeleteFleet); err != nil {
		t.Fatalf("DeleteCheckpoint: %v", err)
	}
	if len(calls) != 1 || calls[0] != ckpt.ID {
		t.Errorf("peerDelete calls = %v, want one call for %s", calls, ckpt.ID)
	}
}

// TestDeleteCheckpointNoForwardSkipsBroadcast: a delete arriving as a
// broadcast (no_forward) must not re-broadcast, or the fleet would loop
// forever.
func TestDeleteCheckpointNoForwardSkipsBroadcast(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	src := mustClaim(t, m, testKey)
	ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "", "")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	called := false
	m.peerDelete = func(context.Context, string) { called = true }

	if err := m.DeleteCheckpoint(t.Context(), ckpt.ID, "", DeleteLocal); err != nil {
		t.Fatalf("DeleteCheckpoint: %v", err)
	}
	if called {
		t.Error("peerDelete called despite no_forward")
	}
}

// TestDeleteCheckpointSharedStoreSkipsBroadcast: an s3 (or other shared)
// backend has one copy every node already sees, so broadcasting would be a
// no-op fan-out; DeleteCheckpoint must skip it.
func TestDeleteCheckpointSharedStoreSkipsBroadcast(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	src := mustClaim(t, m, testKey)
	ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "", "")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	m.ckptsShared = true
	called := false
	m.peerDelete = func(context.Context, string) { called = true }

	if err := m.DeleteCheckpoint(t.Context(), ckpt.ID, "", DeleteFleet); err != nil {
		t.Fatalf("DeleteCheckpoint: %v", err)
	}
	if called {
		t.Error("peerDelete called on a shared-store node")
	}
}

// newHealManager builds a Manager with a healer wired to a puller serving
// ckpt at addrs, mirroring WithPeerHeal's construction directly (tests are in
// the same package).
func newHealManager(t *testing.T, ckpt types.Checkpoint, addrs []string) (*Manager, *healPuller) {
	t.Helper()
	m := newTestManager(t, newFakeEngine())
	puller := &healPuller{ckpt: ckpt}
	m.healer = peer.NewHealer(func(string) []string { return addrs }, puller)
	return m, puller
}

// healPuller fakes peer.Puller: it writes a valid record (meta.json + an
// empty export/ dir the fakeEngine "clones" from) and can block until
// release, so a test can prove concurrent misses dedup to one pull.
type healPuller struct {
	mu      sync.Mutex
	calls   int
	ckpt    types.Checkpoint
	release chan struct{} // non-nil: Pull blocks until closed
}

func (p *healPuller) Pull(_ context.Context, _, _, dst string) error {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.release != nil {
		<-p.release
	}
	if err := os.MkdirAll(filepath.Join(dst, store.ExportDir), 0o750); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dst, store.ExportDir, "mem"), []byte("state"), 0o600); err != nil {
		return err
	}
	meta, err := json.Marshal(p.ckpt)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dst, store.MetaFile), meta, 0o600)
}

func (p *healPuller) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// blockingPuller writes a valid record stamped with the requested id, after
// signaling started and then blocking until release — for proving the
// node-wide heal concurrency cap against distinct, non-contending ids.
type blockingPuller struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingPuller) Pull(_ context.Context, _, id, dst string) error {
	p.once.Do(func() { close(p.started) })
	<-p.release
	if err := os.MkdirAll(filepath.Join(dst, store.ExportDir), 0o750); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dst, store.ExportDir, "mem"), []byte("state"), 0o600); err != nil {
		return err
	}
	meta, err := json.Marshal(types.Checkpoint{ID: id, Key: testKey, CreatedAt: time.Now()})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dst, store.MetaFile), meta, 0o600)
}

// plantHealedRecord writes a valid-shaped staged record for validate tests.
func plantHealedRecord(t *testing.T, dir string, ckpt types.Checkpoint) {
	t.Helper()
	if ckpt.Key == (types.PoolKey{}) {
		ckpt.Key = testKey // a healed record carries a real, branchable key
	}
	if err := os.MkdirAll(filepath.Join(dir, store.ExportDir), 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, store.ExportDir, "mem"), []byte("state"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	meta, err := json.Marshal(ckpt)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, store.MetaFile), meta, 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
}
