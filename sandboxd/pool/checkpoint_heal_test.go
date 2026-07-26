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
// the id's recLock now serializes them, so the second waits for the first's
// publish and never reaches the puller at all.
func TestClaimCheckpointHealDedupsConcurrentPulls(t *testing.T) {
	id := "ck_00000000000000bb"
	ckpt := types.Checkpoint{ID: id, Key: testKey, CreatedAt: time.Now()}
	m, puller := newHealManager(t, ckpt, []string{"peer-a:7777"})
	puller.release = make(chan struct{})

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range 2 {
		wg.Go(func() {
			_, err := m.ClaimCheckpointHeal(t.Context(), id, time.Hour, "")
			errs[i] = err
		})
	}
	waitFor(t, func() bool { return puller.count() >= 1 })
	time.Sleep(50 * time.Millisecond) // let the second goroutine block on the recLock
	close(puller.release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
	if got := puller.count(); got != 1 {
		t.Errorf("puller called %d times for two concurrent heals of one id, want 1", got)
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
	meta, err := json.Marshal(types.Checkpoint{ID: id, Key: testKey, CreatedAt: time.Now()})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dst, store.MetaFile), meta, 0o600)
}

// plantHealedRecord writes a valid-shaped staged record for validate tests.
func plantHealedRecord(t *testing.T, dir string, ckpt types.Checkpoint) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, store.ExportDir), 0o750); err != nil {
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
