package pool

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/peer"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

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

func TestClaimCheckpointHealDedupsConcurrentPulls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		time.Sleep(50 * time.Millisecond)
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
	})
}

func TestDeleteVetoesInFlightHeal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		id := "ck_00000000000000bb"
		ckpt := types.Checkpoint{ID: id, Key: testKey, CreatedAt: time.Now()}
		m, puller := newHealManager(t, ckpt, []string{"peer-a:7777"})
		puller.release = make(chan struct{})

		healDone := make(chan error, 1)
		go func() {
			_, err := m.ClaimCheckpointHeal(t.Context(), id, time.Hour, "")
			healDone <- err
		}()
		waitFor(t, func() bool { return puller.count() >= 1 })

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
	})
}

func TestClaimCheckpointHealCtxCancelReturnsPromptly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		id := "ck_00000000000000bb"
		ckpt := types.Checkpoint{ID: id, Key: testKey, CreatedAt: time.Now()}
		m, puller := newHealManager(t, ckpt, []string{"peer-a:7777"})
		puller.release = make(chan struct{})

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			_, err := m.ClaimCheckpointHeal(ctx, id, time.Hour, "")
			done <- err
		}()
		waitFor(t, func() bool { return puller.count() >= 1 })

		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("ClaimCheckpointHeal after cancel: %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("ClaimCheckpointHeal did not return promptly after ctx cancellation")
		}

		close(puller.release)
		waitFor(t, func() bool { return m.HasCheckpoint(t.Context(), id) })
	})
}

func TestRecLockEvictionWaitsForAllHolders(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	const id = "ck_00000000000000aa"

	l1 := m.recLock(id)
	l1.Lock()
	l2 := m.recLock(id)
	if l2 != l1 {
		t.Fatalf("recLock returned a different mutex while the first holder is still outstanding")
	}
	l1.Unlock()
	m.recDoneEvict(id)
	if !hasRecLock(m, id) {
		t.Fatal("entry evicted while a second holder still references it")
	}

	l3 := m.recLock(id)
	if l3 != l1 {
		t.Fatal("a concurrent recLock diverged onto a different mutex for the same id — the lock-identity split")
	}

	m.recDone(id)
	m.recDoneEvict(id)
	if hasRecLock(m, id) {
		t.Error("entry not evicted once every holder released")
	}
}

func TestRecLockEvictDeferredToLastHolder(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	const id = "ck_00000000000000aa"

	l1 := m.recLock(id)
	l2 := m.recLock(id)
	if l2 != l1 {
		t.Fatal("recLock diverged for the same id")
	}
	m.recDoneEvict(id)
	if !hasRecLock(m, id) {
		t.Fatal("entry evicted while an ordinary holder still references it")
	}
	m.recDone(id)
	if hasRecLock(m, id) {
		t.Error("recLocks entry leaked: a delete's deferred eviction was lost when a plain recDone dropped the last reference")
	}
}

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

func TestValidateHealedCheckpointAcceptsValid(t *testing.T) {
	dir := t.TempDir()
	plantHealedRecord(t, dir, types.Checkpoint{ID: "ck_00000000000000aa"})
	if err := validateHealedCheckpoint(dir, "ck_00000000000000aa"); err != nil {
		t.Errorf("validate: %v, want a valid record accepted", err)
	}
}

func TestValidateHealedCheckpointRejectsMismatchedID(t *testing.T) {
	dir := t.TempDir()
	plantHealedRecord(t, dir, types.Checkpoint{ID: "ck_00000000000000bb"})
	if err := validateHealedCheckpoint(dir, "ck_00000000000000aa"); err == nil {
		t.Error("validate accepted a mismatched id")
	}
}

func TestValidateHealedCheckpointRejectsMissingMeta(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, store.ExportDir), 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := validateHealedCheckpoint(dir, "ck_00000000000000aa"); err == nil {
		t.Error("validate accepted a record with no meta.json")
	}
}

func TestValidateHealedCheckpointRejectsMissingExport(t *testing.T) {
	dir := t.TempDir()

	meta, err := json.Marshal(types.Checkpoint{ID: "ck_00000000000000aa", Key: testKey})
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

func TestValidateHealedCheckpointRejectsArchive(t *testing.T) {
	dir := t.TempDir()
	plantHealedRecord(t, dir, types.Checkpoint{ID: "ck_00000000000000aa", Archive: true})
	if err := validateHealedCheckpoint(dir, "ck_00000000000000aa"); err == nil {
		t.Error("validate accepted an archive-flagged record")
	}
}

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

func TestValidateHealedCheckpointRejectsInvalidKey(t *testing.T) {
	dir := t.TempDir()
	plantHealedRecord(t, dir, types.Checkpoint{ID: "ck_00000000000000aa", Key: types.PoolKey{Net: "bogus"}})
	if err := validateHealedCheckpoint(dir, "ck_00000000000000aa"); err == nil {
		t.Error("validate accepted a record with an invalid key")
	}
}

func TestValidateHealedCheckpointRejectsEgressKey(t *testing.T) {
	dir := t.TempDir()
	egress := testKey
	egress.Net = types.NetEgress
	plantHealedRecord(t, dir, types.Checkpoint{ID: "ck_00000000000000aa", Key: egress})
	if err := validateHealedCheckpoint(dir, "ck_00000000000000aa"); err == nil {
		t.Error("validate accepted a non-branchable egress-lane record")
	}
}

func TestValidateHealedCheckpointRejectsContentlessExport(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, store.ExportDir, "sub"), 0o750); err != nil {
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
		t.Error("validate accepted an export with no regular file")
	}
}

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

func newHealManager(t *testing.T, ckpt types.Checkpoint, addrs []string) (*Manager, *healPuller) {
	t.Helper()
	m := newTestManager(t, newFakeEngine())
	puller := &healPuller{ckpt: ckpt}
	m.healer = peer.NewHealer(func(string) []string { return addrs }, puller)
	return m, puller
}

type healPuller struct {
	mu      sync.Mutex
	calls   int
	ckpt    types.Checkpoint
	release chan struct{}
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

func plantHealedRecord(t *testing.T, dir string, ckpt types.Checkpoint) {
	t.Helper()
	if ckpt.Key == (types.PoolKey{}) {
		ckpt.Key = testKey
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
