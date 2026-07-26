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
// the same missing checkpoint must share one transfer, not each pay for it.
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
	time.Sleep(50 * time.Millisecond) // let the second goroutine join the same flight
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

// newHealManager builds a Manager with a healer wired to a puller serving
// ckpt at addrs, mirroring WithPeerHeal's construction directly (tests are in
// the same package).
func newHealManager(t *testing.T, ckpt types.Checkpoint, addrs []string) (*Manager, *healPuller) {
	t.Helper()
	m := newTestManager(t, newFakeEngine())
	puller := &healPuller{ckpt: ckpt}
	m.healer = peer.New(m.ckpts, func(string) []string { return addrs }, puller)
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
