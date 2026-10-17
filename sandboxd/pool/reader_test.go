package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var readerKey = types.PoolKey{Template: "reader:24.04", Net: types.NetNone, Size: types.SizeSmall}

func TestReaderServesAHibernatedClaimWithoutWaking(t *testing.T) {
	eng := newFakeEngine()
	layers := readerExport(t, eng)
	m := newReaderManager(t, eng)
	seedReaderVMs(m, "sbx-reader-1")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)

	if _, _, _, err := m.WakeAgentSocketOrRead(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrSleeping) {
		t.Fatalf("WakeAgentSocketOrRead = %v, want ErrSleeping", err)
	}
	if sb.Busy() {
		t.Error("a deferred wake left the claim held")
	}
	conn, source, err := m.DialReader(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatalf("DialReader: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if source != sb.HibernateSnap || source == "" {
		t.Errorf("reader source %q, want the claim's hibernate snapshot %q", source, sb.HibernateSnap)
	}
	if len(eng.restores) != 0 || sb.HibernateSnap == "" {
		t.Errorf("claim woke: restores=%v hibernate snapshot=%q", eng.restores, sb.HibernateSnap)
	}
	if got, want := readerOps(eng), []string{"attach:rl0", "attach:rl1", "attach:rcow", "reader-start:/vsock/sbx-reader-1"}; !slices.Equal(got, want) {
		t.Errorf("reader setup ran %v, want %v", got, want)
	}
	cow := eng.volumeSpecs[len(eng.volumeSpecs)-1]
	wantSpecs := []engine.VolumeSpec{{Name: "rl0", Path: layers[0]}, {Name: "rl1", Path: layers[1]}, {Name: "rcow", Path: cow.Path, RW: true}}
	if !slices.Equal(eng.volumeSpecs, wantSpecs) {
		t.Errorf("attached %+v, want read-only layers then the writable copy %+v", eng.volumeSpecs, wantSpecs)
	}
	if want := (engine.ReaderDisks{Layers: []string{"rl0", "rl1"}, Cow: "rcow"}); len(eng.startedReaders) != 1 || !slices.Equal(eng.startedReaders[0].Layers, want.Layers) || eng.startedReaders[0].Cow != want.Cow {
		t.Errorf("view started with %+v, want %+v", eng.startedReaders, want)
	}
	if got, readErr := os.ReadFile(cow.Path); readErr != nil || string(got) != "disk-bytes" {
		t.Errorf("reader disk %q, %v, want the snapshot's copy-on-write bytes", got, readErr)
	}
	if entries, _ := os.ReadDir(filepath.Dir(cow.Path)); len(entries) != 2 {
		t.Errorf("reader dir holds %d entries, want only the disk and its sidecar", len(entries))
	}
	if n := len(m.pools[readerKey].warm); n != 0 {
		t.Errorf("reader pool holds %d warm VMs, want the one reader taken", n)
	}
}

func TestReaderCallsShareOneSetup(t *testing.T) {
	eng := newFakeEngine()
	readerExport(t, eng)
	eng.exportStall = make(chan struct{})
	m := newReaderManager(t, eng)
	seedReaderVMs(m, "sbx-reader-1", "sbx-reader-2")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)

	const callers = 8
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			conn, _, err := m.DialReader(t.Context(), sb.ID, sb.Token)
			if err != nil {
				t.Errorf("DialReader: %v", err)
				return
			}
			_ = conn.Close()
		})
	}
	waitFor(t, func() bool { eng.mu.Lock(); defer eng.mu.Unlock(); return len(eng.exports) > 0 })
	close(eng.exportStall)
	wg.Wait()

	eng.mu.Lock()
	defer eng.mu.Unlock()
	if len(eng.exports) != 1 || len(eng.startedReaders) != 1 {
		t.Errorf("%d callers ran %d exports and %d reader setups, want one of each", callers, len(eng.exports), len(eng.startedReaders))
	}
	if len(eng.readerDials) != callers {
		t.Errorf("%d dials, want one per caller", len(eng.readerDials))
	}
}

func TestReaderIsReleasedAfterItsIdleWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		readerExport(t, eng)
		m := newReaderManager(t, eng)
		seedReaderVMs(m, "sbx-reader-1")
		sb := mustClaim(t, m, testKey)
		hibernateClaim(t, m, sb)
		dialAndClose := func() {
			t.Helper()
			conn, _, err := m.DialReader(t.Context(), sb.ID, sb.Token)
			if err != nil {
				t.Fatalf("DialReader: %v", err)
			}
			_ = conn.Close()
		}
		dialAndClose()
		dir := filepath.Dir(eng.volumeSpecs[len(eng.volumeSpecs)-1].Path)

		time.Sleep(readerIdle * 2 / 3)
		dialAndClose()
		time.Sleep(readerIdle * 2 / 3)
		synctest.Wait()
		if removes, setups := readerLifecycle(eng); slices.Contains(removes, "sbx-reader-1") || setups != 1 {
			t.Fatalf("reader released inside its window after a reuse: removes=%v setups=%d", removes, setups)
		}

		time.Sleep(readerIdle)
		synctest.Wait()
		if removes, _ := readerLifecycle(eng); !slices.Contains(removes, "sbx-reader-1") {
			t.Errorf("removes=%v, want the idle reader's VM destroyed", removes)
		}
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("reader dir survived the release: %v", err)
		}
		m.readers.mu.Lock()
		defer m.readers.mu.Unlock()
		if len(m.readers.live) != 0 {
			t.Error("released reader is still registered")
		}
	})
}

func TestReaderNoConnectionAcquiresIsReleasedAfterTheIdleWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		readerExport(t, eng)
		m := newReaderManager(t, eng)
		seedReaderVMs(m, "sbx-reader-1")
		sb := mustClaim(t, m, testKey)
		hibernateClaim(t, m, sb)
		if err := m.ensureReader(t.Context(), sb); err != nil {
			t.Fatalf("ensureReader: %v", err)
		}

		time.Sleep(readerIdle + time.Second)
		synctest.Wait()
		if removes, _ := readerLifecycle(eng); !slices.Contains(removes, "sbx-reader-1") {
			t.Errorf("removes=%v, want the never-acquired reader destroyed", removes)
		}
		m.readers.mu.Lock()
		defer m.readers.mu.Unlock()
		if len(m.readers.live) != 0 {
			t.Error("the never-acquired reader is still registered")
		}
	})
}

func TestReaderRegistrationRetiresTheOneItReplaces(t *testing.T) {
	eng := newFakeEngine()
	readerExport(t, eng)
	m := newReaderManager(t, eng)
	seedReaderVMs(m, "sbx-reader-2", "sbx-reader-1")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)

	for range 2 {
		if err := m.openReader(t.Context(), sb); err != nil {
			t.Fatalf("openReader: %v", err)
		}
	}
	removes, setups := readerLifecycle(eng)
	if !slices.Contains(removes, "sbx-reader-1") || setups != 2 {
		t.Errorf("removes=%v setups=%d, want the first reader destroyed when the second registered", removes, setups)
	}
	m.readers.mu.Lock()
	defer m.readers.mu.Unlock()
	if live := m.readers.live[sb.ID]; live == nil || live.vm != "sbx-reader-2" {
		t.Errorf("live reader %+v, want the second one", live)
	}
}

func TestReaderEnsureSkipsASetupAnotherFlightFinished(t *testing.T) {
	eng := newFakeEngine()
	readerExport(t, eng)
	m := newReaderManager(t, eng)
	seedReaderVMs(m, "sbx-reader-2", "sbx-reader-1")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)

	for range 2 {
		if err := m.ensureReader(t.Context(), sb); err != nil {
			t.Fatalf("ensureReader: %v", err)
		}
	}
	if removes, setups := readerLifecycle(eng); setups != 1 || len(removes) != 0 {
		t.Errorf("removes=%v setups=%d, want one reader built and none destroyed", removes, setups)
	}
}

func TestReaderKeepsTheClaimsLatestSleepOnly(t *testing.T) {
	eng := newFakeEngine()
	readerExport(t, eng)
	m := newReaderManager(t, eng)
	seedReaderVMs(m, "sbx-reader-2", "sbx-reader-1")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	first, firstSource, err := m.DialReader(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatalf("DialReader: %v", err)
	}
	_ = first.Close()
	if _, _, _, err = m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake: %v", err)
	}
	if got := m.SleepingSource(sb.ID, sb.Token); got != "" {
		t.Fatalf("SleepingSource of a running claim = %q, want none", got)
	}
	if _, _, err = m.DialReader(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrNoReader) {
		t.Fatalf("DialReader of a running claim = %v, want ErrNoReader", err)
	}
	hibernateClaim(t, m, sb)

	second, secondSource, err := m.DialReader(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatalf("DialReader after the second sleep: %v", err)
	}
	_ = second.Close()
	if secondSource == firstSource {
		t.Errorf("both sleeps served from %q", firstSource)
	}
	if len(eng.exports) != 2 || len(eng.startedReaders) != 2 {
		t.Errorf("exports=%d setups=%d, want a fresh reader per sleep", len(eng.exports), len(eng.startedReaders))
	}
	if !slices.Contains(eng.removes, "sbx-reader-1") {
		t.Errorf("removes=%v, want the first sleep's reader destroyed", eng.removes)
	}
}

func TestReaderServesAnArchivedClaimFromTwoFilesOfItsRecord(t *testing.T) {
	eng := newFakeEngine()
	readerExport(t, eng)
	m := newReaderManager(t, eng)
	seedReaderVMs(m, "sbx-reader-1")
	counting := &fetchCountingStore{Store: m.ckpts}
	m.ckpts = counting
	sb := mustClaim(t, m, testKey)
	mustArchive(t, m, sb)
	clones := len(eng.clones) + len(eng.colds)

	if _, _, _, err := m.WakeAgentSocketOrRead(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrSleeping) {
		t.Fatalf("WakeAgentSocketOrRead of an archived claim = %v, want ErrSleeping", err)
	}
	conn, source, err := m.DialReader(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatalf("DialReader: %v", err)
	}
	defer func() { _ = conn.Close() }()

	m.mu.Lock()
	ck := sb.ArchiveCk
	m.mu.Unlock()
	if ck == "" || source != ck {
		t.Errorf("reader source %q, want the archive record %q", source, ck)
	}
	if got, want := counting.files(), []string{"cocoon.json", "cow.raw"}; !slices.Equal(got, want) {
		t.Errorf("fetched files %v, want only %v", got, want)
	}
	if counting.fetches != 0 {
		t.Errorf("%d whole-record fetches, want none", counting.fetches)
	}
	if n := len(eng.clones) + len(eng.colds); n != clones || len(eng.restores) != 0 {
		t.Errorf("an archived read booted a VM: clones/colds %d->%d, restores %v", clones, n, eng.restores)
	}
	if !ckExists(t, m, ck) {
		t.Error("the archive record was consumed")
	}
	cow := eng.volumeSpecs[len(eng.volumeSpecs)-1]
	if got, readErr := os.ReadFile(cow.Path); readErr != nil || string(got) != "disk-bytes" {
		t.Errorf("reader disk %q, %v, want the record's bytes", got, readErr)
	}
	if !strings.HasPrefix(cow.Path, filepath.Join(m.dataDir, readerDir)) {
		t.Errorf("reader attached %s, want a private copy under %s", cow.Path, readerDir)
	}

	m.readers.mu.Lock()
	live := m.readers.live[sb.ID]
	m.readers.mu.Unlock()
	m.dropReader(t.Context(), live, true)
	if entries, _ := os.ReadDir(filepath.Join(m.dataDir, readerDir)); len(entries) != 0 {
		t.Errorf("a released archived reader left %d entries under %s", len(entries), readerDir)
	}
}

func TestReaderRefusesWhatAWakeRefuses(t *testing.T) {
	eng := newFakeEngine()
	readerExport(t, eng)
	cfg := &config.Config{
		DataDir: t.TempDir(), APIToken: "root", Reader: &config.ReaderSpec{Template: readerKey.Template},
		Pools: []config.PoolSpec{archivePool(0), {PoolKey: readerKey, Warm: 1}},
	}
	m, err := NewManager(t.Context(), cfg, eng, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	seedReaderVMs(m, "sbx-reader-1")
	putTenants(t, m, tenants.TenantSpec{Name: "acme", Token: "acme-tok"})
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour, Tenant: "acme"})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	mustArchive(t, m, sb)
	if err = m.DeleteTenant(t.Context(), "acme"); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}

	if _, _, err = m.DialReader(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrNoReader) || !errors.Is(err, ErrTenantRemoved) {
		t.Errorf("DialReader of a removed tenant's archive = %v, want ErrNoReader wrapping ErrTenantRemoved", err)
	}
	if len(eng.startedReaders) != 0 || len(m.pools[readerKey].warm) != 1 {
		t.Errorf("a refused read still took a reader: setups=%d warm=%d", len(eng.startedReaders), len(m.pools[readerKey].warm))
	}
}

func TestReaderRefusesWhileTheDiskIsLow(t *testing.T) {
	eng := newFakeEngine()
	readerExport(t, eng)
	m := newReaderManager(t, eng)
	seedReaderVMs(m, "sbx-reader-1")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	m.diskLow.Store(true)

	if _, _, err := m.DialReader(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrNoReader) || !errors.Is(err, ErrQuota) {
		t.Errorf("DialReader on a full disk = %v, want ErrNoReader wrapping ErrQuota", err)
	}
	if len(eng.exports) != 0 || len(m.pools[readerKey].warm) != 1 {
		t.Errorf("a refused read still exported (%d) or took a reader (%d warm)", len(eng.exports), len(m.pools[readerKey].warm))
	}
}

func TestReaderSetupFailureFallsBackAndCleansUp(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(*fakeEngine)
		warm  []string
	}{
		{"attach fails", func(e *fakeEngine) { e.diskAttachErrFor = "rcow" }, []string{"sbx-reader-1"}},
		{"view fails", func(e *fakeEngine) { e.startReaderErr = errors.New("mount failed") }, []string{"sbx-reader-1"}},
		{"no warm reader", func(*fakeEngine) {}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			readerExport(t, eng)
			tt.setup(eng)
			m := newReaderManager(t, eng)
			seedReaderVMs(m, tt.warm...)
			sb := mustClaim(t, m, testKey)
			hibernateClaim(t, m, sb)

			if _, _, err := m.DialReader(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrNoReader) {
				t.Fatalf("DialReader = %v, want ErrNoReader", err)
			}
			if len(tt.warm) > 0 && !slices.Contains(eng.removes, "sbx-reader-1") {
				t.Errorf("removes=%v, want the failed reader's VM destroyed", eng.removes)
			}
			if entries, _ := os.ReadDir(filepath.Join(m.dataDir, readerDir)); len(entries) != 0 {
				t.Errorf("a failed setup left %d entries in the reader dir", len(entries))
			}
			if len(m.readers.live) != 0 || sb.HibernateSnap == "" || len(eng.restores) != 0 {
				t.Errorf("failed setup changed state: live=%d snap=%q restores=%v", len(m.readers.live), sb.HibernateSnap, eng.restores)
			}
		})
	}
}

func TestSleepingSourceNamesOnlyASettledSleepOfTheCallersClaim(t *testing.T) {
	eng := newFakeEngine()
	m := newReaderManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if got := m.SleepingSource(sb.ID, sb.Token); got != "" {
		t.Errorf("a running claim sleeps from %q, want none", got)
	}
	hibernateClaim(t, m, sb)
	snap := sb.HibernateSnap
	if got := m.SleepingSource(sb.ID, sb.Token); got != snap || snap == "" {
		t.Errorf("a hibernated claim sleeps from %q, want its snapshot %q", got, snap)
	}
	if got := m.SleepingSource(sb.ID, "wrong"); got != "" {
		t.Errorf("a wrong token read the sleep: %q", got)
	}
	for name, mutate := range map[string]func(){
		"transition in flight": func() { sb.PendingSnap = "sbx-hib-next" },
		"failed":               func() { sb.Failed = "vmm down" },
	} {
		m.mu.Lock()
		mutate()
		m.mu.Unlock()
		if got := m.SleepingSource(sb.ID, sb.Token); got != "" {
			t.Errorf("%s: sleeps from %q, want none", name, got)
		}
		m.mu.Lock()
		sb.PendingSnap, sb.Failed = "", ""
		m.mu.Unlock()
	}
}

func TestReaderThatStopsAnsweringIsRetired(t *testing.T) {
	eng := newFakeEngine()
	readerExport(t, eng)
	m := newReaderManager(t, eng)
	seedReaderVMs(m, "sbx-reader-2", "sbx-reader-1")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	eng.readerDialErr = errors.New("vsock refused")

	if _, _, err := m.DialReader(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrNoReader) {
		t.Fatalf("DialReader of a dead reader = %v, want ErrNoReader", err)
	}
	if removes, _ := readerLifecycle(eng); !slices.Contains(removes, "sbx-reader-1") {
		t.Errorf("removes=%v, want the dead reader's VM destroyed", removes)
	}
	eng.readerDialErr = nil
	if conn, _, err := m.DialReader(t.Context(), sb.ID, sb.Token); err != nil {
		t.Errorf("DialReader after the retirement = %v, want a fresh reader", err)
	} else {
		_ = conn.Close()
	}
	if _, setups := readerLifecycle(eng); setups != 2 {
		t.Errorf("%d reader setups, want the retired reader replaced", setups)
	}
}

func TestWakeAgentSocketStampsActivityOnlyWhenItServesTheGuest(t *testing.T) {
	for _, on := range []bool{false, true} {
		t.Run(fmt.Sprintf("reader=%t", on), func(t *testing.T) {
			eng := newFakeEngine()
			m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
			if on {
				m = newReaderManager(t, eng)
			}
			sb := mustClaim(t, m, testKey)
			backdate(m, sb, time.Hour)
			_, _, done, err := m.WakeAgentSocketOrRead(t.Context(), sb.ID, sb.Token)
			if err != nil {
				t.Fatalf("WakeAgentSocketOrRead: %v", err)
			}
			defer done()
			if age := time.Since(sb.LastSeen()); age > time.Minute {
				t.Errorf("a served connection left the claim idle for %v", age)
			}
			hibernateClaim(t, m, sb)
			backdate(m, sb, time.Hour)
			if _, _, _, err = m.WakeAgentSocketOrRead(t.Context(), sb.ID, sb.Token); on != errors.Is(err, ErrSleeping) {
				t.Fatalf("WakeAgentSocketOrRead of a hibernated claim = %v with the reader %t", err, on)
			}
			if age := time.Since(sb.LastSeen()); on && age < 30*time.Minute {
				t.Errorf("a deferred read stamped the claim's idle clock (idle %v)", age)
			}
		})
	}
}

func TestReaderOfAStaleSleepIsRetiredEvenWithAConnectionOpen(t *testing.T) {
	eng := newFakeEngine()
	readerExport(t, eng)
	m := newReaderManager(t, eng)
	seedReaderVMs(m, "sbx-reader-2", "sbx-reader-1")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	open, _, err := m.DialReader(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatalf("DialReader: %v", err)
	}
	defer func() { _ = open.Close() }()
	if _, _, _, err = m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake: %v", err)
	}
	hibernateClaim(t, m, sb)

	next, _, err := m.DialReader(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatalf("DialReader after the second sleep: %v", err)
	}
	_ = next.Close()
	if removes, _ := readerLifecycle(eng); !slices.Contains(removes, "sbx-reader-1") {
		t.Errorf("removes=%v, want the stale reader destroyed although its connection is open", removes)
	}
}

func TestFailedPreHibernateSyncSendsReadsToTheWakePath(t *testing.T) {
	for _, tt := range []struct {
		name    string
		syncErr error
		safe    bool
	}{
		{"sync ok", nil, true},
		{"sync fails", errors.New("guest unreachable"), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			readerExport(t, eng)
			eng.syncErr = tt.syncErr
			m := newReaderManager(t, eng)
			seedReaderVMs(m, "sbx-reader-1")
			sb := mustClaim(t, m, testKey)
			hibernateClaim(t, m, sb)

			if got, _ := newClaimStore(m.dataDir, false).load(); got[sb.ID] == nil || got[sb.ID].ReaderSafe != tt.safe {
				t.Errorf("persisted reader_safe = %+v, want %t", got[sb.ID], tt.safe)
			}
			if got := m.SleepingSource(sb.ID, sb.Token); (got != "") != tt.safe {
				t.Errorf("SleepingSource = %q with a safe sleep %t", got, tt.safe)
			}
			if tt.safe {
				if _, _, _, err := m.WakeAgentSocketOrRead(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrSleeping) {
					t.Fatalf("a safe sleep answered %v, want ErrSleeping", err)
				}
				return
			}
			if _, _, err := m.DialReader(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrNoReader) {
				t.Errorf("DialReader of an unsafe sleep = %v, want ErrNoReader", err)
			}
			sock, _, done, err := m.WakeAgentSocketOrRead(t.Context(), sb.ID, sb.Token)
			if err != nil || sock == "" {
				t.Fatalf("a read of an unsafe sleep = %q, %v, want a wake", sock, err)
			}
			defer done()
			if len(eng.restores) != 1 || len(eng.startedReaders) != 0 {
				t.Errorf("restores=%v reader setups=%d, want the claim woken and no reader", eng.restores, len(eng.startedReaders))
			}
		})
	}
}

func TestWakeClearsReaderSafe(t *testing.T) {
	eng := newFakeEngine()
	m := newReaderManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	if _, _, done, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake: %v", err)
	} else {
		done()
	}
	if got, _ := newClaimStore(m.dataDir, false).load(); got[sb.ID] == nil || got[sb.ID].ReaderSafe {
		t.Errorf("persisted claim after a wake = %+v, want reader_safe cleared", got[sb.ID])
	}
}

func TestReaderOffWakesAsBefore(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)

	if _, _, err := m.DialReader(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrNoReader) {
		t.Errorf("DialReader with the reader off = %v, want ErrNoReader", err)
	}
	sock, _, done, err := m.WakeAgentSocketOrRead(t.Context(), sb.ID, sb.Token)
	if err != nil || sock == "" {
		t.Fatalf("WakeAgentSocketOrRead with the reader off = %q, %v, want a wake", sock, err)
	}
	defer done()
	if len(eng.restores) != 1 || sb.HibernateSnap != "" {
		t.Errorf("restores=%v snapshot=%q, want the claim woken", eng.restores, sb.HibernateSnap)
	}
}

func TestReaderOnLeavesRunningClaimsAlone(t *testing.T) {
	eng := newFakeEngine()
	m := newReaderManager(t, eng)
	sb := mustClaim(t, m, testKey)

	sock, _, done, err := m.WakeAgentSocketOrRead(t.Context(), sb.ID, sb.Token)
	if err != nil || sock != sb.VsockSocket {
		t.Fatalf("WakeAgentSocketOrRead of a running claim = %q, %v, want its own socket", sock, err)
	}
	defer done()
	if !sb.Busy() {
		t.Error("the connection did not hold the running claim")
	}
	if len(eng.restores) != 0 || len(eng.startedReaders) != 0 {
		t.Errorf("a running claim triggered restores=%v setups=%d", eng.restores, len(eng.startedReaders))
	}
}

func TestHibernateSyncsTheGuestOnlyWithTheReaderOn(t *testing.T) {
	for _, on := range []bool{true, false} {
		t.Run(fmt.Sprintf("reader=%t", on), func(t *testing.T) {
			eng := newFakeEngine()
			var m *Manager
			if on {
				m = newReaderManager(t, eng)
			} else {
				m = newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
			}
			sb := mustClaim(t, m, testKey)
			eng.hibernateStall = make(chan struct{})
			hibernated := make(chan struct{})
			go func() {
				hibernateClaim(t, m, sb)
				close(hibernated)
			}()
			waitFor(t, func() bool { eng.mu.Lock(); defer eng.mu.Unlock(); return len(eng.hibernates) == 1 })
			eng.mu.Lock()
			syncedBeforeCapture := slices.Clone(eng.syncs)
			eng.mu.Unlock()
			close(eng.hibernateStall)
			<-hibernated

			want := []string(nil)
			if on {
				want = []string{sb.VsockSocket}
			}
			if !slices.Equal(syncedBeforeCapture, want) {
				t.Errorf("synced %v before the capture, want %v", syncedBeforeCapture, want)
			}
		})
	}
}

func TestReconcileClearsReaderLeftovers(t *testing.T) {
	eng := newFakeEngine()
	m := newReaderManager(t, eng)
	stale := filepath.Join(m.dataDir, readerDir, "sb_old-aabbcc")
	if err := os.MkdirAll(stale, 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a reader dir from a previous run survived the restart: %v", err)
	}
}

func newReaderManager(t *testing.T, eng *fakeEngine) *Manager {
	t.Helper()
	cfg := &config.Config{
		DataDir: t.TempDir(),
		Reader:  &config.ReaderSpec{Template: readerKey.Template},
		Pools:   []config.PoolSpec{{PoolKey: testKey, Warm: 1}, {PoolKey: readerKey, Warm: 1}},
	}
	m, err := NewManager(t.Context(), cfg, eng, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	return m
}

func seedReaderVMs(m *Manager, names ...string) {
	for _, name := range names {
		m.pools[readerKey].warm = append(m.pools[readerKey].warm, &types.Sandbox{VMName: name, Key: readerKey, VsockSocket: "/vsock/" + name})
	}
}

func hibernateClaim(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
}

// readerExport makes every snapshot export carry the disk, a sidecar naming two layers, and a memory image a reader must drop.
func readerExport(t *testing.T, eng *fakeEngine) []string {
	t.Helper()
	dir := t.TempDir()
	layers := []string{filepath.Join(dir, "base.erofs"), filepath.Join(dir, "top.erofs")}
	for _, layer := range layers {
		if err := os.WriteFile(layer, []byte("erofs"), 0o600); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	sidecar := fmt.Sprintf(`{"storage_configs":[{"path":%q,"role":"layer"},{"path":%q,"role":"layer"},{"path":"/run/cow.raw","role":"cow"},{"path":"/run/cidata.img","role":"cidata"}]}`, layers[0], layers[1])
	eng.exportFiles = map[string]string{"cow.raw": "disk-bytes", "cocoon.json": sidecar, "memory-ranges": "guest-memory"}
	return layers
}

func readerOps(eng *fakeEngine) []string {
	eng.mu.Lock()
	defer eng.mu.Unlock()
	return slices.DeleteFunc(slices.Clone(eng.volumeOps), func(op string) bool {
		return !strings.HasPrefix(op, "attach:") && !strings.HasPrefix(op, "reader-start:")
	})
}

func readerLifecycle(eng *fakeEngine) (removes []string, setups int) {
	eng.mu.Lock()
	defer eng.mu.Unlock()
	return slices.Clone(eng.removes), len(eng.startedReaders)
}

type fetchCountingStore struct {
	store.Store
	mu      sync.Mutex
	fetches int
	fetched []string
}

func (s *fetchCountingStore) Fetch(ctx context.Context, id string) (string, []byte, string, error) {
	s.mu.Lock()
	s.fetches++
	s.mu.Unlock()
	return s.Store.Fetch(ctx, id)
}

func (s *fetchCountingStore) FetchFile(ctx context.Context, id, rel, dst string) error {
	s.mu.Lock()
	s.fetched = append(s.fetched, rel)
	s.mu.Unlock()
	return s.Store.FetchFile(ctx, id, rel, dst)
}

func (s *fetchCountingStore) files() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := slices.Clone(s.fetched)
	slices.Sort(out)
	return out
}
