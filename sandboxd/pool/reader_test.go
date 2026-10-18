package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestReaderServesAHibernatedClaimWithoutWaking(t *testing.T) {
	eng := newFakeEngine()
	m := newReaderManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	seedReaderVMs(m, testKey, "sbx-reader-1")

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
	if want := []string{"snapshot:sbx-reader-1:" + sb.HibernateSnap}; !slices.Equal(eng.attachedCopies, want) {
		t.Errorf("attached %v, want the snapshot copied onto the reader VM %v", eng.attachedCopies, want)
	}
	if want := []string{"snap-" + sb.HibernateSnap}; !slices.Equal(eng.startedReaders, want) {
		t.Errorf("view started over %v, want the serial the attach returned %v", eng.startedReaders, want)
	}
	if n := len(m.pools[testKey].warm); n != 0 {
		t.Errorf("claim pool holds %d warm VMs, want the one reader taken", n)
	}
	if entries, _ := os.ReadDir(filepath.Join(m.dataDir, readerDir)); len(entries) != 0 {
		t.Errorf("a hibernated read left %d entries under %s", len(entries), readerDir)
	}
}

func TestReaderTakesItsVMFromTheClaimsOwnPool(t *testing.T) {
	eng := newFakeEngine()
	m := newReaderManager(t, eng, config.PoolSpec{PoolKey: otherKey, Warm: 1})
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	seedReaderVMs(m, testKey, "sbx-own")
	seedReaderVMs(m, otherKey, "sbx-foreign")

	conn, _, err := m.DialReader(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatalf("DialReader: %v", err)
	}
	_ = conn.Close()
	if want := []string{"snapshot:sbx-own:" + sb.HibernateSnap}; !slices.Equal(eng.attachedCopies, want) {
		t.Errorf("attached %v, want the claim's own pool VM %v", eng.attachedCopies, want)
	}
	if n := len(m.pools[otherKey].warm); n != 1 {
		t.Errorf("another pool holds %d warm VMs, want it untouched", n)
	}
}

func TestReaderServesOnlyPoolsThatAskForIt(t *testing.T) {
	eng := newFakeEngine()
	m := newReaderManager(t, eng, config.PoolSpec{PoolKey: otherKey, Warm: 1})
	sb := mustClaim(t, m, otherKey)
	hibernateClaim(t, m, sb)
	seedReaderVMs(m, otherKey, "sbx-reader-1")

	if _, _, err := m.DialReader(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrNoReader) {
		t.Errorf("DialReader of a pool without reader = %v, want ErrNoReader", err)
	}
	sock, _, done, err := m.WakeAgentSocketOrRead(t.Context(), sb.ID, sb.Token)
	if err != nil || sock == "" {
		t.Fatalf("WakeAgentSocketOrRead = %q, %v, want a wake", sock, err)
	}
	defer done()
	if len(eng.restores) != 1 || len(eng.attachedCopies) != 0 || len(m.pools[otherKey].warm) != 1 {
		t.Errorf("restores=%v attached=%v warm=%d, want a wake with no reader taken", eng.restores, eng.attachedCopies, len(m.pools[otherKey].warm))
	}
}

func TestReloadTurnsTheReaderOnForLaterSleeps(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	next := *m.cfg
	next.Pools = []config.PoolSpec{{PoolKey: testKey, Warm: 1, Reader: true}}
	res, err := m.ReloadConfig(t.Context(), &next)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if want := []string{"pools[rt:24.04 none small].reader"}; !slices.Equal(res.Changed, want) {
		t.Errorf("changed %v, want %v", res.Changed, want)
	}
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	if _, _, _, err = m.WakeAgentSocketOrRead(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrSleeping) {
		t.Errorf("WakeAgentSocketOrRead after the reload = %v, want ErrSleeping", err)
	}
}

func TestReaderHoldsTheTransitionLockWhileTheSnapshotIsCopied(t *testing.T) {
	eng := newFakeEngine()
	eng.attachStall = make(chan struct{})
	m := newReaderManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	seedReaderVMs(m, testKey, "sbx-reader-1")

	var wg sync.WaitGroup
	wg.Go(func() {
		conn, _, err := m.DialReader(t.Context(), sb.ID, sb.Token)
		if err != nil {
			t.Errorf("DialReader: %v", err)
			return
		}
		_ = conn.Close()
	})
	waitFor(t, func() bool { eng.mu.Lock(); defer eng.mu.Unlock(); return len(eng.attachedCopies) > 0 })
	if sb.Transition.TryLock() {
		sb.Transition.Unlock()
		t.Error("the transition lock was free while the snapshot was being copied")
	}
	close(eng.attachStall)
	wg.Wait()
	if !sb.Transition.TryLock() {
		t.Fatal("the reader kept the transition lock after the copy")
	}
	sb.Transition.Unlock()
}

func TestReaderCallsShareOneSetup(t *testing.T) {
	eng := newFakeEngine()
	eng.attachStall = make(chan struct{})
	m := newReaderManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	seedReaderVMs(m, testKey, "sbx-reader-1", "sbx-reader-2")

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
	waitFor(t, func() bool { eng.mu.Lock(); defer eng.mu.Unlock(); return len(eng.attachedCopies) > 0 })
	close(eng.attachStall)
	wg.Wait()

	eng.mu.Lock()
	defer eng.mu.Unlock()
	if len(eng.attachedCopies) != 1 || len(eng.startedReaders) != 1 {
		t.Errorf("%d callers ran %d attaches and %d reader setups, want one of each", callers, len(eng.attachedCopies), len(eng.startedReaders))
	}
	if len(eng.readerDials) != callers {
		t.Errorf("%d dials, want one per caller", len(eng.readerDials))
	}
}

func TestReaderIsReleasedAfterItsIdleWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newReaderManager(t, eng)
		sb := mustClaim(t, m, testKey)
		hibernateClaim(t, m, sb)
		seedReaderVMs(m, testKey, "sbx-reader-1")
		dialAndClose := func() {
			t.Helper()
			conn, _, err := m.DialReader(t.Context(), sb.ID, sb.Token)
			if err != nil {
				t.Fatalf("DialReader: %v", err)
			}
			_ = conn.Close()
		}
		dialAndClose()

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
		m := newReaderManager(t, eng)
		sb := mustClaim(t, m, testKey)
		hibernateClaim(t, m, sb)
		seedReaderVMs(m, testKey, "sbx-reader-1")
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
	m := newReaderManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	seedReaderVMs(m, testKey, "sbx-reader-2", "sbx-reader-1")

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
	m := newReaderManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	seedReaderVMs(m, testKey, "sbx-reader-2", "sbx-reader-1")

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
	m := newReaderManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	seedReaderVMs(m, testKey, "sbx-reader-2", "sbx-reader-1")
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
	if len(eng.attachedCopies) != 2 || len(eng.startedReaders) != 2 {
		t.Errorf("attaches=%d setups=%d, want a fresh reader per sleep", len(eng.attachedCopies), len(eng.startedReaders))
	}
	if !slices.Contains(eng.removes, "sbx-reader-1") {
		t.Errorf("removes=%v, want the first sleep's reader destroyed", eng.removes)
	}
}

func TestReaderServesAnArchivedClaimFromTheFilesItsEnvelopeLists(t *testing.T) {
	eng := newFakeEngine()
	readerExport(eng)
	m := newReaderManager(t, eng)
	counting := &fetchCountingStore{Store: m.ckpts}
	m.ckpts = counting
	sb := mustClaim(t, m, testKey)
	mustArchive(t, m, sb)
	seedReaderVMs(m, testKey, "sbx-reader-1")
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
	if got, want := counting.files(), []string{"disk.img", envelopeFile}; !slices.Equal(got, want) {
		t.Errorf("fetched files %v, want only %v", got, want)
	}
	if counting.fetches != 0 {
		t.Errorf("%d whole-record fetches, want none", counting.fetches)
	}
	if want := []string{"disk.img", envelopeFile}; !slices.Equal(eng.attachedDirFiles, want) {
		t.Errorf("attached directory held %v, want the envelope and its listed file %v", eng.attachedDirFiles, want)
	}
	if want := []string{"dir:sbx-reader-1"}; !slices.Equal(eng.attachedCopies, want) {
		t.Errorf("attached %v, want the directory copied onto the reader VM %v", eng.attachedCopies, want)
	}
	if n := len(eng.clones) + len(eng.colds); n != clones || len(eng.restores) != 0 {
		t.Errorf("an archived read booted a VM: clones/colds %d->%d, restores %v", clones, n, eng.restores)
	}
	if !ckExists(t, m, ck) {
		t.Error("the archive record was consumed")
	}
	if entries, _ := os.ReadDir(filepath.Join(m.dataDir, readerDir)); len(entries) != 0 {
		t.Errorf("the fetched copy outlived its attach: %d entries under %s", len(entries), readerDir)
	}
}

func TestReaderRefusesAnArchiveWithoutUsableAttachFiles(t *testing.T) {
	for _, tt := range []struct{ name, envelope string }{
		{"older export", `{"version":1}`},
		{"empty list", `{"version":1,"disk_attach_files":[]}`},
		{"parent path", `{"version":1,"disk_attach_files":["../escape"]}`},
		{"nested path", `{"version":1,"disk_attach_files":["sub/disk.img"]}`},
		{"absolute path", `{"version":1,"disk_attach_files":["/etc/passwd"]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			eng.exportFiles = map[string]string{envelopeFile: tt.envelope, "disk.img": "disk-bytes"}
			m := newReaderManager(t, eng)
			sb := mustClaim(t, m, testKey)
			mustArchive(t, m, sb)
			seedReaderVMs(m, testKey, "sbx-reader-1")

			if _, _, err := m.DialReader(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrNoReader) {
				t.Fatalf("DialReader = %v, want ErrNoReader", err)
			}
			if len(m.pools[testKey].warm) != 1 || len(eng.attachedCopies) != 0 {
				t.Errorf("a refused archive took a reader: warm=%d attached=%v", len(m.pools[testKey].warm), eng.attachedCopies)
			}
			if entries, _ := os.ReadDir(filepath.Join(m.dataDir, readerDir)); len(entries) != 0 {
				t.Errorf("a refused archive left %d entries under %s", len(entries), readerDir)
			}
		})
	}
}

func TestReaderRefusesWhatAWakeRefuses(t *testing.T) {
	eng := newFakeEngine()
	readerExport(eng)
	pool := archivePool(0)
	pool.Reader = true
	cfg := &config.Config{DataDir: t.TempDir(), APIToken: "root", Pools: []config.PoolSpec{pool}}
	m, err := NewManager(t.Context(), cfg, eng, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	putTenants(t, m, tenants.TenantSpec{Name: "acme", Token: "acme-tok"})
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour, Tenant: "acme"})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	mustArchive(t, m, sb)
	seedReaderVMs(m, testKey, "sbx-reader-1")
	if err = m.DeleteTenant(t.Context(), "acme"); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}

	if _, _, err = m.DialReader(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrNoReader) || !errors.Is(err, ErrTenantRemoved) {
		t.Errorf("DialReader of a removed tenant's archive = %v, want ErrNoReader wrapping ErrTenantRemoved", err)
	}
	if len(eng.startedReaders) != 0 || len(m.pools[testKey].warm) != 1 {
		t.Errorf("a refused read still took a reader: setups=%d warm=%d", len(eng.startedReaders), len(m.pools[testKey].warm))
	}
}

func TestReaderRefusesWhileTheDiskIsLow(t *testing.T) {
	eng := newFakeEngine()
	m := newReaderManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	seedReaderVMs(m, testKey, "sbx-reader-1")
	m.diskLow.Store(true)

	if _, _, err := m.DialReader(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrNoReader) || !errors.Is(err, ErrQuota) {
		t.Errorf("DialReader on a full disk = %v, want ErrNoReader wrapping ErrQuota", err)
	}
	if len(eng.attachedCopies) != 0 || len(m.pools[testKey].warm) != 1 {
		t.Errorf("a refused read still attached (%d) or took a reader (%d warm)", len(eng.attachedCopies), len(m.pools[testKey].warm))
	}
}

func TestReaderSetupFailureFallsBackAndCleansUp(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(*fakeEngine)
		warm  []string
	}{
		{"attach fails", func(e *fakeEngine) { e.diskAttachErr = errors.New("attach failed") }, []string{"sbx-reader-1"}},
		{"view fails", func(e *fakeEngine) { e.startReaderErr = errors.New("mount failed") }, []string{"sbx-reader-1"}},
		{"no warm reader", func(*fakeEngine) {}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			tt.setup(eng)
			m := newReaderManager(t, eng)
			sb := mustClaim(t, m, testKey)
			hibernateClaim(t, m, sb)
			seedReaderVMs(m, testKey, tt.warm...)

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
	m := newReaderManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	seedReaderVMs(m, testKey, "sbx-reader-2", "sbx-reader-1")
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
	m := newReaderManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	seedReaderVMs(m, testKey, "sbx-reader-2", "sbx-reader-1")
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
			eng.syncErr = tt.syncErr
			m := newReaderManager(t, eng)
			sb := mustClaim(t, m, testKey)
			hibernateClaim(t, m, sb)
			seedReaderVMs(m, testKey, "sbx-reader-1")

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

func newReaderManager(t *testing.T, eng *fakeEngine, extra ...config.PoolSpec) *Manager {
	t.Helper()
	cfg := &config.Config{DataDir: t.TempDir(), Pools: append([]config.PoolSpec{{PoolKey: testKey, Warm: 1, Reader: true}}, extra...)}
	m, err := NewManager(t.Context(), cfg, eng, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	return m
}

func seedReaderVMs(m *Manager, key types.PoolKey, names ...string) {
	for _, name := range names {
		m.pools[key].warm = append(m.pools[key].warm, &types.Sandbox{VMName: name, Key: key, VsockSocket: "/vsock/" + name})
	}
}

func hibernateClaim(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
}

// readerExport makes every snapshot export carry an envelope that lists one disk file for a reader to attach, and a memory image it must not fetch.
func readerExport(eng *fakeEngine) {
	eng.exportFiles = map[string]string{
		envelopeFile:    `{"version":1,"disk_attach_files":["disk.img"]}`,
		"disk.img":      "disk-bytes",
		"memory-ranges": "guest-memory",
	}
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
