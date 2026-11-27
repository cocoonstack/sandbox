package pool

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

var xlargeKey = types.PoolKey{Template: testKey.Template, Net: testKey.Net, Size: types.SizeXLarge}

func TestResizeMovesAHibernatedClaimToTheTargetPoolAndColdBootsIt(t *testing.T) {
	eng := newFakeEngine()
	m := newResizeManager(t, eng, "40G")
	sb := mustClaim(t, m, testKey)
	eng.vmStorage = map[string]int64{sb.VMName: 20 << 30}
	if err := m.Resize(t.Context(), sb.ID, types.SizeXLarge); !errors.Is(err, ErrNotResizable) {
		t.Errorf("resize of a running claim = %v, want ErrNotResizable", err)
	}
	hibernateClaim(t, m, sb)
	snap := sb.HibernateSnap
	if err := m.Resize(t.Context(), sb.ID, "huge"); !errors.Is(err, ErrBadKey) {
		t.Errorf("resize to an unknown size = %v, want ErrBadKey", err)
	}
	if err := m.Resize(t.Context(), sb.ID, types.SizeLarge); !errors.Is(err, ErrNotResizable) {
		t.Errorf("resize to a size this node has no pool for = %v, want ErrNotResizable", err)
	}
	if len(eng.resizes) != 0 {
		t.Fatalf("engine resizes %v before any accepted resize, want none", eng.resizes)
	}

	if err := m.Resize(t.Context(), sb.ID, types.SizeXLarge); err != nil {
		t.Fatalf("resize: %v", err)
	}

	if want := []string{sb.VMName + " 4/8G 40G"}; !slices.Equal(eng.resizes, want) {
		t.Errorf("engine resizes %v, want %v", eng.resizes, want)
	}
	got, _ := m.Sandbox(sb.ID)
	if got.Key != xlargeKey || got.CPUCount != 4 || got.Hibernated || got.Restarts != 1 {
		t.Errorf("summary %+v, want a 4-vCPU xlarge claim cold-booted out of hibernation", got)
	}
	if row := journaled(t, m)[sb.ID]; row == nil || row.Key != xlargeKey || row.HibernateSnap != "" {
		t.Errorf("journaled %+v, want the xlarge key without a hibernate snapshot", row)
	}
	if len(eng.restores) != 0 {
		t.Errorf("engine restores %v, want none: a resize never restores the old memory image", eng.restores)
	}
	waitFor(t, func() bool { return eng.snapRemoved(snap) })
	events := usageEventsOf(t, m, sb.ID)
	if !strings.HasSuffix(events, "hibernate,resize,vmm_restart") {
		t.Errorf("events %s, want the resize and then the restart that enters running", events)
	}
	i := slices.IndexFunc(usageJournal(t, m), func(ev usage.Event) bool { return ev.Event == "resize" })
	if ev := usageJournal(t, m)[i]; ev.Size != string(types.SizeXLarge) || ev.KeyHash != xlargeKey.Hash() || ev.Template != xlargeKey.Template || ev.VMName != sb.VMName || ev.Deadline.IsZero() {
		t.Errorf("resize event %+v, want the xlarge shape, the vm and the deadline", ev)
	}
	if next, _ := m.byID(sb.ID); next == sb || sb.Key != testKey || sb.HibernateSnap != "" {
		t.Errorf("published record is the old one (%v), the old record's key moved to %v, or it still names snapshot %q", next == sb, sb.Key, sb.HibernateSnap)
	}
	if _, err := m.wakeResolved(t.Context(), sb); err != nil || len(eng.restores) != 0 {
		t.Errorf("wake through the replaced record = %v with restores %v, want it to reach the running guest and restore nothing", err, eng.restores)
	}
}

func TestResizeGrowsTheDiskOnlyToAPoolStorageAboveIt(t *testing.T) {
	for _, tc := range []struct {
		name    string
		storage string
		disk    int64
		want    string
	}{
		{"the pool sets no storage", "", 20 << 30, "4/8G"},
		{"the disk already has the pool's storage", "40G", 40 << 30, "4/8G"},
		{"the disk is above the pool's storage", "40G", 80 << 30, "4/8G"},
		{"the disk is below the pool's storage", "40G", 20 << 30, "4/8G 40G"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := newResizeManager(t, eng, tc.storage)
			sb := mustClaim(t, m, testKey)
			eng.vmStorage = map[string]int64{sb.VMName: tc.disk}
			hibernateClaim(t, m, sb)

			if err := m.Resize(t.Context(), sb.ID, types.SizeXLarge); err != nil {
				t.Fatalf("resize: %v", err)
			}

			if want := []string{sb.VMName + " " + tc.want}; !slices.Equal(eng.resizes, want) {
				t.Errorf("engine resizes %v, want %v", eng.resizes, want)
			}
		})
	}
}

func TestAResizeWhoseColdBootFailsLeavesAClaimOnlyAColdBootCanRun(t *testing.T) {
	eng := newFakeEngine()
	m := newResizeManager(t, eng, "")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	snap := sb.HibernateSnap
	eng.startFails = 1

	if err := m.Resize(t.Context(), sb.ID, types.SizeXLarge); err == nil {
		t.Fatal("resize reported success although the cold boot failed")
	}

	got, _ := m.Sandbox(sb.ID)
	if got.Key != xlargeKey || got.Hibernated || got.Restarts != 0 {
		t.Errorf("summary %+v, want the xlarge claim, no longer hibernated and not yet restarted", got)
	}
	waitFor(t, func() bool { return eng.snapRemoved(snap) })
	runLiveness(t, m)
	if got, _ = m.Sandbox(sb.ID); got.Key != xlargeKey || got.Restarts != 1 || got.Failed != "" {
		t.Errorf("summary after the liveness pass %+v, want the xlarge claim restarted from its disk", got)
	}
	if len(eng.restores) != 0 {
		t.Errorf("engine restores %v, want none: the old memory image must not run under the new size", eng.restores)
	}
	if events := usageEventsOf(t, m, sb.ID); !strings.HasSuffix(events, "hibernate,resize,vmm_exit,vmm_restart") {
		t.Errorf("events %s, want the resize, then the liveness pass's exit and restart", events)
	}
}

func TestAResizePastTheLeaseOfATierClaimGivesAFreshLease(t *testing.T) {
	m := newTestManager(t, newFakeEngine(),
		config.PoolSpec{PoolKey: testKey, ArchiveAfterSeconds: 3600}, config.PoolSpec{PoolKey: xlargeKey, ArchiveAfterSeconds: 3600})
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	hibernateClaim(t, m, sb)
	expire(m, sb)

	if err = m.Resize(t.Context(), sb.ID, types.SizeXLarge); err != nil {
		t.Fatalf("resize: %v", err)
	}

	got, _ := m.Sandbox(sb.ID)
	if lease := time.Until(got.Deadline); lease < 59*time.Minute {
		t.Fatalf("lease after a resize past the lease = %v, want the claim's hour again, as a wake gives", lease)
	}
	events := usageJournal(t, m)
	i := slices.IndexFunc(events, func(ev usage.Event) bool { return ev.Event == "resize" })
	if i < 0 || !events[i].Deadline.Equal(got.Deadline) {
		t.Errorf("resize event deadline differs from the renewed lease %v: %+v", got.Deadline, events)
	}
	m.reapOnce(t.Context())
	if _, ok := m.byID(sb.ID); !ok {
		t.Error("the reaper destroyed the resized claim")
	}
}

func TestCallsThatMeetAResizeNeverRestoreTheOldMemoryImage(t *testing.T) {
	eng := newFakeEngine()
	m := newResizeManager(t, eng, "")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	cred := Cred{Token: sb.Token}
	results := make(chan error, 3)
	eng.resizeHook = func() {
		go func() { _, err := m.wakeResolved(t.Context(), sb); results <- err }()
		go func() { _, _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, cred); results <- err }()
		go func() { _, err := m.Renew(t.Context(), sb.ID, cred, time.Hour, ""); results <- err }()
		time.Sleep(10 * time.Millisecond)
	}

	if err := m.Resize(t.Context(), sb.ID, types.SizeXLarge); err != nil {
		t.Fatalf("resize: %v", err)
	}

	for range cap(results) {
		if err := <-results; err != nil && !errors.Is(err, ErrUnknownSandbox) {
			t.Errorf("a call that met the resize = %v, want success or ErrUnknownSandbox", err)
		}
	}
	if got, _ := m.Sandbox(sb.ID); got.Key != xlargeKey || got.Hibernated || len(eng.restores) != 0 {
		t.Errorf("summary %+v with restores %v, want the running xlarge claim and no restore", got, eng.restores)
	}
}

func TestAResizeThatCocoonRefusesChangesNothing(t *testing.T) {
	eng := newFakeEngine()
	m := newResizeManager(t, eng, "")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	eng.resizeErr = errors.New("vm resize: vm is not stopped; run vm stop first")

	if err := m.Resize(t.Context(), sb.ID, types.SizeXLarge); err == nil {
		t.Fatal("resize reported success although cocoon refused it")
	}

	assertUnresized(t, m, eng, sb)
}

func TestAResizeWhoseJournalWriteFailsIsUndone(t *testing.T) {
	eng := newFakeEngine()
	m := newResizeManager(t, eng, "")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	snap := sb.HibernateSnap
	breakStore(t, m)

	if err := m.Resize(t.Context(), sb.ID, types.SizeXLarge); err == nil {
		t.Fatal("resize reported success although the journal write failed")
	}

	healStore(t, m)
	if sb.HibernateSnap != snap {
		t.Fatalf("the undone record names snapshot %q, want %q back", sb.HibernateSnap, snap)
	}
	assertUnresized(t, m, eng, sb)
	if _, err := m.wakeResolved(t.Context(), sb); err != nil || len(eng.restores) != 1 {
		t.Errorf("wake after the undone resize = %v with restores %v, want the old record to wake from its snapshot", err, eng.restores)
	}
}

func TestResizeNeedsACocoonWithVMResize(t *testing.T) {
	eng := newFakeEngine()
	m := newResizeManager(t, eng, "")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	eng.resizeSupportErr = errors.New("resize requires Cocoon >= v0.8.1; installed version is v0.8.0")

	if err := m.Resize(t.Context(), sb.ID, types.SizeXLarge); !errors.Is(err, ErrNotResizable) {
		t.Errorf("resize on an older cocoon = %v, want ErrNotResizable", err)
	}

	assertUnresized(t, m, eng, sb)
}

func newResizeManager(t *testing.T, eng *fakeEngine, storage string) *Manager {
	t.Helper()
	return newTestManager(t, eng, config.PoolSpec{PoolKey: testKey}, config.PoolSpec{PoolKey: xlargeKey, Storage: storage})
}

func assertUnresized(t *testing.T, m *Manager, eng *fakeEngine, sb *types.Sandbox) {
	t.Helper()
	m.mu.Lock()
	cur, waking := m.claimed[sb.ID], sb.Waking
	m.mu.Unlock()
	if cur != sb || waking {
		t.Errorf("the claim's record was replaced (%v) or is still marked waking (%v)", cur != sb, waking)
	}
	got, _ := m.Sandbox(sb.ID)
	if got.Key != testKey || !got.Hibernated || got.Restarts != 0 {
		t.Errorf("summary %+v, want the hibernated claim at its old size", got)
	}
	if row := journaled(t, m)[sb.ID]; row == nil || row.Key != testKey || row.HibernateSnap != sb.HibernateSnap {
		t.Errorf("journaled %+v, want the old key and the hibernate snapshot", row)
	}
	if eng.snapRemoved(sb.HibernateSnap) || len(eng.starts) != 0 {
		t.Errorf("snapshot removed %v, starts %v; want the snapshot kept and no cold boot", eng.snapRemoved(sb.HibernateSnap), eng.starts)
	}
	if events := usageEventsOf(t, m, sb.ID); strings.Contains(events, "resize") {
		t.Errorf("events %s, want no resize event for a resize that did not happen", events)
	}
}
