package pool

import (
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
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

func TestAResizePastTheLeaseRenewsItByTheRuleOfThePoolTheClaimLeaves(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, ArchiveAfterSeconds: 3600}, config.PoolSpec{PoolKey: xlargeKey})
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
		t.Fatalf("lease after a resize past the lease = %v, want the claim's hour again, as a wake in the pool it leaves gives", lease)
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

func TestAClaimMadeWithoutAPoolLayerTakesTheLayerOfThePoolAResizeMovesItInto(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = outboundtest.SockRoot(t)
	m := egressManager(t, eng, config.PoolSpec{PoolKey: xlargeKey})
	editEgress(t, m, func(cfg *config.Config) {
		cfg.EgressClasses = []config.EgressClass{{Name: "desk", Egress: &egress.Policy{Allow: []egress.Rule{{Host: "b.test"}}}}}
	})
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "acme", EgressClass: "desk"})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, ok := m.view.Load().out.Resolve(sb); !ok || sb.Layer != types.LayerUnpooled {
		t.Fatalf("layer %q before the resize, want a claim of a key in no pool with its class's policy", sb.Layer)
	}
	hibernateClaim(t, m, sb)

	if err = m.Resize(t.Context(), sb.ID, types.SizeXLarge); err != nil {
		t.Fatalf("resize: %v", err)
	}

	next, _ := m.byID(sb.ID)
	if _, ok := m.view.Load().out.Resolve(next); ok || next.Layer != types.LayerPooled {
		t.Errorf("layer %q after the resize, want no egress, as a claim made from the policy-less pool has none", next.Layer)
	}
	if row := journaled(t, m)[sb.ID]; row == nil || row.Layer != types.LayerPooled {
		t.Errorf("journaled %+v, want the pooled layer so a restart keeps it", row)
	}

	clone, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "acme", EgressClass: "desk"})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	m.mu.Lock()
	clone.PolicySource = types.PoolKey{Template: "promoted-from", Net: testKey.Net, Size: testKey.Size}
	m.mu.Unlock()
	hibernateClaim(t, m, clone)
	if err = m.Resize(t.Context(), clone.ID, types.SizeXLarge); err != nil {
		t.Fatalf("resize of a promoted template's clone: %v", err)
	}
	if next, _ = m.byID(clone.ID); next.Layer != types.LayerUnpooled {
		t.Errorf("layer %q after the resize of a clone whose policy comes from another pool, want it unchanged", next.Layer)
	}
}

func TestAResizeNeedsTheTargetPoolToInterceptEveryInjectHost(t *testing.T) {
	allowOnly := &egress.Policy{Allow: []egress.Rule{{Host: "api.github.com"}}}
	for _, tc := range []struct {
		name   string
		target *egress.Policy
		reload bool
		want   string
	}{
		{"the target pool only allows the host", allowOnly, false, "KEY: api.github.com is not intercepted"},
		{"the target pool intercepts the host", interceptPolicy(), false, ""},
		{"a reload took the intercept from both pools before the resize", interceptPolicy(), true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng := newFakeEngine()
			eng.sockRoot = outboundtest.SockRoot(t)
			m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: interceptPolicy()}, config.PoolSpec{PoolKey: xlargeKey, Egress: tc.target})
			sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: injectEnv("k", "api.github.com")})
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			hibernateClaim(t, m, sb)
			if tc.reload {
				editEgress(t, m, func(cfg *config.Config) {
					for i := range cfg.Pools {
						cfg.Pools[i].Egress = allowOnly
					}
				})
			}

			err = m.Resize(t.Context(), sb.ID, types.SizeXLarge)

			if tc.want == "" {
				if err != nil {
					t.Fatalf("resize: %v, want it to add no gap and go through", err)
				}
				if next, _ := m.byID(sb.ID); next.Key != xlargeKey || credentialsFor(next, "api.github.com") != "KEY:X-Api-Key=k" {
					t.Errorf("key %v with credentials %q, want the xlarge claim still injecting", next.Key, credentialsFor(next, "api.github.com"))
				}
				return
			}
			if !errors.Is(err, ErrNotResizable) || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("resize = %v, want ErrNotResizable naming %q", err, tc.want)
			}
			if len(eng.resizes) != 0 {
				t.Errorf("engine resizes %v, want none", eng.resizes)
			}
			assertUnresized(t, m, eng, sb)
		})
	}
}

func TestAResizeRefusesAClaimThatHoldsVolumes(t *testing.T) {
	eng := newFakeEngine()
	m := newResizeManager(t, eng, "")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	m.mu.Lock()
	sb.VolumesWant = []types.Volume{{Name: "v"}}
	m.mu.Unlock()

	err := m.Resize(t.Context(), sb.ID, types.SizeXLarge)

	if !errors.Is(err, ErrNotResizable) || !strings.HasSuffix(err.Error(), ErrNotResizable.Error()+": volume mounts do not survive a cold boot") {
		t.Errorf("resize of a claim with volumes = %v, want ErrNotResizable with the reason alone", err)
	}
	if len(eng.resizes) != 0 {
		t.Errorf("engine resizes %v, want none", eng.resizes)
	}
}

func TestAResizeIsRefusedWhenThePoolLayerItTakesDropsAnInjectHost(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = outboundtest.SockRoot(t)
	m := egressManager(t, eng, config.PoolSpec{PoolKey: xlargeKey})
	editEgress(t, m, func(cfg *config.Config) {
		cfg.EgressClasses = []config.EgressClass{{Name: "desk", Egress: interceptPolicy()}}
	})
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "acme", EgressClass: "desk", Env: injectEnv("k", "api.github.com")})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	hibernateClaim(t, m, sb)

	err = m.Resize(t.Context(), sb.ID, types.SizeXLarge)

	if !errors.Is(err, ErrNotResizable) || !strings.Contains(err.Error(), "KEY: api.github.com is not intercepted") {
		t.Errorf("resize = %v, want ErrNotResizable: the policy-less pool's layer leaves the claim no egress", err)
	}
	if len(eng.resizes) != 0 {
		t.Errorf("engine resizes %v, want none", eng.resizes)
	}
}

func TestAResizeRechecksAnInjectEntryWrittenWhileCocoonResized(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = outboundtest.SockRoot(t)
	m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: interceptPolicy()}, config.PoolSpec{PoolKey: xlargeKey, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "api.github.com"}}}})
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	hibernateClaim(t, m, sb)
	var patchErr error
	eng.resizeHook = func() {
		patchErr = m.PatchEnv(t.Context(), sb.ID, injectPatch("X-Api-Key", "api.github.com"), "")
	}

	err = m.Resize(t.Context(), sb.ID, types.SizeXLarge)

	if patchErr != nil {
		t.Fatalf("patch during the resize: %v, want it admitted under the small pool's policy", patchErr)
	}
	if !errors.Is(err, ErrNotResizable) || !strings.Contains(err.Error(), "KEY: api.github.com is not intercepted") {
		t.Errorf("resize = %v, want ErrNotResizable naming the inject written while cocoon resized", err)
	}
	assertUnresized(t, m, eng, sb)
	if got := credentialsFor(sb, "api.github.com"); got != "KEY:X-Api-Key=k" {
		t.Errorf("credentials %q, want the patched inject kept under the small pool", got)
	}
}

func TestCallsThatMeetAResizeNeverRestoreTheOldMemoryImage(t *testing.T) {
	eng := newFakeEngine()
	m := newResizeManager(t, eng, "")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	cred := Cred{Token: sb.Token}
	results, waited, again := make(chan error, 3), make(chan error, 4), make(chan error, 1)
	eng.resizeHook = func() {
		go func() { waited <- m.lockTransition(sb) }()
		go func() { waited <- m.restartFailed(t.Context(), sb) }()
		go func() { waited <- m.rewriteArchiveRow(t.Context(), sb, false) }()
		go func() { waited <- m.markMoving(sb, &outMove{}) }()
		go func() { again <- m.Resize(t.Context(), sb.ID, types.SizeXLarge) }()
		go func() { _, err := m.wakeResolved(t.Context(), sb); results <- err }()
		go func() { _, _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, cred); results <- err }()
		go func() { _, err := m.Renew(t.Context(), sb.ID, cred, time.Hour, ""); results <- err }()
		time.Sleep(10 * time.Millisecond)
	}

	if err := m.Resize(t.Context(), sb.ID, types.SizeXLarge); err != nil {
		t.Fatalf("resize: %v", err)
	}

	for range cap(results) {
		if err := <-results; err != nil && !errors.Is(err, ErrReplaced) {
			t.Errorf("a call that met the resize = %v, want success or the retry a replaced record answers", err)
		}
	}
	for range cap(waited) {
		if err := <-waited; !errors.Is(err, ErrReplaced) {
			t.Errorf("a control call that waited for the resize = %v, want the retry a replaced record answers", err)
		}
	}
	if err := <-again; !errors.Is(err, ErrReplaced) && !errors.Is(err, ErrNotResizable) {
		t.Errorf("a second resize that met the first = %v, want a retry or a refusal of the running claim", err)
	}
	if got, _ := m.Sandbox(sb.ID); got.Key != xlargeKey || got.Hibernated || len(eng.restores) != 0 {
		t.Errorf("summary %+v with restores %v, want the running xlarge claim and no restore", got, eng.restores)
	}
}

func TestAResizeThatAnEvacuationOvertakesLeavesTheClaimForIt(t *testing.T) {
	eng := newFakeEngine()
	m := newResizeManager(t, eng, "")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	eng.resizeHook = func() {
		m.mu.Lock()
		m.evac = &evacuation{pending: maps.Clone(m.claimed), stop: func() {}}
		m.mu.Unlock()
	}

	if err := m.Resize(t.Context(), sb.ID, types.SizeXLarge); !errors.Is(err, ErrEvacuating) {
		t.Fatalf("resize overtaken by an evacuation = %v, want ErrEvacuating", err)
	}

	assertUnresized(t, m, eng, sb)
}

func TestAFailoverBeforeTheNextReplicaBringsAResizedClaimBackAtItsOldShape(t *testing.T) {
	c := newReplicaCluster(t)
	a, b := c.pair(t)
	if err := a.SetPools(t.Context(), []config.PoolSpec{{PoolKey: xlargeKey}}); err != nil {
		t.Fatalf("add the xlarge pool: %v", err)
	}
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	hibernateClaim(t, a, sb)
	if err := a.Resize(t.Context(), sb.ID, types.SizeXLarge); err != nil {
		t.Fatalf("resize: %v", err)
	}
	resized, _ := a.byID(sb.ID)
	hibernateClaim(t, a, resized)
	if _, err := a.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Hour, ""); err != nil {
		t.Fatalf("renew, which rewrites the replica's row: %v", err)
	}

	c.expire("a")
	mustAdopt(t, b, sb, "adopt the replica taken before the resize")

	moved, _ := b.byID(sb.ID)
	evs := usageJournal(t, b)
	last := evs[len(evs)-1]
	if moved.Key != testKey || last.Event != "adopt" || last.KeyHash != testKey.Hash() || last.Template != testKey.Template || last.Net != string(testKey.Net) || last.Size != string(testKey.Size) {
		t.Errorf("adopted key %+v with last event %+v, want the claim back at its old key and an adopt that carries that shape", moved.Key, last)
	}
}

func TestAResizeOfAReplicatedClaimStopsAtTheSwapWhileTheNodeIsFenced(t *testing.T) {
	c := newReplicaCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	if err := a.SetPools(t.Context(), []config.PoolSpec{{PoolKey: xlargeKey}}); err != nil {
		t.Fatalf("add the xlarge pool: %v", err)
	}
	sb := mustClaim(t, a, testKey)
	replicateNow(t, a, sb)
	hibernateClaim(t, a, sb)
	eng := fakeOf(a)
	eng.resizeHook = a.tripFence

	if err := a.Resize(t.Context(), sb.ID, types.SizeXLarge); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("resize across a lost lease = %v, want ErrLeaseLost", err)
	}

	a.liftFence()
	assertUnresized(t, a, eng, sb)
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
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, ArchiveAfterSeconds: 3600}, config.PoolSpec{PoolKey: xlargeKey})
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	expire(m, sb)
	snap, deadline := sb.HibernateSnap, sb.Deadline
	breakStore(t, m)

	if err := m.Resize(t.Context(), sb.ID, types.SizeXLarge); err == nil {
		t.Fatal("resize reported success although the journal write failed")
	}

	healStore(t, m)
	if sb.HibernateSnap != snap || !sb.Deadline.Equal(deadline) {
		t.Fatalf("the undone record names snapshot %q and deadline %v, want %q and %v back", sb.HibernateSnap, sb.Deadline, snap, deadline)
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
