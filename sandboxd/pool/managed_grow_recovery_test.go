package pool

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
)

func TestManagedBackingObservationCannotDiscardGrowth(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "unknown"}[unknown], func(t *testing.T) {
			m, e, _, _, v := newGrowthTest(t)
			m.managed.mu.Lock()
			observed := m.managed.data.Volumes["data"]
			m.managed.mu.Unlock()
			e.unknown = unknown
			_, growErr := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
			if unknown {
				requireVolumeStatus(t, growErr, 409)
			} else if growErr != nil {
				t.Fatal(growErr)
			}
			before, err := m.GetVolume("data", "t1")
			if err != nil || before.Growth == nil {
				t.Fatalf("growth result unavailable: %+v %v", before, err)
			}
			m.managed.mu.Lock()
			_ = m.managed.markRecovery(observed)
			m.managed.mu.Unlock()
			after, err := m.GetVolume("data", "t1")
			if err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("stale backing observation discarded growth: before=%+v after=%+v error=%v", before, after, err)
			}
		})
	}
}

func TestGrowthMissingVMRecordPreservesDurableOwner(t *testing.T) {
	m, e, sb, cfg, v := newGrowthTest(t)
	e.unknown = true
	_, _ = m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	if err := m.FlushClaims(); err != nil {
		t.Fatal(err)
	}
	m.Close()
	e.mu.Lock()
	delete(e.vms, sb.VMName)
	e.mu.Unlock()
	e.processLives.Store(true)
	m = openGrowthTest(t, cfg, e)
	requireManagedRecoveryFence(t, m, sb.ID)
	claims, err := m.store.load()
	if err != nil {
		t.Fatal(err)
	}
	if claims[sb.ID] == nil {
		t.Fatal("reconcile deleted durable growth owner claim")
	}
	if !m.growthOwnsHold(sb.ID) {
		t.Fatal("expected retained unresolved growth fence")
	}

	m.Close()
	m = openGrowthTest(t, cfg, e)
	requireManagedRecoveryFence(t, m, sb.ID)
}

func TestGrowthChangedVMRecordCannotPublishReplacement(t *testing.T) {
	m, e, sb, cfg, v := newGrowthTest(t)
	e.unknown = true
	_, _ = m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	if err := m.FlushClaims(); err != nil {
		t.Fatal(err)
	}
	m.Close()
	e.mu.Lock()
	e.pids[sb.VMName] = 90001
	e.vms[sb.VMName] = "/tmp/replacement-generation.sock"
	e.mu.Unlock()
	e.growMu.Lock()
	e.binding.VMM.PID = 90001
	e.binding.VMM.Start = 90001
	e.growMu.Unlock()
	m = openGrowthTest(t, cfg, e)
	hOriginal, _ := m.managedHold(sb.ID)
	m.managed.process = func(pid int) (processIdentity, error) {
		if pid == hOriginal.Process.PID {
			return hOriginal.Process, nil
		}
		return e.identity(pid)
	}
	requireManagedRecoveryFence(t, m, sb.ID)
	claims, err := m.store.load()
	if err != nil {
		t.Fatal(err)
	}
	if claims[sb.ID] == nil || claims[sb.ID].VsockSocket != sb.VsockSocket {
		t.Fatal("original durable claim was replaced")
	}

	if !m.growthOwnsHold(sb.ID) {
		t.Fatal("growth not retained")
	}
	h, _ := m.managedHold(sb.ID)
	if h.Process.PID == 90001 {
		t.Fatal("expected original binding preserved")
	}
}

type reviewUnavailableGrowEngine struct{ *growthTestEngine }

func (e *reviewUnavailableGrowEngine) InspectDisk(ctx context.Context, ref engine.DiskRef) (engine.DiskInspection, error) {
	out, err := e.growthTestEngine.InspectDisk(ctx, ref)
	out.Live = engine.DiskObservation{State: "unavailable", Reason: "recorded VMM stopped"}
	return out, err
}

func TestGrowthStoppedACKCanQuarantineAndRelease(t *testing.T) {
	m, e, sb, _, v := newGrowthTest(t)
	e.lag = true
	got, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	if err != nil || got.Growth.State != "pending" || got.Growth.RuntimeOutcome != "acknowledged" {
		t.Fatalf("admission %+v %v", got, err)
	}
	if err = e.Stop(t.Context(), sb.VMName); err != nil {
		t.Fatal(err)
	}
	m.managed.process = func(int) (processIdentity, error) { return processIdentity{}, os.ErrNotExist }
	m.eng = &reviewUnavailableGrowEngine{e}
	for range 3 {
		_, err = m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
		requireVolumeStatus(t, err, 409)
	}
	if e.receipt.Outcome != "acknowledged" {
		t.Fatal("ACK should stay ACK under qualified runtime RefreshLocked")
	}
	if m.growthOwnsHold(sb.ID) {
		t.Fatal("proven stopped acknowledged generation retains fence")
	}
	if err = m.Release(t.Context(), sb.ID, Cred{Operator: true}); err != nil {
		t.Fatal(err)
	}
	got, err = m.GetVolume("data", "t1")
	if err != nil || got.CapacityGeneration != 0 || got.Growth.RuntimeOutcome != "acknowledged" || got.Growth.State != "recovery_required" {
		t.Fatalf("result %+v %v", got, err)
	}
}

func TestGrowthClaimDurabilityFailurePreventsIntent(t *testing.T) {
	m, e, sb, _, v := newGrowthTest(t)
	if m.store.sync {
		t.Fatal("test requires ordinary asynchronous claims")
	}
	oldPath := m.store.path
	m.store.path = t.TempDir()
	defer func() { m.store.path = oldPath }()
	_, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	if err == nil {
		t.Error("growth ignored claim durability failure")
	}
	if e.calls != 0 || m.managed.data.Volumes["data"].Growth != nil {
		t.Fatal("growth admitted before durable claim")
	}
	if _, found := m.Sandbox(sb.ID); !found {
		t.Fatal("failed admission lost owner")
	}
}

type growthInventoryEngine struct {
	*growthTestEngine
	change func(*types.VMRecord)
	omit   bool
}

func (e *growthInventoryEngine) List(ctx context.Context) ([]types.VMRecord, error) {
	records, err := e.growthTestEngine.List(ctx)
	if e.omit {
		return nil, err
	}
	for i := range records {
		e.change(&records[i])
	}
	return records, err
}

func (e *growthInventoryEngine) Inspect(ctx context.Context, name string) (types.VMRecord, bool, error) {
	rec, found, err := e.growthTestEngine.Inspect(ctx, name)
	e.change(&rec)
	return rec, found, err
}

func TestGrowthRecoveryRequiresOriginalInventoryAndProcess(t *testing.T) {
	for _, mutation := range []string{"inventory missing", "immutable VM ID", "process start", "socket", "unreadable process"} {
		t.Run(mutation, func(t *testing.T) {
			m, e, sb, cfg, v := newGrowthTest(t)
			e.unknown = true
			_, growErr := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
			requireVolumeStatus(t, growErr, 409)
			if err := m.FlushClaims(); err != nil {
				t.Fatal(err)
			}
			original, err := os.ReadFile(m.store.path)
			if err != nil {
				t.Fatal(err)
			}
			hold, _ := m.managedHold(sb.ID)
			m.Close()
			m = openGrowthTest(t, cfg, e)
			inv := &growthInventoryEngine{growthTestEngine: e, change: func(*types.VMRecord) {}}
			m.eng = inv
			switch mutation {
			case "inventory missing":
				inv.omit = true
			case "immutable VM ID":
				inv.change = func(rec *types.VMRecord) { rec.ID = "replacement-vm" }
			case "socket":
				inv.change = func(rec *types.VMRecord) { rec.VsockSocket = "/replacement.sock" }
			case "process start":
				m.managed.process = func(int) (processIdentity, error) { p := hold.Process; p.Start++; return p, nil }
			case "unreadable process":
				e.processUnknown.Store(true)
			}
			requireManagedRecoveryFence(t, m, sb.ID)
			after, readErr := os.ReadFile(m.store.path)
			if readErr != nil || string(after) != string(original) {
				t.Fatalf("owner journal changed: %v", readErr)
			}
			current, _ := m.managedHold(sb.ID)
			if !reflect.DeepEqual(hold, current) || !m.growthOwnsHold(sb.ID) {
				t.Fatal("original hold or growth fence changed")
			}
			if e.calls != 1 || len(e.removes) != 0 {
				t.Fatal("startup dispatched or destroyed a fenced generation")
			}
		})
	}
}

func TestGrowthStoppedACKRejectsReplacementAndUnprovenDeath(t *testing.T) {
	for _, mutation := range []string{"process alive", "process unreadable", "replacement VM", "replacement PID", "running record", "creating record"} {
		t.Run(mutation, func(t *testing.T) {
			m, e, sb, _, v := newGrowthTest(t)
			e.lag = true
			if _, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1"); err != nil {
				t.Fatal(err)
			}
			if err := e.Stop(t.Context(), sb.VMName); err != nil {
				t.Fatal(err)
			}
			m.managed.process = func(int) (processIdentity, error) { return processIdentity{}, os.ErrNotExist }
			inv := &growthInventoryEngine{growthTestEngine: e, change: func(*types.VMRecord) {}}
			m.eng = inv
			switch mutation {
			case "process alive":
				m.managed.process = e.identity
			case "process unreadable":
				m.managed.process = func(int) (processIdentity, error) { return processIdentity{}, os.ErrPermission }
			case "replacement VM":
				inv.change = func(rec *types.VMRecord) { rec.ID = "replacement-vm" }
			case "replacement PID":
				inv.change = func(rec *types.VMRecord) { rec.PID++ }
			case "running record":
				inv.change = func(rec *types.VMRecord) { rec.State = "running" }
			case "creating record":
				inv.change = func(rec *types.VMRecord) { rec.State = "creating" }
			}
			_, _ = m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
			if !m.growthOwnsHold(sb.ID) || m.removeVM(t.Context(), sb.VMName) {
				t.Fatal("unverified generation permitted cleanup")
			}
			if len(e.removes) != 0 || e.calls != 1 {
				t.Fatal("unverified generation removed or redispatched")
			}
		})
	}
}

func TestGrowthSettledACKCannotRemoveLaterReplacement(t *testing.T) {
	m, e, sb, _, v := newGrowthTest(t)
	e.lag = true
	if _, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1"); err != nil {
		t.Fatal(err)
	}
	if err := e.Stop(t.Context(), sb.VMName); err != nil {
		t.Fatal(err)
	}
	m.managed.process = func(int) (processIdentity, error) { return processIdentity{}, os.ErrNotExist }
	_, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	requireVolumeStatus(t, err, 409)
	if m.growthOwnsHold(sb.ID) {
		t.Fatal("exact stopped ACK did not settle")
	}
	m.eng = &growthInventoryEngine{growthTestEngine: e, change: func(rec *types.VMRecord) { rec.ID = "replacement-vm" }}
	if m.removeVM(t.Context(), sb.VMName) || len(e.removes) != 0 {
		t.Fatal("settled operation removed replacement VM")
	}
}

type growthRemovalRaceEngine struct {
	*growthTestEngine
	removed string
}

func (e *growthRemovalRaceEngine) Remove(_ context.Context, selector string) error {
	e.removed = selector
	return nil
}

func TestGrowthSettledRemovalUsesImmutableVMID(t *testing.T) {
	m, e, sb, _, v := newGrowthTest(t)
	e.lag = true
	if _, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1"); err != nil {
		t.Fatal(err)
	}
	if err := e.Stop(t.Context(), sb.VMName); err != nil {
		t.Fatal(err)
	}
	m.managed.process = func(int) (processIdentity, error) { return processIdentity{}, os.ErrNotExist }
	_, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	requireVolumeStatus(t, err, 409)
	race := &growthRemovalRaceEngine{growthTestEngine: e}
	m.eng = race
	m.removeVM(t.Context(), sb.VMName)
	if race.removed != "immutable-grow-vm" {
		t.Fatalf("removal used reusable VM selector %q", race.removed)
	}
}

func TestGrowthStoppedACKSettlementSurvivesRestart(t *testing.T) {
	m, e, sb, cfg, v := newGrowthTest(t)
	e.lag = true
	if _, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1"); err != nil {
		t.Fatal(err)
	}
	if err := e.Stop(t.Context(), sb.VMName); err != nil {
		t.Fatal(err)
	}
	dead := func(int) (processIdentity, error) { return processIdentity{}, os.ErrNotExist }
	m.managed.process = dead
	_, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	requireVolumeStatus(t, err, 409)
	if err = m.managed.validate(); err != nil {
		t.Fatal(err)
	}
	m.Close()
	m = openGrowthTest(t, cfg, e)
	m.managed.process = dead
	if err = m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	got, err := m.GetVolume("data", "t1")
	if err != nil || got.Growth.RuntimeOutcome != "acknowledged" || got.Growth.State != "recovery_required" || got.CapacityGeneration != 0 || got.SizeBytes != 1<<30 || len(got.Holders) != 0 {
		t.Fatalf("settled restart: %+v %v", got, err)
	}
	if !m.managed.data.Volumes["data"].Growth.VMMGone || e.calls != 1 {
		t.Fatal("settlement lost or redispatched")
	}
	if err = m.DeleteVolume(t.Context(), "data", v.ID, "t1"); err != nil {
		t.Fatal(err)
	}
}

func TestGrowthRegistryRejectsFalseStoppedSettlement(t *testing.T) {
	for _, outcome := range []string{"unobserved", "outcome_unknown", "acknowledged", "old_vmm_gone"} {
		t.Run(outcome, func(t *testing.T) {
			m, e, sb, _, v := newGrowthTest(t)
			e.lag = true
			if _, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1"); err != nil {
				t.Fatal(err)
			}
			current := m.managed.data.Volumes["data"]
			current.Growth.Outcome, current.Growth.VMMGone = outcome, true
			if m.managed.validate() == nil {
				t.Fatal("pending intent accepted settlement flag")
			}
			current.Growth.State = "recovery_required"
			current.State = volumeRecovery
			m.managed.data.Volumes["data"] = current
			if err := m.managed.validate(); (err == nil) != (outcome == "acknowledged") {
				t.Fatalf("stopped settlement %q accepted incorrectly: %v", outcome, err)
			}
			if _, held := m.managedHold(sb.ID); !held {
				t.Fatal("validation changed owner")
			}
		})
	}
}

func TestGrowthMissingInventoryCannotCompleteBeforeOwnerValidation(t *testing.T) {
	m, e, sb, cfg, v := newGrowthTest(t)
	e.lag = true
	if _, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1"); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(m.store.path)
	if err != nil {
		t.Fatal(err)
	}
	observations := e.observations
	m.Close()
	e.lag = false
	m = openGrowthTest(t, cfg, e)
	m.eng = &growthInventoryEngine{growthTestEngine: e, omit: true, change: func(*types.VMRecord) {}}
	requireManagedRecoveryFence(t, m, sb.ID)
	_, getErr := m.GetVolume("data", "t1")
	requireVolumeStatus(t, getErr, 503)
	got := m.managed.data.Volumes["data"]
	if got.CapacityGeneration != 0 || got.Size != 1<<30 || !m.growthOwnsHold(sb.ID) {
		t.Fatalf("missing owner escaped fence: %+v", got)
	}
	after, readErr := os.ReadFile(m.store.path)
	if readErr != nil || string(after) != string(original) {
		t.Fatalf("original claim changed: %v", readErr)
	}
	if e.observations != observations || len(e.removes) != 0 {
		t.Fatal("unverified inventory observed guest or removed VM")
	}
}

func TestGrowthMissingInventoryPreservesUndispatchedIntent(t *testing.T) {
	m, e, sb, cfg, v := newGrowthTest(t)
	source := m.managed.data.Volumes["data"]
	if err := m.admitGrowth(t.Context(), sb, source, growRequest(), e); err != nil {
		t.Fatal(err)
	}
	m.Close()
	m = openGrowthTest(t, cfg, e)
	m.eng = &growthInventoryEngine{growthTestEngine: e, omit: true, change: func(*types.VMRecord) {}}
	requireManagedRecoveryFence(t, m, sb.ID)
	_, err := m.GetVolume("data", "t1")
	requireVolumeStatus(t, err, 503)
	pending := m.managed.data.Volumes["data"]
	if pending.Growth.State != "pending" || pending.Growth.Outcome != "unobserved" || e.calls != 0 {
		t.Fatalf("undispatched intent changed: %+v", pending)
	}
	m.Close()
	m = openGrowthTest(t, cfg, e)
	if err = m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	got, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	if err != nil || got.Growth.State != "completed" || e.calls != 1 {
		t.Fatalf("recovered inventory cannot resume original intent: %+v %v", got, err)
	}
}

func requireManagedRecoveryFence(t *testing.T, m *Manager, id string) {
	t.Helper()
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireVolumeStatus(t, m.ManagedVolumeStatus(), 503)
	sb, found := m.Sandbox(id)
	if !found || sb.Failed != managedRecoveryFailure {
		t.Fatalf("uncertain managed claim must remain visible and unavailable: %+v %t", sb, found)
	}
}
