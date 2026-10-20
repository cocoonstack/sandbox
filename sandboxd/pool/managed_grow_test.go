package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type growthTestEngine struct {
	*managedTestEngine
	growMu                                       sync.Mutex
	binding                                      engine.DiskBinding
	receipt                                      *engine.DiskReceipt
	calls                                        int
	observations                                 int
	lag, unknown, lostResponse, mutateAfterGuest bool
	guestChange                                  bool
	entered, resume                              chan struct{}
	vmName                                       string
	intentSeen                                   bool
}

func (e *growthTestEngine) DiskAttach(ctx context.Context, name string, spec engine.VolumeSpec) error {
	if err := e.managedTestEngine.DiskAttach(ctx, name, spec); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.disks == nil {
		e.disks = map[string][]types.VMDisk{}
	}
	e.vmName = name
	e.disks[name] = []types.VMDisk{{ID: "actual-ch-id", Name: spec.Name, Path: spec.Path, ReadOnly: !spec.RW}}
	return nil
}

func (e *growthTestEngine) resolveVM(selector string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if selector == "immutable-grow-vm" {
		return e.vmName
	}
	return selector
}

func (e *growthTestEngine) Remove(ctx context.Context, selector string) error {
	return e.managedTestEngine.Remove(ctx, e.resolveVM(selector))
}

func (e *growthTestEngine) Stop(ctx context.Context, selector string) error {
	return e.managedTestEngine.Stop(ctx, e.resolveVM(selector))
}

func (e *growthTestEngine) List(ctx context.Context) ([]types.VMRecord, error) {
	records, err := e.managedTestEngine.List(ctx)
	for i := range records {
		records[i].ID = "immutable-grow-vm"
	}
	return records, err
}

func (e *growthTestEngine) Inspect(ctx context.Context, name string) (types.VMRecord, bool, error) {
	rec, found, err := e.managedTestEngine.Inspect(ctx, e.resolveVM(name))
	rec.ID = "immutable-grow-vm"
	for i := range rec.AttachedDevices.Disks {
		rec.AttachedDevices.Disks[i].ID = "actual-ch-id"
	}
	return rec, found, err
}

func (e *growthTestEngine) InspectDisk(_ context.Context, ref engine.DiskRef) (engine.DiskInspection, error) {
	e.growMu.Lock()
	defer e.growMu.Unlock()
	b := e.binding
	if e.mutateAfterGuest && e.observations > 1 {
		b.VMM.Start++
	}
	return engine.DiskInspection{Requested: ref, Live: engine.DiskObservation{State: "observed", Binding: &b}, LatestReceipt: e.receipt, GuestBlockStatus: "unobserved"}, nil
}

func (e *growthTestEngine) GrowDisk(_ context.Context, req engine.DiskGrowRequest) (engine.DiskGrowResult, error) {
	e.growMu.Lock()
	e.calls++
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(filepath.Dir(req.Expected.Path)), "registry.json"))
	var saved volumeRegistryData
	if err == nil && json.Unmarshal(raw, &saved) == nil {
		v := saved.Volumes[req.Expected.Serial]
		e.intentSeen = saved.Version == 3 && v.Growth != nil && v.Growth.Runtime.Equal(req) && v.State == volumeGrowing
	}
	if !e.intentSeen {
		e.growMu.Unlock()
		return engine.DiskGrowResult{}, errors.New("effect before intent")
	}
	entered, resume := e.entered, e.resume
	e.growMu.Unlock()
	if entered != nil {
		close(entered)
		<-resume
	}
	e.growMu.Lock()
	defer e.growMu.Unlock()
	if err := os.Truncate(req.Expected.Path, req.TargetBytes); err != nil {
		return engine.DiskGrowResult{}, err
	}
	outcome := "acknowledged"
	if e.unknown {
		outcome = "outcome_unknown"
	}
	e.receipt = &engine.DiskReceipt{Request: req, Revision: req.Expected.Revision + 1, Outcome: outcome}
	e.binding.ObservedBytes, e.binding.Revision = req.TargetBytes, req.Expected.Revision+1
	b := e.binding
	result := engine.DiskGrowResult{Receipt: e.receipt, Live: engine.DiskObservation{State: "observed", Binding: &b}, GuestBlockStatus: "unobserved"}
	if e.lostResponse {
		return engine.DiskGrowResult{}, context.DeadlineExceeded
	}
	return result, nil
}

func (e *growthTestEngine) ObserveVolume(_ context.Context, _, serial, mount string) (engine.GuestVolumeObservation, error) {
	e.growMu.Lock()
	defer e.growMu.Unlock()
	e.observations++
	size := e.binding.ObservedBytes
	if e.lag && e.receipt != nil {
		size = e.receipt.Request.Expected.ObservedBytes
	}
	g := engine.GuestVolumeObservation{BootID: "guest-boot", Serial: serial, Device: "/dev/vdc", DevNum: "8:32", MountID: "27", Mount: mount, Root: "/", Mode: "rw", SizeBytes: size}
	if e.guestChange && e.observations > 1 {
		g.MountID = "28"
	}
	return g, nil
}

func openGrowthTest(t *testing.T, cfg *config.Config, e *growthTestEngine) *Manager {
	t.Helper()
	m, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	m.managed.process = e.identity
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func newGrowthTest(t *testing.T) (*Manager, *growthTestEngine, *types.Sandbox, *config.Config, types.ManagedVolumeInfo) {
	t.Helper()
	cfg := managedConfig(t)
	e := &growthTestEngine{managedTestEngine: newManagedEngine()}
	m := openGrowthTest(t, cfg, e)
	if err := m.PutTenant(t.Context(), tenants.TenantSpec{Name: "t1", Token: "token"}); err != nil {
		t.Fatal(err)
	}
	v := createManagedTest(t, m, "data")
	opts := managedOptions("data")
	opts.Volumes[0].AttachOnly = false
	sb, err := m.ClaimProvision(t.Context(), testKey, opts)
	if err != nil {
		t.Fatal(err)
	}
	h, _ := m.managedHold(sb.ID)
	ref := h.Refs[0]
	e.binding = engine.DiskBinding{Ref: engine.DiskRef{VMID: "immutable-grow-vm", DiskID: "actual-ch-id"}, VMM: engine.DiskProcess{BootID: h.Process.BootID, PID: h.Process.PID, Start: h.Process.Start}, Serial: "data", Path: ref.Path, Mode: "read_write", Format: "raw", Backing: engine.DiskBacking{Device: ref.Backing.Device, Inode: ref.Backing.Inode, FDs: []engine.DiskFD{{Number: 250, Access: "read_write"}, {Number: 261, Access: "read_write"}}}, ObservedBytes: ref.Backing.Size}
	return m, e, sb, cfg, v
}

func growRequest() types.VolumeGrow {
	return types.VolumeGrow{SizeGiB: 2, OperationID: "b51b3d2b-4ccc-41d5-b430-3046340c30b0"}
}

func TestManagedGrowthCommitReplayCASAndRestart(t *testing.T) {
	m, e, sb, cfg, v := newGrowthTest(t)
	req := growRequest()
	got, err := m.GrowVolume(t.Context(), "data", v.ID, req, "t1")
	if err != nil || got.SizeBytes != 2<<30 || got.CapacityGeneration != 1 || got.Growth.State != "completed" || got.Growth.GuestSizeBytes != 2<<30 || got.Growth.FilesystemStatus != "not_managed" || !e.intentSeen {
		t.Fatalf("grow %+v %v", got, err)
	}
	h, _ := m.managedHold(sb.ID)
	if h.Refs[0].Backing.Size != got.SizeBytes {
		t.Fatal("held backing not committed atomically")
	}
	if _, err = m.GrowVolume(t.Context(), "data", v.ID, req, "t1"); err != nil || e.calls != 1 {
		t.Fatalf("replay %v calls %d", err, e.calls)
	}
	changed := req
	changed.SizeGiB++
	_, err = m.GrowVolume(t.Context(), "data", v.ID, changed, "t1")
	requireVolumeStatus(t, err, 409)
	changed.OperationID = "c51b3d2b-4ccc-41d5-b430-3046340c30b0"
	_, err = m.GrowVolume(t.Context(), "data", v.ID, changed, "t1")
	requireVolumeStatus(t, err, 412)
	changed.ExpectedGeneration = 1
	if _, err = m.GrowVolume(t.Context(), "data", v.ID, changed, "t1"); err != nil {
		t.Fatal(err)
	}
	_, err = m.GrowVolume(t.Context(), "data", v.ID, req, "t1")
	requireVolumeStatus(t, err, 412)
	if validateErr := m.managed.validate(); validateErr != nil {
		t.Fatal(validateErr)
	}
	if flushErr := m.FlushClaims(); flushErr != nil {
		t.Fatal(flushErr)
	}
	m.Close()
	m = openGrowthTest(t, cfg, e)
	if reconcileErr := m.Reconcile(t.Context()); reconcileErr != nil {
		t.Fatal(reconcileErr)
	}
	got, err = m.GetVolume("data", "t1")
	if err != nil || got.CapacityGeneration != 2 || got.SizeBytes != 3<<30 {
		t.Fatalf("reopened %+v %v", got, err)
	}
}

func TestManagedGrowthACKGuestLagAndLostResponseRecoverWithoutDispatch(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "guest lag", true: "lost response"}[lost], func(t *testing.T) {
			m, e, sb, cfg, v := newGrowthTest(t)
			e.lag = true
			e.lostResponse = lost
			got, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
			if err != nil || got.Growth.State != "pending" || got.CapacityGeneration != 0 || got.SizeBytes != 1<<30 {
				t.Fatalf("pending %+v %v", got, err)
			}
			if !lost && got.Growth.RuntimeOutcome != "acknowledged" {
				t.Fatal("ACK not durable before guest")
			}
			if m.managedPaused(sb) {
				t.Fatal("growth paused data plane")
			}
			if flushErr := m.FlushClaims(); flushErr != nil {
				t.Fatal(flushErr)
			}
			m.Close()
			e.lag = false
			e.lostResponse = false
			m = openGrowthTest(t, cfg, e)
			if reconcileErr := m.Reconcile(t.Context()); reconcileErr != nil {
				t.Fatal(reconcileErr)
			}
			got, err = m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
			if err != nil || got.Growth.State != "completed" || got.CapacityGeneration != 1 || e.calls != 1 {
				t.Fatalf("recovery %+v %v calls=%d", got, err, e.calls)
			}
		})
	}
}

func TestManagedGrowthUnknownFencesLifecycleUntilExplicitExactStop(t *testing.T) {
	m, e, sb, _, v := newGrowthTest(t)
	e.unknown = true
	_, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	requireVolumeStatus(t, err, 409)
	for range 2 {
		_, err = m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
		requireVolumeStatus(t, err, 409)
	}
	if e.calls != 1 {
		t.Fatal("unknown was redispatched")
	}
	requireVolumeStatus(t, m.Release(t.Context(), sb.ID, Cred{Operator: true}), 409)
	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Second)
	m.mu.Unlock()
	m.reapOnce(t.Context())
	if _, ok := m.Sandbox(sb.ID); !ok {
		t.Fatal("ordinary reap dropped grow owner")
	}
	if m.removeVM(t.Context(), sb.VMName) || m.finishManaged(sb.ID, nil) {
		t.Fatal("destructive fallback escaped guard")
	}
	if _, _, runErr := m.beginVolumeRun(t.Context(), sb); runErr == nil {
		t.Fatal("live mutation admitted")
	}
	if sleepErr := m.Hibernate(t.Context(), sb.ID, Cred{Operator: true}); sleepErr == nil {
		t.Fatal("sleep admitted")
	}
	if deleteErr := m.DeleteVolume(t.Context(), "data", v.ID, "t1"); deleteErr == nil {
		t.Fatal("delete admitted")
	}
	e.mu.Lock()
	removedBefore := len(e.removes)
	e.mu.Unlock()
	m.stopVM(t.Context(), sb.VMName)
	m.recoverVMM(t.Context(), sb)
	e.mu.Lock()
	if e.stopped[sb.VMName] || len(e.removes) != removedBefore {
		t.Fatal("growth automatically stopped/removed VM")
	}
	e.mu.Unlock()
	e.growMu.Lock()
	e.receipt.Outcome = "old_vmm_gone"
	e.growMu.Unlock()
	_, err = m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	requireVolumeStatus(t, err, 409)
	if !m.growthOwnsHold(sb.ID) {
		t.Fatal("receipt alone proved process death")
	}
	e.processUnknown.Store(true)
	_, err = m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	requireVolumeStatus(t, err, 409)
	if !m.growthOwnsHold(sb.ID) {
		t.Fatal("process read error proved death")
	}
	e.processUnknown.Store(false)
	if err = e.Stop(t.Context(), sb.VMName); err != nil {
		t.Fatal(err)
	}
	m.managed.process = func(int) (processIdentity, error) { return processIdentity{}, os.ErrNotExist }
	got, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	requireVolumeStatus(t, err, 409)
	if got.Growth.RuntimeOutcome != "old_vmm_gone" || got.SizeBytes != 1<<30 || got.CapacityGeneration != 0 || m.growthOwnsHold(sb.ID) {
		t.Fatalf("quarantine %+v", got)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteVolume(t.Context(), "data", v.ID, "t1"); err != nil {
		t.Fatal("known target quarantine delete", err)
	}
}

func TestManagedGrowthAdmissionSerializesOrdinaryReleaseAndReap(t *testing.T) {
	m, e, sb, _, v := newGrowthTest(t)
	e.entered = make(chan struct{})
	e.resume = make(chan struct{})
	done := make(chan error, 1)
	go func() { _, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1"); done <- err }()
	select {
	case <-e.entered:
	case err := <-done:
		t.Fatalf("grow did not dispatch: %v", err)
	}
	if m.managedLifecycle(sb) {
		t.Fatal("test must cover release without Transition")
	}
	requireVolumeStatus(t, m.Release(t.Context(), sb.ID, Cred{Operator: true}), 409)
	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Second)
	m.mu.Unlock()
	m.reapOnce(t.Context())
	if _, ok := m.Sandbox(sb.ID); !ok {
		t.Fatal("reap dropped grow owner")
	}
	close(e.resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestManagedGrowthRejectsChangedGuestAndClosingRuntime(t *testing.T) {
	for _, runtimeChange := range []bool{false, true} {
		t.Run(map[bool]string{false: "mount replaced", true: "VMM replaced after guest"}[runtimeChange], func(t *testing.T) {
			m, e, _, _, v := newGrowthTest(t)
			e.guestChange = !runtimeChange
			e.mutateAfterGuest = runtimeChange
			_, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
			requireVolumeStatus(t, err, 409)
			got, err := m.GetVolume("data", "t1")
			if err != nil || got.CapacityGeneration != 0 || got.SizeBytes != 1<<30 || got.Growth.State != "recovery_required" {
				t.Fatalf("false commit %+v %v", got, err)
			}
		})
	}
}

func TestManagedGrowthSnapshotFailuresCannotDispatchOrRelease(t *testing.T) {
	for _, failAt := range []int{1, 2, 3, 4} {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			m, e, sb, _, v := newGrowthTest(t)
			calls := 0
			write := m.managed.commitFile
			m.managed.commitFile = func(p string, b []byte) error {
				calls++
				if calls == failAt {
					return errors.New("injected snapshot failure")
				}
				return write(p, b)
			}
			_, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
			if err == nil {
				t.Fatal("storage failure hidden")
			}
			if failAt <= 2 && e.calls != 0 {
				t.Fatal("runtime dispatch preceded durable intent")
			}
			if failAt >= 2 && (m.removeVM(t.Context(), sb.VMName) || m.finishManaged(sb.ID, nil)) {
				t.Fatal("failed snapshot permitted teardown")
			}
		})
	}
}

func TestManagedGrowthMissingClaimAndPriorOrphanPreserveOwner(t *testing.T) {
	m, e, sb, _, v := newGrowthTest(t)
	e.unknown = true
	_, _ = m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	h, _ := m.managedHold(sb.ID)
	m.managed.mu.Lock()
	m.managed.data.Orphans = map[string]processIdentity{sb.VMName: h.Process}
	m.managed.mu.Unlock()
	rec, _, _ := e.Inspect(t.Context(), sb.VMName)
	live := map[string]types.VMRecord{sb.VMName: rec}
	if err := m.recoverGrowth(t.Context(), map[string]*types.Sandbox{}, live); err == nil {
		t.Fatal("missing growth owner must fail closed")
	}
	if _, err := m.recoverManaged(t.Context(), map[string]*types.Sandbox{}, live); err == nil {
		t.Fatal("missing claim must fail closed")
	}
	if err := m.recoverManagedOrphans(t.Context(), map[string]*types.Sandbox{}, live, map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	if removed := m.sweepStaleVMs(t.Context(), live, map[string]bool{}); removed[sb.VMName] {
		t.Fatal("swept grow owner")
	}
	if e.calls != 1 {
		t.Fatal("startup redispatched unknown")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.removes) > 0 {
		t.Fatal("prior orphan removed grow owner")
	}
}

func TestManagedGrowthConcurrentReplayAndReadOnlyGET(t *testing.T) {
	m, e, _, _, v := newGrowthTest(t)
	e.lag = true
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() { _, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1"); errs <- err })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if e.calls != 1 {
		t.Fatalf("concurrent replay dispatched %d times", e.calls)
	}
	observations := e.observations
	got, err := m.GetVolume("data", "t1")
	if err != nil || got.Growth.State != "pending" || e.observations != observations {
		t.Fatalf("GET performed recovery: %+v %v", got, err)
	}
	if _, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "foreign"); err == nil {
		t.Fatal("foreign owner admitted")
	}
	path := m.managed.path(volumeID(v.ID))
	if err := os.Rename(path, path+".replaced"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := m.GetVolume("data", "t1"); err == nil {
		t.Fatal("GET accepted replaced growing backing")
	}
}

func TestManagedGrowthHostOrGuestTargetCannotReplaceACK(t *testing.T) {
	m, e, _, _, v := newGrowthTest(t)
	e.lostResponse = true
	if _, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1"); err != nil {
		t.Fatal(err)
	}
	e.receipt = nil
	_, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
	requireVolumeStatus(t, err, 409)
	got, err := m.GetVolume("data", "t1")
	if err != nil || got.CapacityGeneration != 0 || got.Growth.State != "recovery_required" || e.calls != 1 {
		t.Fatalf("target inferred ACK: %+v %v calls %d", got, err, e.calls)
	}
}

func TestManagedGrowthRegistryRejectsInconsistentIntent(t *testing.T) {
	for _, mutation := range []string{"version", "generation", "request", "owner", "sleep", "state", "backing", "guest", "receipt revision"} {
		t.Run(mutation, func(t *testing.T) {
			m, e, sb, _, v := newGrowthTest(t)
			e.lag = true
			if _, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1"); err != nil {
				t.Fatal(err)
			}
			r := m.managed
			current := r.data.Volumes["data"]
			switch mutation {
			case "version":
				r.data.Version = 2
			case "generation":
				current.CapacityGeneration++
			case "request":
				current.Growth.Runtime.ID = "wrong"
			case "owner":
				delete(r.data.Holds, sb.ID)
			case "sleep":
				h := r.data.Holds[sb.ID]
				h.Sleep = &managedSleep{}
				r.data.Holds[sb.ID] = h
			case "state":
				current.State = volumeReady
			case "backing":
				current.Backing.Size++
			case "guest":
				current.Growth.Guest.MountID = ""
			case "receipt revision":
				current.Growth.Runtime.Expected.Revision = ^uint64(0)
			}
			r.data.Volumes["data"] = current
			if err := r.validate(); err == nil {
				t.Fatal("invalid registry accepted")
			}
		})
	}
}

func TestManagedGrowthRefusesUnmountedOrUnappliedOwner(t *testing.T) {
	for _, mutation := range []string{"unmounted", "unapplied", "read only", "retiring", "live mutation"} {
		t.Run(mutation, func(t *testing.T) {
			m, e, sb, _, v := newGrowthTest(t)
			switch mutation {
			case "unmounted":
				h := m.managed.data.Holds[sb.ID]
				h.Refs[0].Volume.Mount = ""
				m.managed.data.Holds[sb.ID] = h
			case "unapplied":
				sb.VolumesWant = nil
			case "read only":
				h := m.managed.data.Holds[sb.ID]
				h.Refs[0].Volume.Mode = ""
				m.managed.data.Holds[sb.ID] = h
			case "retiring":
				if err := m.retireManaged(sb.ID); err != nil {
					t.Fatal(err)
				}
			case "live mutation":
				m.volumeRuns[sb] = &volumeRun{}
			}
			_, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
			requireVolumeStatus(t, err, 409)
			if e.calls != 0 {
				t.Fatal("ineligible holder dispatched growth")
			}
		})
	}
}

func TestManagedGrowthMigratesVersionTwoBeforeIntentAndKeepsSleepValid(t *testing.T) {
	m, e, sb, _, v := newGrowthTest(t)
	r := m.managed
	r.data.Version = 2
	r.mu.Lock()
	if err := r.commitAndRelock(); err != nil {
		t.Fatal(err)
	}
	r.mu.Unlock()
	write := r.commitFile
	var snapshots []volumeRegistryData
	r.commitFile = func(path string, raw []byte) error {
		var snapshot volumeRegistryData
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			return err
		}
		snapshots = append(snapshots, snapshot)
		return write(path, raw)
	}
	if _, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1"); err != nil {
		t.Fatal(err)
	}
	if !e.intentSeen || snapshots[len(snapshots)-1].Volumes["data"].Growth.SandboxID != sb.ID {
		t.Fatal("migration lost growth owner")
	}
	if len(snapshots) < 2 || snapshots[0].Version != 3 || snapshots[0].Volumes["data"].Growth != nil || snapshots[1].Volumes["data"].Growth == nil {
		t.Fatal("migration was not durable before intent")
	}
	sleeping, _, sb, _ := newSleepTest(t)
	sleeping.managed.data.Version = 3
	sleeping.managed.mu.Lock()
	if err := sleeping.managed.commitAndRelock(); err != nil {
		t.Fatal(err)
	}
	sleeping.managed.mu.Unlock()
	requireSleep(t, sleeping, sb)
	if err := sleeping.managed.validate(); err != nil {
		t.Fatal("version three rejected existing sleep", err)
	}
}

func TestManagedGrowthRetryRequiresDurableIntent(t *testing.T) {
	for _, phase := range []string{"intent", "completion"} {
		t.Run(phase, func(t *testing.T) {
			m, e, _, _, v := newGrowthTest(t)
			r := m.managed
			r.commitFile = func(path string, raw []byte) error {
				var snapshot volumeRegistryData
				if err := json.Unmarshal(raw, &snapshot); err != nil {
					return err
				}
				growth := snapshot.Volumes["data"].Growth
				if growth != nil && (phase == "intent" || growth.State == growthCompleted) {
					return syscall.ENOSPC
				}
				return writeVolumeSnapshot(path, raw)
			}
			_, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
			if err == nil {
				t.Fatal("injected persistence failure did not fail")
			}
			wantCalls := 0
			if phase == "completion" {
				wantCalls = 1
			}
			if e.calls != wantCalls {
				t.Fatalf("runtime calls=%d, want %d", e.calls, wantCalls)
			}
			_, err = m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
			requireVolumeStatus(t, err, 503)
			if e.calls != wantCalls {
				t.Fatal("retry dispatched undurable operation")
			}
			r.commitFile = writeVolumeSnapshot
			m.retryManagedSnapshot()
			waitFor(t, func() bool { return !r.retrying.Load() })
			result, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1")
			if err != nil {
				t.Fatal(err)
			}
			if e.calls != 1 || !e.intentSeen || result.SizeBytes != 2<<30 || result.CapacityGeneration != 1 {
				t.Fatalf("recovered operation did not converge: calls=%d intent=%v result=%+v", e.calls, e.intentSeen, result)
			}
		})
	}
}
