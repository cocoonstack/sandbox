package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type sleepTestEngine struct {
	*managedTestEngine
	sleepMu                   sync.Mutex
	generation                uint64
	reason                    string
	vmID                      string
	sourceID                  string
	hooks                     []engine.VolumeHookCall
	hookErr                   string
	hookEntered, hookContinue chan struct{}
	restoreAfterErr           bool
	replaceSourceOnRestore    bool
	snapshotRemoveErr         error
	snapshotRemoveAfterEffect bool
	snapshotRemoveMutation    string
	mounted                   bool
}

func newSleepTest(t *testing.T) (*Manager, *sleepTestEngine, *types.Sandbox, *config.Config) {
	t.Helper()
	cfg := managedConfig(t)
	cfg.ManagedVolumeHibernate = &config.ManagedVolumeHibernate{HookArgv: []string{"/usr/local/bin/volume-hook"}, HookTimeoutSeconds: 1}
	e := &sleepTestEngine{managedTestEngine: newManagedEngine(), generation: 2, reason: "boot", vmID: "immutable-vm", mounted: true}
	m, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	if err = m.PutTenant(t.Context(), tenants.TenantSpec{Name: "t1", Token: "t1-token"}); err != nil {
		t.Fatal(err)
	}
	createManagedTest(t, m, "data")
	opts := managedOptions("data")
	opts.Volumes[0].AttachOnly = false
	sb, err := m.ClaimProvision(t.Context(), testKey, opts)
	if err != nil {
		t.Fatal(err)
	}
	return m, e, sb, cfg
}

func (e *sleepTestEngine) Inspect(ctx context.Context, name string) (types.VMRecord, bool, error) {
	rec, found, err := e.managedTestEngine.Inspect(ctx, name)
	e.sleepMu.Lock()
	defer e.sleepMu.Unlock()
	rec.ID, rec.TransitionGeneration, rec.LastTransitionReason = e.vmID, e.generation, e.reason
	return rec, found, err
}

func (e *sleepTestEngine) DiskAttach(ctx context.Context, name string, spec engine.VolumeSpec) error {
	if err := e.managedTestEngine.DiskAttach(ctx, name, spec); err != nil {
		return err
	}
	e.mu.Lock()
	if e.disks == nil {
		e.disks = map[string][]types.VMDisk{}
	}
	e.disks[name] = []types.VMDisk{{Name: spec.Name, Path: spec.Path, ReadOnly: !spec.RW}}
	e.mu.Unlock()
	return nil
}

func (e *sleepTestEngine) DetachVolume(ctx context.Context, name, sock, serial, mount string) error {
	if err := e.fakeEngine.DetachVolume(ctx, name, sock, serial, mount); err != nil {
		return err
	}
	e.mu.Lock()
	e.disks[name] = nil
	e.mu.Unlock()
	e.sleepMu.Lock()
	e.mounted = false
	e.sleepMu.Unlock()
	return nil
}

func (e *sleepTestEngine) MountLiveVolume(ctx context.Context, sock, name, mount string, rw bool) error {
	if err := e.fakeEngine.MountLiveVolume(ctx, sock, name, mount, rw); err != nil {
		return err
	}
	e.sleepMu.Lock()
	e.mounted = true
	e.sleepMu.Unlock()
	return nil
}

func (e *sleepTestEngine) VerifyMountedVolume(context.Context, string, string, string) error {
	e.sleepMu.Lock()
	defer e.sleepMu.Unlock()
	if !e.mounted {
		return errors.New("mount absent")
	}
	return nil
}

func (e *sleepTestEngine) Hibernate(ctx context.Context, name, snap string) error {
	err := e.fakeEngine.Hibernate(ctx, name, snap)
	e.mu.Lock()
	stopped := e.stopped[name]
	e.mu.Unlock()
	if stopped {
		e.sleepMu.Lock()
		e.generation++
		e.reason = "stop-user"
		e.sleepMu.Unlock()
	}
	return err
}

func (e *sleepTestEngine) Restore(ctx context.Context, name, snap string) (string, error) {
	sock, err := e.fakeEngine.Restore(ctx, name, snap)
	if err != nil {
		return sock, err
	}
	e.mu.Lock()
	e.pids[name]++
	e.mu.Unlock()
	e.sleepMu.Lock()
	e.generation++
	e.reason = "restore"
	if e.replaceSourceOnRestore {
		e.sourceID = "replacement-source"
	}
	postErr := e.restoreAfterErr
	e.sleepMu.Unlock()
	if postErr {
		return "", errors.New("restore reply lost")
	}
	return sock, nil
}

func (e *sleepTestEngine) InspectHibernateSource(ctx context.Context, name string) (engine.HibernateSource, bool, error) {
	source, found, err := e.fakeEngine.InspectHibernateSource(ctx, name)
	e.sleepMu.Lock()
	defer e.sleepMu.Unlock()
	if found && e.sourceID != "" {
		source.ID = e.sourceID
	}
	return source, found, err
}

func (e *sleepTestEngine) SnapshotRemove(ctx context.Context, name string) error {
	e.sleepMu.Lock()
	err := e.snapshotRemoveErr
	afterEffect := e.snapshotRemoveAfterEffect
	mutation := e.snapshotRemoveMutation
	e.sleepMu.Unlock()
	if err != nil && !afterEffect {
		return err
	}
	if removeErr := e.fakeEngine.SnapshotRemove(ctx, name); removeErr != nil {
		return removeErr
	}
	switch mutation {
	case "vm id":
		e.sleepMu.Lock()
		e.vmID = "replacement-vm"
		e.sleepMu.Unlock()
	case "transition":
		e.sleepMu.Lock()
		e.generation++
		e.reason = "boot"
		e.sleepMu.Unlock()
	}
	return err
}

func (e *sleepTestEngine) RunVolumeHook(_ context.Context, _ string, call engine.VolumeHookCall) error {
	e.sleepMu.Lock()
	e.hooks = append(e.hooks, call)
	fail := e.hookErr == call.Action
	entered, resume := e.hookEntered, e.hookContinue
	e.sleepMu.Unlock()
	if entered != nil {
		entered <- struct{}{}
		<-resume
	}
	if fail {
		return &engine.VolumeHookExit{Code: 1, Output: "hook failed"}
	}
	return nil
}

func requireSleep(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if info, ok := m.Sandbox(sb.ID); !ok || !info.Hibernated {
		t.Fatalf("not hibernated: %+v", info)
	}
	if _, err := m.AgentSocket(sb.ID, sb.Token); !errors.Is(err, ErrPaused) {
		t.Fatalf("sleeping claim served its passive socket: %v", err)
	}
}

// requireRunning checks the passive socket, which refuses while any sleep step is journaled.
func requireRunning(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	if _, err := m.AgentSocket(sb.ID, sb.Token); err != nil {
		t.Fatalf("claim is not running: %v", err)
	}
}

func managedBackingPath(t *testing.T, m *Manager, name string) string {
	t.Helper()
	info, err := m.GetVolume(name, "")
	if err != nil {
		t.Fatal(err)
	}
	return m.managed.path(volumeID(info.ID))
}

// journalSleepPhase rewrites the claim row as a crash at phase would have left it.
func journalSleepPhase(t *testing.T, m *Manager, sb *types.Sandbox, edit func(*types.VolumeSleep)) {
	t.Helper()
	m.mu.Lock()
	next := *sb.VolumeSleep
	edit(&next)
	sb.VolumeSleep = &next
	projectSleepState(sb, &next)
	js := m.store.set(sb)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		t.Fatal(err)
	}
}

func TestManagedSleepRoundTripAndExclusiveHold(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	_, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	requireVolumeStatus(t, err, 409)
	if err = m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	requireRunning(t, m, sb)
	requireExec(t, m, sb)
	if len(e.restores) != 1 || len(e.hooks) != 2 || e.hooks[0].Operation != e.hooks[1].Operation {
		t.Fatalf("effects restores=%v hooks=%v", e.restores, e.hooks)
	}
}

func TestManagedSleepSnapshotNameFitsEngineContract(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if len(e.hibernates) != 1 {
		t.Fatalf("hibernate calls = %v, want one", e.hibernates)
	}
	snapshot := e.hibernates[0]
	if len(snapshot) > 63 {
		t.Fatalf("snapshot name %q has length %d, exceeds Cocoon's 63-character limit", snapshot, len(snapshot))
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,62}$`).MatchString(snapshot) {
		t.Fatalf("snapshot name %q violates Cocoon's name syntax", snapshot)
	}
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
}

func TestManagedSleepAbortKeepsOriginalVM(t *testing.T) {
	for _, failure := range []string{"prepare", "unmount", "detach", "capture"} {
		t.Run(failure, func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			switch failure {
			case "prepare":
				e.hookErr = "prepare"
			case "unmount":
				e.unmountVolumeErr = errors.New("busy")
			case "detach":
				e.detachVolumeErr = errors.New("detach failed")
			case "capture":
				e.hibernateErr = errors.New("capture refused")
			}
			err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token})
			if err == nil {
				t.Fatal("failure hidden")
			}
			if v, ok := errors.AsType[*VolumeError](err); ok && v.Code != "volume_hook_failed" {
				t.Fatalf("a %s failure with a clean resume reported %s, want the step's own error", failure, v.Code)
			}
			requireRunning(t, m, sb)
			if e.stopped[sb.VMName] || len(e.restores) != 0 {
				t.Fatal("failed preparation damaged VM")
			}
			if failure != "capture" && len(e.hibernates) != 0 {
				t.Fatal("captured after failed preparation")
			}
			if e.hooks[len(e.hooks)-1].Action != "resume" {
				t.Fatal("writers not resumed")
			}
			if len(e.snapRemoves) != 0 {
				t.Fatalf("abort removed nonexistent source: %v", e.snapRemoves)
			}
		})
	}
}

func TestManagedSleepMountConflictReportsOnlyTheConflict(t *testing.T) {
	for _, resumeFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "resumed", true: "resume failed"}[resumeFails], func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			e.detachVolumeErr = &engine.VolumeMountConflict{Message: `cannot unmount "/volumes/data"; close guest references and retry`}
			if resumeFails {
				e.hookErr = "resume"
			}
			err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token})
			if _, conflict := errors.AsType[*engine.VolumeMountConflict](err); !conflict {
				t.Fatalf("hibernate error %v lost the guest mount conflict", err)
			}
			if v, ok := errors.AsType[*VolumeError](err); ok {
				t.Fatalf("abort over a verified mount reported %s, want only the guest mount conflict", v.Code)
			}
			requireExec(t, m, sb)
		})
	}
}

func TestManagedSleepWakeFailuresKeepRestoredProcess(t *testing.T) {
	for _, failure := range []string{"restore reply", "attach", "mount"} {
		t.Run(failure, func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			requireSleep(t, m, sb)
			switch failure {
			case "restore reply":
				e.restoreAfterErr = true
			case "attach":
				e.diskAttachErr = errors.New("attach unavailable")
			case "mount":
				e.mountVolumeErr = errors.New("mount failed")
			}
			err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token})
			if failure != "restore reply" && err == nil {
				t.Fatal("wake failure hidden")
			}
			if e.stopped[sb.VMName] || len(e.restores) != 1 {
				t.Fatal("restored VM was discarded")
			}
			e.diskAttachErr, e.mountVolumeErr = nil, nil
			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Fatal(err)
			}
			if len(e.restores) != 1 {
				t.Fatal("restore replayed over live VM")
			}
		})
	}
}

func TestManagedSleepWakeVerifiesBackingFirst(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "replaced"}[replace], func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			requireSleep(t, m, sb)
			path := managedBackingPath(t, m, "data")
			saved := filepath.Join(filepath.Dir(filepath.Dir(path)), "saved.raw")
			if err := os.Rename(path, saved); err != nil {
				t.Fatal(err)
			}
			if replace {
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil || len(e.restores) != 0 {
				t.Fatal("restore ran without exact backing")
			}
			info, err := m.GetVolume("data", "t1")
			if err != nil || info.State != string(volumeRecovery) || info.Available {
				t.Fatalf("missing backing detail: %+v %v", info, err)
			}
			infos := m.Volumes("t1", nil)
			if len(infos) != 1 || infos[0].Available {
				t.Fatalf("missing backing list: %+v", infos)
			}
			_, _, err = m.CreateVolume(t.Context(), "data", types.VolumeCreate{SizeGiB: 1}, "t1")
			if v := requireVolumeStatus(t, err, 503); v.Code != "volume_storage_unavailable" {
				t.Fatalf("create replay over a sleeping volume's lost backing = %s, want volume_storage_unavailable while wake owns it", v.Code)
			}
			_, err = m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
			if v := requireVolumeStatus(t, err, 503); v.Code != "volume_storage_unavailable" {
				t.Fatalf("claim of a sleeping volume's lost backing = %s, want volume_storage_unavailable while wake owns it", v.Code)
			}
			if replace {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Rename(saved, path); err != nil {
				t.Fatal(err)
			}
			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedSleepProvenanceRejectsReplacement(t *testing.T) {
	for _, mismatch := range []string{"id", "transition", "disk path", "disk name"} {
		t.Run(mismatch, func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			requireSleep(t, m, sb)
			e.mountVolumeErr = errors.New("mount failed")
			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
				t.Fatal("expected suspended wake")
			}
			switch mismatch {
			case "id":
				e.vmID = "replacement"
			case "transition":
				e.generation++
				e.reason = "boot"
			case "disk path":
				e.disks[sb.VMName][0].Path = "/wrong"
			case "disk name":
				e.disks[sb.VMName][0].Name = "other"
			}
			e.mountVolumeErr = nil
			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
				t.Fatal("adopted replacement")
			}
			if len(e.restores) != 1 {
				t.Fatal("replayed restore")
			}
		})
	}
}

func TestManagedSleepReleasePreservesBacking(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	path := managedBackingPath(t, m, "data")
	before := slices.Clone(e.volumeOps)
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if len(e.snapshots) != 0 {
		t.Fatal("sleep snapshot leaked")
	}
	if !slices.Equal(before, e.volumeOps) {
		t.Fatal("asleep release touched guest")
	}
	requireManagedClaimable(t, m, "data")
}

func TestManagedSleepRestartEveryPhase(t *testing.T) {
	for _, phase := range []types.SleepPhase{types.SleepPreparing, types.SleepAborting, types.SleepCapturing, types.SleepSleeping, types.SleepWaking, types.SleepResuming} {
		t.Run(string(phase), func(t *testing.T) {
			m, e, sb, cfg := newSleepTest(t)
			original, err := m.GetVolume("data", "t1")
			if err != nil {
				t.Fatal(err)
			}
			requireSleep(t, m, sb)
			path := managedBackingPath(t, m, "data")
			if phase == types.SleepPreparing || phase == types.SleepAborting || phase == types.SleepCapturing {
				e.mu.Lock()
				e.stopped[sb.VMName] = false
				if phase != types.SleepCapturing {
					e.disks[sb.VMName] = []types.VMDisk{{Name: "data", Path: path}}
				}
				e.snapshots = nil
				e.mu.Unlock()
				e.sleepMu.Lock()
				e.generation = 2
				e.reason = "boot"
				e.mounted = phase != types.SleepCapturing
				e.sleepMu.Unlock()
			}
			if phase == types.SleepResuming {
				if _, err = e.Restore(t.Context(), sb.VMName, sb.VolumeSleep.Snapshot); err != nil {
					t.Fatal(err)
				}
			}
			journalSleepPhase(t, m, sb, func(s *types.VolumeSleep) {
				s.Phase = phase
				if phase == types.SleepPreparing || phase == types.SleepAborting || phase == types.SleepCapturing {
					s.SourceID = ""
				}
			})
			if err = m.Close(); err != nil {
				t.Fatal(err)
			}
			cfg.ManagedVolumeHibernate = nil
			recovered := reopenSleepTest(t, cfg, e)
			if _, err = recovered.AgentSocket(sb.ID, sb.Token); !errors.Is(err, ErrPaused) {
				t.Fatalf("restart bypassed journal: %v", err)
			}
			_, err = recovered.ClaimProvision(t.Context(), testKey, managedOptions("data"))
			requireVolumeStatus(t, err, 409)
			if err = recovered.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Fatal(err)
			}
			requireRunning(t, recovered, sb)
			if got, err := recovered.GetVolume("data", "t1"); err != nil || got.ID != original.ID {
				t.Fatalf("recovery changed backing: %+v %v", got, err)
			}
			if !slices.Equal(e.hooks[len(e.hooks)-1].Argv, []string{"/usr/local/bin/volume-hook"}) {
				t.Fatal("recovery lost pinned hook")
			}
		})
	}
}

func TestManagedSleepPostCaptureErrorAdoptsAndPersists(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	e.hibernateErr = errors.New("reply lost")
	e.hibernateErrCompletes = true
	requireSleep(t, m, sb)
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
}

// claimsFaultEngine makes claims.json unwritable when the named engine call starts.
type claimsFaultEngine struct {
	*sleepTestEngine
	on    string
	fault func()
}

func (e *claimsFaultEngine) trip(call string) {
	if e.on == call {
		e.on = ""
		e.fault()
	}
}

func (e *claimsFaultEngine) VerifyMountedVolume(ctx context.Context, sock, name, mount string) error {
	e.trip("verify")
	return e.sleepTestEngine.VerifyMountedVolume(ctx, sock, name, mount)
}

func (e *claimsFaultEngine) DetachVolume(ctx context.Context, name, sock, serial, mount string) error {
	e.trip("detach")
	return e.sleepTestEngine.DetachVolume(ctx, name, sock, serial, mount)
}

func (e *claimsFaultEngine) Hibernate(ctx context.Context, name, snap string) error {
	e.trip("capture")
	return e.sleepTestEngine.Hibernate(ctx, name, snap)
}

func (e *claimsFaultEngine) MountLiveVolume(ctx context.Context, sock, name, mount string, rw bool) error {
	e.trip("mount")
	return e.sleepTestEngine.MountLiveVolume(ctx, sock, name, mount, rw)
}

func TestManagedSleepJournalFailureStopsEffects(t *testing.T) {
	for _, tc := range []struct {
		phase types.SleepPhase
		on    string
	}{{types.SleepPreparing, "verify"}, {types.SleepCapturing, "detach"}, {types.SleepSleeping, "capture"}, {types.SleepWaking, "now"}, {types.SleepResuming, "mount"}} {
		t.Run(string(tc.phase), func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			wake := tc.phase == types.SleepWaking || tc.phase == types.SleepResuming
			if wake {
				requireSleep(t, m, sb)
			}
			tmp := m.store.path + ".tmp"
			fault := func() { _ = os.Mkdir(tmp, 0o700) }
			t.Cleanup(func() {
				_ = os.Remove(tmp)
				waitFor(t, func() bool { return !m.store.recommitting.Load() })
			})
			if tc.on == "now" {
				fault()
			}
			m.eng = &claimsFaultEngine{sleepTestEngine: e, on: tc.on, fault: fault}
			var err error
			if wake {
				err = m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token})
			} else {
				err = m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token})
			}
			if err == nil {
				t.Fatal("persistence failure hidden")
			}
			beforeHooks, beforeRestores, beforeCaptures := len(e.hooks), len(e.restores), len(e.hibernates)
			if tc.phase != types.SleepPreparing {
				if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
					t.Fatal("unpersisted intent served guest")
				}
			}
			if len(e.hooks) != beforeHooks || len(e.restores) != beforeRestores || len(e.hibernates) != beforeCaptures {
				t.Fatal("effect after an unpersisted intent")
			}
			switch tc.phase {
			case types.SleepPreparing:
				if len(e.hooks) != 0 {
					t.Fatal("hook preceded durable intent")
				}
				requireRunning(t, m, sb)
			case types.SleepCapturing:
				if len(e.hibernates) != 0 {
					t.Fatal("capture preceded durable intent")
				}
			case types.SleepWaking:
				if len(e.restores) != 0 {
					t.Fatal("restore preceded durable intent")
				}
			case types.SleepResuming:
				if e.hooks[len(e.hooks)-1].Action == "resume" {
					t.Fatal("resume preceded durable intent")
				}
			}
		})
	}
}

func TestManagedSleepUnknownSourceAndDeadRestoredRefuse(t *testing.T) {
	for _, failure := range []string{"missing source", "source identity", "dead restored", "unknown phase"} {
		t.Run(failure, func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			requireSleep(t, m, sb)
			switch failure {
			case "missing source":
				e.snapshots = nil
			case "source identity":
				journalSleepPhase(t, m, sb, func(s *types.VolumeSleep) { s.SourceID = "other" })
			case "dead restored":
				e.mountVolumeErr = errors.New("mount failed")
				if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
					t.Fatal("expected failed wake")
				}
				e.mountVolumeErr = nil
				e.stopped[sb.VMName] = true
			case "unknown phase":
				journalSleepPhase(t, m, sb, func(s *types.VolumeSleep) { s.Phase = "unknown" })
			}
			restores := len(e.restores)
			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
				t.Fatal("uncertain source served guest")
			}
			if len(e.restores) != restores {
				t.Fatal("unsafe memory replay")
			}
		})
	}
}

func TestManagedSleepDeadBoundVMIsReleaseOnly(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	path := managedBackingPath(t, m, "data")
	e.mountVolumeErr = errors.New("mount failed")
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("mount failure hidden")
	}
	e.mountVolumeErr = nil
	if len(e.snapshots) != 0 {
		t.Fatalf("wake did not consume its source before attaching: snapshots=%v", e.snapshots)
	}
	e.mu.Lock()
	e.stopped[sb.VMName] = true
	e.mu.Unlock()
	restores := len(e.restores)
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("dead bound VM resumed")
	}
	if len(e.restores) != restores {
		t.Fatal("dead bound VM replayed restore")
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	requireManagedClaimable(t, m, "data")
}

func TestManagedSleepExpiryIgnoresArchivePool(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Minute)
	m.pools[sb.Key].archiveAfter = time.Second
	m.mu.Unlock()
	m.reapOnce(t.Context())
	waitFor(t, func() bool { _, live := m.Sandbox(sb.ID); return !live && !e.vmAlive(sb.VMName) })
	waitFor(t, func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		return len(e.snapshots) == 0
	})
	requireManagedClaimable(t, m, "data")
}

func TestManagedSleepUnsupportedPreflightHasNoEffects(t *testing.T) {
	for _, bad := range []string{"disabled", "desired mismatch", "extra disk", "missing mount", "egress"} {
		t.Run(bad, func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			switch bad {
			case "disabled":
				m.managedSleepHook = nil
			case "desired mismatch":
				sb.VolumesWant = nil
			case "extra disk":
				e.disks[sb.VMName] = append(e.disks[sb.VMName], types.VMDisk{Name: "extra", Path: "/extra"})
			case "missing mount":
				e.mounted = false
			case "egress":
				sb.Key.Net = types.NetEgress
			}
			if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
				t.Fatal("unsupported sleep accepted")
			}
			if len(e.hooks) != 0 || len(e.hibernates) != 0 || slices.Contains(e.volumeOps, "detach:data") {
				t.Fatal("unsupported preflight changed guest")
			}
		})
	}
}

func TestManagedSleepRestoreBeforeEffectRetriesSameSource(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	e.restoreErr = errors.New("restore preflight refused")
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("restore failure hidden")
	}
	if !e.stopped[sb.VMName] {
		t.Fatal("failed restore changed VM")
	}
	e.restoreErr = nil
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if len(e.restores) != 2 {
		t.Fatal("safe stopped restore was not retried")
	}
}

func TestManagedSleepWakeWaitsForSourceConsumption(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"active restore lease", errors.New("snapshot is in use")},
		{"remove failure", errors.New("snapshot delete failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			requireSleep(t, m, sb)
			snapshot := e.hibernates[0]
			e.mu.Lock()
			beforeOps := slices.Clone(e.volumeOps)
			e.mu.Unlock()
			e.sleepMu.Lock()
			beforeHooks := len(e.hooks)
			e.snapshotRemoveErr = tc.err
			e.sleepMu.Unlock()

			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
				t.Fatal("wake passed unavailable source-consumption barrier")
			}
			if _, err := m.AgentSocket(sb.ID, sb.Token); !errors.Is(err, ErrPaused) {
				t.Fatalf("failed barrier published guest: %v", err)
			}
			e.mu.Lock()
			afterOps := slices.Clone(e.volumeOps)
			sourcePresent := slices.Contains(e.snapshots, snapshot)
			e.mu.Unlock()
			e.sleepMu.Lock()
			afterHooks := len(e.hooks)
			e.snapshotRemoveErr = nil
			e.sleepMu.Unlock()
			if !slices.Equal(afterOps, beforeOps) || afterHooks != beforeHooks || !sourcePresent {
				t.Fatalf("barrier failure reached guest: ops=%v hooks=%d source=%v", afterOps, afterHooks, sourcePresent)
			}
			generation := e.generation
			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Fatal(err)
			}
			if len(e.restores) != 1 || e.generation != generation {
				t.Fatalf("barrier retry replayed restore: restores=%d generation=%d want=%d", len(e.restores), e.generation, generation)
			}
		})
	}
}

func TestManagedSleepWakeAcceptsRemoveErrorAfterSourceGone(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	e.snapshotRemoveErr = errors.New("snapshot remove reply lost")
	e.snapshotRemoveAfterEffect = true
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if len(e.snapRemoves) != 1 || len(e.snapshots) != 0 {
		t.Fatalf("remove after-effect was not accepted: removes=%v snapshots=%v", e.snapRemoves, e.snapshots)
	}
}

func TestManagedSleepWakeRetriesAfterConsumedSource(t *testing.T) {
	m, e, sb, cfg := newSleepTest(t)
	requireSleep(t, m, sb)
	e.diskAttachErr = errors.New("attach unavailable")
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("attach failure hidden")
	}
	if len(e.snapshots) != 0 || len(e.snapRemoves) != 1 {
		t.Fatalf("source was not consumed before attach: removes=%v snapshots=%v", e.snapRemoves, e.snapshots)
	}
	generation := e.generation
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	e.diskAttachErr = nil
	recovered := reopenSleepTest(t, cfg, e)
	if err := recovered.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if len(e.restores) != 1 || len(e.snapRemoves) != 1 || e.generation != generation {
		t.Fatalf("consumed-source retry repeated effect: restores=%d removes=%d generation=%d want=%d", len(e.restores), len(e.snapRemoves), e.generation, generation)
	}
}

func TestManagedSleepWakeRefusesReplacementSourceAfterRestore(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	e.replaceSourceOnRestore = true
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("replacement source crossed restore barrier")
	}
	if len(e.snapRemoves) != 0 {
		t.Fatalf("replacement source was consumed: %v", e.snapRemoves)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.disks[sb.VMName]) != 0 {
		t.Fatalf("replacement source reached attach: %+v", e.disks[sb.VMName])
	}
}

func TestManagedSleepWakeRevalidatesAfterSourceConsumption(t *testing.T) {
	for _, mutation := range []string{"vm id", "transition"} {
		t.Run(mutation, func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			requireSleep(t, m, sb)
			e.mu.Lock()
			beforeOps := slices.Clone(e.volumeOps)
			e.mu.Unlock()
			e.snapshotRemoveMutation = mutation
			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
				t.Fatal("changed restored VM crossed source barrier")
			}
			e.mu.Lock()
			defer e.mu.Unlock()
			if len(e.snapshots) != 0 || !slices.Equal(e.volumeOps, beforeOps) {
				t.Fatalf("revalidation failure reached guest: before=%v after=%v snapshots=%v", beforeOps, e.volumeOps, e.snapshots)
			}
		})
	}
}

func TestManagedSleepResumingWaitsForSourceConsumption(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	if _, err := e.Restore(t.Context(), sb.VMName, e.hibernates[0]); err != nil {
		t.Fatal(err)
	}
	journalSleepPhase(t, m, sb, func(s *types.VolumeSleep) { s.Phase = types.SleepResuming })
	e.mu.Lock()
	beforeOps := slices.Clone(e.volumeOps)
	e.mu.Unlock()
	e.sleepMu.Lock()
	beforeHooks := len(e.hooks)
	e.snapshotRemoveErr = errors.New("snapshot is in use")
	e.sleepMu.Unlock()
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("resuming guest passed unavailable source-consumption barrier")
	}
	e.mu.Lock()
	afterOps := slices.Clone(e.volumeOps)
	e.mu.Unlock()
	e.sleepMu.Lock()
	afterHooks := len(e.hooks)
	e.snapshotRemoveErr = nil
	e.sleepMu.Unlock()
	if !slices.Equal(afterOps, beforeOps) || afterHooks != beforeHooks {
		t.Fatalf("resuming barrier failure reached guest: ops=%v hooks=%d", afterOps, afterHooks)
	}
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if len(e.restores) != 1 {
		t.Fatalf("resuming barrier retry replayed restore: %v", e.restores)
	}
}

func TestManagedSleepAdoptsRestoredProcessAcrossCrash(t *testing.T) {
	m, e, sb, cfg := newSleepTest(t)
	requireSleep(t, m, sb)
	journalSleepPhase(t, m, sb, func(s *types.VolumeSleep) { s.Phase = types.SleepWaking })
	if _, err := e.Restore(t.Context(), sb.VMName, e.hibernates[0]); err != nil {
		t.Fatal(err)
	}
	e.snapshots = nil
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := reopenSleepTest(t, cfg, e)
	if err := recovered.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if len(e.restores) != 1 {
		t.Fatal("crashed wake replayed restore over live process")
	}
}

func TestManagedSleepCaptureCrashAdoptsStoppedSnapshot(t *testing.T) {
	m, e, sb, cfg := newSleepTest(t)
	tmp := m.store.path + ".tmp"
	m.eng = &claimsFaultEngine{sleepTestEngine: e, on: "capture", fault: func() { _ = os.Mkdir(tmp, 0o700) }}
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("lost commit hidden")
	}
	if !e.stopped[sb.VMName] {
		t.Fatal("capture did not take effect")
	}
	saved, err := os.ReadFile(m.store.path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(tmp); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !m.store.recommitting.Load() && m.store.synced() })
	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(m.store.path, saved, 0o600); err != nil {
		t.Fatal(err)
	}
	recovered := reopenSleepTest(t, cfg, e)
	if err = recovered.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if len(e.hibernates) != 1 || len(e.restores) != 1 {
		t.Fatal("recovery repeated capture")
	}
}

func TestManagedSleepInitialClaimBarrierPrecedesIntent(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	m.store.path = filepath.Join(t.TempDir(), "missing", "claims.json")
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("claim barrier failed silently")
	}
	requireRunning(t, m, sb)
	if len(e.hooks) != 0 || len(e.hibernates) != 0 {
		t.Fatal("effect preceded durable credentials")
	}
}
