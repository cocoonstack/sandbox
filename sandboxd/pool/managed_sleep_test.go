package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
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
	m.managed.process = e.identity
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

func (e *sleepTestEngine) identity(pid int) (processIdentity, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for name, p := range e.pids {
		if p == pid && !e.stopped[name] {
			if _, exists := e.vms[name]; exists {
				return processIdentity{PID: pid, Start: uint64(pid), BootID: "test-boot"}, nil
			}
		}
	}
	return processIdentity{}, os.ErrNotExist
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
	case "generation":
		e.sleepMu.Lock()
		e.generation++
		e.sleepMu.Unlock()
	case "process":
		e.mu.Lock()
		for vmName := range e.pids {
			e.pids[vmName]++
		}
		e.mu.Unlock()
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
		return errors.New("hook failed")
	}
	return nil
}

func requireSleep(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	h, _ := m.managedHold(sb.ID)
	if h.Sleep == nil || h.Sleep.Phase != sleepSleeping {
		t.Fatalf("not sleeping: %+v", h)
	}
}

func TestManagedSleepRoundTripAndExclusiveHold(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	_, fenced, release, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	requireSleep(t, m, sb)
	select {
	case <-fenced:
	default:
		t.Fatal("existing stream not fenced")
	}
	_, err = m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	requireVolumeStatus(t, err, 409)
	_, err = m.AgentSocket(sb.ID, sb.Token)
	if !errors.Is(err, ErrPaused) {
		t.Fatalf("passive socket = %v", err)
	}
	if err = m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	_, fresh, unhold, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatal(err)
	}
	defer unhold()
	select {
	case <-fresh:
		t.Fatal("new stream inherited old fence")
	default:
	}
	h, _ := m.managedHold(sb.ID)
	if h.Sleep != nil || h.Process.PID == 1001 {
		t.Fatalf("wake not bound: %+v", h)
	}
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
			if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
				t.Fatal("failure hidden")
			}
			h, _ := m.managedHold(sb.ID)
			if h.Sleep != nil || e.stopped[sb.VMName] || len(e.restores) != 0 {
				t.Fatalf("failed preparation damaged VM: %+v", h)
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

func TestManagedSleepWakeFailuresKeepRestoredProcess(t *testing.T) {
	for _, failure := range []string{"restore reply", "attach", "mount", "resume"} {
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
			case "resume":
				e.hookErr = "resume"
			}
			err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token})
			if failure != "restore reply" && err == nil {
				t.Fatal("wake failure hidden")
			}
			if e.stopped[sb.VMName] || len(e.restores) != 1 {
				t.Fatal("restored VM was discarded")
			}
			e.diskAttachErr, e.mountVolumeErr = nil, nil
			e.hookErr = ""
			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Fatal(err)
			}
			if len(e.restores) != 1 {
				t.Fatal("restore replayed over live VM")
			}
		})
	}
}

func TestManagedSleepBackingRefusesBeforeRestore(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "replaced"}[replace], func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			requireSleep(t, m, sb)
			h, _ := m.managedHold(sb.ID)
			path := h.Refs[0].Path
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
			if _, _, err = m.CreateVolume(t.Context(), "data", types.VolumeCreate{SizeGiB: 1}, "t1"); err == nil {
				t.Fatal("missing backing create replay succeeded")
			}
			if _, err = m.ClaimProvision(t.Context(), testKey, managedOptions("data")); err == nil {
				t.Fatal("missing backing admitted second writer")
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
	for _, mismatch := range []string{"id", "generation", "process", "disk path", "disk name"} {
		t.Run(mismatch, func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			requireSleep(t, m, sb)
			e.hookErr = "resume"
			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
				t.Fatal("expected suspended resume")
			}
			switch mismatch {
			case "id":
				e.vmID = "replacement"
			case "generation":
				e.generation++
			case "process":
				e.pids[sb.VMName]++
			case "disk path":
				e.disks[sb.VMName][0].Path = "/wrong"
			case "disk name":
				e.disks[sb.VMName][0].Name = "other"
			}
			e.hookErr = ""
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
	h, _ := m.managedHold(sb.ID)
	before := slices.Clone(e.volumeOps)
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if _, held := m.managedHold(sb.ID); held {
		t.Fatal("hold not released")
	}
	if _, err := os.Stat(h.Refs[0].Path); err != nil {
		t.Fatal(err)
	}
	if len(e.snapshots) != 0 {
		t.Fatal("sleep snapshot leaked")
	}
	if !slices.Equal(before, e.volumeOps) {
		t.Fatal("asleep release touched guest")
	}
}

func TestManagedSleepRestartEveryPhase(t *testing.T) {
	for _, phase := range []sleepPhase{sleepPreparing, sleepAborting, sleepCapturing, sleepSleeping, sleepWaking, sleepResuming} {
		t.Run(string(phase), func(t *testing.T) {
			m, e, sb, cfg := newSleepTest(t)
			original, _ := m.managedHold(sb.ID)
			requireSleep(t, m, sb)
			h, _ := m.managedHold(sb.ID)
			next := *h.Sleep
			next.Phase = phase
			if phase == sleepPreparing || phase == sleepAborting || phase == sleepCapturing {
				e.mu.Lock()
				e.stopped[sb.VMName] = false
				if phase != sleepCapturing {
					e.disks[sb.VMName] = []types.VMDisk{{Name: "data", Path: h.Refs[0].Path}}
				}
				e.snapshots = nil
				e.mu.Unlock()
				e.sleepMu.Lock()
				e.generation = 2
				e.reason = "boot"
				e.mounted = phase != sleepCapturing
				e.sleepMu.Unlock()
				next.SourceID = ""
			}
			if phase == sleepResuming {
				if _, err := e.Restore(t.Context(), sb.VMName, next.Snapshot); err != nil {
					t.Fatal(err)
				}
				rec, _, _ := e.Inspect(t.Context(), sb.VMName)
				p, err := e.identity(rec.PID)
				if err != nil {
					t.Fatal(err)
				}
				next.Restored = p
				h.Process = p
			}
			if err := m.managed.commitManagedSleep(h, &next); err != nil {
				t.Fatal(err)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			cfg.ManagedVolumeHibernate = nil
			recovered, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = recovered.Close() })
			recovered.managed.process = e.identity
			if err = recovered.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err = recovered.AgentSocket(sb.ID, sb.Token); !errors.Is(err, ErrPaused) {
				t.Fatalf("restart bypassed journal: %v", err)
			}
			if _, held := recovered.managedHold(sb.ID); !held {
				t.Fatal("restart freed sleep owner")
			}
			if err = recovered.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Fatal(err)
			}
			got, _ := recovered.managedHold(sb.ID)
			if got.Sleep != nil || got.Refs[0] != original.Refs[0] {
				t.Fatalf("recovery changed backing: %+v", got)
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

func TestManagedSleepJournalFailureStopsEffects(t *testing.T) {
	for _, phase := range []sleepPhase{sleepPreparing, sleepCapturing, sleepSleeping, sleepWaking, sleepResuming} {
		t.Run(string(phase), func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			if phase == sleepWaking || phase == sleepResuming {
				requireSleep(t, m, sb)
			}
			commit := m.managed.commitFile
			m.managed.commitFile = func(path string, raw []byte) error {
				if strings.Contains(string(raw), `"phase":"`+string(phase)+`"`) {
					return errors.New("injected journal write failure")
				}
				return commit(path, raw)
			}
			var err error
			if phase == sleepWaking || phase == sleepResuming {
				err = m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token})
			} else {
				err = m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token})
			}
			if err == nil {
				t.Fatal("persistence failure hidden")
			}
			beforeHooks, beforeRestores, beforeCaptures := len(e.hooks), len(e.restores), len(e.hibernates)
			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
				t.Fatal("poisoned registry served guest")
			}
			if len(e.hooks) != beforeHooks || len(e.restores) != beforeRestores || len(e.hibernates) != beforeCaptures {
				t.Fatal("effect after poisoned registry")
			}
			if phase == sleepPreparing && len(e.hooks) != 0 {
				t.Fatal("hook preceded durable intent")
			}
			if phase == sleepCapturing && len(e.hibernates) != 0 {
				t.Fatal("capture preceded durable intent")
			}
			if phase == sleepWaking && len(e.restores) != 0 {
				t.Fatal("restore preceded durable intent")
			}
			if phase == sleepResuming && e.hooks[len(e.hooks)-1].Action == "resume" {
				t.Fatal("resume preceded durable intent")
			}
		})
	}
}

func TestManagedSleepUnknownSourceAndDeadResumingRefuse(t *testing.T) {
	for _, failure := range []string{"missing source", "source identity", "dead resuming"} {
		t.Run(failure, func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			requireSleep(t, m, sb)
			switch failure {
			case "missing source":
				e.snapshots = nil
			case "source identity":
				h, _ := m.managedHold(sb.ID)
				next := *h.Sleep
				next.SourceID = "other"
				if err := m.managed.commitManagedSleep(h, &next); err != nil {
					t.Fatal(err)
				}
			case "dead resuming":
				e.hookErr = "resume"
				if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
					t.Fatal("expected failed resume")
				}
				e.stopped[sb.VMName] = true
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
	e.hookErr = "resume"
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("resume failure hidden")
	}
	h, _ := m.managedHold(sb.ID)
	if h.Sleep == nil || h.Sleep.Restored == (processIdentity{}) || len(e.snapshots) != 0 {
		t.Fatalf("wake did not bind before consuming source: %+v snapshots=%v", h, e.snapshots)
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
	if _, held := m.managedHold(sb.ID); held {
		t.Fatal("release retained dead bound owner")
	}
	if _, err := os.Stat(h.Refs[0].Path); err != nil {
		t.Fatal(err)
	}
}

func TestManagedSleepReleaseWaitsForPrepare(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	_, fenced, release, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	e.hookEntered = make(chan struct{}, 2)
	e.hookContinue = make(chan struct{})
	hibernate := make(chan error, 1)
	released := make(chan error, 1)
	go func() { hibernate <- m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
	<-e.hookEntered
	select {
	case <-fenced:
	default:
		t.Fatal("prepare started before existing streams were fenced")
	}

	if info, ok := m.Sandbox(sb.ID); !ok || info.Hibernated {
		t.Fatal("preparing was reported as successfully hibernated")
	}
	if _, err := m.AgentSocket(sb.ID, sb.Token); !errors.Is(err, ErrPaused) {
		t.Fatalf("prepare exposed guest: %v", err)
	}
	go func() { released <- m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
	select {
	case err := <-released:
		t.Fatalf("release overtook prepare: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(e.hookContinue)
	if err := <-hibernate; err != nil {
		t.Fatal(err)
	}
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	if _, held := m.managedHold(sb.ID); held {
		t.Fatal("release retained completed hold")
	}
}

func TestManagedSleepRegistryUpgradeAndCorruption(t *testing.T) {
	for _, corruption := range []string{"phase", "version downgrade", "missing VM ID", "invalid hook", "sleeping restored process"} {
		t.Run(corruption, func(t *testing.T) {
			m, _, sb, cfg := newSleepTest(t)
			if m.managed.data.Version != 1 {
				t.Fatal("new store unexpectedly upgraded")
			}
			requireSleep(t, m, sb)
			if m.managed.data.Version != 2 {
				t.Fatal("sleep did not fence old registry readers")
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(cfg.ManagedVolumeDir, "registry.json")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var data volumeRegistryData
			if err = json.Unmarshal(raw, &data); err != nil {
				t.Fatal(err)
			}
			h := data.Holds[sb.ID]
			switch corruption {
			case "phase":
				h.Sleep.Phase = "unknown"
			case "version downgrade":
				data.Version = 1
			case "missing VM ID":
				h.Sleep.VMID = ""
			case "invalid hook":
				h.Sleep.Hook.HookArgv = nil
			case "sleeping restored process":
				h.Sleep.Restored = h.Process
			}
			data.Holds[sb.ID] = h
			raw, err = json.Marshal(data)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			r, err := openVolumeRegistry(cfg.ManagedVolumeDir, "test-node")
			if err != nil {
				t.Fatal(err)
			}
			defer r.close()
			if r.fault == nil {
				t.Fatal("corrupt sleep journal was accepted")
			}
		})
	}
}

func TestManagedSleepMissingClaimRetainsOwnership(t *testing.T) {
	m, e, sb, cfg := newSleepTest(t)
	requireSleep(t, m, sb)
	h, _ := m.managedHold(sb.ID)
	if err := m.store.commitDurable(m.store.del(sb.ID)); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	recovered.managed.process = e.identity
	if err = recovered.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireVolumeStatus(t, recovered.ManagedVolumeStatus(), 503)
	if _, exists := recovered.Sandbox(sb.ID); exists {
		t.Fatal("missing credentials were reconstructed")
	}
	if _, held := recovered.managedHold(sb.ID); !held {
		t.Fatal("missing claim freed sleep owner")
	}
	if !slices.Contains(e.snapshots, h.Sleep.Snapshot) {
		t.Fatal("missing claim lost recovery source")
	}
}

func TestManagedSleepExpiryIgnoresArchivePool(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Minute)
	m.pools[sb.Key].archiveAfter = time.Second
	m.mu.Unlock()
	m.reapOnce(t.Context())
	deadline := time.Now().Add(time.Second)
	for {
		if _, held := m.managedHold(sb.ID); !held {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expired managed sleep stayed held")
		}
		time.Sleep(time.Millisecond)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.snapshots) != 0 {
		t.Fatal("expiry retained sleep source")
	}
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

func TestManagedSleepReadyJournalFailureCannotBypassGate(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	commit := m.managed.commitFile
	m.managed.commitFile = func(path string, raw []byte) error {
		if !strings.Contains(string(raw), `"sleep":`) {
			return errors.New("ready journal failed")
		}
		return commit(path, raw)
	}
	m.managedSleepHook = nil
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("ready journal failure hidden")
	}
	if _, err := m.AgentSocket(sb.ID, sb.Token); !errors.Is(err, ErrPaused) {
		t.Fatalf("passive gate bypassed: %v", err)
	}
	hooks, restores := len(e.hooks), len(e.restores)
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("poisoned completed journal served guest")
	}
	if len(e.hooks) != hooks || len(e.restores) != restores {
		t.Fatal("effect after ready commit failure")
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
			h, held := m.managedHold(sb.ID)
			if !held || h.Sleep == nil || h.Sleep.Phase != sleepWaking || h.Sleep.Restored == (processIdentity{}) {
				t.Fatalf("failed barrier lost waking owner: %+v", h)
			}
			if _, err := m.AgentSocket(sb.ID, sb.Token); !errors.Is(err, ErrPaused) {
				t.Fatalf("failed barrier published guest: %v", err)
			}
			e.mu.Lock()
			afterOps := slices.Clone(e.volumeOps)
			sourcePresent := slices.Contains(e.snapshots, h.Sleep.Snapshot)
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
	h, held := m.managedHold(sb.ID)
	if !held || h.Sleep == nil || h.Sleep.Restored == (processIdentity{}) {
		t.Fatalf("failed attach lost bound waking owner: %+v", h)
	}
	if len(e.snapshots) != 0 || len(e.snapRemoves) != 1 {
		t.Fatalf("source was not consumed before attach: removes=%v snapshots=%v", e.snapRemoves, e.snapshots)
	}
	generation := e.generation
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	e.diskAttachErr = nil
	recovered, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	recovered.managed.process = e.identity
	if err = recovered.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = recovered.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
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
	h, held := m.managedHold(sb.ID)
	if !held || h.Sleep == nil || h.Sleep.Restored == (processIdentity{}) || len(e.snapRemoves) != 0 {
		t.Fatalf("replacement source changed ownership: %+v removes=%v", h, e.snapRemoves)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.disks[sb.VMName]) != 0 {
		t.Fatalf("replacement source reached attach: %+v", e.disks[sb.VMName])
	}
}

func TestManagedSleepWakeRevalidatesAfterSourceConsumption(t *testing.T) {
	for _, mutation := range []string{"vm id", "generation", "process"} {
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
			h, held := m.managedHold(sb.ID)
			if !held || h.Sleep == nil || h.Sleep.Restored == (processIdentity{}) || len(e.snapshots) != 0 {
				t.Fatalf("revalidation failure lost owner: %+v snapshots=%v", h, e.snapshots)
			}
			e.mu.Lock()
			defer e.mu.Unlock()
			if !slices.Equal(e.volumeOps, beforeOps) {
				t.Fatalf("revalidation failure reached guest: before=%v after=%v", beforeOps, e.volumeOps)
			}
		})
	}
}

func TestManagedSleepResumingWaitsForSourceConsumption(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	h, _ := m.managedHold(sb.ID)
	if _, err := e.Restore(t.Context(), sb.VMName, h.Sleep.Snapshot); err != nil {
		t.Fatal(err)
	}
	rec, _, err := e.Inspect(t.Context(), sb.VMName)
	if err != nil {
		t.Fatal(err)
	}
	p, err := e.identity(rec.PID)
	if err != nil {
		t.Fatal(err)
	}
	next := *h.Sleep
	next.Phase = sleepResuming
	next.Restored = p
	h.Process = p
	if err = m.managed.commitManagedSleep(h, &next); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	beforeOps := slices.Clone(e.volumeOps)
	e.mu.Unlock()
	e.sleepMu.Lock()
	beforeHooks := len(e.hooks)
	e.snapshotRemoveErr = errors.New("snapshot is in use")
	e.sleepMu.Unlock()
	if err = m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
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
	if err = m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if len(e.restores) != 1 {
		t.Fatalf("resuming barrier retry replayed restore: %v", e.restores)
	}
}

func TestManagedSleepAdoptsRestoredProcessAcrossCrash(t *testing.T) {
	m, e, sb, cfg := newSleepTest(t)
	requireSleep(t, m, sb)
	h, _ := m.managedHold(sb.ID)
	if err := m.setSleepPhase(h, sleepWaking); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Restore(t.Context(), sb.VMName, h.Sleep.Snapshot); err != nil {
		t.Fatal(err)
	}
	e.snapshots = nil
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	recovered.managed.process = e.identity
	if err = recovered.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = recovered.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if len(e.restores) != 1 {
		t.Fatal("crashed wake replayed restore over live process")
	}
}

func TestManagedSleepCaptureCrashAdoptsStoppedSnapshot(t *testing.T) {
	m, e, sb, cfg := newSleepTest(t)
	commit := m.managed.commitFile
	m.managed.commitFile = func(path string, raw []byte) error {
		if strings.Contains(string(raw), `"phase":"sleeping"`) {
			return errors.New("sleep commit lost")
		}
		return commit(path, raw)
	}
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("lost commit hidden")
	}
	if !e.stopped[sb.VMName] {
		t.Fatal("capture did not take effect")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	recovered.managed.process = e.identity
	if err = recovered.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
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
	h, _ := m.managedHold(sb.ID)
	if h.Sleep != nil || len(e.hooks) != 0 || len(e.hibernates) != 0 {
		t.Fatal("effect preceded durable credentials")
	}
}
