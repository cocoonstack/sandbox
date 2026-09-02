package pool

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestAttachOnlyClaimAttachesWithoutMounting(t *testing.T) {
	path := writeVolumeImage(t, "scratch.img", "data")
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}})

	sb, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{
		{Name: "scratch", Mode: types.VolumeModeRW, AttachOnly: true},
	})
	if err != nil {
		t.Fatalf("ClaimProvision: %v", err)
	}
	want := []types.Volume{{Name: "scratch", Mode: types.VolumeModeRW}}
	if !slices.Equal(sb.Volumes, want) {
		t.Errorf("applied volumes=%v, want %v", sb.Volumes, want)
	}
	if specs := eng.volumeSpecs; len(specs) != 1 || !specs[0].RW || specs[0].Path != path {
		t.Errorf("attached specs=%+v, want one writable disk at %s", specs, path)
	}
	if len(eng.volumeMounts) != 0 {
		t.Errorf("mounts=%v, want none", eng.volumeMounts)
	}
	if volumeDirty(path) {
		t.Error("attach-only claim marked the image dirty, promising a flush it cannot make")
	}
	if holders := volumeHoldersOf(m, "scratch"); holders != (volumeHolders{writers: 1}) {
		t.Errorf("registry=%+v, want the writer counted", holders)
	}

	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if ops := eng.volumeOpsLog(); slices.ContainsFunc(ops, func(op string) bool {
		return strings.HasPrefix(op, "umount:") || op == "sync"
	}) {
		t.Errorf("release ran %v, want no unmount or sync", ops)
	}
	if got := eng.syncCount(); got != 0 {
		t.Errorf("guest syncs=%d, want none", got)
	}
	if volumeDirty(path) {
		t.Error("release of an attach-only claim left a marker")
	}
	if holders := volumeHoldersOf(m, "scratch"); holders != (volumeHolders{}) {
		t.Errorf("registry after release=%+v, want empty", holders)
	}
}

func TestAttachOnlyClaimAttachesEveryVolumeConcurrently(t *testing.T) {
	dataset := writeVolumeImage(t, "dataset.img", "dataset")
	scratch := writeVolumeImage(t, "scratch.img", "scratch")
	cache := writeVolumeImage(t, "cache.img", "cache")
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{
		{Name: "dataset", Path: dataset},
		{Name: "scratch", Path: scratch, Writable: true},
		{Name: "cache", Path: cache, Writable: true},
	})
	requested := []types.Volume{
		{Name: "scratch", Mode: types.VolumeModeRW, AttachOnly: true},
		{Name: "dataset", AttachOnly: true},
		{Name: "cache", Mode: types.VolumeModeRW, AttachOnly: true},
	}
	var attaches sync.WaitGroup
	attaches.Add(len(requested))
	eng.attachRendezvous = &attaches

	sb, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", requested)
	if err != nil {
		t.Fatalf("ClaimProvision: %v", err)
	}
	want := []types.Volume{
		{Name: "scratch", Mode: types.VolumeModeRW},
		{Name: "dataset"},
		{Name: "cache", Mode: types.VolumeModeRW},
	}
	if !slices.Equal(sb.Volumes, want) {
		t.Errorf("volumes=%v, want the request order %v", sb.Volumes, want)
	}
	wantOps := []string{"attach:cache", "attach:dataset", "attach:scratch", "probe", "provision"}
	if ops := slices.Sorted(slices.Values(eng.volumeOpsLog())); !slices.Equal(ops, wantOps) {
		t.Errorf("volume ops=%v, want exactly %v", eng.volumeOpsLog(), wantOps)
	}
	if volumeDirty(scratch) || volumeDirty(cache) {
		t.Error("attach-only writers marked their images dirty")
	}
}

func TestClaimProvisionAttachFailureKeepsHoldsUntilRemoval(t *testing.T) {
	scratch := writeVolumeImage(t, "scratch.img", "scratch")
	broken := writeVolumeImage(t, "broken.img", "broken")
	eng := newFakeEngine()
	eng.diskAttachErrFor = "broken"
	eng.removeStall = make(chan struct{})
	var attaches sync.WaitGroup
	attaches.Add(2)
	eng.attachRendezvous = &attaches
	m := newVolumeManager(t, eng, []config.VolumeSpec{
		{Name: "scratch", Path: scratch, Writable: true},
		{Name: "broken", Path: broken},
	})
	requested := []types.Volume{
		{Name: "scratch", Mode: types.VolumeModeRW, AttachOnly: true},
		{Name: "broken", AttachOnly: true},
	}

	claimErr := make(chan error, 1)
	go func() {
		_, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", requested)
		claimErr <- err
	}()
	attaches.Wait()
	eng.mu.Lock()
	vmCount := len(eng.vms)
	var vmName string
	for name := range eng.vms {
		vmName = name
	}
	eng.removeErrFor = vmName
	eng.attachRendezvous = nil
	eng.mu.Unlock()
	close(eng.removeStall)
	err := <-claimErr

	if vmCount != 1 {
		t.Fatalf("live VMs before cleanup=%d, want 1", vmCount)
	}
	if err == nil || !strings.Contains(err.Error(), `attach volume "broken"`) {
		t.Fatalf("ClaimProvision: %v, want the failing volume's attach error", err)
	}
	if holders := volumeHoldersOf(m, "scratch"); holders != (volumeHolders{writers: 1}) {
		t.Errorf("scratch registry after failed removal=%+v, want writer retained", holders)
	}
	if holders := volumeHoldersOf(m, "broken"); holders != (volumeHolders{readers: 1}) {
		t.Errorf("broken registry after failed removal=%+v, want reader retained", holders)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", requested[:1]); !errors.Is(err, ErrVolumeBusy) {
		t.Errorf("claim against surviving VM: %v, want ErrVolumeBusy", err)
	}

	eng.removeErrFor = ""
	m.retryRemovals(t.Context()).Wait()

	if !eng.removed(vmName) {
		t.Errorf("removes=%v, want %s drained", eng.removedNames(), vmName)
	}
	for _, name := range []string{"scratch", "broken"} {
		if holders := volumeHoldersOf(m, name); holders != (volumeHolders{}) {
			t.Errorf("registry for %s after retry=%+v, want empty", name, holders)
		}
	}
}

func TestClaimWarmAttachFailureKeepsHoldsUntilRemoval(t *testing.T) {
	scratch := writeVolumeImage(t, "scratch.img", "scratch")
	broken := writeVolumeImage(t, "broken.img", "broken")
	eng := newFakeEngine()
	eng.diskAttachErrFor = "broken"
	eng.removeErrFor = "sbx-warm"
	eng.vms["sbx-warm"] = "/vsock/warm"
	var attaches sync.WaitGroup
	attaches.Add(2)
	eng.attachRendezvous = &attaches
	m := newVolumePoolManager(t, eng, t.TempDir(), []config.VolumeSpec{
		{Name: "scratch", Path: scratch, Writable: true},
		{Name: "broken", Path: broken},
	})
	warm := &types.Sandbox{VMName: "sbx-warm", Key: testKey, VsockSocket: "/vsock/warm"}
	m.pools[testKey].warm = append(m.pools[testKey].warm, warm)
	requested := []types.Volume{
		{Name: "scratch", Mode: types.VolumeModeRW, AttachOnly: true},
		{Name: "broken", AttachOnly: true},
	}

	_, err := m.ClaimWarm(t.Context(), testKey, 0, "", "", requested)
	if err == nil || !strings.Contains(err.Error(), `attach volume "broken"`) {
		t.Fatalf("ClaimWarm: %v, want the failing volume's attach error", err)
	}
	if holders := volumeHoldersOf(m, "scratch"); holders != (volumeHolders{writers: 1}) {
		t.Errorf("scratch registry after failed removal=%+v, want writer retained", holders)
	}
	if holders := volumeHoldersOf(m, "broken"); holders != (volumeHolders{readers: 1}) {
		t.Errorf("broken registry after failed removal=%+v, want reader retained", holders)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", requested[:1]); !errors.Is(err, ErrVolumeBusy) {
		t.Errorf("claim against surviving warm VM: %v, want ErrVolumeBusy", err)
	}

	eng.removeErrFor = ""
	m.retryRemovals(t.Context()).Wait()

	if !eng.removed(warm.VMName) {
		t.Errorf("removes=%v, want %s drained", eng.removedNames(), warm.VMName)
	}
	for _, name := range []string{"scratch", "broken"} {
		if holders := volumeHoldersOf(m, name); holders != (volumeHolders{}) {
			t.Errorf("registry for %s after retry=%+v, want empty", name, holders)
		}
	}
}

func TestFinalizeQuotaFailureKeepsHoldsUntilRemoval(t *testing.T) {
	scratch := writeVolumeImage(t, "scratch.img", "scratch")
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "scratch", Path: scratch, Writable: true}})
	m.maxClaims = 1
	var attaches sync.WaitGroup
	attaches.Add(2)
	eng.attachRendezvous = &attaches
	writable := []types.Volume{{Name: "scratch", Mode: types.VolumeModeRW}}

	claimErr := make(chan error, 1)
	go func() {
		_, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", writable)
		claimErr <- err
	}()
	waitFor(t, func() bool { return slices.Contains(eng.volumeOpsLog(), "attach:scratch") })
	eng.mu.Lock()
	vmCount := len(eng.vms)
	var vmName string
	for name := range eng.vms {
		vmName = name
	}
	eng.mu.Unlock()
	filler, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", nil)
	if err != nil {
		t.Fatalf("volume-less claim: %v", err)
	}
	eng.mu.Lock()
	eng.removeErrFor = vmName
	eng.attachRendezvous = nil
	eng.mu.Unlock()
	attaches.Done()
	err = <-claimErr

	if vmCount != 1 {
		t.Fatalf("live VMs at the attach=%d, want only the volume claim's", vmCount)
	}
	if !errors.Is(err, ErrQuota) {
		t.Fatalf("volume claim: %v, want ErrQuota from the finalize re-check", err)
	}
	if ops := eng.volumeOpsLog(); !slices.Contains(ops, "umount:/volumes/scratch") {
		t.Errorf("volume ops=%v, want the quota loser's mount unmounted", ops)
	}
	if holders := volumeHoldersOf(m, "scratch"); holders != (volumeHolders{writers: 1}) {
		t.Errorf("registry after failed removal=%+v, want writer retained", holders)
	}
	if !volumeDirty(scratch) {
		t.Error("the marker cleared before the surviving VM was confirmed gone")
	}
	if err := m.Release(t.Context(), filler.ID, Cred{Token: filler.Token}); err != nil {
		t.Fatalf("release the volume-less claim: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", writable); !errors.Is(err, ErrVolumeBusy) {
		t.Errorf("claim against the surviving VM: %v, want ErrVolumeBusy", err)
	}

	eng.mu.Lock()
	eng.removeErrFor = ""
	eng.mu.Unlock()
	m.retryRemovals(t.Context()).Wait()

	if !eng.removed(vmName) {
		t.Errorf("removes=%v, want %s drained", eng.removedNames(), vmName)
	}
	if holders := volumeHoldersOf(m, "scratch"); holders != (volumeHolders{}) {
		t.Errorf("registry after the drain=%+v, want empty", holders)
	}
	if volumeDirty(scratch) {
		t.Error("the quiesced mount left its marker, 409-poisoning every later ro claim")
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "scratch"}}); err != nil {
		t.Errorf("ro claim after the drain: %v, want the image left clean", err)
	}
}

func TestFinalizeTenantQuotaFailureQuiescesAndUncountsTenant(t *testing.T) {
	scratch := writeVolumeImage(t, "scratch.img", "scratch")
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "scratch", Path: scratch, Writable: true}})
	m.tenantMax = map[string]int{"acme": 1}
	var attaches sync.WaitGroup
	attaches.Add(2)
	eng.attachRendezvous = &attaches

	claimErr := make(chan error, 1)
	go func() {
		_, err := m.ClaimProvision(t.Context(), testKey, 0, "acme", "",
			[]types.Volume{{Name: "scratch", Mode: types.VolumeModeRW}})
		claimErr <- err
	}()
	waitFor(t, func() bool { return slices.Contains(eng.volumeOpsLog(), "attach:scratch") })
	eng.mu.Lock()
	var vmName string
	for name := range eng.vms {
		vmName = name
	}
	eng.mu.Unlock()
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "acme", "", nil); err != nil {
		t.Fatalf("volume-less tenant claim: %v", err)
	}
	eng.mu.Lock()
	eng.attachRendezvous = nil
	eng.mu.Unlock()
	attaches.Done()
	err := <-claimErr

	if !errors.Is(err, ErrQuota) {
		t.Fatalf("tenant volume claim: %v, want ErrQuota from the finalize re-check", err)
	}
	if counts := m.TenantClaims(); counts["acme"] != 1 {
		t.Errorf("tenant claims=%v, want only the claim that won counted", counts)
	}
	if ops := eng.volumeOpsLog(); !slices.Contains(ops, "umount:/volumes/scratch") {
		t.Errorf("volume ops=%v, want the tenant loser's mount unmounted", ops)
	}
	if !eng.removed(vmName) {
		t.Errorf("removes=%v, want the loser's VM %s gone", eng.removedNames(), vmName)
	}
	if holders := volumeHoldersOf(m, "scratch"); holders != (volumeHolders{}) {
		t.Errorf("registry after the removal=%+v, want empty", holders)
	}
	if volumeDirty(scratch) {
		t.Error("the tenant loser left its marker, 409-poisoning every later ro claim")
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "scratch"}}); err != nil {
		t.Errorf("ro claim after the tenant loss: %v, want the image left clean", err)
	}
}

func TestAttachOnlyClaimKeepsAdmissionExclusion(t *testing.T) {
	path := writeVolumeImage(t, "scratch.img", "data")
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}})
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{
		{Name: "scratch", Mode: types.VolumeModeRW, AttachOnly: true},
	}); err != nil {
		t.Fatalf("attach-only writer: %v", err)
	}

	for _, tt := range []struct {
		name    string
		request []types.Volume
	}{
		{"mounted writer", []types.Volume{{Name: "scratch", Mode: types.VolumeModeRW}}},
		{"mounted reader", []types.Volume{{Name: "scratch"}}},
		{"attach-only reader", []types.Volume{{Name: "scratch", AttachOnly: true}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", tt.request); !errors.Is(err, ErrVolumeBusy) {
				t.Errorf("claim under an attach-only writer: %v, want ErrVolumeBusy", err)
			}
		})
	}
}

func TestAttachOnlyReadOnlyClaimRefusesDirtyImage(t *testing.T) {
	path := writeVolumeImage(t, "scratch.img", "data")
	m := newVolumeManager(t, newFakeEngine(), []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}})
	if err := markVolumeDirty(path); err != nil {
		t.Fatalf("mark dirty: %v", err)
	}

	_, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "scratch", AttachOnly: true}})
	if !errors.Is(err, ErrVolumeNeedsRecovery) {
		t.Errorf("attach-only read-only claim of a dirty image: %v, want ErrVolumeNeedsRecovery", err)
	}
}

func TestQuiesceVolumesSkipsAttachOnlyEntries(t *testing.T) {
	scratch := writeVolumeImage(t, "scratch.img", "scratch")
	mounted := writeVolumeImage(t, "mounted.img", "mounted")
	eng := newFakeEngine()
	eng.vms["sbx-mixed"] = "/vsock/mixed"
	m := newVolumeManager(t, eng, []config.VolumeSpec{
		{Name: "scratch", Path: scratch, Writable: true},
		{Name: "mounted", Path: mounted, Writable: true},
	})
	sb := &types.Sandbox{
		ID: "sb_mixed", VMName: "sbx-mixed", Key: testKey, VsockSocket: "/vsock/mixed",
		Volumes: []types.Volume{
			{Name: "scratch", Mode: types.VolumeModeRW},
			{Name: "mounted", Mount: "/volumes/mounted", Mode: types.VolumeModeRW},
		},
	}

	td := m.quiesceVolumes(t.Context(), sb)

	if want := []string{"umount:/volumes/mounted"}; !slices.Equal(eng.volumeOpsLog(), want) {
		t.Errorf("quiesce ops=%v, want %v", eng.volumeOpsLog(), want)
	}
	if !slices.Equal(td.clears, []string{mounted}) {
		t.Errorf("marker clears=%v, want only %s", td.clears, mounted)
	}
	if !slices.Equal(td.holds, sb.Volumes) {
		t.Errorf("holds=%v, want every entry %v", td.holds, sb.Volumes)
	}
}

func TestWritableMountsCountsOnlyQuiescableEntries(t *testing.T) {
	for _, tt := range []struct {
		name    string
		volumes []types.Volume
		want    int
	}{
		{"none", nil, 0},
		{"read-only mount", []types.Volume{{Name: "a", Mount: "/a"}}, 0},
		{"attach-only writer", []types.Volume{{Name: "a", Mode: types.VolumeModeRW}}, 0},
		{"writable mount", []types.Volume{{Name: "a", Mount: "/a", Mode: types.VolumeModeRW}}, 1},
		{"mixed", []types.Volume{
			{Name: "a", Mount: "/a", Mode: types.VolumeModeRW},
			{Name: "b", Mode: types.VolumeModeRW},
			{Name: "c", Mount: "/c"},
		}, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := writableMounts(tt.volumes); got != tt.want {
				t.Errorf("writableMounts(%v)=%d, want %d", tt.volumes, got, tt.want)
			}
		})
	}
}
