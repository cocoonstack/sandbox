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

// TestAttachOnlyClaimAttachesWithoutMounting: the whole mount contract belongs
// to the caller, so a writable attach-only claim leaves no marker to clear and
// no mount to quiesce — the device only goes away with the VM.
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
	// Every attach must be in flight at once: a sequential apply blocks here.
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

// TestAttachOnlyClaimKeepsAdmissionExclusion: what protects other claims is
// unchanged — only this claim's own mount contract moved to the caller.
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
