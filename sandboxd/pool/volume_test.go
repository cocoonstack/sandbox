package pool

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestClaimProvisionAppliesVolumesInOrder(t *testing.T) {
	first := writeVolumeImage(t, "imagenet.img", "first")
	second := writeVolumeImage(t, "weights.img", "second")
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{
		{Name: "imagenet", Path: first, DirectIO: "off"},
		{Name: "weights", Path: second, DirectIO: "on"},
	})
	requested := []types.Volume{{Name: "weights", Mount: "/models"}, {Name: "imagenet"}}
	wantApplied := []types.Volume{
		{Name: "weights", Mount: "/models"},
		{Name: "imagenet", Mount: "/volumes/imagenet"},
	}

	sb, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", requested)
	if err != nil {
		t.Fatalf("ClaimProvision: %v", err)
	}
	if !slices.Equal(sb.Volumes, wantApplied) {
		t.Errorf("volumes=%v, want %v", sb.Volumes, wantApplied)
	}
	wantOps := []string{
		"provision", "probe",
		"attach:weights", "mount:weights:/models",
		"attach:imagenet", "mount:imagenet:/volumes/imagenet",
	}
	if !slices.Equal(eng.volumeOps, wantOps) {
		t.Errorf("operations=%v, want %v", eng.volumeOps, wantOps)
	}
	if !slices.Equal(eng.volumeMounts, wantApplied) {
		t.Errorf("mounts=%v, want %v", eng.volumeMounts, wantApplied)
	}
	if got := eng.volumeSpecs; len(got) != 2 || got[0].Path != second || got[0].DirectIO != "on" || got[1].Path != first {
		t.Errorf("attached specs=%+v", got)
	}
	persisted, err := newClaimStore(m.dataDir).load()
	if err != nil {
		t.Fatalf("load claims: %v", err)
	}
	if got := persisted[sb.ID]; got == nil || !slices.Equal(got.Volumes, wantApplied) {
		t.Errorf("persisted volumes=%v, want %v", got, wantApplied)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	for path, want := range map[string]string{first: "first", second: "second"} {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read backing image: %v", err)
		}
		if string(got) != want {
			t.Errorf("backing image %s=%q, want %q", path, got, want)
		}
	}
}

func TestClaimWarmAppliesVolumesAndRefillsAfterFailure(t *testing.T) {
	path := writeVolumeImage(t, "data.img", "data")
	for _, tt := range []struct {
		name      string
		mountErr  error
		wantClaim bool
	}{
		{name: "success", wantClaim: true},
		{name: "mount failure", mountErr: errors.New("mount failed")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			eng.mountVolumeErr = tt.mountErr
			m := newVolumePoolManager(t, eng, t.TempDir(), []config.VolumeSpec{{Name: "data", Path: path}})
			warm := &types.Sandbox{VMName: "sbx-warm", Key: testKey, VsockSocket: "/vsock/warm"}
			m.pools[testKey].warm = append(m.pools[testKey].warm, warm)

			sb, err := m.ClaimWarm(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "data"}})
			if tt.wantClaim && err != nil {
				t.Fatalf("ClaimWarm: %v", err)
			}
			if tt.wantClaim {
				wantVolumes := []types.Volume{{Name: "data", Mount: "/volumes/data"}}
				if sb.VMName != warm.VMName || !slices.Equal(sb.Volumes, wantVolumes) {
					t.Errorf("sandbox=%+v, want warm VM with %v", sb, wantVolumes)
				}
			} else {
				if err == nil {
					t.Fatal("ClaimWarm succeeded")
				}
				if !eng.removed(warm.VMName) {
					t.Errorf("removes=%v, want failed warm VM destroyed", eng.removedNames())
				}
			}
			if len(eng.colds)+len(eng.clones) != 0 {
				t.Errorf("warm claim provisioned colds=%v clones=%v", eng.colds, eng.clones)
			}
			wantOps := []string{"attach:data", "mount:data:/volumes/data"}
			if !slices.Equal(eng.volumeOps, wantOps) {
				t.Errorf("operations=%v, want %v", eng.volumeOps, wantOps)
			}
			select {
			case <-m.refillKick:
			default:
				t.Error("warm pop did not request refill")
			}
		})
	}
}

func TestClaimProvisionVolumeFailureDestroysVM(t *testing.T) {
	path := writeVolumeImage(t, "data.img", "data")
	for _, tt := range []struct {
		name           string
		setup          func(*fakeEngine)
		wantLast       string
		cancelOnAttach bool
	}{
		{"attach", func(eng *fakeEngine) { eng.diskAttachErr = errors.New("attach failed") }, "attach:data", false},
		{"guest setup", func(eng *fakeEngine) { eng.mountVolumeErr = errors.New("mount failed") }, "mount:data:/volumes/data", false},
		{"canceled caller", func(eng *fakeEngine) { eng.diskAttachErr = errors.New("attach failed") }, "attach:data", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			tt.setup(eng)
			m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "data", Path: path, DirectIO: "off"}})
			ctx := t.Context()
			if tt.cancelOnAttach {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				eng.diskAttachCancel = cancel
			}

			_, err := m.ClaimProvision(ctx, testKey, 0, "", "", []types.Volume{{Name: "data"}})
			if err == nil {
				t.Fatal("ClaimProvision succeeded")
			}
			if len(eng.volumeOps) == 0 || eng.volumeOps[len(eng.volumeOps)-1] != tt.wantLast {
				t.Errorf("operations=%v, want last %q", eng.volumeOps, tt.wantLast)
			}
			if len(eng.removes) != 1 {
				t.Errorf("removes=%v, want failed VM destroyed", eng.removes)
			}
			if _, gauges := m.Info(); gauges.Claimed != 0 {
				t.Errorf("claimed=%d, want 0", gauges.Claimed)
			}
			if persisted, err := newClaimStore(m.dataDir).load(); err != nil || len(persisted) != 0 {
				t.Errorf("persisted=%v err=%v, want no claim", persisted, err)
			}
		})
	}
}

func TestClaimProvisionRejectsInvalidVolumesBeforeProvision(t *testing.T) {
	path := writeVolumeImage(t, "data.img", "data")
	tooMany := make([]types.Volume, types.MaxClaimVolumes+1)
	for i := range tooMany {
		tooMany[i].Name = string(rune('a' + i))
	}
	for _, tt := range []struct {
		name    string
		key     types.PoolKey
		catalog []config.VolumeSpec
		volumes []types.Volume
	}{
		{"invalid name", testKey, []config.VolumeSpec{{Name: "data", Path: path}}, []types.Volume{{Name: "Data"}}},
		{"reserved name", testKey, []config.VolumeSpec{{Name: "data", Path: path}}, []types.Volume{{Name: "cocoon-data"}}},
		{"duplicate", testKey, []config.VolumeSpec{{Name: "data", Path: path}}, []types.Volume{{Name: "data"}, {Name: "data"}}},
		{"too many", testKey, nil, tooMany},
		{"unknown", testKey, []config.VolumeSpec{{Name: "data", Path: path}}, []types.Volume{{Name: "other"}}},
		{"invalid mount", testKey, []config.VolumeSpec{{Name: "data", Path: path}}, []types.Volume{{Name: "data", Mount: "relative"}}},
		{"firecracker", types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall, Engine: types.EngineFC}, []config.VolumeSpec{{Name: "data", Path: path}}, []types.Volume{{Name: "data"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, tt.catalog)
			if _, err := m.ClaimProvision(t.Context(), tt.key, 0, "", "", tt.volumes); !errors.Is(err, ErrBadVolume) {
				t.Errorf("got %v, want ErrBadVolume", err)
			}
			if len(eng.colds)+len(eng.clones) != 0 {
				t.Errorf("provisioned colds=%v clones=%v", eng.colds, eng.clones)
			}
		})
	}
}

func TestClaimProvisionPromotedRejectsMissingTemplateBeforeProvision(t *testing.T) {
	path := writeVolumeImage(t, "data.img", "data")
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "data", Path: path}})

	_, err := m.ClaimProvisionPromoted(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "data"}})
	if !errors.Is(err, ErrVolumeUnavailable) {
		t.Errorf("error=%v, want ErrVolumeUnavailable", err)
	}
	if len(eng.colds)+len(eng.clones) != 0 {
		t.Errorf("provisioned colds=%v clones=%v", eng.colds, eng.clones)
	}
}

func TestClaimProvisionPromotedAppliesVolumesFromTemplate(t *testing.T) {
	path := writeVolumeImage(t, "data.img", "data")
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "data", Path: path}})
	parent := mustClaim(t, m, testKey)
	key, digest, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:volume", "")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	beforeColds, beforeClones := len(eng.colds), len(eng.clones)
	eng.volumeOps = nil

	sb, err := m.ClaimProvisionPromoted(t.Context(), key, 0, "", "", []types.Volume{{Name: "data"}})
	if err != nil {
		t.Fatalf("ClaimProvisionPromoted: %v", err)
	}
	if len(eng.colds) != beforeColds || len(eng.clones) != beforeClones+1 {
		t.Errorf("provisioned colds=%v clones=%v, want one template clone and no cold boot", eng.colds, eng.clones)
	}
	wantOps := []string{"provision", "probe", "attach:data", "mount:data:/volumes/data"}
	if !slices.Equal(eng.volumeOps, wantOps) {
		t.Errorf("operations=%v, want %v", eng.volumeOps, wantOps)
	}
	if sb.TemplateDigest != digest {
		t.Errorf("template digest=%q, want %q", sb.TemplateDigest, digest)
	}
}

func TestClaimProvisionVolumeACL(t *testing.T) {
	path := writeVolumeImage(t, "data.img", "data")
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{
		{Name: "private", Path: path, Tenants: []string{"acme"}},
		{Name: "public", Path: path},
	})

	_, forbiddenErr := m.ClaimProvision(t.Context(), testKey, 0, "beta", "", []types.Volume{{Name: "private"}})
	_, unknownErr := m.ClaimProvision(t.Context(), testKey, 0, "beta", "", []types.Volume{{Name: "unknown"}})
	if forbiddenErr == nil || unknownErr == nil || forbiddenErr.Error() != unknownErr.Error() {
		t.Fatalf("forbidden=%q unknown=%q, want byte-identical errors", forbiddenErr, unknownErr)
	}
	if !errors.Is(forbiddenErr, ErrBadVolume) {
		t.Errorf("forbidden error %v, want ErrBadVolume", forbiddenErr)
	}
	if len(eng.colds)+len(eng.clones) != 0 {
		t.Errorf("rejected claims provisioned colds=%v clones=%v", eng.colds, eng.clones)
	}
	for _, tt := range []struct {
		name, tenant, volume string
	}{
		{"listed tenant", "acme", "private"},
		{"root", "", "private"},
		{"public", "beta", "public"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := m.ClaimProvision(t.Context(), testKey, 0, tt.tenant, "", []types.Volume{{Name: tt.volume}}); err != nil {
				t.Errorf("ClaimProvision: %v", err)
			}
		})
	}
}

func TestVolumesFiltersScopeAndStatsPaths(t *testing.T) {
	publicPath := writeVolumeImage(t, "public.img", "public")
	privatePath := writeVolumeImage(t, "private.img", "private")
	missingPath := filepath.Join(t.TempDir(), "missing.img")
	m := newVolumeManager(t, newFakeEngine(), []config.VolumeSpec{
		{Name: "public", Path: publicPath},
		{Name: "acme", Path: privatePath, Tenants: []string{"acme"}},
		{Name: "beta", Path: missingPath, Tenants: []string{"beta"}},
	})
	holders := map[string]int{"acme": 2, "beta": 3, "peer-only": 2, "public": 4}

	wantRoot := []types.VolumeInfo{
		{Name: "acme", DefaultMount: "/volumes/acme", SizeBytes: int64(len("private")), Available: true, Nodes: 2},
		{Name: "beta", DefaultMount: "/volumes/beta", Nodes: 3},
		{Name: "peer-only", DefaultMount: "/volumes/peer-only", Nodes: 2},
		{Name: "public", DefaultMount: "/volumes/public", SizeBytes: int64(len("public")), Available: true, Nodes: 4},
	}
	if got := m.Volumes("", holders); !slices.Equal(got, wantRoot) {
		t.Errorf("root volumes=%+v, want %+v", got, wantRoot)
	}
	wantAcme := []types.VolumeInfo{wantRoot[0], wantRoot[3]}
	if got := m.Volumes("acme", holders); !slices.Equal(got, wantAcme) {
		t.Errorf("acme volumes=%+v, want %+v", got, wantAcme)
	}
	wantBeta := []types.VolumeInfo{wantRoot[1], wantRoot[3]}
	if got := m.Volumes("beta", holders); !slices.Equal(got, wantBeta) {
		t.Errorf("beta volumes=%+v, want %+v", got, wantBeta)
	}
	if got := m.VolumeNames(); !slices.Equal(got, []string{"acme", "public"}) {
		t.Errorf("available names=%v, want [acme public]", got)
	}
	if err := os.WriteFile(missingPath, []byte("beta"), 0o600); err != nil {
		t.Fatalf("distribute missing image: %v", err)
	}
	if got := m.VolumeNames(); !slices.Equal(got, []string{"acme", "beta", "public"}) {
		t.Errorf("names after image distribution=%v, want [acme beta public]", got)
	}
	if err := os.Remove(missingPath); err != nil {
		t.Fatalf("remove distributed image: %v", err)
	}
	if got := m.VolumeNames(); !slices.Equal(got, []string{"acme", "public"}) {
		t.Errorf("names after image removal=%v, want [acme public]", got)
	}
}

func TestVolumePlacementChecksAccessAndLocalAvailability(t *testing.T) {
	path := writeVolumeImage(t, "data.img", "data")
	m := newVolumeManager(t, newFakeEngine(), []config.VolumeSpec{
		{Name: "local", Path: path, Tenants: []string{"acme"}},
		{Name: "remote", Path: filepath.Join(t.TempDir(), "missing.img"), Tenants: []string{"acme"}},
	})

	if local, err := m.VolumePlacement(testKey, "acme", []string{"local"}); err != nil || !local {
		t.Errorf("local placement=(%v, %v), want true, nil", local, err)
	}
	if local, err := m.VolumePlacement(testKey, "acme", []string{"local", "remote"}); err != nil || local {
		t.Errorf("partial local placement=(%v, %v), want false, nil", local, err)
	}
	_, forbiddenErr := m.VolumePlacement(testKey, "beta", []string{"local"})
	_, unknownErr := m.VolumePlacement(testKey, "beta", []string{"unknown"})
	if forbiddenErr == nil || unknownErr == nil || forbiddenErr.Error() != unknownErr.Error() {
		t.Errorf("forbidden=%q unknown=%q, want byte-identical errors", forbiddenErr, unknownErr)
	}
	if local, err := m.VolumePlacement(testKey, "", []string{"peer-only"}); err != nil || local {
		t.Errorf("root peer-only placement=(%v, %v), want false, nil", local, err)
	}
	badKey := types.PoolKey{Template: "rt:24.04", Net: "lan", Size: types.SizeSmall, Engine: types.EngineCH}
	if _, err := m.VolumePlacement(badKey, "", []string{"local"}); !errors.Is(err, ErrBadKey) {
		t.Errorf("invalid key error=%v, want ErrBadKey", err)
	}
}

func TestVolumeClaimUsageAndScopedSummaries(t *testing.T) {
	path := writeVolumeImage(t, "data.img", "data")
	m := newVolumeManager(t, newFakeEngine(), []config.VolumeSpec{{Name: "data", Path: path}})
	request := []types.Volume{{Name: "data", Mount: "/datasets"}}
	wantApplied := []types.Volume{{Name: "data", Mount: "/datasets"}}
	acme, err := m.ClaimProvision(t.Context(), testKey, 0, "acme", "", request)
	if err != nil {
		t.Fatalf("acme claim: %v", err)
	}
	_, betaErr := m.ClaimProvision(t.Context(), testKey, 0, "beta", "", request)
	if betaErr != nil {
		t.Fatalf("beta claim: %v", betaErr)
	}

	if got := m.Sandboxes("acme"); len(got) != 1 || got[0].ID != acme.ID || !slices.Equal(got[0].Volumes, wantApplied) {
		t.Errorf("acme summaries=%+v, want only its volume claim", got)
	}
	if got := m.Sandboxes(""); len(got) != 2 {
		t.Errorf("root summaries=%+v, want both claims", got)
	}
	if got, ok := m.Sandbox(acme.ID); !ok || !slices.Equal(got.Volumes, wantApplied) {
		t.Errorf("sandbox summary=%+v ok=%v, want applied volume", got, ok)
	}

	raw, err := os.ReadFile(filepath.Join(m.dataDir, "usage.jsonl"))
	if err != nil {
		t.Fatalf("read usage journal: %v", err)
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		var event usageEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("decode usage event: %v", err)
		}
		if event.Event == "claim" && event.ID == acme.ID {
			if !slices.Equal(event.Volumes, []string{"data"}) {
				t.Errorf("claim event volumes=%v, want [data]", event.Volumes)
			}
			return
		}
	}
	t.Fatal("acme claim usage event not found")
}

func TestClaimProvisionRejectsMissingVolumePathBeforeProvision(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "data", Path: filepath.Join(t.TempDir(), "missing.img")}})

	_, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "data"}})
	if err == nil || !strings.Contains(err.Error(), "missing.img") {
		t.Errorf("got %v, want missing-path error", err)
	}
	if len(eng.colds)+len(eng.clones) != 0 {
		t.Errorf("provisioned colds=%v clones=%v", eng.colds, eng.clones)
	}
}

func newVolumeManager(t *testing.T, eng *fakeEngine, volumes []config.VolumeSpec) *Manager {
	t.Helper()
	return newVolumeManagerAt(t, eng, t.TempDir(), volumes)
}

func newVolumePoolManager(t *testing.T, eng *fakeEngine, dataDir string, volumes []config.VolumeSpec) *Manager {
	t.Helper()
	m, err := NewManager(t.Context(), &config.Config{
		DataDir: dataDir,
		Pools:   []config.PoolSpec{{PoolKey: testKey, Warm: 1}},
		Volumes: volumes,
	}, eng, testSecrets(t))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	return m
}

func writeVolumeImage(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write volume image: %v", err)
	}
	return path
}
