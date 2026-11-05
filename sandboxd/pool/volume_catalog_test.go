package pool

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestRuntimeCatalogPersistsNormalizedSemantics(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, nil)
	path := writeVolumeImage(t, "runtime", "backing bytes stay intact")
	spec := types.VolumeRegistration{Path: filepath.Dir(path) + "/./runtime", Writable: true, Tenants: []string{"beta", "acme", "acme"}}
	before := m.catalogSnapshot()
	if err := m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatalf("RegisterVolume: %v", err)
	}
	if before["runtime"] != nil || spec.Tenants[0] != "beta" {
		t.Fatal("registration mutated a captured catalog or the caller's spec")
	}
	normalized := types.VolumeRegistration{Path: path, DirectIO: types.DirectIOOff, Writable: true, Tenants: []string{"acme", "beta"}}
	if err := m.RegisterVolume(t.Context(), "runtime", normalized); err != nil {
		t.Fatalf("normalized retry: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(m.dataDir, runtimeVolumesFile))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var saved map[string]types.VolumeRegistration
	if err = json.Unmarshal(raw, &saved); err != nil || len(saved) != 1 || saved["runtime"].Path != path || !slices.Equal(saved["runtime"].Tenants, normalized.Tenants) {
		t.Fatalf("saved registrations: %s, %v", raw, err)
	}
	if st, statErr := os.Stat(filepath.Join(m.dataDir, runtimeVolumesFile)); statErr != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("metadata mode: %v %v", st, statErr)
	}
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "runtime"}}})
	if err != nil {
		t.Fatalf("ClaimProvision: %v", err)
	}
	m2 := newVolumeManagerAt(t, eng, m.dataDir, nil)
	if got := m2.Volumes("acme", nil); len(got) != 1 || got[0].Name != "runtime" || !got[0].Available || !got[0].Writable {
		t.Fatalf("catalog absent before reconcile: %+v", got)
	}
	if got := m2.Volumes("forbidden", nil); len(got) != 0 {
		t.Fatalf("ACL changed at restart: %+v", got)
	}
	if err = m2.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, err = m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "runtime", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("recovered holder missing: %v", err)
	}
	changed := normalized
	changed.DirectIO = "on"
	if err = m2.RegisterVolume(t.Context(), "runtime", changed); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("changed semantics: %v", err)
	}
	if err = m2.UnregisterVolume(t.Context(), "runtime"); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("unregister of a held volume: %v", err)
	}
	if err = m2.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	for range 2 {
		if err = m2.UnregisterVolume(t.Context(), "runtime"); err != nil {
			t.Fatalf("UnregisterVolume: %v", err)
		}
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "backing bytes stay intact" {
		t.Fatalf("backing changed: %q %v", got, err)
	}
	if got := newVolumeManagerAt(t, eng, m.dataDir, nil).VolumeNames(); len(got) != 0 {
		t.Fatalf("unregister not durable: %v", got)
	}
}

func TestRuntimeCatalogChangesAreAudited(t *testing.T) {
	cfg := &config.Config{DataDir: t.TempDir(), AuditLog: true}
	m, initErr := NewManager(t.Context(), cfg, newFakeEngine(), testSecrets(t), testBackends(t, cfg))
	if initErr != nil {
		t.Fatalf("NewManager: %v", initErr)
	}
	spec := types.VolumeRegistration{Path: "/images/data"}
	restore := blockCatalogRename(t, m)
	if err := m.RegisterVolume(t.Context(), "data", spec); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("failed register: %v", err)
	}
	restore()
	for range 2 {
		if err := m.RegisterVolume(t.Context(), "data", spec); err != nil {
			t.Fatalf("RegisterVolume: %v", err)
		}
	}
	restore = blockCatalogRename(t, m)
	if err := m.UnregisterVolume(t.Context(), "data"); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("failed unregister: %v", err)
	}
	restore()
	for range 2 {
		if err := m.UnregisterVolume(t.Context(), "data"); err != nil {
			t.Fatalf("UnregisterVolume: %v", err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(m.dataDir, "audit.jsonl"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	want := []auditFrame{{Op: "volumes", Added: []string{"data"}}, {Op: "volumes", Removed: []string{"data"}}, {Op: "volumes", Removed: []string{"data"}}}
	if len(lines) != len(want) {
		t.Fatalf("audit records: %s", raw)
	}
	for i, line := range lines {
		var got auditFrame
		if err := json.Unmarshal([]byte(line), &got); err != nil || got.Op != want[i].Op || !slices.Equal(got.Added, want[i].Added) || !slices.Equal(got.Removed, want[i].Removed) {
			t.Fatalf("audit record %d: %s, %v", i, line, err)
		}
	}
}

func TestRuntimeCatalogRegistersUncheckedBacking(t *testing.T) {
	link := filepath.Join(t.TempDir(), "backing-link")
	if err := os.Symlink(writeVolumeImage(t, "backing", "bytes"), link); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	for _, tt := range []struct {
		name, path string
		wantErr    error
	}{{"symlink", link, nil}, {"missing", filepath.Join(t.TempDir(), "missing"), ErrVolumeUnavailable}} {
		t.Run(tt.name, func(t *testing.T) {
			m := newVolumeManager(t, newFakeEngine(), nil)
			registration := types.VolumeRegistration{Path: tt.path, Writable: true}
			if err := m.RegisterVolume(t.Context(), "runtime", registration); err != nil {
				t.Fatalf("register: %v", err)
			}
			m2 := newVolumeManagerAt(t, newFakeEngine(), m.dataDir, nil)
			if err := m2.RegisterVolume(t.Context(), "runtime", registration); err != nil {
				t.Fatalf("idempotent retry after restart: %v", err)
			}
			if infos := m2.Volumes("", nil); len(infos) != 1 || infos[0].Available != (tt.wantErr == nil) {
				t.Fatalf("availability: %+v", infos)
			}
			if _, err := m2.resolveVolumes(t.Context(), "", []types.Volume{{Name: "runtime"}}); !errors.Is(err, tt.wantErr) {
				t.Fatalf("resolve = %v, want %v", err, tt.wantErr)
			}
			if err := m2.UnregisterVolume(t.Context(), "runtime"); err != nil {
				t.Fatalf("UnregisterVolume: %v", err)
			}
		})
	}
}

func TestRuntimeCatalogRejectsCollisionsAndInvalidRegistrations(t *testing.T) {
	static := liveCatalog(t)
	eng := newFakeEngine()
	other := writeVolumeImage(t, "other-runtime", "bytes")
	m := newVolumeManager(t, eng, append(slices.Clone(static), config.VolumeSpec{Name: "unexplained", Path: other}))
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "unexplained"}}}); err != nil {
		t.Fatalf("ClaimProvision: %v", err)
	}
	m = newVolumeManagerAt(t, eng, m.dataDir, static)
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	alias := filepath.Dir(static[0].Path) + "/./" + filepath.Base(static[0].Path)
	for _, tt := range []struct {
		name string
		spec types.VolumeRegistration
		want error
	}{
		{"data", types.VolumeRegistration{Path: other}, ErrVolumeRegistrationConflict},
		{"runtime", types.VolumeRegistration{Path: static[0].Path}, ErrVolumeRegistrationConflict},
		{"runtime", types.VolumeRegistration{Path: alias}, ErrVolumeRegistrationConflict},
		{"unexplained", types.VolumeRegistration{Path: other}, ErrVolumeRegistrationConflict},
		{"cocoon-reserved", types.VolumeRegistration{Path: other}, ErrBadVolume},
		{"bad/name", types.VolumeRegistration{Path: other}, ErrBadVolume},
		{"runtime", types.VolumeRegistration{Path: "relative"}, ErrBadVolume},
		{"runtime", types.VolumeRegistration{Path: other, DirectIO: "invalid"}, ErrBadVolume},
		{"runtime", types.VolumeRegistration{Path: other, Tenants: []string{"has space"}}, ErrBadVolume},
		{"runtime", types.VolumeRegistration{Path: other, Tenants: []string{""}}, ErrBadVolume},
		{"runtime", types.VolumeRegistration{Path: other, Tenants: []string{"_leading"}}, ErrBadVolume},
		{"runtime", types.VolumeRegistration{Path: other, Tenants: []string{strings.Repeat("a", 64)}}, ErrBadVolume},
	} {
		err := m.RegisterVolume(t.Context(), tt.name, tt.spec)
		if !errors.Is(err, tt.want) {
			t.Errorf("register %s %+v = %v, want %v", tt.name, tt.spec, err, tt.want)
		}
	}
	if _, err := os.Stat(filepath.Join(m.dataDir, runtimeVolumesFile)); !errors.Is(err, os.ErrNotExist) || m.catalog.entries["runtime"] != nil {
		t.Fatalf("rejected registration persisted: stat=%v entry=%+v", err, m.catalog.entries["runtime"])
	}
	for _, tt := range []struct {
		name string
		want error
	}{{"data", ErrVolumeRegistrationConflict}, {"unexplained", ErrVolumeRegistrationConflict}, {"bad/name", ErrBadVolume}} {
		if err := m.UnregisterVolume(t.Context(), tt.name); !errors.Is(err, tt.want) {
			t.Errorf("unregister %s = %v, want %v", tt.name, err, tt.want)
		}
	}
	if err := m.RegisterVolume(t.Context(), "runtime", types.VolumeRegistration{Path: "/images/data", Tenants: []string{"future-tenant", "team/project:v1"}}); err != nil {
		t.Fatalf("future tenant: %v", err)
	}
}

func TestRuntimeCatalogVolumeRootsBoundRegistration(t *testing.T) {
	newRootedManager := func(t *testing.T, eng *fakeEngine, dataDir string, roots ...string) (*Manager, error) {
		t.Helper()
		cfg := &config.Config{DataDir: dataDir, VolumeRoots: roots}
		return NewManager(t.Context(), cfg, eng, testSecrets(t), testBackends(t, cfg))
	}
	for _, tt := range []struct {
		name, path string
		roots      []string
		want       error
	}{
		{"inside a root", "/srv/data/a.img", []string{"/mnt/other", "/srv/data"}, nil},
		{"equal to a root", "/srv/data/", []string{"/srv/data"}, nil},
		{"sibling sharing a prefix", "/srv/data2/a.img", []string{"/srv/data"}, ErrBadVolume},
		{"dot-dot out of a root", "/srv/data/../etc/a.img", []string{"/srv/data"}, ErrBadVolume},
		{"outside every root", "/etc/a.img", []string{"/srv/data"}, ErrBadVolume},
		{"filesystem root", "/etc/a.img", []string{"/"}, nil},
		{"no roots", "/etc/a.img", nil, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m, err := newRootedManager(t, newFakeEngine(), t.TempDir(), tt.roots...)
			if err != nil {
				t.Fatalf("newRootedManager: %v", err)
			}
			if err := m.RegisterVolume(t.Context(), "runtime", types.VolumeRegistration{Path: tt.path}); !errors.Is(err, tt.want) {
				t.Fatalf("register %s under %v = %v, want %v", tt.path, tt.roots, err, tt.want)
			}
		})
	}
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, nil)
	images := map[string]string{"outside": writeVolumeImage(t, "outside", "bytes"), "inside": writeVolumeImage(t, "inside", "bytes")}
	for name, path := range images {
		if err := m.RegisterVolume(t.Context(), name, types.VolumeRegistration{Path: path}); err != nil {
			t.Fatalf("RegisterVolume: %v", err)
		}
	}
	outside, inside := filepath.Dir(images["outside"]), filepath.Dir(images["inside"])
	wide, err := newRootedManager(t, eng, m.dataDir, outside, inside)
	if err != nil || len(wide.VolumeNames()) != 2 {
		t.Fatalf("restart with both registrations inside volume_roots: %v", err)
	}
	before, err := os.ReadFile(filepath.Join(m.dataDir, runtimeVolumesFile))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if _, err = newRootedManager(t, eng, m.dataDir, inside); err == nil {
		t.Fatal("restart accepted a persisted registration outside volume_roots")
	}
	after, err := os.ReadFile(filepath.Join(m.dataDir, runtimeVolumesFile))
	if err != nil || string(after) != string(before) {
		t.Fatalf("refused startup changed saved registrations: %s, %v", after, err)
	}
	if err := wide.UnregisterVolume(t.Context(), "outside"); err != nil {
		t.Fatalf("UnregisterVolume: %v", err)
	}
	if narrowed, err := newRootedManager(t, eng, m.dataDir, inside); err != nil || !slices.Equal(narrowed.VolumeNames(), []string{"inside"}) {
		t.Fatalf("restart after unregistering the out-of-root entry: %v", err)
	}
}

func TestRuntimeCatalogFailedWriteLeavesUnrelatedAdmissionsAvailable(t *testing.T) {
	unregister := func(m *Manager, _ types.VolumeRegistration) error { return m.UnregisterVolume(t.Context(), "affected") }
	register := func(m *Manager, spec types.VolumeRegistration) error {
		return m.RegisterVolume(t.Context(), "affected", spec)
	}
	for _, tt := range []struct {
		name               string
		before             []string
		write              func(*Manager, types.VolumeRegistration) error
		resolved           []types.Volume
		wantResolved, want error
	}{
		{"register", []string{"healthy"}, register, []types.Volume{{Name: "healthy"}}, nil, ErrVolumeUnavailable},
		{"unregister", []string{"healthy", "affected"}, unregister, []types.Volume{{Name: "healthy"}, {Name: "affected"}}, ErrVolumeUnavailable, ErrVolumeUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m, sb := newTenantVolumeManager(t, eng, liveCatalog(t))
			specs := map[string]types.VolumeRegistration{
				"healthy":  {Path: writeVolumeImage(t, "healthy", "bytes")},
				"affected": {Path: writeVolumeImage(t, "affected", "bytes")},
			}
			for _, name := range tt.before {
				if err := m.RegisterVolume(t.Context(), name, specs[name]); err != nil {
					t.Fatalf("RegisterVolume: %v", err)
				}
			}
			resolved, err := m.resolveVolumes(t.Context(), "", tt.resolved)
			if err != nil {
				t.Fatalf("resolveVolumes: %v", err)
			}
			blockCatalogRename(t, m)
			if err = tt.write(m, specs["affected"]); !errors.Is(err, ErrVolumeCatalogUnavailable) {
				t.Fatalf("obstructed catalog write: %v", err)
			}
			holds, hold, err := m.admitVolumes("", resolved)
			m.unreserveVolumes(holds)
			m.dropHold(hold)
			if !errors.Is(err, tt.wantResolved) {
				t.Fatalf("admission resolved before the write = %v, want %v", err, tt.wantResolved)
			}
			for _, volumes := range [][]types.Volume{{{Name: "affected"}}, {{Name: "healthy"}, {Name: "affected"}}} {
				if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: volumes}); !errors.Is(err, tt.want) {
					t.Errorf("root claim %v = %v, want %v", volumes, err, tt.want)
				}
			}
			if len(eng.volumeSpecs) != 0 {
				t.Fatal("refused request reached device attachment")
			}
			for _, name := range []string{"data", "healthy", "affected"} {
				if got := volumeHoldersOf(m, name); got != (volumeHolders{}) {
					t.Fatalf("refused request reserved %s: %+v", name, got)
				}
			}
			for _, name := range []string{"data", "healthy"} {
				if err := claimAndRelease(t, m, types.Volume{Name: name}); err != nil {
					t.Errorf("unrelated %s claim: %v", name, err)
				}
			}
			if _, err := m.SetVolumes(t.Context(), sb.ID, "acme", []types.Volume{{Name: "data"}, {Name: "healthy"}}); err != nil {
				t.Errorf("unrelated live addition: %v", err)
			}
		})
	}
}

func TestResolvedCatalogBindingRequiresSameEntry(t *testing.T) {
	m := newVolumeManager(t, newFakeEngine(), nil)
	spec := types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes")}
	if err := m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatalf("RegisterVolume: %v", err)
	}
	resolved, err := m.resolveVolumes(t.Context(), "", []types.Volume{{Name: "runtime"}})
	if err != nil {
		t.Fatalf("resolveVolumes: %v", err)
	}
	if err = m.RegisterVolume(t.Context(), "other", types.VolumeRegistration{Path: writeVolumeImage(t, "other", "bytes")}); err != nil {
		t.Fatalf("RegisterVolume: %v", err)
	}
	holds, hold, err := m.admitVolumes("", resolved)
	if err != nil {
		t.Fatalf("unrelated registration invalidated resolution: %v", err)
	}
	m.unreserveVolumes(holds)
	m.dropHold(hold)
	if err = m.UnregisterVolume(t.Context(), "runtime"); err != nil {
		t.Fatalf("UnregisterVolume: %v", err)
	}
	if err = m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatalf("RegisterVolume: %v", err)
	}
	if _, _, err = m.admitVolumes("", resolved); !errors.Is(err, ErrVolumeUnavailable) {
		t.Fatalf("same-file reuse admitted stale resolution: %v", err)
	}
	if got := volumeHoldersOf(m, "runtime"); got != (volumeHolders{}) {
		t.Fatalf("stale binding leaked reservation: %+v", got)
	}
}

func TestRuntimeCatalogRejectsInvalidMetadata(t *testing.T) {
	static := liveCatalog(t)
	for _, tt := range []struct{ name, raw string }{
		{"malformed", "{"},
		{"name", `{"bad/name":{"path":"/data"}}`},
		{"relative", `{"runtime":{"path":"relative"}}`},
		{"directio", `{"runtime":{"path":"/data","directio":"bad"}}`},
		{"duplicate-name", `{"runtime":{"path":"/one"},"runtime":{"path":"/two"}}`},
		{"duplicate-path", `{"runtime":{"path":"/one"},"second":{"path":"/one"}}`},
		{"static-name", `{"data":{"path":"/other"}}`},
		{"static-path", fmt.Sprintf(`{"runtime":{"path":%q}}`, static[0].Path)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, static)
			if err := os.WriteFile(filepath.Join(m.dataDir, runtimeVolumesFile), []byte(tt.raw), 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			cfg := &config.Config{DataDir: m.dataDir, Volumes: static}
			if _, err := NewManager(t.Context(), cfg, eng, testSecrets(t), testBackends(t, cfg)); err == nil {
				t.Fatalf("accepted %s metadata", tt.name)
			}
		})
	}
}

func blockCatalogRename(t *testing.T, m *Manager) func() {
	t.Helper()
	record := filepath.Join(m.dataDir, runtimeVolumesFile)
	raw, err := os.ReadFile(record)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadFile: %v", err)
	}
	existed := err == nil
	if err := errors.Join(os.RemoveAll(record), os.Mkdir(record, 0o700)); err != nil {
		t.Fatalf("block the catalog rename: %v", err)
	}
	restore := sync.OnceFunc(func() {
		if err := os.Remove(record); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		if existed {
			if err := os.WriteFile(record, raw, 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
		}
	})
	t.Cleanup(restore)
	return restore
}

func claimAndRelease(t *testing.T, m *Manager, volumes ...types.Volume) error {
	t.Helper()
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: volumes})
	if err != nil {
		return err
	}
	return m.Release(t.Context(), sb.ID, Cred{Token: sb.Token})
}
