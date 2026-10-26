package pool

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

func TestRuntimeCatalogPersistsNormalizedSemantics(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, nil)
	path := writeVolumeImage(t, "runtime", "backing bytes stay intact")
	spec := types.VolumeRegistration{Path: filepath.Dir(path) + "/./runtime", Writable: true, Tenants: []string{"beta", "acme", "acme"}}
	before := m.catalogSnapshot()
	if err := m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatal(err)
	}
	if before["runtime"] != nil || spec.Tenants[0] != "beta" {
		t.Fatal("registration mutated a captured catalog or the caller's spec")
	}
	normalized := types.VolumeRegistration{Path: path, DirectIO: types.DirectIOOff, Writable: true, Tenants: []string{"acme", "beta"}}
	if err := m.RegisterVolume(t.Context(), "runtime", normalized); err != nil {
		t.Fatalf("normalized retry: %v", err)
	}
	if st, err := os.Stat(filepath.Join(m.dataDir, runtimeVolumesFile)); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("metadata mode: %v %v", st, err)
	}
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "runtime"}}})
	if err != nil {
		t.Fatal(err)
	}
	m2 := newVolumeManagerAt(t, eng, m.dataDir, nil)
	if got := m2.Volumes("acme", nil); len(got) != 1 || got[0].Name != "runtime" || !got[0].Available || !got[0].Writable {
		t.Fatalf("catalog absent before reconcile: %+v", got)
	}
	if got := m2.Volumes("forbidden", nil); len(got) != 0 {
		t.Fatalf("ACL changed at restart: %+v", got)
	}
	if err = m2.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	for range 2 {
		if err = m2.UnregisterVolume(t.Context(), "runtime"); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "backing bytes stay intact" {
		t.Fatalf("backing changed: %q %v", got, err)
	}
	if got := newVolumeManagerAt(t, eng, m.dataDir, nil).VolumeNames(); len(got) != 0 {
		t.Fatalf("unregister not durable: %v", got)
	}
}

func TestRuntimeCatalogRegistersUncheckedBacking(t *testing.T) {
	link := filepath.Join(t.TempDir(), "backing-link")
	if err := os.Symlink(writeVolumeImage(t, "backing", "bytes"), link); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path string
		wantErr    error
	}{{"symlink", link, nil}, {"missing", filepath.Join(t.TempDir(), "missing"), ErrVolumeUnavailable}} {
		t.Run(tc.name, func(t *testing.T) {
			m := newVolumeManager(t, newFakeEngine(), nil)
			registration := types.VolumeRegistration{Path: tc.path, Writable: true}
			if err := m.RegisterVolume(t.Context(), "runtime", registration); err != nil {
				t.Fatalf("register: %v", err)
			}
			m2 := newVolumeManagerAt(t, newFakeEngine(), m.dataDir, nil)
			if err := m2.RegisterVolume(t.Context(), "runtime", registration); err != nil {
				t.Fatalf("idempotent retry after restart: %v", err)
			}
			if infos := m2.Volumes("", nil); len(infos) != 1 || infos[0].Available != (tc.wantErr == nil) {
				t.Fatalf("availability: %+v", infos)
			}
			if _, err := m2.resolveVolumes(t.Context(), "", []types.Volume{{Name: "runtime"}}); !errors.Is(err, tc.wantErr) {
				t.Fatalf("resolve = %v, want %v", err, tc.wantErr)
			}
			if err := m2.UnregisterVolume(t.Context(), "runtime"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRuntimeCatalogRejectsCollisionsAndInvalidRegistrations(t *testing.T) {
	static := liveCatalog(t)
	m := newVolumeManager(t, newFakeEngine(), static)
	other := writeVolumeImage(t, "other-runtime", "bytes")
	sb := mustClaim(t, m, testKey)
	sb.Volumes = []types.Volume{{Name: "unexplained", Mount: "/data"}}
	if err := m.store.commit(m.store.set(sb)); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Dir(static[0].Path) + "/./" + filepath.Base(static[0].Path)
	for _, tc := range []struct {
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
	} {
		if err := m.RegisterVolume(t.Context(), tc.name, tc.spec); !errors.Is(err, tc.want) {
			t.Errorf("register %s %+v = %v, want %v", tc.name, tc.spec, err, tc.want)
		}
	}
	for _, tc := range []struct {
		name string
		want error
	}{{"data", ErrVolumeRegistrationConflict}, {"unexplained", ErrVolumeRegistrationConflict}, {"bad/name", ErrBadVolume}} {
		if err := m.UnregisterVolume(t.Context(), tc.name); !errors.Is(err, tc.want) {
			t.Errorf("unregister %s = %v, want %v", tc.name, err, tc.want)
		}
	}
}

func TestRuntimeCatalogPendingWriteConverges(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	spec := types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes")}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := m.RegisterVolume(canceled, "runtime", spec); !errors.Is(err, context.Canceled) || errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("cancellation before acceptance: %v", err)
	}
	m.retryVolumeCatalog(t.Context())
	if _, err := os.Stat(filepath.Join(m.dataDir, runtimeVolumesFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled proposal left catalog: %v", err)
	}
	restore := blockCatalogRename(t, m)
	if err := m.RegisterVolume(t.Context(), "runtime", spec); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("write failure: %v", err)
	}
	if err := m.UnregisterVolume(t.Context(), "runtime"); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("failed retry cleared pending write: %v", err)
	}
	for _, err := range []error{m.RegisterVolume(t.Context(), "bad/name", spec), m.UnregisterVolume(t.Context(), "bad/name")} {
		if !errors.Is(err, ErrBadVolume) || errors.Is(err, ErrVolumeCatalogUnavailable) {
			t.Fatalf("invalid name behind a pending write: %v", err)
		}
	}
	m.retryVolumeCatalog(t.Context())
	for _, volumes := range [][]types.Volume{nil, {{Name: "data"}}} {
		if err := claimAndRelease(t, m, volumes...); err != nil {
			t.Fatalf("pending write blocked unrelated claim %v: %v", volumes, err)
		}
	}
	restore()
	installPendingCatalogRecord(t, m)
	if got := newVolumeManagerAt(t, eng, m.dataDir, nil).VolumeNames(); !slices.Equal(got, []string{"runtime"}) {
		t.Fatalf("possible replacement not recovered: %v", got)
	}
	if _, err := m.resolveVolumes(t.Context(), "", []types.Volume{{Name: "runtime"}}); !errors.Is(err, ErrVolumeUnavailable) {
		t.Fatalf("unpublished admission: %v", err)
	}
	if err := m.UnregisterVolume(canceled, "runtime"); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("canceled retry cleared pending write: %v", err)
	}
	m.retryVolumeCatalog(t.Context())
	if !slices.Contains(m.VolumeNames(), "runtime") {
		t.Fatal("retry lost accepted registration")
	}
	restore = blockCatalogRename(t, m)
	if err := m.UnregisterVolume(t.Context(), "runtime"); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("failed unregister: %v", err)
	}
	restore()
	installPendingCatalogRecord(t, m)
	if got := newVolumeManagerAt(t, eng, m.dataDir, nil).VolumeNames(); len(got) != 0 {
		t.Fatalf("possible unregister replacement not recovered: %v", got)
	}
	if err := m.RegisterVolume(t.Context(), "replacement", spec); err != nil {
		t.Fatalf("pending unregister did not converge before reuse: %v", err)
	}
	if got := newVolumeManagerAt(t, eng, m.dataDir, nil).VolumeNames(); !slices.Equal(got, []string{"replacement"}) {
		t.Fatalf("durable reuse: %v", got)
	}
}

func TestRuntimeCatalogPendingWriteFencesOnlyAffectedName(t *testing.T) {
	unregister := func(m *Manager, _ types.VolumeRegistration) error { return m.UnregisterVolume(t.Context(), "affected") }
	register := func(m *Manager, spec types.VolumeRegistration) error {
		return m.RegisterVolume(t.Context(), "affected", spec)
	}
	for _, tc := range []struct {
		name               string
		before             []string
		write              func(*Manager, types.VolumeRegistration) error
		resolved           []types.Volume
		wantResolved, want error
	}{
		{"register", []string{"healthy"}, register, []types.Volume{{Name: "healthy"}}, nil, ErrVolumeUnavailable},
		{"unregister", []string{"healthy", "affected"}, unregister, []types.Volume{{Name: "healthy"}, {Name: "affected"}}, ErrVolumeCatalogUnavailable, ErrVolumeCatalogUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng := newFakeEngine()
			m, sb := newTenantVolumeManager(t, eng, liveCatalog(t))
			specs := map[string]types.VolumeRegistration{
				"healthy":  {Path: writeVolumeImage(t, "healthy", "bytes")},
				"affected": {Path: writeVolumeImage(t, "affected", "bytes"), Tenants: []string{"other"}},
			}
			for _, name := range tc.before {
				if err := m.RegisterVolume(t.Context(), name, specs[name]); err != nil {
					t.Fatal(err)
				}
			}
			resolved, err := m.resolveVolumes(t.Context(), "", tc.resolved)
			if err != nil {
				t.Fatal(err)
			}
			blockCatalogRename(t, m)
			if err = tc.write(m, specs["affected"]); !errors.Is(err, ErrVolumeCatalogUnavailable) {
				t.Fatalf("obstructed catalog write: %v", err)
			}
			holds, err := m.admitVolumes("", resolved)
			m.unreserveVolumes(holds)
			if !errors.Is(err, tc.wantResolved) {
				t.Fatalf("admission resolved before the write = %v, want %v", err, tc.wantResolved)
			}
			for _, volumes := range [][]types.Volume{{{Name: "affected"}}, {{Name: "healthy"}, {Name: "affected"}}} {
				if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: volumes}); !errors.Is(err, tc.want) {
					t.Errorf("root claim %v = %v, want %v", volumes, err, tc.want)
				}
			}
			_, deniedErr := m.SetVolumes(t.Context(), sb.ID, "acme", []types.Volume{{Name: "data"}, {Name: "affected"}})
			_, missingErr := m.SetVolumes(t.Context(), sb.ID, "acme", []types.Volume{{Name: "data"}, {Name: "missing"}})
			if !errors.Is(deniedErr, ErrVolumeUnavailable) || !errors.Is(missingErr, ErrVolumeUnavailable) || deniedErr.Error() != missingErr.Error() {
				t.Errorf("private name distinguishable: denied=%v missing=%v", deniedErr, missingErr)
			}
			if len(eng.volumeSpecs) != 0 {
				t.Fatal("refused request reached device attachment")
			}
			for _, name := range []string{"data", "healthy", "affected", "missing"} {
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
		t.Fatal(err)
	}
	resolved, err := m.resolveVolumes(t.Context(), "", []types.Volume{{Name: "runtime"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.RegisterVolume(t.Context(), "other", types.VolumeRegistration{Path: writeVolumeImage(t, "other", "bytes")}); err != nil {
		t.Fatal(err)
	}
	holds, err := m.admitVolumes("", resolved)
	if err != nil {
		t.Fatalf("unrelated registration invalidated resolution: %v", err)
	}
	m.unreserveVolumes(holds)
	if err = m.UnregisterVolume(t.Context(), "runtime"); err != nil {
		t.Fatal(err)
	}
	if err = m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatal(err)
	}
	if _, err = m.admitVolumes("", resolved); !errors.Is(err, ErrVolumeUnavailable) {
		t.Fatalf("same-file reuse admitted stale resolution: %v", err)
	}
	if got := volumeHoldersOf(m, "runtime"); got != (volumeHolders{}) {
		t.Fatalf("stale binding leaked reservation: %+v", got)
	}
}

func TestRuntimeCatalogRejectsInvalidMetadata(t *testing.T) {
	static := liveCatalog(t)
	for _, tc := range []struct{ name, raw string }{
		{"malformed", "{"},
		{"version", `{"version":2,"volumes":{}}`},
		{"null", `{"version":1,"volumes":null}`},
		{"name", `{"version":1,"volumes":{"bad/name":{"path":"/data"}}}`},
		{"relative", `{"version":1,"volumes":{"runtime":{"path":"relative"}}}`},
		{"directio", `{"version":1,"volumes":{"runtime":{"path":"/data","directio":"bad"}}}`},
		{"duplicate-name", `{"version":1,"volumes":{"runtime":{"path":"/one"},"runtime":{"path":"/two"}}}`},
		{"duplicate-path", `{"version":1,"volumes":{"runtime":{"path":"/one"},"second":{"path":"/one"}}}`},
		{"static-name", `{"version":1,"volumes":{"data":{"path":"/other"}}}`},
		{"static-path", fmt.Sprintf(`{"version":1,"volumes":{"runtime":{"path":%q}}}`, static[0].Path)},
		{"unknown-field", `{"version":1,"volumes":{},"unexpected":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, static)
			if err := os.WriteFile(filepath.Join(m.dataDir, runtimeVolumesFile), []byte(tc.raw), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg := &config.Config{DataDir: m.dataDir, Volumes: static}
			if _, err := NewManager(t.Context(), cfg, eng, testSecrets(t), testBackends(t, cfg)); err == nil {
				t.Fatalf("accepted %s metadata", tc.name)
			}
		})
	}
}

func TestRuntimeCatalogAcceptanceRechecksReferences(t *testing.T) {
	m := newVolumeManager(t, newFakeEngine(), nil)
	if err := m.RegisterVolume(t.Context(), "runtime", types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes")}); err != nil {
		t.Fatal(err)
	}
	entries := maps.Clone(m.catalogSnapshot())
	delete(entries, "runtime")
	resolved, err := m.resolveVolumes(t.Context(), "", []types.Volume{{Name: "runtime"}})
	if err != nil {
		t.Fatal(err)
	}
	holds, err := m.admitVolumes("", resolved)
	if err != nil {
		t.Fatal(err)
	}
	m.catalog.writeMu.Lock()
	defer m.catalog.writeMu.Unlock()
	if err := m.writeCatalog(t.Context(), "runtime", entries); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("accepted removal prepared before reservation: %v", err)
	}
	m.unreserveVolumes(holds)
	if err := m.writeCatalog(t.Context(), "runtime", entries); err != nil {
		t.Fatalf("reservation retirement did not unblock removal: %v", err)
	}
}

func blockCatalogRename(t *testing.T, m *Manager) func() {
	t.Helper()
	record := filepath.Join(m.dataDir, runtimeVolumesFile)
	raw, err := os.ReadFile(record)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	existed := err == nil
	if err := errors.Join(os.RemoveAll(record), os.Mkdir(record, 0o700)); err != nil {
		t.Fatal(err)
	}
	restore := sync.OnceFunc(func() {
		if err := os.Remove(record); err != nil {
			t.Fatal(err)
		}
		if existed {
			if err := os.WriteFile(record, raw, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Cleanup(restore)
	return restore
}

// installPendingCatalogRecord models the canonical bytes after a successful rename and an uncertain directory sync.
func installPendingCatalogRecord(t *testing.T, m *Manager) {
	t.Helper()
	m.mu.Lock()
	proposal := m.catalog.pending
	m.mu.Unlock()
	if proposal == nil {
		t.Fatal("possible replacement fixture requires an accepted pending write")
	}
	if err := utils.WriteFileSync(filepath.Join(m.dataDir, runtimeVolumesFile), proposal.raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
