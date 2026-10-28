package pool

import (
	"encoding/json/v2"
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
		{"runtime", types.VolumeRegistration{Path: "/srv/\xff"}, ErrBadVolume},
	} {
		if err := m.RegisterVolume(t.Context(), tc.name, tc.spec); !errors.Is(err, tc.want) {
			t.Errorf("register %s %+v = %v, want %v", tc.name, tc.spec, err, tc.want)
		}
	}
	if _, err := os.Stat(filepath.Join(m.dataDir, runtimeVolumesFile)); !errors.Is(err, os.ErrNotExist) || m.catalog.entries["runtime"] != nil {
		t.Fatalf("rejected registration persisted: stat=%v entry=%+v", err, m.catalog.entries["runtime"])
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

func TestRuntimeCatalogVolumeRootsBoundRegistration(t *testing.T) {
	newRootedManager := func(t *testing.T, eng *fakeEngine, dataDir string, roots ...string) (*Manager, error) {
		t.Helper()
		cfg := &config.Config{DataDir: dataDir, VolumeRoots: roots}
		return NewManager(t.Context(), cfg, eng, testSecrets(t), testBackends(t, cfg))
	}
	for _, tc := range []struct {
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
		t.Run(tc.name, func(t *testing.T) {
			m, err := newRootedManager(t, newFakeEngine(), t.TempDir(), tc.roots...)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.RegisterVolume(t.Context(), "runtime", types.VolumeRegistration{Path: tc.path}); !errors.Is(err, tc.want) {
				t.Fatalf("register %s under %v = %v, want %v", tc.path, tc.roots, err, tc.want)
			}
		})
	}
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, nil)
	images := map[string]string{"outside": writeVolumeImage(t, "outside", "bytes"), "inside": writeVolumeImage(t, "inside", "bytes")}
	for name, path := range images {
		if err := m.RegisterVolume(t.Context(), name, types.VolumeRegistration{Path: path}); err != nil {
			t.Fatal(err)
		}
	}
	outside, inside := filepath.Dir(images["outside"]), filepath.Dir(images["inside"])
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "outside"}}})
	if err != nil {
		t.Fatal(err)
	}
	wide, err := newRootedManager(t, eng, m.dataDir, outside, inside)
	if err != nil || wide.catalogSnapshot()["outside"] == nil {
		t.Fatalf("restart with the registration inside volume_roots: %v", err)
	}
	m2, err := newRootedManager(t, eng, m.dataDir, inside)
	if err != nil {
		t.Fatalf("restart refused a persisted registration outside volume_roots: %v", err)
	}
	if err = m2.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := m2.catalogSnapshot(); got["outside"] != nil || got["inside"] == nil {
		t.Fatalf("narrowed catalog = %v, want only the in-root registration", slices.Sorted(maps.Keys(got)))
	}
	if got := volumeHoldersOf(m2, "outside"); got.readers != 1 {
		t.Fatalf("recovered claim holders of the skipped registration = %+v", got)
	}
	if _, err = m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "outside"}}}); !errors.Is(err, ErrVolumeUnavailable) {
		t.Fatalf("claim of the skipped registration = %v, want ErrVolumeUnavailable", err)
	}
	if err = m2.UnregisterVolume(t.Context(), "outside"); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("delete of referenced skipped name: %v", err)
	}
	if err = m2.RegisterVolume(t.Context(), "outside", types.VolumeRegistration{Path: filepath.Join(inside, "replacement")}); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("rebind of referenced skipped name: %v", err)
	}
	if err = claimAndRelease(t, m2, types.Volume{Name: "inside"}); err != nil {
		t.Fatalf("in-root registration: %v", err)
	}
	if err = m2.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("release of a recovered claim holding the skipped registration: %v", err)
	}
	if err = m2.RegisterVolume(t.Context(), "later", types.VolumeRegistration{Path: filepath.Join(inside, "later")}); err != nil {
		t.Fatal(err)
	}
	if wide, err = newRootedManager(t, eng, m.dataDir, outside, inside); err != nil || wide.catalogSnapshot()["outside"] != nil {
		t.Fatalf("a later catalog write kept the skipped registration: %v", err)
	}
}

func TestRuntimeCatalogFailedWriteLeavesUnrelatedAdmissionsAvailable(t *testing.T) {
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
		{"unregister", []string{"healthy", "affected"}, unregister, []types.Volume{{Name: "healthy"}, {Name: "affected"}}, ErrVolumeUnavailable, ErrVolumeUnavailable},
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

func TestAdoptedClaimReferencesVolumeBeforeItsClaimWrite(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	if err := b.RegisterVolume(t.Context(), "data", types.VolumeRegistration{Path: writeVolumeImage(t, "adopted", "bytes")}); err != nil {
		t.Fatal(err)
	}
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	rec, _, err := c.reg.Get(t.Context(), sb.ID)
	claim := &types.Sandbox{}
	if err == nil {
		err = json.Unmarshal(rec.Claim, claim)
	}
	// the row is a peer's record, so the adopter cannot assume it names no volumes
	claim.Volumes = []types.Volume{{Name: "data", Mount: "/data"}}
	if err == nil {
		rec.Claim, err = json.Marshal(claim)
	}
	if err == nil {
		_, err = c.reg.Put(t.Context(), rec)
	}
	if err != nil {
		t.Fatal(err)
	}
	c.expire("a")
	// an earlier claims.json write in flight keeps the adoption's write from starting
	b.store.writeMu.Lock()
	unlock := sync.OnceFunc(b.store.writeMu.Unlock)
	t.Cleanup(unlock)
	adopted := make(chan error, 1)
	go func() {
		ok, adoptErr := b.AdoptArchived(t.Context(), sb.ID, sb.Token)
		if adoptErr == nil && !ok {
			adoptErr = errors.New("not adopted")
		}
		adopted <- adoptErr
	}()
	waitFor(t, func() bool { _, held := b.byID(sb.ID); return held })
	err = b.UnregisterVolume(t.Context(), "data")
	unlock()
	if adoptErr := <-adopted; adoptErr != nil {
		t.Fatal(adoptErr)
	}
	if !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("unregister while a published adoption references the volume = %v", err)
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

func claimAndRelease(t *testing.T, m *Manager, volumes ...types.Volume) error {
	t.Helper()
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: volumes})
	if err != nil {
		return err
	}
	return m.Release(t.Context(), sb.ID, Cred{Token: sb.Token})
}
