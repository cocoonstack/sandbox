package pool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

func TestRuntimeCatalogPersistsNormalizedSemantics(t *testing.T) {
	requireCatalogRoot(t)
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, nil)
	path := writeVolumeImage(t, "runtime", "backing bytes stay intact")
	spec := types.VolumeRegistration{Path: filepath.Dir(path) + "/./runtime", Writable: true, Tenants: []string{"beta", "acme", "acme"}}
	if err := m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatal(err)
	}
	spec.Path = path
	spec.DirectIO = types.DirectIOOff
	spec.Tenants = []string{"acme", "beta"}
	if err := m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatalf("normalized retry: %v", err)
	}
	st, statErr := os.Stat(filepath.Join(m.dataDir, "runtime-volumes.json"))
	if statErr != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("metadata mode: %v %v", st, statErr)
	}
	m2 := newVolumeManagerAt(t, eng, m.dataDir, nil)
	if got := m2.VolumeNames(); !slices.Equal(got, []string{"runtime"}) {
		t.Fatalf("recovered names: %v", got)
	}
	if _, err := m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "forbidden", Volumes: []types.Volume{{Name: "runtime"}}}); !errors.Is(err, ErrVolumeUnavailable) {
		t.Fatalf("ACL changed at restart: %v", err)
	}
	resolved, err := m2.resolveVolumes(t.Context(), "acme", []types.Volume{{Name: "runtime", Mode: types.VolumeModeRW}})
	if err != nil || len(resolved) != 1 || resolved[0].disk.DirectIO != "off" || !resolved[0].disk.RW {
		t.Fatalf("policy roundtrip: %+v %v", resolved, err)
	}
	changed := spec
	changed.DirectIO = "on"
	if err := m2.RegisterVolume(t.Context(), "runtime", changed); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("changed semantics: %v", err)
	}
	if err := m2.UnregisterVolume(t.Context(), "runtime"); err != nil {
		t.Fatal(err)
	}
	if err := m2.UnregisterVolume(t.Context(), "runtime"); err != nil {
		t.Fatalf("absent retry: %v", err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "backing bytes stay intact" {
		t.Fatalf("backing changed: %q %v", got, err)
	}
	if got := newVolumeManagerAt(t, eng, m.dataDir, nil).VolumeNames(); len(got) != 0 {
		t.Fatalf("unregister not durable: %v", got)
	}
}

func TestRuntimeCatalogCollisionsAndRecoveryNames(t *testing.T) {
	requireCatalogRoot(t)
	eng := newFakeEngine()
	static := liveCatalog(t)
	m := newVolumeManager(t, eng, static)
	other := writeVolumeImage(t, "other-runtime", "bytes")
	for _, tc := range []struct{ name, path string }{{"data", other}, {"runtime", static[0].Path}, {"runtime", filepath.Dir(static[0].Path) + "/./" + filepath.Base(static[0].Path)}} {
		if err := m.RegisterVolume(t.Context(), tc.name, types.VolumeRegistration{Path: tc.path}); !errors.Is(err, ErrVolumeRegistrationConflict) {
			t.Fatalf("collision %+v: %v", tc, err)
		}
	}
	if err := m.UnregisterVolume(t.Context(), "data"); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("static removal: %v", err)
	}
	sb := mustClaim(t, m, testKey)
	sb.Volumes = []types.Volume{{Name: "unexplained", Mount: "/data"}}
	if err := m.store.commit(m.store.set(sb)); err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterVolume(t.Context(), "unexplained", types.VolumeRegistration{Path: other}); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("rebound unexplained recovery name: %v", err)
	}
	if err := m.UnregisterVolume(t.Context(), "unexplained"); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("absent referenced removal: %v", err)
	}
	for _, spec := range []types.VolumeRegistration{{Path: "relative"}, {Path: other, DirectIO: "invalid"}, {Path: other + "missing"}} {
		if err := m.RegisterVolume(t.Context(), "runtime", spec); !errors.Is(err, ErrBadVolume) {
			t.Fatalf("invalid spec %+v: %v", spec, err)
		}
	}
}

func TestRuntimeCatalogPostRenameFailureFencesAndConverges(t *testing.T) {
	requireCatalogRoot(t)
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, nil)
	path := writeVolumeImage(t, "runtime", "bytes")
	m.catalogReplace = func(path string, raw []byte) error {
		if err := utils.WriteFileSync(path, raw, 0o600); err != nil {
			return err
		}
		return errors.New("directory sync failed after rename")
	}
	spec := types.VolumeRegistration{Path: path, Writable: true}
	if err := m.RegisterVolume(t.Context(), "runtime", spec); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("post-rename failure: %v", err)
	}
	m2 := newVolumeManagerAt(t, eng, m.dataDir, nil)
	if !slices.Equal(m2.VolumeNames(), []string{"runtime"}) {
		t.Fatal("actual renamed record not recovered")
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "runtime"}}}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("uncertain admission: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); err != nil {
		t.Fatalf("volume-free claim blocked: %v", err)
	}
	m.catalogReplace = nil
	if err := m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatalf("retry pending register: %v", err)
	}
	m.catalogReplace = func(path string, raw []byte) error {
		if err := utils.WriteFileSync(path, raw, 0o600); err != nil {
			return err
		}
		return errors.New("directory sync failed after rename")
	}
	if err := m.UnregisterVolume(t.Context(), "runtime"); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("uncertain unregister: %v", err)
	}
	if got := newVolumeManagerAt(t, eng, m.dataDir, nil).VolumeNames(); len(got) != 0 {
		t.Fatalf("unregister rename did not happen: %v", got)
	}
	if err := m.RegisterVolume(t.Context(), "replacement", types.VolumeRegistration{Path: path}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("pending unregister reuse: %v", err)
	}
	m.catalogReplace = nil
	if err := m.RegisterVolume(t.Context(), "replacement", types.VolumeRegistration{Path: path}); err != nil {
		t.Fatalf("retry did not converge before new request: %v", err)
	}
}

func TestResolvedCatalogBindingCannotSurviveUnregisterReuse(t *testing.T) {
	requireCatalogRoot(t)
	m := newVolumeManager(t, newFakeEngine(), nil)
	path := writeVolumeImage(t, "runtime", "original")
	spec := types.VolumeRegistration{Path: path, Writable: true}
	if err := m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatal(err)
	}
	resolved, err := m.resolveVolumes(t.Context(), "", []types.Volume{{Name: "runtime"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.UnregisterVolume(t.Context(), "runtime"); err != nil {
		t.Fatal(err)
	}
	spec.Path = writeVolumeImage(t, "replacement", "replacement")
	if err := m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := m.admitVolumes("", resolved); !errors.Is(err, ErrVolumeUnavailable) {
		t.Fatalf("stale binding admitted: %v", err)
	}
	if got := volumeHoldersOf(m, "runtime"); got != (volumeHolders{}) {
		t.Fatalf("stale binding leaked reservation: %+v", got)
	}
}

func TestRuntimeCatalogMissingBackingAndUnrestrictedBackingOwner(t *testing.T) {
	requireCatalogRoot(t)
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, nil)
	path := writeVolumeImage(t, "runtime", "bytes")
	if err := os.Chown(path, 65534, 65534); err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterVolume(t.Context(), "runtime", types.VolumeRegistration{Path: path}); err != nil {
		t.Fatalf("nonroot backing owner: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	m2 := newVolumeManagerAt(t, eng, m.dataDir, nil)
	infos := m2.Volumes("", nil)
	if len(infos) != 1 || infos[0].Available {
		t.Fatalf("missing backing lost mapping: %+v", infos)
	}
	if _, err := m2.resolveVolumes(t.Context(), "", []types.Volume{{Name: "runtime"}}); !errors.Is(err, ErrVolumeUnavailable) {
		t.Fatalf("missing backing resolved: %v", err)
	}
	if err := m2.RegisterVolume(t.Context(), "runtime", types.VolumeRegistration{Path: path}); err != nil {
		t.Fatalf("idempotent missing mapping: %v", err)
	}
}

func requireCatalogRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("real runtime metadata mutation requires effective uid zero")
	}
}

func TestRuntimeRegistrationNormalization(t *testing.T) {
	input := types.VolumeRegistration{Path: "/images/./data", Tenants: []string{"beta", "acme", "acme"}}
	normalized, err := normalizeRegistration("data", input)
	if err != nil || normalized.Path != "/images/data" || normalized.DirectIO != types.DirectIOOff || !slices.Equal(normalized.Tenants, []string{"acme", "beta"}) {
		t.Fatalf("normalization: %+v %v", normalized, err)
	}
	normalized.Tenants[0] = "caller-copy"
	if input.Tenants[1] != "acme" {
		t.Fatal("normalization mutated caller input")
	}
	for _, tc := range []struct {
		name string
		spec types.VolumeRegistration
	}{{"cocoon-reserved", input}, {"bad/name", input}, {"data", types.VolumeRegistration{Path: "relative"}}, {"data", types.VolumeRegistration{Path: "/data", DirectIO: "bad"}}} {
		if _, err := normalizeRegistration(tc.name, tc.spec); !errors.Is(err, ErrBadVolume) {
			t.Fatalf("invalid registration %+v: %v", tc, err)
		}
	}
}

func TestStaticOnlyCatalogDoesNotNeedRoot(t *testing.T) {
	m := newVolumeManager(t, newFakeEngine(), liveCatalog(t))
	if len(m.VolumeNames()) == 0 {
		t.Fatal("static-only startup lost catalog")
	}
	if os.Geteuid() != 0 {
		if err := m.RegisterVolume(t.Context(), "runtime", types.VolumeRegistration{Path: "/data"}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
			t.Fatalf("nonroot write: %v", err)
		}
	}
}

func TestRuntimeCatalogRejectsInvalidMetadata(t *testing.T) {
	requireCatalogRoot(t)
	for _, kind := range []string{"malformed", "version", "null", "name", "relative", "directio", "duplicate-name", "duplicate-path", "static-name", "static-path", "unknown-field", "symlink", "mode", "owner", "directory"} {
		t.Run(kind, func(t *testing.T) {
			eng := newFakeEngine()
			static := liveCatalog(t)
			m := newVolumeManager(t, eng, static)
			path := writeVolumeImage(t, "runtime", "bytes")
			spec := fmt.Sprintf(`{"path":%q,"directio":"auto"}`, path)
			raw := fmt.Sprintf(`{"version":1,"volumes":{"runtime":%s}}`, spec)
			switch kind {
			case "malformed":
				raw = "{"
			case "version":
				raw = `{"version":2,"volumes":{}}`
			case "null":
				raw = `{"version":1,"volumes":null}`
			case "name":
				raw = fmt.Sprintf(`{"version":1,"volumes":{"bad/name":%s}}`, spec)
			case "relative":
				raw = `{"version":1,"volumes":{"runtime":{"path":"relative"}}}`
			case "directio":
				raw = `{"version":1,"volumes":{"runtime":{"path":"/data","directio":"bad"}}}`
			case "duplicate-name":
				raw = fmt.Sprintf(`{"version":1,"volumes":{"runtime":%s,"runtime":%s}}`, spec, spec)
			case "duplicate-path":
				raw = fmt.Sprintf(`{"version":1,"volumes":{"runtime":%s,"second":%s}}`, spec, spec)
			case "static-name":
				raw = fmt.Sprintf(`{"version":1,"volumes":{"data":%s}}`, spec)
			case "static-path":
				raw = fmt.Sprintf(`{"version":1,"volumes":{"runtime":{"path":%q}}}`, static[0].Path)
			case "unknown-field":
				raw = `{"version":1,"volumes":{},"unexpected":true}`
			}
			record := filepath.Join(m.dataDir, runtimeVolumesFile)
			if err := os.WriteFile(record, []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "symlink":
				if err := os.Rename(record, record+".target"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(record+".target", record); err != nil {
					t.Fatal(err)
				}
			case "mode":
				if err := os.Chmod(record, 0o644); err != nil {
					t.Fatal(err)
				}
			case "owner":
				if err := os.Chown(record, 65534, 65534); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Chmod(m.dataDir, 0o777); err != nil {
					t.Fatal(err)
				}
			}
			cfg := &config.Config{DataDir: m.dataDir, Volumes: static}
			if _, err := NewManager(t.Context(), cfg, eng, testSecrets(t), testBackends(t, cfg)); err == nil {
				t.Fatalf("accepted %s metadata", kind)
			}
		})
	}
}

func TestRuntimeCatalogRetirementBlocksUnregisterAndReuse(t *testing.T) {
	requireCatalogRoot(t)
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, nil)
	path := writeVolumeImage(t, "runtime", "unchanged")
	spec := types.VolumeRegistration{Path: path, Writable: true}
	if err := m.RegisterVolume(t.Context(), "data", spec); err != nil {
		t.Fatal(err)
	}
	sb := mustClaim(t, m, testKey)
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); err != nil {
		t.Fatal(err)
	}
	m.store.recommitting.Store(true)
	tmp := m.store.path + ".tmp"
	eng.liveVolumeHook = func(verb string) error {
		if verb == "detach" {
			return os.Mkdir(tmp, 0o700)
		}
		return nil
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err == nil {
		t.Fatal("expected failed result commit")
	}
	eng.liveVolumeHook = nil
	if err := m.UnregisterVolume(t.Context(), "data"); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("unregister unretired name: %v", err)
	}
	if err := m.RegisterVolume(t.Context(), "data", types.VolumeRegistration{Path: writeVolumeImage(t, "different", "different")}); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("rebind unretired name: %v", err)
	}
	if err := m.RegisterVolume(t.Context(), "same-file", spec); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("alias unretired path: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("same-file writer: %v", err)
	}
	if _, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "retirement-fenced", ""); !errors.Is(err, ErrVolumeCapture) {
		t.Fatalf("capture: %v", err)
	}
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrVolumeCapture) {
		t.Fatalf("hibernate before retirement: %v", err)
	}
	if err := os.Remove(tmp); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err != nil {
		t.Fatal(err)
	}
	if err := m.UnregisterVolume(t.Context(), "data"); err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterVolume(t.Context(), "same-file", spec); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeCatalogIOLeavesManagerAndTransitionUnlocked(t *testing.T) {
	requireCatalogRoot(t)
	for _, operation := range []string{"stat", "replace"} {
		t.Run(operation, func(t *testing.T) {
			m := newVolumeManager(t, newFakeEngine(), liveCatalog(t))
			sb := mustClaim(t, m, testKey)
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			barrier := func() { once.Do(func() { close(entered) }); <-resume }
			if operation == "stat" {
				m.volumeStat = func(path string) (os.FileInfo, error) { barrier(); return os.Stat(path) }
			} else {
				m.catalogReplace = func(path string, raw []byte) error {
					if err := utils.WriteFileSync(path, raw, 0o600); err != nil {
						return err
					}
					barrier()
					return nil
				}
			}
			done := make(chan error, 1)
			go func() {
				if operation == "stat" {
					_, err := m.resolveVolumes(t.Context(), "", []types.Volume{{Name: "data"}})
					done <- err
				} else {
					done <- m.RegisterVolume(t.Context(), "runtime", types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes")})
				}
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("I/O did not block")
			}
			if !m.mu.TryLock() {
				t.Fatal("I/O holds manager lock")
			}
			m.mu.Unlock()
			if !sb.Transition.TryLock() {
				t.Fatal("I/O holds transition lock")
			}
			sb.Transition.Unlock()
			close(resume)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
