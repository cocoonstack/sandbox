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

func TestRuntimeCatalogLoadsOperatorManagedRecord(t *testing.T) {
	for _, setup := range []struct {
		name    string
		prepare func(*testing.T, string)
	}{
		{"mode", func(t *testing.T, record string) {
			if err := os.Chmod(record, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlink", func(t *testing.T, record string) {
			if err := os.Rename(record, record+".target"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(record+".target", record); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(setup.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, nil)
			if err := m.RegisterVolume(t.Context(), "runtime", types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes")}); err != nil {
				t.Fatal(err)
			}
			setup.prepare(t, filepath.Join(m.dataDir, runtimeVolumesFile))
			restarted := newVolumeManagerAt(t, eng, m.dataDir, nil)
			if got := restarted.VolumeNames(); !slices.Equal(got, []string{"runtime"}) {
				t.Fatalf("operator-managed record not loaded: %v", got)
			}
			if err := restarted.UnregisterVolume(t.Context(), "runtime"); err != nil {
				t.Fatal(err)
			}
			if got := newVolumeManagerAt(t, eng, m.dataDir, nil).VolumeNames(); len(got) != 0 {
				t.Fatalf("replacement not loaded: %v", got)
			}
		})
	}
}

func TestRuntimeCatalogUsesStaticPathValidation(t *testing.T) {
	regular := writeVolumeImage(t, "backing", "unchanged")
	link := filepath.Join(t.TempDir(), "backing-link")
	if err := os.Symlink(regular, link); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{regular, link, os.DevNull, t.TempDir(), filepath.Join(t.TempDir(), "missing")} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			spec := config.VolumeSpec{Name: "runtime", Path: path, DirectIO: types.DirectIOOff, Writable: true}
			if err := config.ValidateVolumeCatalog([]config.VolumeSpec{spec}); err != nil {
				t.Fatalf("static validation: %v", err)
			}
			m := newVolumeManager(t, newFakeEngine(), nil)
			registration := types.VolumeRegistration{Path: path, Writable: true}
			if err := m.RegisterVolume(t.Context(), "runtime", registration); err != nil {
				t.Fatalf("runtime validation differs from static: %v", err)
			}
			m2 := newVolumeManagerAt(t, newFakeEngine(), m.dataDir, nil)
			if err := m2.RegisterVolume(t.Context(), "runtime", registration); err != nil {
				t.Fatalf("idempotent retry after restart: %v", err)
			}
			if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
				if _, resolveErr := m2.resolveVolumes(t.Context(), "", []types.Volume{{Name: "runtime"}}); !errors.Is(resolveErr, ErrVolumeUnavailable) {
					t.Fatalf("missing backing resolved: %v", resolveErr)
				}
			}
			if err := m2.UnregisterVolume(t.Context(), "runtime"); err != nil {
				t.Fatal(err)
			}
		})
	}
	if got, err := os.ReadFile(regular); err != nil || string(got) != "unchanged" {
		t.Fatalf("registration touched backing: %q %v", got, err)
	}
}

func TestRuntimeCatalogCollisionsAndRecoveryNames(t *testing.T) {
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
	for _, spec := range []types.VolumeRegistration{{Path: "relative"}, {Path: other, DirectIO: "invalid"}} {
		if err := m.RegisterVolume(t.Context(), "runtime", spec); !errors.Is(err, ErrBadVolume) {
			t.Fatalf("invalid spec %+v: %v", spec, err)
		}
	}
}

func TestRuntimeCatalogWriteFailureFencesAndConverges(t *testing.T) {
	m := newVolumeManager(t, newFakeEngine(), nil)
	m.store.path = filepath.Join(t.TempDir(), "claims.json")
	spec := types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes"), Writable: true}
	restore := blockCatalogRename(t, m)
	if err := m.RegisterVolume(t.Context(), "runtime", spec); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("write failure: %v", err)
	}
	if _, err := m.resolveVolumes(t.Context(), "", []types.Volume{{Name: "runtime"}}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("pending admission: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); err != nil {
		t.Fatalf("volume-free claim blocked: %v", err)
	}
	restore()
	if err := m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatalf("retry pending register: %v", err)
	}
	if got := newVolumeManagerAt(t, newFakeEngine(), m.dataDir, nil).VolumeNames(); !slices.Equal(got, []string{"runtime"}) {
		t.Fatalf("durable registration: %v", got)
	}
	restore = blockCatalogRename(t, m)
	if err := m.UnregisterVolume(t.Context(), "runtime"); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("failed unregister: %v", err)
	}
	restore()
	if err := m.RegisterVolume(t.Context(), "replacement", spec); err != nil {
		t.Fatalf("pending unregister did not converge before reuse: %v", err)
	}
	if got := newVolumeManagerAt(t, newFakeEngine(), m.dataDir, nil).VolumeNames(); !slices.Equal(got, []string{"replacement"}) {
		t.Fatalf("durable reuse: %v", got)
	}
}

func TestRuntimeCatalogWriteFailureOnlyFencesAffectedName(t *testing.T) {
	for _, unregister := range []bool{false, true} {
		t.Run(fmt.Sprintf("unregister=%t", unregister), func(t *testing.T) {
			m := newVolumeManager(t, newFakeEngine(), liveCatalog(t))
			for _, name := range []string{"healthy", "affected"} {
				if name == "affected" && !unregister {
					continue
				}
				if err := m.RegisterVolume(t.Context(), name, types.VolumeRegistration{Path: writeVolumeImage(t, name, "bytes")}); err != nil {
					t.Fatal(err)
				}
			}
			sb := mustClaim(t, m, testKey)
			var before []resolvedVolume
			if unregister {
				var err error
				before, err = m.resolveVolumes(t.Context(), "", []types.Volume{{Name: "healthy"}, {Name: "affected"}})
				if err != nil {
					t.Fatal(err)
				}
			}
			restore := blockCatalogRename(t, m)
			defer restore()
			var err error
			if unregister {
				err = m.UnregisterVolume(t.Context(), "affected")
			} else {
				err = m.RegisterVolume(t.Context(), "affected", types.VolumeRegistration{Path: writeVolumeImage(t, "affected", "bytes")})
			}
			if !errors.Is(err, ErrVolumeCatalogUnavailable) {
				t.Fatalf("obstructed catalog write: %v", err)
			}
			if unregister {
				if _, err := m.admitVolumes("", before); !errors.Is(err, ErrVolumeCatalogUnavailable) {
					t.Fatalf("resolved mixed admission: %v", err)
				}
			}
			for _, volumes := range [][]types.Volume{{{Name: "affected"}}, {{Name: "healthy"}, {Name: "affected"}}} {
				if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: volumes}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
					t.Fatalf("affected claim: %v", err)
				}
				if _, err := m.SetVolumes(t.Context(), sb.ID, "", volumes); !errors.Is(err, ErrVolumeCatalogUnavailable) {
					t.Fatalf("affected live addition: %v", err)
				}
			}
			for _, name := range []string{"healthy", "affected"} {
				if got := volumeHoldersOf(m, name); got != (volumeHolders{}) {
					t.Fatalf("refused request reserved %s: %+v", name, got)
				}
			}
			for _, name := range []string{"data", "healthy"} {
				if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: name}}}); err != nil {
					t.Errorf("unrelated %s claim: %v", name, err)
				}
			}
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}, {Name: "healthy"}}); err != nil {
				t.Errorf("unrelated live addition: %v", err)
			}
		})
	}
}

func TestRuntimeCatalogPossibleReplacementFencesAndConverges(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, nil)
	spec := types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes"), Writable: true}
	restore := blockCatalogRename(t, m)
	if err := m.RegisterVolume(t.Context(), "runtime", spec); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("write failure: %v", err)
	}
	restore()
	installPendingCatalogRecord(t, m)
	if got := newVolumeManagerAt(t, eng, m.dataDir, nil).VolumeNames(); !slices.Equal(got, []string{"runtime"}) {
		t.Fatalf("possible replacement not recovered: %v", got)
	}
	if _, err := m.resolveVolumes(t.Context(), "", []types.Volume{{Name: "runtime"}}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("uncertain admission: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); err != nil {
		t.Fatalf("volume-free claim blocked: %v", err)
	}
	if err := m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatalf("retry pending register: %v", err)
	}
	restore = blockCatalogRename(t, m)
	if err := m.UnregisterVolume(t.Context(), "runtime"); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("uncertain unregister: %v", err)
	}
	restore()
	installPendingCatalogRecord(t, m)
	if got := newVolumeManagerAt(t, eng, m.dataDir, nil).VolumeNames(); len(got) != 0 {
		t.Fatalf("possible unregister replacement not recovered: %v", got)
	}
	if err := m.RegisterVolume(t.Context(), "replacement", spec); err != nil {
		t.Fatalf("pending unregister did not converge before reuse: %v", err)
	}
}

func TestResolvedCatalogBindingCannotSurviveUnregisterReuse(t *testing.T) {
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
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, nil)
	path := writeVolumeImage(t, "runtime", "bytes")
	if os.Geteuid() == 0 {
		if err := os.Chown(path, 65534, 65534); err != nil {
			t.Fatal(err)
		}
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
	}{{"cocoon-reserved", input}, {"bad/name", input}, {"data", types.VolumeRegistration{Path: "relative"}}, {"data", types.VolumeRegistration{Path: "/data", DirectIO: "bad"}}, {"data", types.VolumeRegistration{Path: "/data", Tenants: []string{""}}}, {"data", types.VolumeRegistration{Path: "/data", Tenants: []string{"has space"}}}} {
		if _, err := normalizeRegistration(tc.name, tc.spec); !errors.Is(err, ErrBadVolume) {
			t.Fatalf("invalid registration %+v: %v", tc, err)
		}
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

func TestRuntimeCatalogRetirementBlocksUnregisterAndReuse(t *testing.T) {
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

func TestCatalogSnapshotSurvivesRegistration(t *testing.T) {
	m := newVolumeManager(t, newFakeEngine(), liveCatalog(t))
	before := m.catalogSnapshot()
	if err := m.RegisterVolume(t.Context(), "runtime", types.VolumeRegistration{Path: "/images/runtime"}); err != nil {
		t.Fatal(err)
	}
	if before["runtime"] != nil {
		t.Fatal("registration mutated captured catalog")
	}
	captured := m.catalogSnapshot()
	if err := m.UnregisterVolume(t.Context(), "runtime"); err != nil {
		t.Fatal(err)
	}
	if captured["runtime"] == nil {
		t.Fatal("unregister mutated captured catalog")
	}
}

func TestRuntimeCatalogCancellationBeforeAcceptanceDoesNotFenceClaims(t *testing.T) {
	m := newVolumeManager(t, newFakeEngine(), liveCatalog(t))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := m.RegisterVolume(ctx, "runtime", types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes")})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("cancellation before acceptance: %v", err)
	}
	m.retryVolumeCatalog(t.Context())
	if _, err := os.Stat(filepath.Join(m.dataDir, runtimeVolumesFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled proposal left catalog: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}}); err != nil {
		t.Fatalf("canceled registration fenced static claim: %v", err)
	}
}

func TestRuntimeCatalogPendingWriteSurvivesFailedAndCanceledRetry(t *testing.T) {
	m := newVolumeManager(t, newFakeEngine(), liveCatalog(t))
	restore := blockCatalogRename(t, m)
	spec := types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes")}
	if err := m.RegisterVolume(t.Context(), "runtime", spec); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("initial write failure: %v", err)
	}
	if err := m.UnregisterVolume(t.Context(), "runtime"); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("failed retry cleared pending write: %v", err)
	}
	m.retryVolumeCatalog(t.Context())
	if _, err := m.resolveVolumes(t.Context(), "", []types.Volume{{Name: "data"}}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("pending replacement lost fence: %v", err)
	}
	restore()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := m.UnregisterVolume(ctx, "runtime"); !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("canceled retry cleared pending write: %v", err)
	}
	m.retryVolumeCatalog(t.Context())
	if !slices.Contains(m.VolumeNames(), "runtime") {
		t.Fatal("retry lost accepted registration")
	}
	if _, err := m.resolveVolumes(t.Context(), "", []types.Volume{{Name: "data"}}); err != nil {
		t.Fatalf("repair did not restore static admission: %v", err)
	}
}

func TestResolvedCatalogBindingChangesOnlyForReusedName(t *testing.T) {
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
	if err := m.UnregisterVolume(t.Context(), "runtime"); err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := m.admitVolumes("", resolved); !errors.Is(err, ErrVolumeUnavailable) {
		t.Fatalf("same-file reuse admitted stale resolution: %v", err)
	}
}

func TestRuntimeCatalogIgnoresAbandonedTemporaryFiles(t *testing.T) {
	m := newVolumeManager(t, newFakeEngine(), nil)
	path := writeVolumeImage(t, "runtime", "bytes")
	raw := fmt.Sprintf(`{"version":1,"volumes":{"abandoned":{"path":%q}}}`, path)
	tmp := filepath.Join(m.dataDir, runtimeVolumesFile+".interrupted.tmp")
	if err := os.WriteFile(tmp, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := newVolumeManagerAt(t, newFakeEngine(), m.dataDir, nil).VolumeNames(); len(got) != 0 {
		t.Fatalf("temporary record promoted without canonical record: %v", got)
	}
	if err := m.RegisterVolume(t.Context(), "runtime", types.VolumeRegistration{Path: path}); err != nil {
		t.Fatal(err)
	}
	if got := newVolumeManagerAt(t, newFakeEngine(), m.dataDir, nil).VolumeNames(); !slices.Equal(got, []string{"runtime"}) {
		t.Fatalf("temporary record overrode canonical record: %v", got)
	}
	if got, err := os.ReadFile(tmp); err != nil || string(got) != raw {
		t.Fatalf("abandoned evidence changed: %q %v", got, err)
	}
}

func TestRuntimeCatalogAcceptsDaemonDataDirectorySymlink(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "state")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "linked-state")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o777); err != nil {
		t.Fatal(err)
	}
	eng := newFakeEngine()
	m := newVolumeManagerAt(t, eng, link, nil)
	spec := types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes")}
	if err := m.RegisterVolume(t.Context(), "runtime", spec); err != nil {
		t.Fatal(err)
	}
	restarted := newVolumeManagerAt(t, eng, link, nil)
	if got := restarted.VolumeNames(); !slices.Equal(got, []string{"runtime"}) {
		t.Fatalf("reloaded catalog: %v", got)
	}
	if err := restarted.UnregisterVolume(t.Context(), "runtime"); err != nil {
		t.Fatal(err)
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
	if got := newVolumeManagerAt(t, newFakeEngine(), m.dataDir, nil).VolumeNames(); len(got) != 0 {
		t.Fatalf("removal was not persisted: %v", got)
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
	if existed {
		if err := os.Remove(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(record, 0o700); err != nil {
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

func installPendingCatalogRecord(t *testing.T, m *Manager) {
	t.Helper()
	m.mu.Lock()
	proposal := m.catalog.pending
	m.mu.Unlock()
	if proposal == nil {
		t.Fatal("possible replacement fixture requires an accepted pending write")
	}
	// Model the canonical bytes after a successful rename and uncertain directory sync.
	if err := utils.WriteFileSync(filepath.Join(m.dataDir, runtimeVolumesFile), proposal.raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
