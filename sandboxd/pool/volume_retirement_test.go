package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestRetiringVolumeHoldsSurviveRestart(t *testing.T) {
	for _, action := range []string{"release", "reap", "quarantine"} {
		for _, mode := range []string{"ro", "rw"} {
			t.Run(action+"/"+mode, func(t *testing.T) {
				eng := newFakeEngine()
				catalog := liveCatalog(t)
				m := newVolumeManager(t, eng, catalog)
				sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: mode}}})
				if err != nil {
					t.Fatal(err)
				}
				eng.removeErrFor = sb.VMName
				switch action {
				case "release":
					if checkErr := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); checkErr == nil {
						t.Fatal("release accepted surviving VM")
					}
				case "reap":
					sb.Deadline = time.Now().Add(-time.Second)
					m.reapOnce(t.Context())
					waitFor(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); _, ok := m.pendingRemovals[sb.VMName]; return ok })
				case "quarantine":
					if gone, quarantineErr := m.quarantineClaim(t.Context(), sb); gone || quarantineErr != nil {
						t.Fatal("quarantine accepted surviving VM")
					}
				}
				claims, retiring, err := m.store.loadJournal()
				if err != nil || claims[sb.ID] != nil || len(retiring[sb.VMName].Holds) != 1 {
					t.Fatalf("journal=%v retiring=%v err=%v", claims, retiring, err)
				}
				reloaded := newClaimStore(m.dataDir, false)
				loaded, loadErr := reloaded.load()
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				if saveErr := reloaded.save(loaded); saveErr != nil {
					t.Fatal(saveErr)
				}
				m2 := newVolumeManagerAt(t, eng, m.dataDir, catalog)
				if checkErr := m2.Reconcile(t.Context()); checkErr != nil {
					t.Fatal(checkErr)
				}
				if _, checkErr := m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: "rw"}}}); !errors.Is(checkErr, ErrVolumeBusy) {
					t.Fatalf("writer while VM survives: %v", checkErr)
				}
				if saveErr := m2.store.save(m2.claimed); saveErr != nil {
					t.Fatal(saveErr)
				}
				_, retiring, err = m2.store.loadJournal()
				if err != nil || len(retiring) != 1 {
					t.Fatalf("save lost retirement: %v %v", retiring, err)
				}
				eng.removeErrFor = ""
				m2.retryRemovals(t.Context()).Wait()
				_, retiring, err = m2.store.loadJournal()
				if err != nil || len(retiring) != 0 {
					t.Fatalf("completed retirement remained: %v %v", retiring, err)
				}
				if got := volumeHoldersOf(m2, "data"); got != (volumeHolders{}) {
					t.Fatalf("retirement leaked holds: %+v", got)
				}
				writer, err := m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: "rw"}}})
				if err != nil {
					t.Fatalf("writer after removal: %v", err)
				}
				if err := m2.Release(t.Context(), writer.ID, Cred{Token: writer.Token}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestRetirementCompletionMustBeDurableBeforeAdmission(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}})
	if err != nil {
		t.Fatal(err)
	}
	m.eng = &retirementWriteFailureEngine{fakeEngine: eng, afterRemove: func() {
		if checkErr := os.Mkdir(m.store.path+".tmp", 0o700); checkErr != nil {
			t.Fatal(checkErr)
		}
	}}
	if checkErr := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); checkErr == nil {
		t.Fatal("release accepted uncommitted retirement deletion")
	}
	if !eng.removed(sb.VMName) {
		t.Fatal("test did not reach VM removal")
	}
	if _, checkErr := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: "rw"}}}); !errors.Is(checkErr, ErrVolumeBusy) {
		t.Fatalf("writer before retirement deletion: %v", checkErr)
	}
	m.store.writeMu.Lock()
	err = os.Remove(m.store.path + ".tmp")
	m.store.writeMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !m.recommitting.Load() })
	_, retiring, err := m.store.loadJournal()
	if err != nil || len(retiring) != 1 {
		t.Fatalf("uncertain completion lost retirement: %v %v", retiring, err)
	}
	m2 := newVolumeManagerAt(t, eng, m.dataDir, catalog)
	if reconcileErr := m2.Reconcile(t.Context()); reconcileErr != nil {
		t.Fatal(reconcileErr)
	}
	if got := volumeHoldersOf(m2, "data"); got != (volumeHolders{}) {
		t.Fatalf("gone VM retained hold: %+v", got)
	}
	_, retiring, err = m2.store.loadJournal()
	if err != nil || len(retiring) != 0 {
		t.Fatalf("gone VM retained retirement: %v %v", retiring, err)
	}
}

func TestRetirementRetriesFailedDirtyMarkerRemoval(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: "rw"}}})
	if err != nil {
		t.Fatal(err)
	}
	marker := volumeDirtyPath(catalog[0].Path)
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(marker, 0o700); err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(marker, "busy")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("release accepted failed marker removal")
	}
	if got := volumeHoldersOf(m, "data"); got.writers != 1 {
		t.Fatalf("marker failure released hold: %+v", got)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	m.retryRemovals(t.Context()).Wait()
	if volumeDirty(catalog[0].Path) || volumeHoldersOf(m, "data") != (volumeHolders{}) {
		t.Fatal("marker retry did not finish retirement")
	}
}

func TestClaimsJournalRejectsUnsupportedAndConflictingRecords(t *testing.T) {
	for _, raw := range []string{
		`{"format_version":3,"claims":{},"volume_retirements":{}}`,
		`{"format_version":2,"claims":{"sb_a":{"id":"sb_a","vm_name":"sbx-a"}},"volume_retirements":{"sbx-a":{"sandbox_id":"sb_a","vm_name":"sbx-a","holds":[{"name":"data"}]}}}`,
		`{"format_version":2,"claims":{}}`,
	} {
		store := newClaimStore(t.TempDir(), false)
		if err := os.WriteFile(store.path, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := store.load(); err == nil {
			t.Fatalf("accepted invalid journal: %s", raw)
		}
	}
	store := newClaimStore(t.TempDir(), false)
	if err := store.save(map[string]*types.Sandbox{}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}
	var legacy map[string]*types.Sandbox
	if err := json.Unmarshal(raw, &legacy); err == nil {
		t.Fatal("legacy reader silently accepted versioned journal")
	}
}

func TestUnknownOrphanFencesNewAdmissionAndHeldRepair(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	eng.mountVolumeErr = errors.New("interrupted mount")
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); err == nil {
		t.Fatal("expected pending volume")
	}
	eng.mountVolumeErr = nil
	const orphan = "sbx-unknown"
	eng.vms[orphan] = "/vsock/unknown"
	eng.removeErrFor = orphan
	m2 := newVolumeManagerAt(t, eng, m.dataDir, catalog)
	if reconcileErr := m2.Reconcile(t.Context()); reconcileErr != nil {
		t.Fatal(reconcileErr)
	}
	before := len(eng.volumeSpecs)
	if _, err := m2.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); !errors.Is(err, ErrVolumeUnavailable) {
		t.Fatalf("held repair bypassed orphan fence: %v", err)
	}
	if len(eng.volumeSpecs) != before {
		t.Fatal("held repair reached disk attach")
	}
	if _, err := m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "other"}}}); !errors.Is(err, ErrVolumeUnavailable) {
		t.Fatalf("new claim bypassed orphan fence: %v", err)
	}
	if _, err := m2.GetVolumes(sb.ID, sb.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := m2.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{}); err != nil {
		t.Fatalf("cleanup blocked by orphan fence: %v", err)
	}
	eng.removeErrFor = ""
	m2.retryRemovals(t.Context()).Wait()
	if _, err := m2.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "other"}}); err != nil {
		t.Fatalf("confirmed orphan removal did not open admission: %v", err)
	}
}

func TestReconcileRejectsUncommittedVolumeQuarantine(t *testing.T) {
	eng := newFakeEngine()
	m := egressManager(t, eng, config.PoolSpec{PoolKey: egKey, Egress: egPolicy})
	sb := &types.Sandbox{ID: "sb_quarantine", VMName: "sbx-quarantine", Key: egKey, Volumes: []types.Volume{{Name: "data", Mount: "/data"}}, VolumesWant: []types.Volume{{Name: "data", Mount: "/data"}}}
	eng.vms[sb.VMName] = "/run/quarantine.sock"
	eng.snapshots = []string{forkPrefix + "orphan"}
	if err := m.store.save(map[string]*types.Sandbox{sb.ID: sb}); err != nil {
		t.Fatal(err)
	}
	m.eng = &quarantineWriteFailureEngine{fakeEngine: eng, path: m.store.path}
	withOutbound(t, m, func(o *outbound.Options) { o.Sweep = func(map[string]bool) error { return nil } })
	t.Cleanup(func() {
		m.store.writeMu.Lock()
		err := os.Remove(m.store.path + ".tmp")
		m.store.writeMu.Unlock()
		if err != nil {
			t.Error(err)
		}
		waitFor(t, func() bool { return !m.recommitting.Load() })
	})
	if err := m.Reconcile(t.Context()); err == nil {
		t.Error("startup accepted a claim whose quarantine could not be persisted")
	}
	if eng.removed(sb.VMName) {
		t.Error("quarantine removed VM before its retirement was durable")
	}
	if m.claimed[sb.ID] == nil || volumeHoldersOf(m, "data").readers != 1 {
		t.Error("failed quarantine lost live claim or hold")
	}
}

type retirementWriteFailureEngine struct {
	*fakeEngine
	afterRemove func()
}

func (e *retirementWriteFailureEngine) Remove(ctx context.Context, name string) error {
	err := e.fakeEngine.Remove(ctx, name)
	if err == nil && e.afterRemove != nil {
		e.afterRemove()
		e.afterRemove = nil
	}
	return err
}

type quarantineWriteFailureEngine struct {
	*fakeEngine
	path string
}

func (e *quarantineWriteFailureEngine) SnapshotRemove(ctx context.Context, name string) error {
	if err := os.Mkdir(e.path+".tmp", 0o700); err != nil {
		return err
	}
	return e.fakeEngine.SnapshotRemove(ctx, name)
}
