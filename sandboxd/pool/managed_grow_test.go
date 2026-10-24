package pool

import (
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func growManagedTest(t *testing.T, m *Manager, name, id string, sizeGiB int64) (types.ManagedVolumeInfo, error) {
	t.Helper()
	return m.GrowVolume(t.Context(), name, id, sizeGiB, "t1")
}

func requireBackingSize(t *testing.T, path string, want int64) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil || st.Size() != want {
		t.Fatalf("backing file: %v %v, want %d bytes", st, err, want)
	}
}

func TestManagedGrowUnheld(t *testing.T) {
	cfg := managedConfig(t)
	m := openManagedTest(t, cfg, newManagedEngine())
	v := createManagedTest(t, m, "data")
	path := m.managed.path(volumeID(v.ID))
	grown, err := growManagedTest(t, m, "data", v.ID, 2)
	if err != nil || grown.SizeBytes != 2<<30 || grown.State != "ready" {
		t.Fatalf("grow 1->2 GiB: %+v %v", grown, err)
	}
	requireBackingSize(t, path, 2<<30)
	if got, getErr := m.GetVolume("data", "t1"); getErr != nil || got.SizeBytes != 2<<30 || got.State != "ready" {
		t.Fatalf("GET after grow: %+v %v, want ready at 2 GiB", got, getErr)
	}

	registry := filepath.Join(cfg.ManagedVolumeDir, "registry.json")
	before, err := os.ReadFile(registry)
	if err != nil {
		t.Fatal(err)
	}
	if replay, replayErr := growManagedTest(t, m, "data", v.ID, 2); replayErr != nil || replay.SizeBytes != 2<<30 {
		t.Fatalf("same target again: %+v %v, want 2 GiB unchanged", replay, replayErr)
	}
	if after, readErr := os.ReadFile(registry); readErr != nil || string(after) != string(before) {
		t.Fatalf("same target rewrote the registry: %v", readErr)
	}
	requireBackingSize(t, path, 2<<30)

	_, err = growManagedTest(t, m, "data", v.ID, 1)
	requireVolumeStatus(t, err, 409)
	requireBackingSize(t, path, 2<<30)

	sb, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = growManagedTest(t, m, "data", v.ID, 3)
	if busy := requireVolumeStatus(t, err, 409); len(busy.Holders) != 1 || busy.Holders[0].SandboxID != sb.ID {
		t.Fatalf("grow of a held volume named %+v, want holder %s", busy.Holders, sb.ID)
	}
	requireBackingSize(t, path, 2<<30)
}

func TestManagedGrowIntentSettles(t *testing.T) {
	t.Run("grown file", func(t *testing.T) {
		cfg := managedConfig(t)
		e := newManagedEngine()
		m := openManagedTest(t, cfg, e)
		v := createManagedTest(t, m, "data")
		createManagedTest(t, m, "other")
		path := m.managed.path(volumeID(v.ID))
		writes := 0
		m.managed.commitFile = func(path string, raw []byte) error {
			writes++
			if writes == 2 {
				return errors.New("registry write after the truncate failed")
			}
			return writeVolumeSnapshot(path, raw)
		}
		_, err := growManagedTest(t, m, "data", v.ID, 2)
		if ve := requireVolumeStatus(t, err, 503); ve.Code != "volume_storage_unavailable" {
			t.Fatalf("failed grow commit: %+v, want the retryable storage error", ve)
		}
		requireBackingSize(t, path, 2<<30)
		m.Close()
		m = openManagedTest(t, cfg, e)
		if got, err := m.GetVolume("data", "t1"); err != nil || got.State != "ready" || got.SizeBytes != 2<<30 {
			t.Fatalf("reopen after a lost grow commit: %+v %v, want ready at 2 GiB", got, err)
		}
		if _, err := m.ClaimProvision(t.Context(), testKey, managedOptions("other")); err != nil {
			t.Fatalf("claim of another volume after reopen: %v", err)
		}
	})
	t.Run("file not grown", func(t *testing.T) {
		cfg := managedConfig(t)
		e := newManagedEngine()
		m := openManagedTest(t, cfg, e)
		v := createManagedTest(t, m, "data")
		path := m.managed.path(volumeID(v.ID))
		m.Close()
		registry := filepath.Join(cfg.ManagedVolumeDir, "registry.json")
		raw, err := os.ReadFile(registry)
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err = json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		doc["volumes"].(map[string]any)["data"].(map[string]any)["grow_to"] = 2 << 30
		if raw, err = json.Marshal(doc); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(registry, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		m = openManagedTest(t, cfg, e)
		if got, err := m.GetVolume("data", "t1"); err != nil || got.State != "ready" || got.SizeBytes != 1<<30 {
			t.Fatalf("reopen with an unapplied grow intent: %+v %v, want ready at 1 GiB", got, err)
		}
		requireBackingSize(t, path, 1<<30)
		if _, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data")); err != nil {
			t.Fatalf("claim after the intent settled: %v", err)
		}
	})
}
