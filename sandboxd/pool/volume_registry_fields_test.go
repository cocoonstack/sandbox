package pool

import (
	"encoding/json/v2"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

func TestManagedRegistryPreservesSupportedAdditiveFields(t *testing.T) {
	for _, mode := range []string{"sleep", "growth"} {
		t.Run(mode, func(t *testing.T) {
			var m *Manager
			var cfg *config.Config
			var id string
			if mode == "sleep" {
				manager, _, sb, c := newSleepTest(t)
				m, cfg, id = manager, c, sb.ID
				requireSleep(t, m, sb)
			} else {
				manager, _, sb, c, v := newGrowthTest(t)
				m, cfg, id = manager, c, sb.ID
				if _, err := m.GrowVolume(t.Context(), "data", v.ID, growRequest(), "t1"); err != nil {
					t.Fatal(err)
				}
			}
			m.Close()
			path := filepath.Join(cfg.ManagedVolumeDir, "registry.json")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var doc map[string]any
			if err = json.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			volume := doc["volumes"].(map[string]any)["data"].(map[string]any)
			hold := doc["holds"].(map[string]any)[id].(map[string]any)
			ref := hold["refs"].([]any)[0].(map[string]any)
			objects := []map[string]any{doc, volume, volume["backing"].(map[string]any), hold, hold["process"].(map[string]any), ref, ref["volume"].(map[string]any), ref["backing"].(map[string]any)}
			if mode == "sleep" {
				sleep := hold["sleep"].(map[string]any)
				objects = append(objects, sleep, sleep["hook"].(map[string]any), sleep["before_restore"].(map[string]any))
			} else {
				growth := volume["growth"].(map[string]any)
				objects = append(objects, growth, growth["request"].(map[string]any), growth["runtime"].(map[string]any), growth["guest"].(map[string]any))
			}
			for _, object := range objects {
				object["future_note"] = map[string]any{"value": "preserve"}
			}
			raw, err = json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			r, err := openVolumeRegistry(cfg.ManagedVolumeDir, "node")
			if err != nil {
				t.Fatal(err)
			}
			defer r.close()
			if r.fault != nil {
				t.Fatal(r.fault)
			}
			r.mu.Lock()
			err = r.commitLocked()
			r.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var persisted map[string]any
			if err := json.Unmarshal(after, &persisted); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(doc, persisted) {
				t.Fatalf("optional fields changed across a supported registry rewrite\nbefore=%s\nafter=%s", raw, after)
			}
		})
	}
}

func TestManagedRegistryExtensionsFollowObjectIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, old, next string
		typ             reflect.Type
	}{
		{"removed operation", `{"state":"ready","sleep":{"operation":"old","future":true},"future":true}`, `{"state":"claimed"}`, reflect.TypeFor[volumeHold]()},
		{"new process", `{"boot_id":"boot","pid":12,"start":3,"future":true}`, `{"boot_id":"boot","pid":12,"start":4}`, reflect.TypeFor[processIdentity]()},
		{"new volume", `{"id":"old","name":"data","future":true}`, `{"id":"new","name":"data"}`, reflect.TypeFor[managedVolume]()},
		{"removed growth", `{"id":"same","growth":{"future":true}}`, `{"id":"same"}`, reflect.TypeFor[managedVolume]()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := mergeRegistryFields([]byte(tc.next), []byte(tc.old), tc.typ)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if _, exists := got["sleep"]; exists {
				t.Fatal("removed sleep was resurrected")
			}
			if _, exists := got["growth"]; exists {
				t.Fatal("removed growth was resurrected")
			}
			if tc.name != "removed operation" {
				if _, exists := got["future"]; exists {
					t.Fatal("metadata crossed an identity boundary")
				}
			}
		})
	}
}
