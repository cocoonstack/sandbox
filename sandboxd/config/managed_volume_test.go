package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestManagedVolumeStoreExcludesStartupCleanupPaths(t *testing.T) {
	root := t.TempDir()
	data := filepath.Join(root, "data")
	for _, tc := range []struct {
		path  string
		allow bool
	}{
		{filepath.Join(data, "managed"), true},
		{filepath.Join(root, "outside"), true},
		{filepath.Join(data, "readers-backup", "managed"), true},
		{filepath.Join(data, "forks", "managed"), true},
		{filepath.Join(data, "readers"), false},
		{filepath.Join(data, "readers", "managed"), false},
		{filepath.Join(data, "fork-state"), false},
		{filepath.Join(data, "fork-state", "managed"), false},
		{filepath.Join(data, "goldens"), false},
		{filepath.Join(data, "goldens", "managed"), false},
	} {
		t.Run(tc.path, func(t *testing.T) {
			cfg := Config{DataDir: data, ManagedVolumeDir: tc.path, APIToken: "root"}
			err := cfg.ValidateManagedVolumeStore()
			if (err == nil) != tc.allow {
				t.Fatalf("path %q accepted=%v, want %v: %v", tc.path, err == nil, tc.allow, err)
			}
		})
	}
}

func TestManagedVolumeStoreResolvesDataDirectoryAliases(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "real")
	alias := filepath.Join(root, "alias")
	if err := os.Mkdir(real, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	for _, tc := range []struct {
		name, data, managed string
		allow               bool
	}{
		{"alias-readers", alias, filepath.Join(real, "readers", "managed"), false},
		{"alias-goldens", alias, filepath.Join(real, "goldens"), false},
		{"alias-fork", alias, filepath.Join(real, "fork-persistent"), false},
		{"alias-new-suffix", filepath.Join(alias, "new", "data"), filepath.Join(real, "new", "data", "readers", "managed"), false},
		{"alias-safe", alias, filepath.Join(real, "managed"), true},
		{"relative", "real", filepath.Join(real, "readers", "managed"), false},
		{"relative-safe", "real", filepath.Join(real, "readers-safe"), true},
		{"empty", "", filepath.Join(root, "fork-store"), false},
		{"empty-safe", "", filepath.Join(root, "managed"), true},
		{"ancestor", filepath.Join(real, "data"), real, true},
		{"same-root", real, real, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{DataDir: tc.data, ManagedVolumeDir: tc.managed, APIToken: "root"}
			err := cfg.ValidateManagedVolumeStore()
			if (err == nil) != tc.allow {
				t.Fatalf("data=%q managed=%q accepted=%v, want %v: %v", tc.data, tc.managed, err == nil, tc.allow, err)
			}
		})
	}
	if _, err := os.Stat(filepath.Join(real, "new")); !os.IsNotExist(err) {
		t.Fatalf("validation created data directories: %v", err)
	}
}

func TestManagedVolumeStoreRejectsCleanupDirectoryAliases(t *testing.T) {
	for _, namespace := range []string{"readers", "goldens"} {
		t.Run(namespace, func(t *testing.T) {
			root := t.TempDir()
			data := filepath.Join(root, "data")
			external := filepath.Join(root, "external")
			for _, dir := range []string{data, external} {
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink(external, filepath.Join(data, namespace)); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				path  string
				allow bool
			}{
				{external, false},
				{filepath.Join(external, "managed"), false},
				{filepath.Join(data, "managed"), true},
				{root, true},
			} {
				cfg := Config{DataDir: data, ManagedVolumeDir: tc.path, APIToken: "root"}
				err := cfg.ValidateManagedVolumeStore()
				if (err == nil) != tc.allow {
					t.Errorf("%s alias target %q accepted=%v, want %v: %v", namespace, tc.path, err == nil, tc.allow, err)
				}
			}
			if _, err := os.Stat(filepath.Join(external, "managed")); !os.IsNotExist(err) {
				t.Fatalf("validation created managed directory: %v", err)
			}
		})
	}
}

func TestManagedVolumeStoreProtectsBackingDirectoryFromCleanupAliases(t *testing.T) {
	for _, namespace := range []string{"readers", "goldens"} {
		for _, target := range []string{"disks", "disks/nested", "disks-backup", "work/readers"} {
			t.Run(namespace+"/"+target, func(t *testing.T) {
				root := t.TempDir()
				data := filepath.Join(root, "data")
				managed := filepath.Join(root, "managed")
				aliasTarget := filepath.Join(managed, target)
				for _, dir := range []string{data, aliasTarget} {
					if err := os.MkdirAll(dir, 0700); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.Symlink(aliasTarget, filepath.Join(data, namespace)); err != nil {
					t.Fatal(err)
				}
				cfg := Config{DataDir: data, ManagedVolumeDir: managed, APIToken: "root"}
				allow := target == "disks-backup" || target == "work/readers"
				if err := cfg.ValidateManagedVolumeStore(); (err == nil) != allow {
					t.Fatalf("cleanup alias %q accepted=%v, want %v: %v", target, err == nil, allow, err)
				}
			})
		}
	}
}
