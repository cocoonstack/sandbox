package pool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func BenchmarkVolumeAdmission(b *testing.B) {
	m := volumeBenchManager(b)
	requested := []types.Volume{{Name: "bench-data", Mount: "/mnt/data"}}
	b.ReportAllocs()
	for b.Loop() {
		resolved, err := m.resolveVolumes(b.Context(), "", requested)
		if err != nil {
			b.Fatal(err)
		}
		if len(resolved) != 1 || resolved[0].applied != requested[0] {
			b.Fatalf("resolved volumes: %+v", resolved)
		}
		applied := appliedVolumes(resolved)
		m.mu.Lock()
		err = m.checkResolvedLocked(resolved)
		if err == nil {
			m.adoptVolumes(applied)
			m.releaseVolumes(applied)
		}
		m.mu.Unlock()
		if err != nil {
			b.Fatal(err)
		}
	}
	if len(m.catalog.holders) != 0 {
		b.Fatalf("unreleased admission holds: %v", m.catalog.holders)
	}
}

func volumeBenchManager(b *testing.B) *Manager {
	b.Helper()
	m := newTestManager(b, newFakeEngine(), config.PoolSpec{PoolKey: testKey})
	m.claimed = checkpointBenchClaims(false)
	if err := m.store.save(m.claimed); err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(b.TempDir(), "data.raw")
	if err := os.WriteFile(path, []byte("benchmark backing"), 0o600); err != nil {
		b.Fatal(err)
	}
	if err := m.RegisterVolume(b.Context(), "bench-data", types.VolumeRegistration{Path: path}); err != nil {
		b.Fatal(err)
	}
	return m
}
