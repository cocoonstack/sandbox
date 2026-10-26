package pool

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func BenchmarkVolumeAdmissionWithCheckpoints(b *testing.B) {
	for _, checkpoints := range []bool{false, true} {
		b.Run(fmt.Sprintf("claims=5000/checkpoints=%t", checkpoints), func(b *testing.B) {
			m := volumeBenchManager(b, checkpoints)
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
				err = m.checkResolved(resolved, "")
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
		})
	}
}

func BenchmarkVolumeHoldsWithCheckpoints(b *testing.B) {
	for _, checkpoints := range []bool{false, true} {
		b.Run(fmt.Sprintf("claims=5000/checkpoints=%t", checkpoints), func(b *testing.B) {
			m := volumeBenchManager(b, checkpoints)
			b.ReportAllocs()
			for b.Loop() {
				m.mu.Lock()
				held := 0
				for _, sb := range m.claimed {
					if m.hasVolumeHolds(sb) {
						held++
					}
				}
				m.mu.Unlock()
				if held != 0 {
					b.Fatalf("unrelated claims hold %d volumes", held)
				}
			}
		})
	}
}

func volumeBenchManager(b *testing.B, checkpoints bool) *Manager {
	b.Helper()
	m := newTestManager(b, newFakeEngine(), config.PoolSpec{PoolKey: testKey})
	m.claimed = checkpointBenchClaims(checkpoints)
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
