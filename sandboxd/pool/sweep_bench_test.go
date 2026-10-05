package pool

import (
	"fmt"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const sweepClaims = 5000

func BenchmarkOnVMChange(b *testing.B) {
	m := sweepManager(b, 0)
	change := engine.VMChange{Kind: engine.VMDeleted, VM: engine.VMStatus{Name: "sbx-gone"}}
	b.ReportAllocs()
	for b.Loop() {
		m.onVMChange(b.Context(), change)
	}
}

func BenchmarkIdleSweepScan(b *testing.B) {
	for _, idle := range []int{0, 600} {
		b.Run(fmt.Sprintf("idle=%ds", idle), func(b *testing.B) {
			m := sweepManager(b, idle)
			b.ReportAllocs()
			for b.Loop() {
				m.idleOnce(b.Context())
			}
		})
	}
}

func sweepManager(b *testing.B, idleSeconds int) *Manager {
	b.Helper()
	m := newTestManager(b, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, IdleHibernateSeconds: idleSeconds})
	now := time.Now()
	m.mu.Lock()
	for i := range sweepClaims {
		sb := &types.Sandbox{ID: fmt.Sprintf("sb_%05d", i), VMName: fmt.Sprintf("sbx-%05d", i), Key: testKey, Deadline: now.Add(time.Hour)}
		sb.TouchAt(now)
		m.claimed[sb.ID] = sb
	}
	m.mu.Unlock()
	return m
}
