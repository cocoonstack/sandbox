package types

import (
	"sync"
	"testing"
	"time"
)

// BenchmarkSandboxTouch measures the relay hot-path last-activity stamp; the
// lock-free atomic store must stay flat under -cpu parallelism (no contention).
func BenchmarkSandboxTouch(b *testing.B) {
	sb := &Sandbox{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sb.Touch()
		}
	})
}

// BenchmarkSandboxTouchMutex is the pre-E1a baseline (mutex-guarded time.Time)
// for contrast: it degrades with parallelism where the atomic stays flat.
func BenchmarkSandboxTouchMutex(b *testing.B) {
	var (
		mu sync.Mutex
		t  time.Time
	)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			t = time.Now()
			mu.Unlock()
		}
	})
	_ = t
}
