package types

import (
	"sync"
	"testing"
	"time"
)

func BenchmarkSandboxTouch(b *testing.B) {
	sb := &Sandbox{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sb.Touch()
		}
	})
}

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
