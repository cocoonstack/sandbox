package pool

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func benchClaims(n int) map[string]*types.Sandbox {
	claims := make(map[string]*types.Sandbox, n)
	for i := range n {
		id := fmt.Sprintf("sb_%016x", i)
		claims[id] = &types.Sandbox{
			ID: id, Token: "tok", VMName: "sbx-" + id,
			Key:      types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall},
			Deadline: time.Now().Add(time.Hour),
		}
	}
	return claims
}

// BenchmarkStoreSaveScaling measures the claims-journal persist cost (marshal
// + write + rename) as the live-claim set grows.
func BenchmarkStoreSaveScaling(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("claims=%d", n), func(b *testing.B) {
			st := newClaimStore(b.TempDir())
			claims := benchClaims(n)
			b.ResetTimer()
			for range b.N {
				if err := st.save(claims); err != nil {
					b.Fatalf("save: %v", err)
				}
			}
		})
	}
}

// BenchmarkStorePersistContention reports how long a concurrent manager-mutex
// acquire (a data-plane op) waits while the journal is being persisted, under
// the old "write under the lock" path (save) vs the new "marshal under the lock,
// write off it" split (snapshot+commit). The ns/acquire metric is the win.
func BenchmarkStorePersistContention(b *testing.B) {
	for _, n := range []int{100, 1000} {
		claims := benchClaims(n)
		b.Run(fmt.Sprintf("under-lock/n=%d", n), func(b *testing.B) {
			benchPersistContention(b, claims, false)
		})
		b.Run(fmt.Sprintf("off-lock/n=%d", n), func(b *testing.B) {
			benchPersistContention(b, claims, true)
		})
	}
}

func benchPersistContention(b *testing.B, claims map[string]*types.Sandbox, offLock bool) {
	s := newClaimStore(b.TempDir())
	var mu sync.Mutex
	var waitNs, waits atomic.Int64
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			t := time.Now()
			mu.Lock()
			waitNs.Add(time.Since(t).Nanoseconds())
			waits.Add(1)
			mu.Unlock()
		}
	}()

	b.ResetTimer()
	for range b.N {
		if offLock {
			mu.Lock()
			snap := s.snapshot(claims) // marshal under the lock
			mu.Unlock()
			_ = s.commit(snap) // write off the lock
		} else {
			mu.Lock()
			_ = s.save(claims) // marshal + write both under the lock
			mu.Unlock()
		}
	}
	b.StopTimer()
	close(done)
	if w := waits.Load(); w > 0 {
		b.ReportMetric(float64(waitNs.Load())/float64(w), "ns/acquire")
	}
}
