package pool

import (
	"fmt"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// BenchmarkStoreSaveScaling measures the claims-journal write path (marshal
// + write + rename, inside the pool mutex today) as the live-claim set
// grows. M4-6 is trigger-gated: move save() to a store-owned sequential
// writer only if this shows the in-mutex write dominating at scale.
func BenchmarkStoreSaveScaling(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("claims=%d", n), func(b *testing.B) {
			st := newStore(b.TempDir())
			claims := make(map[string]*types.Sandbox, n)
			for i := range n {
				id := fmt.Sprintf("sb_%016x", i)
				claims[id] = &types.Sandbox{
					ID: id, Token: "tok", VMName: "sbx-" + id,
					Key:      types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall},
					Deadline: time.Now().Add(time.Hour),
				}
			}
			b.ResetTimer()
			for range b.N {
				if err := st.save(claims); err != nil {
					b.Fatalf("save: %v", err)
				}
			}
		})
	}
}
