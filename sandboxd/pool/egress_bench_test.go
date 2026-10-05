package pool

import (
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func BenchmarkArmEgress(b *testing.B) {
	for _, contended := range []bool{false, true} {
		b.Run(fmt.Sprintf("contended=%v", contended), func(b *testing.B) {
			m := egressManager(b, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: egPolicy})
			root := outboundtest.SockRoot(b)
			stop := make(chan struct{})
			var wg sync.WaitGroup
			if contended {
				wg.Go(func() {
					for {
						select {
						case <-stop:
							return
						default:
						}
						m.mu.Lock()
						_ = len(m.claimed)
						m.mu.Unlock()
					}
				})
			}
			var n atomic.Int64
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := "sb_" + strconv.FormatInt(n.Add(1), 10)
					sb := &types.Sandbox{ID: id, Key: testKey, Layer: types.LayerPooled, VsockSocket: filepath.Join(root, id)}
					if err := m.out.ArmProxy(b.Context(), sb); err != nil {
						b.Fatal(err)
					}
					m.out.Disarm(id, true)
				}
			})
			close(stop)
			wg.Wait()
		})
	}
}
