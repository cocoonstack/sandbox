package pool

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func BenchmarkStoreSaveScaling(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("claims=%d", n), func(b *testing.B) {
			st := newClaimStore(b.TempDir(), false)
			claims := benchClaims(n)
			for b.Loop() {
				if err := st.save(claims); err != nil {
					b.Fatalf("save: %v", err)
				}
			}
		})
	}
}

func BenchmarkStorePersistContention(b *testing.B) {
	for _, n := range []int{100, 1000} {
		claims := benchClaims(n)
		for _, arm := range []string{"under-lock", "rebuild", "incremental"} {
			b.Run(fmt.Sprintf("%s/n=%d", arm, n), func(b *testing.B) {
				benchPersistContention(b, claims, arm)
			})
		}
	}
}

func BenchmarkStoreCommitEnv(b *testing.B) {
	envs := map[string]types.Env{"none": nil, "typical": benchEnv(5, 64), "cap": benchEnv(8, 64<<10/8-8)}
	for _, n := range []int{50, 200} {
		for _, arm := range []string{"none", "typical", "cap"} {
			for _, durable := range []bool{false, true} {
				b.Run(fmt.Sprintf("claims=%d/env=%s/sync=%t", n, arm, durable), func(b *testing.B) {
					claims := benchClaims(n)
					for _, sb := range claims {
						sb.Env = envs[arm]
					}
					s := newClaimStore(b.TempDir(), durable)
					if err := s.save(claims); err != nil {
						b.Fatalf("save: %v", err)
					}
					one := claims[fmt.Sprintf("sb_%016x", 0)]
					for b.Loop() {
						if err := s.commit(s.set(one)); err != nil {
							b.Fatalf("commit: %v", err)
						}
					}
					if fi, err := os.Stat(s.path); err == nil {
						b.ReportMetric(float64(fi.Size()), "file-bytes")
					}
				})
			}
		}
	}
}

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

func benchPersistContention(b *testing.B, claims map[string]*types.Sandbox, arm string) {
	s := newClaimStore(b.TempDir(), false)
	s.reset(claims)
	one := claims[fmt.Sprintf("sb_%016x", 0)]
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

	for b.Loop() {
		switch arm {
		case "under-lock":
			mu.Lock()
			_ = s.save(claims)
			mu.Unlock()
		case "rebuild":
			mu.Lock()
			snap := s.reset(claims)
			mu.Unlock()
			_ = s.commit(snap)
		default:
			mu.Lock()
			snap := s.set(one)
			mu.Unlock()
			_ = s.commit(snap)
		}
	}
	close(done)
	if w := waits.Load(); w > 0 {
		b.ReportMetric(float64(waitNs.Load())/float64(w), "ns/acquire")
	}
}

// benchEnv returns n guest entries whose names and values total n*(8+valueBytes) bytes.
func benchEnv(n, valueBytes int) types.Env {
	env := make(types.Env, n)
	for i := range n {
		env[fmt.Sprintf("ENV_%04d", i)] = types.EnvVar{Value: strings.Repeat("v", valueBytes)}
	}
	return env
}
