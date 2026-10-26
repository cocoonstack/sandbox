package pool

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func BenchmarkClaimCommitWithCheckpoints(b *testing.B) {
	for _, checkpoints := range []bool{false, true} {
		b.Run(fmt.Sprintf("claims=5000/checkpoints=%t", checkpoints), func(b *testing.B) {
			claims := checkpointBenchClaims(checkpoints)
			s := newClaimStore(b.TempDir(), false)
			if err := s.save(claims); err != nil {
				b.Fatal(err)
			}
			one := claims["sb_0000000000000000"]
			b.ReportAllocs()
			for b.Loop() {
				one.Deadline = one.Deadline.Add(time.Second)
				if err := s.commit(s.set(one)); err != nil {
					b.Fatal(err)
				}
			}
			loaded, err := newClaimStore(filepath.Dir(s.path), false).load()
			if err != nil || len(loaded) != 5000 || !loaded[one.ID].Deadline.Equal(one.Deadline) || loaded[one.ID].ReplicaCk != one.ReplicaCk {
				b.Fatalf("commit output: rows=%d err=%v", len(loaded), err)
			}
		})
	}
}

func BenchmarkPinnedArchiveCheckpoints(b *testing.B) {
	for _, checkpoints := range []bool{false, true} {
		b.Run(fmt.Sprintf("claims=5000/checkpoints=%t", checkpoints), func(b *testing.B) {
			m := newTestManager(b, newFakeEngine(), config.PoolSpec{PoolKey: testKey})
			m.claimed = checkpointBenchClaims(checkpoints)
			if err := m.store.save(m.claimed); err != nil {
				b.Fatal(err)
			}
			want := 0
			if checkpoints {
				want = 5000
			}
			b.ReportAllocs()
			for b.Loop() {
				if got := len(m.pinnedArchiveCks()); got != want {
					b.Fatalf("pinned=%d want=%d", got, want)
				}
			}
		})
	}
}

func checkpointBenchClaims(checkpoints bool) map[string]*types.Sandbox {
	claims := benchClaims(5000)
	if checkpoints {
		for id, sb := range claims {
			sb.ReplicaCk = "ck_" + id[3:]
		}
	}
	return claims
}
