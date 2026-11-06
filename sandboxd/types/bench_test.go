package types

import "testing"

func BenchmarkSandboxTouch(b *testing.B) {
	sb := &Sandbox{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sb.Touch()
		}
	})
}
