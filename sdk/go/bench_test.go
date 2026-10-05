package sandbox

import (
	"archive/tar"
	"fmt"
	"io"
	"testing"
)

func BenchmarkWriteFile(b *testing.B) {
	for _, size := range []int{64, 4 << 10, 256 << 10} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			sb := fakeSandbox(b)
			data := make([]byte, size)
			b.ReportAllocs()
			for b.Loop() {
				if err := sb.WriteFile(b.Context(), "/f", data, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkExecKept(b *testing.B) {
	sb := fakeSandbox(b)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := sb.Exec(b.Context(), "echo", "hi"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPushSmallFiles(b *testing.B) {
	sb := fakeSandbox(b)
	body := make([]byte, 1<<10)
	b.ReportAllocs()
	for b.Loop() {
		pr, pw := io.Pipe()
		go func() {
			tw := tar.NewWriter(pw)
			for i := range 500 {
				_ = tw.WriteHeader(&tar.Header{Name: fmt.Sprintf("d/f%03d", i), Mode: 0o600, Size: int64(len(body))})
				_, _ = tw.Write(body)
			}
			_ = tw.Close()
			_ = pw.Close()
		}()
		if err := sb.Push(b.Context(), "/push", pr); err != nil {
			b.Fatal(err)
		}
	}
}
