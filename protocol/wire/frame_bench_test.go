package wire

import (
	"encoding/json"
	"fmt"
	"testing"
)

func BenchmarkDecodeBulk(b *testing.B) {
	for _, size := range []int{4 << 10, 256 << 10} {
		payload := benchPayload(size)
		frame := benchFrame(b, &Stdout{Data: payload})
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				if _, err := DecodeResponse(frame); err != nil {
					b.Fatalf("decode: %v", err)
				}
			}
		})
		b.Run(fmt.Sprintf("%dKiB-json", size>>10), func(b *testing.B) {
			b.SetBytes(int64(size))
			for b.Loop() {
				var s Stdout
				if err := json.Unmarshal(frame, &s); err != nil {
					b.Fatalf("unmarshal: %v", err)
				}
			}
		})
	}
}

func BenchmarkDecodeControl(b *testing.B) {
	frame := benchFrame(b, &Exit{Code: 0})
	b.ReportAllocs()
	for b.Loop() {
		if _, err := DecodeResponse(frame); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

func BenchmarkEncodeControl(b *testing.B) {
	req := &Exec{Argv: []string{"sh", "-c", "echo hi"}, Cwd: "/root"}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := EncodeRequest(req); err != nil {
			b.Fatalf("encode: %v", err)
		}
	}
}

func benchPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}
	return payload
}

func benchFrame(b *testing.B, r Response) []byte {
	b.Helper()
	frame, err := EncodeResponse(r)
	if err != nil {
		b.Fatalf("encode: %v", err)
	}
	return frame
}
