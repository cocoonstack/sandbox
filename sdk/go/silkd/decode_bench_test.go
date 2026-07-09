package silkd

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func BenchmarkDecodeStdout(b *testing.B) {
	payload := make([]byte, 256*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	frame := []byte(`{"type":"stdout","data":"` + base64.StdEncoding.EncodeToString(payload) + `"}`)
	b.SetBytes(int64(len(payload)))
	b.Run("fast", func(b *testing.B) {
		for range b.N {
			if _, err := DecodeResponse(frame); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("json", func(b *testing.B) {
		for range b.N {
			var s Stdout
			if err := json.Unmarshal(frame, &s); err != nil {
				b.Fatal(err)
			}
		}
	})
}
