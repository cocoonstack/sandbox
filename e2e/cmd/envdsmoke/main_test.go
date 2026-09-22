package main

import (
	"encoding/binary"
	"io"
	"strings"
	"testing"
)

func TestReadEnvelopesReturnsTheDataMessages(t *testing.T) {
	got, err := readEnvelopes(strings.NewReader(frame(0, `{"event":{"start":{}}}`) + frame(0, `{"event":{"data":{}}}`) + frame(2, `{}`)))
	if err != nil {
		t.Fatalf("readEnvelopes: %v", err)
	}
	want := []string{`{"event":{"start":{}}}`, `{"event":{"data":{}}}`}
	if len(got) != len(want) {
		t.Fatalf("got %d messages %q, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("message %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestReadEnvelopesRejects(t *testing.T) {
	tests := []struct {
		name   string
		stream string
		want   string
	}{
		{"an error in the end-of-stream envelope", frame(0, `{"event":{"start":{}}}`) + frame(2, `{"error":{"code":"internal","message":"exec failed"}}`), "rpc failed"},
		{"a stream that ends before its end-of-stream envelope", frame(0, `{"event":{"start":{}}}`), "stream ended before"},
		{"an empty stream", "", "stream ended before"},
		{"an unparsable end-of-stream envelope", frame(2, `not json`), "parse end-of-stream envelope"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := readEnvelopes(strings.NewReader(tt.stream))
			if err == nil {
				t.Fatalf("readEnvelopes accepted %s", tt.name)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestReadEnvelopesRejectsATruncatedBody(t *testing.T) {
	full := frame(0, `{"event":{"start":{}}}`)
	if _, err := readEnvelopes(strings.NewReader(full[:len(full)-3])); err == nil {
		t.Fatal("readEnvelopes accepted a truncated message body")
	}
}

func TestReadEnvelopesRejectsAShortHeader(t *testing.T) {
	if _, err := readEnvelopes(io.LimitReader(strings.NewReader(frame(0, `{}`)), 3)); err == nil {
		t.Fatal("readEnvelopes accepted a truncated header")
	}
}

func frame(flag byte, payload string) string {
	head := make([]byte, 5)
	head[0] = flag
	binary.BigEndian.PutUint32(head[1:], uint32(len(payload)))
	return string(head) + payload
}
