package server

import (
	"testing"
	"time"
)

func TestTTLFieldReachesTheClampForAHugeCount(t *testing.T) {
	if got := (TTLField{TTLSeconds: 9223372037}).TTL(); got <= 24*time.Hour {
		t.Errorf("TTL() = %v for a huge ttl_seconds, want it to reach the 24h clamp", got)
	}
}
