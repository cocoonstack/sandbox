package types

import (
	"math"
	"testing"
	"time"
)

func TestSecondsSaturatesInsteadOfWrapping(t *testing.T) {
	for _, n := range []int{9223372037, 10_000_000_000, math.MaxInt64} {
		if got := Seconds(n); got <= 24*time.Hour {
			t.Errorf("Seconds(%d) = %v, want a duration past any cap, never a wrapped one", n, got)
		}
	}
	if got := (TTLField{TTLSeconds: 9223372037}).TTL(); got <= 24*time.Hour {
		t.Errorf("TTL() = %v for a huge ttl_seconds, want it to reach the 24h clamp", got)
	}
	for _, n := range []int{-1, -10_000_000_000, math.MinInt64} {
		if got := Seconds(n); got != 0 {
			t.Errorf("Seconds(%d) = %v, want 0 (the server default), never a wrapped positive", n, got)
		}
	}
	if got := Seconds(90); got != 90*time.Second {
		t.Errorf("Seconds(90) = %v", got)
	}
}

func TestValidateSizes(t *testing.T) {
	for _, tt := range []struct {
		size Size
		ok   bool
	}{
		{SizeSmall, true},
		{SizeMedium, true},
		{SizeLarge, true},
		{SizeXLarge, true},
		{Size("huge"), false},
		{Size(""), false},
	} {
		t.Run(string(tt.size), func(t *testing.T) {
			err := PoolKey{Template: "rt:24.04", Net: NetNone, Size: tt.size}.Validate()
			if (err == nil) != tt.ok {
				t.Errorf("Validate: %v, want ok=%v", err, tt.ok)
			}
		})
	}
}

func TestXLargeSpec(t *testing.T) {
	spec, ok := SizeXLarge.Spec()
	if !ok || spec.CPU != 4 || spec.Memory != "8G" {
		t.Errorf("got %+v ok=%v, want {4 8G} true", spec, ok)
	}
}
