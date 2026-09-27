package types

import (
	"math"
	"strings"
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

func TestMetadataValidateNamesTheFailedBound(t *testing.T) {
	for _, tt := range []struct {
		name string
		md   Metadata
		want string
	}{
		{"absent", nil, ""},
		{"at the bounds", Metadata{strings.Repeat("k", maxMetadataKeyBytes): strings.Repeat("v", maxMetadataValueBytes), "e": ""}, ""},
		{"printable punctuation", Metadata{"a.b/c:d e~": "x=y&z"}, ""},
		{"sixteen pairs", pairsOf(maxMetadataPairs, 1), ""},
		{"exactly the total", pairsOf(8, maxMetadataBytes/8-1), ""},
		{"pair count", pairsOf(maxMetadataPairs+1, 1), "at most 16 pairs"},
		{"empty key", Metadata{"": "v"}, "metadata key"},
		{"long key", Metadata{strings.Repeat("k", maxMetadataKeyBytes+1): "v"}, "metadata key"},
		{"equals in key", Metadata{"a=b": "v"}, "metadata key"},
		{"ampersand in key", Metadata{"a&b": "v"}, "metadata key"},
		{"control byte in key", Metadata{"a\tb": "v"}, "metadata key"},
		{"non-ASCII key", Metadata{"caf\u00e9": "v"}, "metadata key"},
		{"long value", Metadata{"k": strings.Repeat("v", maxMetadataValueBytes+1)}, "metadata value"},
		{"total", pairsOf(8, maxMetadataBytes/8), "total at most 4096 bytes"},
	} {
		err := tt.md.Validate()
		if tt.want == "" {
			if err != nil {
				t.Errorf("%s: %v, want accepted", tt.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: %v, want an error naming %q", tt.name, err, tt.want)
		}
	}
}

func TestMetadataMatchesEveryFilterPair(t *testing.T) {
	md := Metadata{"team": "a", "env": "prod", "empty": ""}
	for _, tt := range []struct {
		filter Metadata
		want   bool
	}{
		{nil, true},
		{Metadata{"team": "a"}, true},
		{Metadata{"team": "a", "env": "prod"}, true},
		{Metadata{"empty": ""}, true},
		{Metadata{"team": "a", "env": "dev"}, false},
		{Metadata{"missing": ""}, false},
	} {
		if got := md.Matches(tt.filter); got != tt.want {
			t.Errorf("Matches(%v) = %t, want %t", tt.filter, got, tt.want)
		}
	}
	if Metadata(nil).Matches(Metadata{"team": "a"}) {
		t.Error("a claim without metadata matched a non-empty filter")
	}
}

func pairsOf(n, valueBytes int) Metadata {
	md := make(Metadata, n)
	for i := range n {
		md[string(rune('a'+i))] = strings.Repeat("v", valueBytes)
	}
	return md
}
