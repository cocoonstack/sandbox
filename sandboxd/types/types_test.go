package types

import (
	"encoding/json/v2"
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
		{"sixteen pairs", pairsOf(maxMetadataPairs, "v"), ""},
		{"exactly the JSON total", withValue(pairsOf(8, strings.Repeat("v", 504)), "a", strings.Repeat("v", 511)), ""},
		{"printable UTF-8 value", Metadata{"k": "caf\u00e9 \u4e2d\u6587"}, ""},
		{"pair count", pairsOf(maxMetadataPairs+1, "v"), "at most 16 pairs"},
		{"empty key", Metadata{"": "v"}, "metadata key"},
		{"long key", Metadata{strings.Repeat("k", maxMetadataKeyBytes+1): "v"}, "metadata key"},
		{"equals in key", Metadata{"a=b": "v"}, "metadata key"},
		{"ampersand in key", Metadata{"a&b": "v"}, "metadata key"},
		{"control byte in key", Metadata{"a\tb": "v"}, "metadata key"},
		{"non-ASCII key", Metadata{"caf\u00e9": "v"}, "metadata key"},
		{"long value", Metadata{"k": strings.Repeat("v", maxMetadataValueBytes+1)}, "metadata value"},
		{"control byte in value", Metadata{"k": "a\x01b"}, "printable UTF-8"},
		{"tab in value", Metadata{"k": "a\tb"}, "printable UTF-8"},
		{"invalid UTF-8 value", Metadata{"k": "a\xffb"}, "printable UTF-8"},
		{"one byte over the JSON total", withValue(pairsOf(8, strings.Repeat("v", 504)), "a", strings.Repeat("v", 512)), "at most 4096 bytes as JSON"},
		{"quotes count as escaped", pairsOf(8, strings.Repeat(`"`, 300)), "at most 4096 bytes as JSON"},
		{"backslashes count as escaped", pairsOf(8, strings.Repeat(`\`, 300)), "at most 4096 bytes as JSON"},
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

func TestMetadataEncodedSizeIsTheDeterministicJSONLength(t *testing.T) {
	for _, md := range []Metadata{
		nil,
		{},
		{"k": ""},
		{"a.b/c:d e~": "x=y&z", "q\"k": `say "hi"`, "b\\k": `c:\dir\`},
		{"k": "caf\u00e9 \u4e2d\u6587 <>&", "l": "\u2028\u2029"},
		pairsOf(maxMetadataPairs, strings.Repeat(`"`, 40)),
	} {
		b, err := json.Marshal(md, json.Deterministic(true))
		if err != nil {
			t.Fatal(err)
		}
		if got := md.encodedSize(); got != len(b) {
			t.Errorf("encodedSize(%v) = %d, want the %d bytes of %s", md, got, len(b), b)
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

func TestExpireActionValidateAndOr(t *testing.T) {
	for _, a := range []ExpireAction{"", ExpireDestroy, ExpireArchive} {
		if err := a.Validate(); err != nil {
			t.Errorf("Validate(%q) = %v, want accepted", a, err)
		}
	}
	if err := ExpireAction("pause").Validate(); err == nil || !strings.Contains(err.Error(), "on_expire") {
		t.Errorf("Validate(pause) = %v, want an error naming on_expire", err)
	}
	for _, tt := range []struct {
		requested, current, want ExpireAction
	}{
		{"", ExpireArchive, ExpireArchive},
		{"", "", ""},
		{ExpireDestroy, ExpireArchive, ""},
		{ExpireArchive, "", ExpireArchive},
	} {
		if got := tt.requested.Or(tt.current); got != tt.want {
			t.Errorf("%q.Or(%q) = %q, want %q", tt.requested, tt.current, got, tt.want)
		}
	}
}

func withValue(md Metadata, k, v string) Metadata {
	md[k] = v
	return md
}

func pairsOf(n int, value string) Metadata {
	md := make(Metadata, n)
	for i := range n {
		md[string(rune('a'+i))] = value
	}
	return md
}
