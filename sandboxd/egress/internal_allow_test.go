package egress

import (
	"net/netip"
	"slices"
	"strings"
	"testing"
)

func TestParseInternalAllow(t *testing.T) {
	for _, tt := range []struct {
		entry  string
		prefix string
		ports  []uint16
	}{
		{"10.8.0.0/16", "10.8.0.0/16", nil},
		{"10.8.0.1/32:18090,9000", "10.8.0.1/32", []uint16{18090, 9000}},
		{"fdc8::/16", "fdc8::/16", nil},
		{"fdc8::1/128:443", "fdc8::1/128", []uint16{443}},
		{"::/0:65535", "::/0", []uint16{65535}},
	} {
		t.Run(tt.entry, func(t *testing.T) {
			got, err := ParseInternalAllow(tt.entry)
			if err != nil {
				t.Fatalf("ParseInternalAllow: %v", err)
			}
			if got.Prefix != netip.MustParsePrefix(tt.prefix) || !slices.Equal(got.Ports, tt.ports) {
				t.Errorf("got %v %v, want %s %v", got.Prefix, got.Ports, tt.prefix, tt.ports)
			}
		})
	}
}

func TestParseInternalAllowRejects(t *testing.T) {
	for _, tt := range []struct {
		entry, want string
	}{
		{"10.8.0.1", "no '/'"},
		{"10.8.0.1:80", "no '/'"},
		{"10.8.0.1/32:", `port ""`},
		{"10.8.0.1/32:0", `port "0"`},
		{"10.8.0.1/32:65536", `port "65536"`},
		{"10.8.0.1/32:http", `port "http"`},
		{"10.8.0.1/32:80,,443", `port ""`},
		{"10.8.0.1/32:+80", `port "+80"`},
		{"10.8.0.1/32:80,80", "port 80 repeated"},
		{"fdc8::/16:", `port ""`},
		{"10.8.0.1/33:80", "out of range"},
		{"[fdc8::]/16:443", "ParseAddr"},
	} {
		t.Run(tt.entry, func(t *testing.T) {
			if _, err := ParseInternalAllow(tt.entry); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("ParseInternalAllow: %v, want error containing %q", err, tt.want)
			}
		})
	}
}
