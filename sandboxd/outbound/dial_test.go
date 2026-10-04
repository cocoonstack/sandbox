package outbound

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
)

func TestEgressDialerBlocksInternal(t *testing.T) {
	blocked := []string{
		"127.0.0.1:80", "169.254.169.254:80", "10.0.0.5:443", "192.168.1.1:22",
		"172.16.0.1:80", "255.255.255.255:80", "[::1]:80", "[::ffff:127.0.0.1]:80",
		"[fe80::1]:80", "[fc00::1]:80", "[ff02::1]:80",

		"0.6.6.6:80", "100.100.100.200:80", "192.0.0.1:80", "192.0.2.1:80",
		"192.88.99.2:80", "198.18.0.1:80", "198.19.255.255:80", "198.51.100.1:80",
		"203.0.113.1:80", "240.1.2.3:80",

		"[100::1]:80", "[100:0:0:1::1]:80", "[2001:db8::1]:80", "[3fff::1]:80",
		"[5f00::1]:80", "[::127.0.0.1]:80",
		"[2001::a9fe:a9fe]:80",
		"[2001:2::1]:80",
		"[2001:10::1]:80",
		"[64:ff9b:1::8.8.8.8]:80",
		"[2002:a9fe:a9fe::]:80",
		"[64:ff9b::a9fe:a9fe]:80",
		"[64:ff9b::a00:1]:80",
	}
	for _, addr := range blocked {
		if err := newDialer(func() []egress.InternalAllow { return nil }).Control("tcp", addr, nil); err == nil {
			t.Errorf("dial to internal %s allowed; SSRF not blocked", addr)
		}
	}

	for _, addr := range []string{"93.184.216.34:443", "[2606:4700:4700::1111]:443", "[64:ff9b::5db8:d822]:443"} {
		if err := newDialer(func() []egress.InternalAllow { return nil }).Control("tcp", addr, nil); err != nil {
			t.Errorf("dial to public %s blocked: %v", addr, err)
		}
	}
}

func TestEgressDialerAdmitsOnlyNamedInternalPrefixes(t *testing.T) {
	d := newDialer(func() []egress.InternalAllow { return parseInternalAllow([]string{"fdc8::/16", "10.8.0.0/16"}) })
	check := func(addr string) error {
		return d.Control("tcp", addr, nil)
	}
	for name, tc := range map[string]struct {
		addr    string
		allowed bool
	}{
		"public v4":    {"93.184.216.34:443", true},
		"public v6":    {"[2606:2800:220:1:248:1893:25c8:1946]:443", true},
		"corporate v6": {"[fdc8:17:9:200f::1]:443", true},
		"corporate v4": {"10.8.7.149:443", true},

		"another sandbox on the bridge": {"[fd00:c0c0:38::5]:443", false},
		"the host's bridge gateway":     {"[fd00:c0c0:38::1]:443", false},
		"other private v4":              {"192.168.1.1:443", false},
		"loopback":                      {"127.0.0.1:443", false},
		"cloud metadata":                {"169.254.169.254:80", false},
		"CGNAT metadata":                {"100.100.100.200:80", false},
	} {
		t.Run(name, func(t *testing.T) {
			err := check(tc.addr)
			if tc.allowed && err != nil {
				t.Errorf("%s was blocked: %v", tc.addr, err)
			}
			if !tc.allowed && err == nil {
				t.Errorf("%s was allowed; it must not be reachable from a guest", tc.addr)
			}
		})
	}
}

func TestEgressDialerWildcardAllowsEverything(t *testing.T) {
	d := newDialer(func() []egress.InternalAllow { return parseInternalAllow([]string{"0.0.0.0/0", "::/0"}) })
	for _, addr := range []string{
		"93.184.216.34:443",
		"[fdc8:17:9:200f::1]:443",
		"10.8.7.149:443",
		"[fd00:c0c0:38::5]:443",
		"127.0.0.1:7777",
		"169.254.169.254:80",
	} {
		if err := d.Control("tcp", addr, nil); err != nil {
			t.Errorf("%s blocked under a wildcard allow-list: %v", addr, err)
		}
	}
}

func TestEgressDialerScopesInternalAllowToPorts(t *testing.T) {
	d := newDialer(func() []egress.InternalAllow {
		return parseInternalAllow([]string{"10.8.0.1/32:18090,9000", "fdc8::/16:443", "10.9.0.0/16"})
	})
	for name, tc := range map[string]struct {
		addr    string
		allowed bool
	}{
		"listed v4 port":        {"10.8.0.1:18090", true},
		"second listed v4 port": {"10.8.0.1:9000", true},
		"listed v6 port":        {"[fdc8:17:9:200f::1]:443", true},
		"bare prefix any port":  {"10.9.3.4:22", true},
		"NAT64 listed port":     {"[64:ff9b::a08:1]:18090", true},

		"unlisted v4 port":   {"10.8.0.1:22", false},
		"kubelet":            {"10.8.0.1:10250", false},
		"unlisted v6 port":   {"[fdc8:17:9:200f::1]:22", false},
		"NAT64 unlisted":     {"[64:ff9b::a08:1]:6443", false},
		"outside the prefix": {"10.8.0.2:18090", false},
	} {
		t.Run(name, func(t *testing.T) {
			err := d.Control("tcp", tc.addr, nil)
			if tc.allowed && err != nil {
				t.Errorf("%s was blocked: %v", tc.addr, err)
			}
			if !tc.allowed && (err == nil || !strings.Contains(err.Error(), "blocked internal address")) {
				t.Errorf("%s: %v, want blocked internal address", tc.addr, err)
			}
		})
	}
}

func TestTheTunnelTargetPrefersIPv4(t *testing.T) {
	h := newHost(t, &fakeEngine{}, &config.Config{}, func(o *Options) {
		o.Verdict = func(netip.Addr, uint16) (bool, error) { return false, nil }
	})
	target, internal, err := h.upstreamTarget(t.Context(), "localhost:80")
	if err != nil || internal || target != netip.MustParseAddrPort("127.0.0.1:80") {
		t.Errorf("target %v internal %v err %v, want 127.0.0.1:80 over ::1", target, internal, err)
	}
}
