package outbound

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"syscall"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
)

var (
	nat64Range = netip.MustParsePrefix("64:ff9b::/96") // RFC 6052 NAT64; the embedded v4 is checked instead

	// internalRanges lists the IANA special-purpose prefixes IsGlobalUnicast/IsPrivate leave in.
	internalRanges = []netip.Prefix{
		netip.MustParsePrefix("0.0.0.0/8"),       // "this network"
		netip.MustParsePrefix("100.64.0.0/10"),   // RFC 6598 CGNAT; some clouds host metadata here
		netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments
		netip.MustParsePrefix("192.0.2.0/24"),    // TEST-NET-1
		netip.MustParsePrefix("192.88.99.2/32"),  // 6a44-relay anycast
		netip.MustParsePrefix("198.18.0.0/15"),   // benchmarking
		netip.MustParsePrefix("198.51.100.0/24"), // TEST-NET-2
		netip.MustParsePrefix("203.0.113.0/24"),  // TEST-NET-3
		netip.MustParsePrefix("240.0.0.0/4"),     // reserved

		netip.MustParsePrefix("::/96"),          // deprecated IPv4-compatible; embeds a v4 unmapped
		netip.MustParsePrefix("64:ff9b:1::/48"), // RFC 8215 local-use NAT64; embeds a v4
		netip.MustParsePrefix("100::/64"),       // discard-only
		netip.MustParsePrefix("100:0:0:1::/64"), // dummy prefix
		netip.MustParsePrefix("2001::/23"),      // IETF protocol assignments (Teredo, benchmarking, ORCHID)
		netip.MustParsePrefix("2001:db8::/32"),  // documentation
		netip.MustParsePrefix("2002::/16"),      // 6to4; embeds a v4
		netip.MustParsePrefix("3fff::/20"),      // documentation
		netip.MustParsePrefix("5f00::/16"),      // SRv6 SIDs
	}
)

// upstreamTarget checks every address addr resolves to: a re-admitted internal one dials direct, a blocked one fails, else the first IPv4 (or the first) is the tunnel target.
func (h *Host) upstreamTarget(ctx context.Context, addr string) (netip.AddrPort, bool, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return netip.AddrPort{}, false, err
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return netip.AddrPort{}, false, fmt.Errorf("egress: bad port in %q: %w", addr, err)
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return netip.AddrPort{}, false, err
	}
	var target netip.AddrPort
	for _, ip := range ips {
		ap := netip.AddrPortFrom(ip.Unmap(), uint16(port))
		internal, err := h.o.Verdict(ap.Addr(), ap.Port())
		if err != nil {
			return netip.AddrPort{}, false, err
		}
		if internal {
			return ap, true, nil
		}
		if !target.IsValid() || target.Addr().Is6() && ap.Addr().Is4() {
			target = ap
		}
	}
	if !target.IsValid() {
		return netip.AddrPort{}, false, fmt.Errorf("egress: %s resolves to no address", host)
	}
	return target, false, nil
}

func newDialer(allow func() []egress.InternalAllow) *net.Dialer {
	return &net.Dialer{Control: func(_, address string, _ syscall.RawConn) error {
		ap, err := netip.ParseAddrPort(address)
		if err != nil {
			return fmt.Errorf("egress: unresolved address %q: %w", address, err)
		}
		_, err = destVerdict(allow(), ap.Addr(), ap.Port())
		return err
	}}
}

// destVerdict reports whether ip is an internal address allow re-admits; any other internal address is an error.
func destVerdict(allow []egress.InternalAllow, ip netip.Addr, port uint16) (bool, error) {
	ip = ip.Unmap()
	if nat64Range.Contains(ip) {
		b := ip.As16()
		ip = netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]})
	}
	// after the NAT64 unwrap and before the block, so a named prefix wins
	if slices.ContainsFunc(allow, func(a egress.InternalAllow) bool { return a.Admits(ip, port) }) {
		return true, nil
	}
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		slices.ContainsFunc(internalRanges, func(p netip.Prefix) bool { return p.Contains(ip) }) {
		return false, fmt.Errorf("egress: blocked internal address %s", ip)
	}
	return false, nil
}

// parseInternalAllow turns the allow-list into entries; config validation already rejected bad ones.
func parseInternalAllow(entries []string) []egress.InternalAllow {
	out := make([]egress.InternalAllow, 0, len(entries))
	for _, e := range entries {
		if a, err := egress.ParseInternalAllow(e); err == nil {
			out = append(out, a)
		}
	}
	return out
}
