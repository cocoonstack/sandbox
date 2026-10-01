package egress

import (
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

// InternalAllow re-admits Prefix through the proxy's internal-address guard on Ports; empty means every port.
type InternalAllow struct {
	Prefix netip.Prefix
	Ports  []uint16
}

// ParseInternalAllow reads "prefix" or "prefix:port,port"; the colon after the prefix length is unambiguous in both families.
func ParseInternalAllow(s string) (InternalAllow, error) {
	cidr, list, scoped := s, "", false
	if addr, rest, ok := strings.CutLast(s, "/"); ok {
		var bits string
		bits, list, scoped = strings.Cut(rest, ":")
		cidr = addr + "/" + bits
	}
	p, err := netip.ParsePrefix(cidr)
	if err != nil {
		return InternalAllow{}, err
	}
	a := InternalAllow{Prefix: p}
	if !scoped {
		return a, nil
	}
	for field := range strings.SplitSeq(list, ",") {
		port, err := strconv.ParseUint(field, 10, 16)
		if err != nil || port == 0 {
			return InternalAllow{}, fmt.Errorf("port %q is not in 1-65535", field)
		}
		if slices.Contains(a.Ports, uint16(port)) {
			return InternalAllow{}, fmt.Errorf("port %d repeated", port)
		}
		a.Ports = append(a.Ports, uint16(port))
	}
	return a, nil
}

// Admits reports whether ip, already unwrapped from NAT64, and port fall inside the entry.
func (a InternalAllow) Admits(ip netip.Addr, port uint16) bool {
	return a.Prefix.Contains(ip) && (len(a.Ports) == 0 || slices.Contains(a.Ports, port))
}
