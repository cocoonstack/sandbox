package tunnet

import (
	"encoding/binary"
	"net/netip"
)

const (
	// VXLANHdrLen is the VXLAN header; EthHdrLen the inner Ethernet header sandboxd builds.
	VXLANHdrLen = 8
	EthHdrLen   = 14
	// Overhead is what a VXLAN datagram adds before the inner IP packet.
	Overhead = VXLANHdrLen + EthHdrLen

	etherIPv4 = 0x0800
	etherARP  = 0x0806
	vxlanI    = 0x08
	arpLen    = 28
	arpReq    = 1
	arpReply  = 2
)

var broadcast = [6]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

// MAC derives a claim's MAC from its overlay address: a locally administered prefix and the IPv4 bytes.
func MAC(addr netip.Addr) [6]byte {
	a := addr.As4()
	return [6]byte{0x02, 0x00, a[0], a[1], a[2], a[3]}
}

func appendHeader(b []byte, vni uint32, dst, src [6]byte, etype uint16) []byte {
	b = binary.BigEndian.AppendUint32(append(b, vxlanI, 0, 0, 0), vni<<8)
	b = append(b, dst[:]...)
	b = append(b, src[:]...)
	return binary.BigEndian.AppendUint16(b, etype)
}

// parseVXLAN returns the Ethernet payload and type of a datagram for vni; false for anything else.
func parseVXLAN(d []byte, vni uint32) ([]byte, uint16, bool) {
	if len(d) < Overhead || d[0]&vxlanI == 0 {
		return nil, 0, false
	}
	if uint32(d[4])<<16|uint32(d[5])<<8|uint32(d[6]) != vni {
		return nil, 0, false
	}
	return d[Overhead:], binary.BigEndian.Uint16(d[Overhead-2:]), true
}

// arpRequest reads an IPv4-over-Ethernet ARP request: who asks, from which MAC, for which address.
func arpRequest(p []byte) (sender netip.Addr, mac [6]byte, target netip.Addr, ok bool) {
	if len(p) < arpLen || binary.BigEndian.Uint16(p) != 1 || binary.BigEndian.Uint16(p[2:]) != etherIPv4 ||
		p[4] != 6 || p[5] != 4 || binary.BigEndian.Uint16(p[6:]) != arpReq {
		return netip.Addr{}, mac, netip.Addr{}, false
	}
	copy(mac[:], p[8:14])
	return netip.AddrFrom4([4]byte(p[14:18])), mac, netip.AddrFrom4([4]byte(p[24:28])), true
}

func appendARP(b []byte, op uint16, sha [6]byte, spa netip.Addr, tha [6]byte, tpa netip.Addr) []byte {
	b = append(b, 0, 1, 0x08, 0x00, 6, 4)
	b = binary.BigEndian.AppendUint16(b, op)
	s, t := spa.As4(), tpa.As4()
	b = append(b, sha[:]...)
	b = append(b, s[:]...)
	b = append(b, tha[:]...)
	return append(b, t[:]...)
}

// gratuitousARP is a broadcast ARP request announcing addr at its MAC, which a learning VXLAN device turns into an FDB and a neighbor entry.
func gratuitousARP(vni uint32, addr netip.Addr) []byte {
	mac := MAC(addr)
	b := appendHeader(make([]byte, 0, Overhead+arpLen), vni, broadcast, mac, etherARP)
	return appendARP(b, arpReq, mac, addr, [6]byte{}, addr)
}

func arpReplyFor(vni uint32, addr, sender netip.Addr, senderMAC [6]byte) []byte {
	mac := MAC(addr)
	b := appendHeader(make([]byte, 0, Overhead+arpLen), vni, senderMAC, mac, etherARP)
	return appendARP(b, arpReply, mac, addr, senderMAC, sender)
}
