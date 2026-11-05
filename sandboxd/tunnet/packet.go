package tunnet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
)

const (
	// HelloLen is the size of the hello each side sends first.
	HelloLen = 12
	// VnetHdrLen is the virtio-net header that leads every packet in a frame.
	VnetHdrLen = 10
	// MaxFrame is the largest frame body: a virtio-net header and a 64 KiB GSO packet.
	MaxFrame = VnetHdrLen + 65535
	// Version is the packet protocol version this side speaks.
	Version = 1
	// FlagOn in the host hello turns packet mode on for the guest.
	FlagOn = 0x01

	// Offload bits a hello carries, equal to the kernel's TUN_F_* values.
	OffloadCSUM   = 0x01
	OffloadTSO4   = 0x02
	OffloadTSO6   = 0x04
	OffloadTSOECN = 0x08
	OffloadUSO4   = 0x20
	OffloadUSO6   = 0x40

	// IPv4 protocols the tun path carries.
	ProtoICMP = 1
	ProtoTCP  = 6
	ProtoUDP  = 17

	needsCsum = 0x01
	srcAt     = 12
	dstAt     = 16
	tcpFin    = 0x01
	tcpSyn    = 0x02
	tcpRst    = 0x04
)

var (
	// GuestAddr is the address every guest's tun0 carries.
	GuestAddr = netip.AddrFrom4([4]byte{100, 64, 0, 2})

	errMagic = errors.New("tun hello: bad magic")

	magic = [4]byte{'S', 'L', 'K', 'T'}
)

// Hello is the 12 bytes each side sends first: version, the offloads its tun accepts, the MTU and flags (host only).
type Hello struct {
	Version  uint16
	Offloads uint16
	MTU      uint16
	Flags    uint16
}

// Encode lays the hello out on the wire.
func (h Hello) Encode() [HelloLen]byte {
	var b [HelloLen]byte
	copy(b[:4], magic[:])
	binary.LittleEndian.PutUint16(b[4:], h.Version)
	binary.LittleEndian.PutUint16(b[6:], h.Offloads)
	binary.LittleEndian.PutUint16(b[8:], h.MTU)
	binary.LittleEndian.PutUint16(b[10:], h.Flags)
	return b
}

// ReadHello reads and decodes one hello.
func ReadHello(r io.Reader) (Hello, error) {
	var b [HelloLen]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return Hello{}, err
	}
	if [4]byte(b[:4]) != magic {
		return Hello{}, errMagic
	}
	return Hello{
		Version:  binary.LittleEndian.Uint16(b[4:]),
		Offloads: binary.LittleEndian.Uint16(b[6:]),
		MTU:      binary.LittleEndian.Uint16(b[8:]),
		Flags:    binary.LittleEndian.Uint16(b[10:]),
	}, nil
}

// FrameLen validates a frame's length prefix.
func FrameLen(head [4]byte) (int, error) {
	n := int(binary.LittleEndian.Uint32(head[:]))
	if n < VnetHdrLen || n > MaxFrame {
		return 0, fmt.Errorf("tun frame length %d", n)
	}
	return n, nil
}

// Packet is the IPv4 header view of one frame body: a virtio-net header, then the packet.
type Packet struct {
	Proto            uint8
	Src, Dst         netip.Addr
	SrcPort, DstPort uint16
	// End marks a TCP FIN or RST; Open a SYN without ACK.
	End, Open bool
	ICMPType uint8
}

// Parse reads frame's IPv4 header and refuses, with the reason, what the first version does not carry: other versions, IP options, fragments, a length that disagrees with the frame.
func Parse(frame []byte) (Packet, Drop) {
	if len(frame) < VnetHdrLen+20 {
		return Packet{}, DropMalformed
	}
	ip := frame[VnetHdrLen:]
	switch {
	case ip[0]>>4 != 4:
		return Packet{}, DropNotIPv4
	case ip[0]&0x0f != 5:
		return Packet{}, DropOptions
	case int(binary.BigEndian.Uint16(ip[2:])) != len(ip):
		return Packet{}, DropMalformed
	case binary.BigEndian.Uint16(ip[6:])&0x3fff != 0:
		return Packet{}, DropFragment
	}
	p := Packet{
		Proto: ip[9],
		Src:   netip.AddrFrom4([4]byte(ip[srcAt : srcAt+4])),
		Dst:   netip.AddrFrom4([4]byte(ip[dstAt : dstAt+4])),
	}
	if !headerFits(frame, p.Proto) {
		return Packet{}, DropGSO
	}
	l4 := ip[20:]
	switch p.Proto {
	case ProtoTCP:
		if len(l4) < 20 || int(l4[12]>>4)*4 < 20 || int(l4[12]>>4)*4 > len(l4) {
			return Packet{}, DropMalformed
		}
		p.SrcPort, p.DstPort = binary.BigEndian.Uint16(l4), binary.BigEndian.Uint16(l4[2:])
		p.End = l4[13]&(tcpFin|tcpRst) != 0
		p.Open = l4[13]&(tcpSyn|tcpAck) == tcpSyn
	case ProtoUDP:
		if len(l4) < 8 {
			return Packet{}, DropMalformed
		}
		p.SrcPort, p.DstPort = binary.BigEndian.Uint16(l4), binary.BigEndian.Uint16(l4[2:])
	case ProtoICMP:
		if len(l4) < 8 {
			return Packet{}, DropMalformed
		}
		p.ICMPType = l4[0]
	default:
		return Packet{}, DropProto
	}
	return p, DropNone
}

// RewriteSrc overwrites the source address whatever it was.
func RewriteSrc(frame []byte, to netip.Addr) {
	rewrite(frame, srcAt, to.As4())
}

// RewriteDst overwrites the destination address.
func RewriteDst(frame []byte, to netip.Addr) {
	rewrite(frame, dstAt, to.As4())
}

// wireCost is what a parsed frame puts on the wire once segmented: its packet count and IP bytes; false when a packet would exceed mtu.
func wireCost(frame []byte, mtu int) (segs, bytes int, ok bool) {
	ip := frame[VnetHdrLen:]
	if frame[1]&^gsoECN == gsoNone {
		return 1, len(ip), len(ip) <= mtu
	}
	hdrs := 20 + 8
	if ip[9] == ProtoTCP {
		hdrs = 20 + int(ip[32]>>4)*4
	}
	size := int(binary.LittleEndian.Uint16(frame[4:]))
	if hdrs+size > mtu {
		return 0, 0, false
	}
	segs = max((len(ip)-hdrs+size-1)/size, 1)
	return segs, len(ip) + (segs-1)*hdrs, true
}

// rewriteQuotedSrc moves the source of the packet an ICMP error quotes from one address to another and refreshes both checksums, so the guest's socket lookup finds it.
func rewriteQuotedSrc(frame []byte, from, to netip.Addr) {
	ip := frame[VnetHdrLen:]
	if len(ip) < 28+20 || [4]byte(ip[28+srcAt:28+srcAt+4]) != from.As4() {
		return
	}
	q, f, t := ip[28:], from.As4(), to.As4()
	copy(q[srcAt:], t[:])
	binary.BigEndian.PutUint16(q[10:], ^adjust(^binary.BigEndian.Uint16(q[10:]), f, t))
	binary.BigEndian.PutUint16(ip[22:], 0)
	binary.BigEndian.PutUint16(ip[22:], ^fold(onesSum(ip[20:], 0)))
}

// headerFits checks the virtio-net header against the packet: a GSO type matches the protocol, and a partial checksum sits in the L4 header's checksum field.
func headerFits(frame []byte, proto uint8) bool {
	field := 16
	switch frame[1] &^ gsoECN {
	case gsoNone:
		if proto == ProtoUDP {
			field = 6
		}
	case gsoTCPv4:
		if proto != ProtoTCP || binary.LittleEndian.Uint16(frame[4:]) == 0 {
			return false
		}
	case gsoUDPL4:
		if proto != ProtoUDP || binary.LittleEndian.Uint16(frame[4:]) == 0 {
			return false
		}
		field = 6
	default:
		return false
	}
	if frame[0]&needsCsum == 0 {
		return true
	}
	return proto != ProtoICMP && binary.LittleEndian.Uint16(frame[6:]) == 20 && int(binary.LittleEndian.Uint16(frame[8:])) == field
}

// rewrite patches the IPv4 header checksum and the L4 checksum: a partial one holds the uncomplemented pseudo-header sum, a UDP checksum of 0 stays 0.
func rewrite(frame []byte, at int, to [4]byte) {
	ip := frame[VnetHdrLen:]
	from := [4]byte(ip[at : at+4])
	if from == to {
		return
	}
	copy(ip[at:at+4], to[:])
	check := binary.BigEndian.Uint16(ip[10:])
	binary.BigEndian.PutUint16(ip[10:], ^adjust(^check, from, to))
	var field int
	partial := frame[0]&needsCsum != 0
	l4 := int(ip[0]&0x0f) * 4
	switch {
	case partial:
		field = int(binary.LittleEndian.Uint16(frame[6:])) + int(binary.LittleEndian.Uint16(frame[8:]))
	case ip[9] == ProtoTCP:
		field = l4 + 16
	case ip[9] == ProtoUDP:
		field = l4 + 6
	default:
		return
	}
	if field+2 > len(ip) {
		return
	}
	sum := binary.BigEndian.Uint16(ip[field:])
	switch {
	case partial:
		sum = adjust(sum, from, to)
	case ip[9] == ProtoUDP && sum == 0:
	default:
		sum = ^adjust(^sum, from, to)
	}
	binary.BigEndian.PutUint16(ip[field:], sum)
}

// adjust moves a ones'-complement sum from one address to another (RFC 1624).
func adjust(sum uint16, from, to [4]byte) uint16 {
	s := uint32(sum)
	for i := 0; i < 4; i += 2 {
		s += uint32(^binary.BigEndian.Uint16(from[i:]))
		s += uint32(binary.BigEndian.Uint16(to[i:]))
	}
	for s>>16 != 0 {
		s = s&0xffff + s>>16
	}
	return uint16(s)
}
