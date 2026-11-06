package tunnet

import "encoding/binary"

const (
	gsoNone  = 0
	gsoTCPv4 = 1
	gsoUDPL4 = 5
	gsoECN   = 0x80

	tcpPsh = 0x08
	tcpCwr = 0x80
)

// appendSegments appends frame's packets to out behind hdr, splitting a GSO frame and finishing a partial checksum; it returns out, the length of every datagram but the last, and the count, zero for a dropped frame.
func appendSegments(out, hdr, frame []byte) ([]byte, int, int) {
	if len(frame) < VnetHdrLen+20 {
		return out, 0, 0
	}
	ip := frame[VnetHdrLen:]
	ihl := int(ip[0]&0x0f) * 4
	if ihl < 20 || len(ip) < ihl {
		return out, 0, 0
	}
	gsoSize := int(binary.LittleEndian.Uint16(frame[4:]))
	switch frame[1] &^ gsoECN {
	case gsoNone:
		if frame[0]&needsCsum != 0 {
			start := int(binary.LittleEndian.Uint16(frame[6:]))
			field := start + int(binary.LittleEndian.Uint16(frame[8:]))
			if field+2 > len(ip) || start > field {
				return out, 0, 0
			}
			sum := ^fold(onesSum(ip[start:], 0))
			if sum == 0 && ip[9] == ProtoUDP {
				sum = 0xffff
			}
			binary.BigEndian.PutUint16(ip[field:], sum)
		}
		return append(append(out, hdr...), ip...), len(hdr) + len(ip), 1
	case gsoTCPv4:
		if len(ip) < ihl+20 || gsoSize == 0 || ip[9] != ProtoTCP {
			return out, 0, 0
		}
		return split(out, hdr, ip, ihl, int(ip[ihl+12]>>4)*4, gsoSize)
	case gsoUDPL4:
		if len(ip) < ihl+8 || gsoSize == 0 || ip[9] != ProtoUDP {
			return out, 0, 0
		}
		return split(out, hdr, ip, ihl, 8, gsoSize)
	default:
		return out, 0, 0
	}
}

// split cuts the payload after ihl+l4len header bytes into size-byte pieces, each with its own IP and L4 header, straight into out.
func split(out, hdr, ip []byte, ihl, l4len, size int) ([]byte, int, int) {
	hdrs := ihl + l4len
	if l4len < 8 || len(ip) < hdrs || ip[9] == ProtoTCP && l4len < 20 {
		return out, 0, 0
	}
	payload := ip[hdrs:]
	tcp := ip[9] == ProtoTCP
	id := binary.BigEndian.Uint16(ip[4:])
	var seq uint32
	if tcp {
		seq = binary.BigEndian.Uint32(ip[ihl+4:])
	}
	segs := 0
	for off := 0; off < len(payload) || segs == 0; off += size {
		n := min(size, len(payload)-off)
		out = append(out, hdr...)
		at := len(out)
		out = append(append(out, ip[:hdrs]...), payload[off:off+n]...)
		seg := out[at:]
		binary.BigEndian.PutUint16(seg[2:], uint16(hdrs+n)) //nolint:gosec // a segment fits a packet
		binary.BigEndian.PutUint16(seg[4:], id+uint16(segs))
		binary.BigEndian.PutUint16(seg[10:], 0)
		binary.BigEndian.PutUint16(seg[10:], ^fold(onesSum(seg[:ihl], 0)))
		l4 := seg[ihl:]
		field := 6
		if tcp {
			field = 16
			binary.BigEndian.PutUint32(l4[4:], seq+uint32(off))
			if off+n < len(payload) {
				l4[13] &^= tcpFin | tcpPsh
			}
			if segs > 0 {
				l4[13] &^= tcpCwr
			}
		} else {
			binary.BigEndian.PutUint16(l4[4:], uint16(l4len+n)) //nolint:gosec // a segment fits a datagram
		}
		binary.BigEndian.PutUint16(l4[field:], 0)
		sum := ^fold(onesSum(l4, pseudoSum(seg, l4len+n)))
		if sum == 0 && !tcp {
			sum = 0xffff
		}
		binary.BigEndian.PutUint16(l4[field:], sum)
		segs++
	}
	return out, len(hdr) + min(hdrs+size, len(ip)), segs
}

// pseudoSum is the IPv4 pseudo-header sum for an L4 segment of l4len bytes.
func pseudoSum(ip []byte, l4len int) uint64 {
	return onesSum(ip[srcAt:dstAt+4], uint64(ip[9])+uint64(l4len)) //nolint:gosec // l4len is a segment length
}

// onesSum adds b to s as big-endian 16-bit words, four at a time.
func onesSum(b []byte, s uint64) uint64 {
	for len(b) >= 8 {
		s += uint64(binary.BigEndian.Uint32(b)) + uint64(binary.BigEndian.Uint32(b[4:]))
		b = b[8:]
	}
	for len(b) >= 2 {
		s += uint64(binary.BigEndian.Uint16(b))
		b = b[2:]
	}
	if len(b) == 1 {
		s += uint64(b[0]) << 8
	}
	return s
}

func fold(s uint64) uint16 {
	for s>>16 != 0 {
		s = s&0xffff + s>>16
	}
	return uint16(s)
}
