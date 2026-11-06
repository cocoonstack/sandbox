package tunnet

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"
)

func TestSegmentSplitsTCPAtGSOSizeWithValidHeaders(t *testing.T) {
	f := gsoFrame(ProtoTCP, 5000, 1400)
	ip := f[VnetHdrLen:]
	ip[20+13] = tcpFin | tcpPsh | tcpCwr
	payload := slices.Clone(ip[40:])
	got := segments(f)
	if len(got) != 4 {
		t.Fatalf("segments = %d, want 4", len(got))
	}
	var joined []byte
	for i, seg := range got {
		pkt := append(make([]byte, VnetHdrLen), seg...)
		if !validIP(pkt) || !validL4(pkt) {
			t.Errorf("segment %d checksum invalid", i)
		}
		if seq := binary.BigEndian.Uint32(seg[24:]); seq != 1000+uint32(i*1400) {
			t.Errorf("segment %d seq %d", i, seq)
		}
		last := i == len(got)-1
		if fl := seg[33]; (fl&tcpFin != 0) != last || (fl&tcpPsh != 0) != last || (fl&tcpCwr != 0) != (i == 0) {
			t.Errorf("segment %d flags %#x", i, fl)
		}
		joined = append(joined, seg[40:]...)
	}
	if !bytes.Equal(joined, payload) {
		t.Error("payload not preserved across segments")
	}
}

func TestSegmentSplitsUDPAndFinishesPartialChecksums(t *testing.T) {
	got := segments(gsoFrame(ProtoUDP, 3000, 1000))
	for i, pkt := range got {
		full := append(make([]byte, VnetHdrLen), pkt...)
		if !validIP(full) || !validL4(full) || binary.BigEndian.Uint16(pkt[24:]) != 1008 {
			t.Errorf("udp segment %d invalid", i)
		}
	}
	if len(got) != 3 {
		t.Errorf("udp segments = %d, want 3", len(got))
	}
	for _, pkt := range segments(frame(ProtoTCP, GuestAddr, remote, true)) {
		if !validL4(append(make([]byte, VnetHdrLen), pkt...)) {
			t.Error("partial checksum not finished")
		}
	}
}

func TestSegmentWritesEachPacketBehindTheHeader(t *testing.T) {
	hdr := []byte("vxlan-and-eth-header..")
	out, stride, n := appendSegments([]byte("kept"), hdr, gsoFrame(ProtoTCP, 3000, 1400))
	if n != 3 || stride != len(hdr)+1440 || len(out) != 4+2*stride+len(hdr)+40+200 {
		t.Fatalf("n %d stride %d len %d", n, stride, len(out))
	}
	for at := 4; at < len(out); at += stride {
		if !bytes.HasPrefix(out[at:], hdr) {
			t.Errorf("datagram at %d does not start with the header", at)
		}
	}
}

func segments(f []byte) [][]byte {
	out, stride, n := appendSegments(nil, nil, f)
	got := make([][]byte, 0, n)
	for range n {
		k := min(stride, len(out))
		got, out = append(got, out[:k]), out[k:]
	}
	return got
}

func gsoFrame(proto uint8, payload, size int) []byte {
	l4 := 20
	if proto == ProtoUDP {
		l4 = 8
	}
	f := make([]byte, VnetHdrLen+20+l4+payload)
	ip := f[VnetHdrLen:]
	ip[0], ip[8], ip[9] = 0x45, 64, proto
	binary.BigEndian.PutUint16(ip[2:], uint16(len(ip)))
	s, d := GuestAddr.As4(), remote.As4()
	copy(ip[srcAt:], s[:])
	copy(ip[dstAt:], d[:])
	seg := ip[20:]
	binary.BigEndian.PutUint16(seg, 40000)
	binary.BigEndian.PutUint16(seg[2:], 443)
	for i := l4; i < len(seg); i++ {
		seg[i] = byte(i * 7)
	}
	f[1] = gsoUDPL4
	binary.LittleEndian.PutUint16(f[8:], 6)
	if proto == ProtoTCP {
		f[1] = gsoTCPv4
		binary.BigEndian.PutUint32(seg[4:], 1000)
		seg[12] = 5 << 4
		binary.LittleEndian.PutUint16(f[8:], 16)
	}
	f[0] = needsCsum
	binary.LittleEndian.PutUint16(f[6:], 20)
	binary.LittleEndian.PutUint16(f[4:], uint16(size))
	return f
}
