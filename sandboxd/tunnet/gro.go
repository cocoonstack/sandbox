package tunnet

import (
	"bytes"
	"encoding/binary"
)

const (
	tcpAck = 0x10

	ipTTL = 8
)

type tcpSegment struct {
	seq     uint32
	hdr     int
	payload int
	push    bool
}

// coalescer merges in-order TCP segments of one flow into a GSO frame for the guest, as kernel GRO does; only the receive goroutine uses it.
type coalescer struct {
	l    *link
	bp   *[]byte
	n    int
	segs int
	mss  int
	next uint32
	push bool
}

// add takes an admitted frame held in bp; false leaves both with the caller to enqueue unmerged.
func (c *coalescer) add(r *Router, l *link, bp *[]byte, frame []byte) bool {
	if !l.merge {
		c.flush(r)
		return false
	}
	seg, ok := segmentOf(frame)
	if !ok {
		c.flush(r)
		return false
	}
	if c.joins(l, frame, seg) {
		c.append(frame, seg)
		r.pool.Put(bp)
		if seg.push || seg.payload < c.mss {
			c.flush(r)
		}
		return true
	}
	c.flush(r)
	*bp = (*bp)[:cap(*bp)]
	c.l, c.bp, c.n, c.segs = l, bp, len(frame), 1
	c.mss, c.next, c.push = seg.payload, seg.seq+uint32(seg.payload), seg.push //nolint:gosec // a payload fits a sequence step
	if seg.push {
		c.flush(r)
	}
	return true
}

// flush hands the pending frame to its link, as one GSO frame when it holds more than one segment.
func (c *coalescer) flush(r *Router) {
	if c.bp == nil {
		return
	}
	b := (*c.bp)[:4+c.n]
	frame := b[4:]
	if c.segs > 1 {
		c.finish(frame)
	}
	binary.LittleEndian.PutUint32(b, uint32(c.n)) //nolint:gosec // bounded by MaxFrame
	*c.bp = b
	if !c.l.enqueue(c.bp) {
		r.drops[DropQueueFull].Add(1)
		r.pool.Put(c.bp)
	}
	c.l, c.bp = nil, nil
}

// joins reports whether frame continues the pending train: same flow, the next sequence, the same ack, TTL, TOS and options, and room left.
func (c *coalescer) joins(l *link, frame []byte, seg tcpSegment) bool {
	if c.bp == nil || c.l != l || seg.seq != c.next || seg.payload > c.mss || c.n+seg.payload > MaxFrame {
		return false
	}
	p := (*c.bp)[4 : 4+c.n]
	ip, pip := frame[VnetHdrLen:], p[VnetHdrLen:]
	return int(pip[32]>>4)*4 == seg.hdr && ip[1] == pip[1] && ip[ipTTL] == pip[ipTTL] &&
		bytes.Equal(ip[srcAt:dstAt+4], pip[srcAt:dstAt+4]) && bytes.Equal(ip[20:24], pip[20:24]) &&
		bytes.Equal(ip[28:32], pip[28:32]) && bytes.Equal(ip[34:36], pip[34:36]) && bytes.Equal(ip[40:20+seg.hdr], pip[40:20+seg.hdr])
}

func (c *coalescer) append(frame []byte, seg tcpSegment) {
	payload := frame[VnetHdrLen+20+seg.hdr:]
	copy((*c.bp)[4+c.n:], payload)
	c.n += len(payload)
	c.segs++
	c.next += uint32(len(payload)) //nolint:gosec // a payload fits a sequence step
	c.push = c.push || seg.push
}

// finish turns the merged packet into a GSO frame with a partial checksum, which the guest takes as already verified.
func (c *coalescer) finish(frame []byte) {
	ip := frame[VnetHdrLen:]
	hdr := int(ip[32]>>4) * 4
	binary.BigEndian.PutUint16(ip[2:], uint16(len(ip))) //nolint:gosec // bounded by MaxFrame
	binary.BigEndian.PutUint16(ip[10:], 0)
	binary.BigEndian.PutUint16(ip[10:], ^fold(onesSum(ip[:20], 0)))
	if c.push {
		ip[20+13] |= tcpPsh
	}
	binary.BigEndian.PutUint16(ip[20+16:], fold(pseudoSum(ip, len(ip)-20)))
	frame[0], frame[1] = needsCsum, gsoTCPv4
	binary.LittleEndian.PutUint16(frame[2:], uint16(20+hdr))
	binary.LittleEndian.PutUint16(frame[4:], uint16(c.mss)) //nolint:gosec // an MSS fits
	binary.LittleEndian.PutUint16(frame[6:], 20)
	binary.LittleEndian.PutUint16(frame[8:], 16)
}

// segmentOf accepts a data segment with only ACK or ACK+PSH set and a valid checksum; GRO never vouches for a checksum it did not check.
func segmentOf(frame []byte) (tcpSegment, bool) {
	ip := frame[VnetHdrLen:]
	if ip[9] != ProtoTCP || frame[1] != gsoNone || frame[0] != 0 || len(ip) < 40 {
		return tcpSegment{}, false
	}
	tcp := ip[20:]
	hdr := int(tcp[12]>>4) * 4
	flags := tcp[13]
	if hdr < 20 || len(tcp) <= hdr || flags&^tcpPsh != tcpAck {
		return tcpSegment{}, false
	}
	if fold(onesSum(tcp, pseudoSum(ip, len(tcp)))) != 0xffff {
		return tcpSegment{}, false
	}
	return tcpSegment{seq: binary.BigEndian.Uint32(tcp[4:]), hdr: hdr, payload: len(tcp) - hdr, push: flags&tcpPsh != 0}, true
}
