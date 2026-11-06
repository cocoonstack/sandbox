package tunnet

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const (
	freedFile = "tun-freed.log"
	// compactAfter is how far the log may grow past twice its in-window releases before it is rewritten with them.
	compactAfter = 1024
)

// ErrNoAddress means every overlay address is held or still inside its reuse window.
var ErrNoAddress = errors.New("no free overlay address")

type Addresses interface {
	// Assign returns claimID's address, allocating one on the first call.
	Assign(ctx context.Context, claimID string) (netip.Addr, error)
	// Release frees addr; it becomes reusable once the reuse window has passed.
	Release(ctx context.Context, claimID string, addr netip.Addr) error
}

var _ Addresses = (*Local)(nil)

// Local allocates one node's overlay addresses in memory; held addresses come from Seed, and each release is appended to a log in the data dir so its reuse window outlives a restart.
type Local struct {
	path   string
	subnet netip.Prefix
	reuse  time.Duration

	io        sync.Mutex
	mu        sync.Mutex
	next      uint64
	lines     int
	compactAt int
	freed     map[netip.Addr]time.Time
	held      map[netip.Addr]string
	owned     map[string]netip.Addr
}

// NewLocal loads the releases still inside the reuse window from dir.
func NewLocal(dir string, subnet netip.Prefix, reuse time.Duration) (*Local, error) {
	l := &Local{
		path: filepath.Join(dir, freedFile), subnet: subnet.Masked(), reuse: reuse,
		freed: map[netip.Addr]time.Time{}, held: map[netip.Addr]string{}, owned: map[string]netip.Addr{},
	}
	raw, err := os.ReadFile(l.path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("read %s: %w", freedFile, err)
	}
	now := time.Now()
	for line := range bytes.Lines(raw) {
		a, sec, _ := strings.Cut(strings.TrimSpace(string(line)), " ")
		addr, addrErr := netip.ParseAddr(a)
		at, atErr := strconv.ParseInt(sec, 10, 64)
		if addrErr == nil && atErr == nil && now.Sub(time.Unix(at, 0)) < reuse {
			l.freed[addr] = time.Unix(at, 0)
		}
	}
	if err = utils.WriteFileSync(l.path, l.snapshot(), 0o600); err != nil {
		return nil, err
	}
	return l, nil
}

// Seed marks the addresses the node's claims already hold.
func (l *Local) Seed(held map[string]netip.Addr) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for id, addr := range held {
		l.held[addr], l.owned[id] = id, addr
	}
}

// Assign hands claimID the next free address after the cursor, or the one it already holds; it does no I/O.
func (l *Local) Assign(_ context.Context, claimID string) (netip.Addr, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if addr, ok := l.owned[claimID]; ok {
		return addr, nil
	}
	now := time.Now()
	// Among blocked+1 consecutive candidates one is free unless the subnet itself is full.
	tries := min(uint64(len(l.held)+len(l.freed))+1, uint64(1)<<(32-l.subnet.Bits())-2)
	for range tries {
		addr := HostAt(l.subnet, l.next)
		l.next++
		if _, ok := l.held[addr]; ok {
			continue
		}
		if at, ok := l.freed[addr]; ok {
			if now.Sub(at) < l.reuse {
				continue
			}
			delete(l.freed, addr)
		}
		l.held[addr], l.owned[claimID] = claimID, addr
		return addr, nil
	}
	return netip.Addr{}, ErrNoAddress
}

// Release frees addr unless another claim holds it, and appends the release to the log outside the lock Assign takes.
func (l *Local) Release(_ context.Context, claimID string, addr netip.Addr) error {
	l.io.Lock()
	defer l.io.Unlock()
	l.mu.Lock()
	if owner, ok := l.held[addr]; ok && owner != claimID {
		l.mu.Unlock()
		return nil
	}
	delete(l.held, addr)
	if l.owned[claimID] == addr {
		delete(l.owned, claimID)
	}
	now := time.Now()
	l.freed[addr] = now
	if l.lines++; l.lines > l.compactAt {
		maps.DeleteFunc(l.freed, func(_ netip.Addr, at time.Time) bool { return now.Sub(at) >= l.reuse })
		b := l.snapshot()
		l.mu.Unlock()
		return utils.WriteFileSync(l.path, b, 0o600)
	}
	l.mu.Unlock()
	f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("open %s: %w", freedFile, err)
	}
	_, err = f.Write(appendFreed(nil, addr, now))
	err = cmp.Or(err, f.Close())
	if err != nil {
		return fmt.Errorf("append %s: %w", freedFile, err)
	}
	return nil
}

func (l *Local) snapshot() []byte {
	var b []byte
	for addr, at := range l.freed {
		b = appendFreed(b, addr, at)
	}
	l.lines, l.compactAt = len(l.freed), 2*len(l.freed)+compactAfter
	return b
}

// HostAt maps a counter onto subnet's host addresses in order, skipping the network and broadcast addresses.
func HostAt(subnet netip.Prefix, n uint64) netip.Addr {
	base := binary.BigEndian.Uint32(subnet.Masked().Addr().AsSlice())
	hosts := uint64(1)<<(32-subnet.Bits()) - 2
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], base+1+uint32(n%hosts)) //nolint:gosec // n%hosts is below 2^28
	return netip.AddrFrom4(b)
}

func appendFreed(b []byte, addr netip.Addr, at time.Time) []byte {
	b = addr.AppendTo(b)
	b = strconv.AppendInt(append(b, ' '), at.Unix(), 10)
	return append(b, '\n')
}
