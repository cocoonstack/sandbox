package pool

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
	"github.com/cocoonstack/sandbox/sandboxd/tunnet"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var anyPolicy = &egress.Policy{Allow: []egress.Rule{{Host: "*"}}}

func TestATunPoolClaimGetsAnAddressAndAPacketDoor(t *testing.T) {
	eng, addrs := newFakeEngine(), &fakeAddrs{}
	plain := tplKey("plain")
	m := tunManager(t, eng, addrs, config.PoolSpec{PoolKey: testKey, Tun: true, Egress: anyPolicy}, config.PoolSpec{PoolKey: plain, Egress: anyPolicy})
	sb := mustClaim(t, m, testKey)
	if !sb.TunAddr.IsValid() {
		t.Fatal("a claim of a tun pool has no overlay address")
	}
	if !tunHello(t, sb) {
		t.Error("the packet door of a tun claim refused the guest or answered off")
	}
	waitFor(t, func() bool { return slices.Equal(tunVerbsOf(eng), []string{"up " + sb.VsockSocket}) })
	if got := journaled(t, m)[sb.ID].TunAddr; got != sb.TunAddr {
		t.Errorf("journaled address %s, want %s", got, sb.TunAddr)
	}
	other := mustClaim(t, m, plain)
	if other.TunAddr.IsValid() {
		t.Errorf("a claim of a pool without tun got the address %s", other.TunAddr)
	}
	if doorOpen(other) {
		t.Error("a claim of a pool without tun has a packet door")
	}
	if got := tunVerbsOf(eng); len(got) != 1 {
		t.Errorf("tun verbs %q, want none for the claim without tun", got)
	}
}

func TestReleaseFreesTheOverlayAddress(t *testing.T) {
	eng, addrs := newFakeEngine(), &fakeAddrs{}
	m := tunManager(t, eng, addrs, config.PoolSpec{PoolKey: testKey, Tun: true, Egress: anyPolicy})
	sb := mustClaim(t, m, testKey)
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got, want := addrs.releasedList(), []string{sb.ID + " " + sb.TunAddr.String()}; !slices.Equal(got, want) {
		t.Errorf("released %q, want %q", got, want)
	}
	if doorOpen(sb) {
		t.Error("the packet door outlived the release")
	}
}

func TestAForkGetsItsOwnOverlayAddress(t *testing.T) {
	eng, addrs := newFakeEngine(), &fakeAddrs{}
	m := tunManager(t, eng, addrs, config.PoolSpec{PoolKey: testKey, Tun: true, Egress: anyPolicy})
	parent := mustClaim(t, m, testKey)
	children, err := m.Fork(t.Context(), parent.ID, Cred{Token: parent.Token}, 2, time.Hour, "", "")
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	seen := map[netip.Addr]bool{parent.TunAddr: true}
	for _, c := range children {
		if !c.TunAddr.IsValid() || seen[c.TunAddr] {
			t.Errorf("child %s has address %s, want its own", c.ID, c.TunAddr)
		}
		seen[c.TunAddr] = true
		if !tunHello(t, c) {
			t.Errorf("child %s has no live packet door", c.ID)
		}
	}
}

func TestABranchOfADisabledClaimStaysDisabled(t *testing.T) {
	eng, addrs := newFakeEngine(), &fakeAddrs{}
	m := tunManager(t, eng, addrs, config.PoolSpec{PoolKey: testKey, Tun: true, Egress: anyPolicy})
	sb := mustClaim(t, m, testKey)
	if err := m.SetTun(t.Context(), sb.ID, new(false), new("res")); err != nil {
		t.Fatalf("set tun: %v", err)
	}
	ck, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "", "")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	branch, err := m.ClaimCheckpoint(t.Context(), ck.ID, ClaimOptions{TTL: time.Hour})
	if err != nil {
		t.Fatalf("claim checkpoint: %v", err)
	}
	if !branch.TunOff || branch.TunProfile != "res" || doorOpen(branch) {
		t.Errorf("branch tun_off %v profile %q door %v, want the source's switch kept", branch.TunOff, branch.TunProfile, doorOpen(branch))
	}
	if _, _, err = m.Promote(t.Context(), sb.ID, Cred{Token: sb.Token}, "tpl", ""); !errors.Is(err, ErrTunOffPromote) {
		t.Errorf("promote of a disabled claim: %v, want refused", err)
	}
}

func TestAFailedAssignLeavesTheClaimWithoutTun(t *testing.T) {
	eng, addrs := newFakeEngine(), &fakeAddrs{err: errors.New("tun address store: connection refused")}
	m := tunManager(t, eng, addrs, config.PoolSpec{PoolKey: testKey, Tun: true, Egress: anyPolicy})
	sb := mustClaim(t, m, testKey)
	if sb.TunAddr.IsValid() {
		t.Errorf("claim holds %s after the address store failed", sb.TunAddr)
	}
	if doorOpen(sb) {
		t.Error("a claim without an address has a packet door")
	}
	if got := tunVerbsOf(eng); len(got) != 0 {
		t.Errorf("tun verbs %q, want none", got)
	}
}

func TestSetTunRewritesAHibernatedClaimWithoutWakingIt(t *testing.T) {
	eng, addrs := newFakeEngine(), &fakeAddrs{}
	m := tunManager(t, eng, addrs, config.PoolSpec{PoolKey: testKey, Tun: true, Egress: anyPolicy})
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	if err := m.SetTun(t.Context(), sb.ID, new(false), new("res")); err != nil {
		t.Fatalf("set tun: %v", err)
	}
	if hibernated(m) != 1 {
		t.Error("set tun woke the hibernated claim")
	}
	if got := journaled(t, m)[sb.ID]; !got.TunOff || got.TunProfile != "res" || got.TunAddr != sb.TunAddr {
		t.Errorf("journaled tun_off %v profile %q address %s, want off, res and the address kept", got.TunOff, got.TunProfile, got.TunAddr)
	}
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("wake: %v", err)
	}
	if doorOpen(sb) {
		t.Error("a disabled claim got its packet door back at the wake")
	}
}

func TestDisablingARunningClaimRevokesItsPacketPath(t *testing.T) {
	eng, addrs := newFakeEngine(), &fakeAddrs{}
	m := tunManager(t, eng, addrs, config.PoolSpec{PoolKey: testKey, Tun: true, Egress: anyPolicy})
	sb := mustClaim(t, m, testKey)
	stream := openStream(t, sb)
	if err := m.SetTun(t.Context(), sb.ID, new(false), nil); err != nil {
		t.Fatalf("disable: %v", err)
	}
	_ = stream.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := stream.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Errorf("the established stream read %v, want EOF", err)
	}
	if doorOpen(sb) {
		t.Error("the packet door still accepts after the disable")
	}
	waitFor(t, func() bool {
		return slices.Equal(tunVerbsOf(eng), []string{"up " + sb.VsockSocket, "down " + sb.VsockSocket})
	})
	if err := m.SetTun(t.Context(), sb.ID, new(true), nil); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if !tunHello(t, sb) {
		t.Error("the re-enabled claim has no live packet door")
	}
}

func TestADisableTheWakeMissedStillTurnsTheGuestOff(t *testing.T) {
	eng, addrs := newFakeEngine(), &fakeAddrs{}
	m := tunManager(t, eng, addrs, config.PoolSpec{PoolKey: testKey, Tun: true, Egress: anyPolicy})
	sb := mustClaim(t, m, testKey)
	m.mu.Lock()
	sb.TunOff = true
	m.mu.Unlock()
	if tunHello(t, sb) {
		t.Error("a claim disabled after its door was bound still answered on")
	}
}

func TestOnlyADisableRunsBesideATransition(t *testing.T) {
	eng, addrs := newFakeEngine(), &fakeAddrs{}
	m := tunManager(t, eng, addrs, config.PoolSpec{PoolKey: testKey, Tun: true, Egress: anyPolicy})
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	sb.Transition.Lock()
	for _, req := range []struct {
		enabled *bool
		profile *string
	}{{new(true), nil}, {nil, new("res")}} {
		if err := m.SetTun(t.Context(), sb.ID, req.enabled, req.profile); !errors.Is(err, ErrPaused) {
			t.Errorf("set tun %v %v during a transition: %v, want ErrPaused", req.enabled, req.profile, err)
		}
	}
	if err := m.SetTun(t.Context(), sb.ID, new(false), nil); err != nil {
		t.Errorf("disable during a transition: %v", err)
	}
	sb.Transition.Unlock()
}

func TestASetTunResendRewritesTheArchiveRowAFailedWriteMissed(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	reg := &hookRegistry{Registry: c.reg, failPut: true}
	a.archives = reg
	if err := a.SetTun(t.Context(), sb.ID, new(false), nil); err == nil {
		t.Fatal("disable with the registry down succeeded")
	}
	reg.failPut = false
	if err := a.SetTun(t.Context(), sb.ID, new(false), nil); err != nil {
		t.Fatalf("resend: %v", err)
	}
	if !rowClaim(t, c, sb).TunOff {
		t.Error("the archive row still has the claim on after the resend")
	}
}

func TestFlushClaimsPersistsTheLiveTunBytes(t *testing.T) {
	eng, addrs := newFakeEngine(), &fakeAddrs{}
	m := tunManager(t, eng, addrs, config.PoolSpec{PoolKey: testKey, Tun: true, Egress: anyPolicy})
	sb := mustClaim(t, m, testKey)
	stream := openStream(t, sb)
	pkt := udpFrame(netip.MustParseAddr("93.184.216.34"))
	if _, err := stream.Write(binary.LittleEndian.AppendUint32(nil, uint32(len(pkt)))); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write(pkt); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { used, _ := m.out.TunUsed(sb.ID); return used > 0 })
	if err := m.FlushClaims(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := journaled(t, m)[sb.ID].TunBytes; got != int64(len(pkt)-tunnet.VnetHdrLen) {
		t.Errorf("journaled tun bytes %d, want the %d sent", got, len(pkt)-tunnet.VnetHdrLen)
	}
}

func TestATunFlowEndWritesOneAuditRecord(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{DataDir: dir, AuditLog: true}
	m, err := NewManager(t.Context(), cfg, newFakeEngine(), testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	start := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	m.recordTunFlow(t.Context(), "sb_t", "acme", "", tunnet.FlowRecord{
		Proto: tunnet.ProtoUDP, Dst: netip.MustParseAddr("93.184.216.34"), Port: 443, Allowed: true,
		Up: 1200, Down: 64000, Start: start, End: start.Add(time.Minute),
	})
	raw, err := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
	if err != nil {
		t.Fatalf("read audit journal: %v", err)
	}
	want := `{"t":"2026-11-05T12:01:00Z","id":"sb_t","op":"tun_flow","path":"tun","tenant":"acme","proto":"udp","dst_addr":"93.184.216.34:443",` +
		`"decision":"allow","sent":1200,"received":64000,"start":"2026-11-05T12:00:00Z","end":"2026-11-05T12:01:00Z","profile":"default"}`
	if got := strings.TrimSpace(string(raw)); got != want {
		t.Errorf("audit record\n%s\nwant\n%s", got, want)
	}
}

func tunManager(t *testing.T, eng *fakeEngine, addrs tunnet.Addresses, pools ...config.PoolSpec) *Manager {
	t.Helper()
	eng.sockRoot = outboundtest.SockRoot(t)
	tun := &config.TunConfig{Subnet: "240.0.0.0/4", Listen: "127.0.0.1:4789", VNI: 1, Port: 4789, MTU: 1450, Exits: map[string]config.TunExit{"default": {VTEP: "127.0.0.2", MAC: "02:aa:00:00:00:01"}}}
	cfg := &config.Config{DataDir: t.TempDir(), Tun: tun, Pools: pools}
	b := testBackends(t, cfg)
	b.TunRouter, b.TunAddrs = tunnet.New(fakeUnderlay{}, tun.Overlay()), addrs
	m, err := NewManager(t.Context(), cfg, eng, testSecrets(t), b)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	t.Cleanup(func() { settleCaptures(m) })
	return m
}

// tunHello sends a guest hello on sb's packet door and reports whether the host answered on.
func tunHello(t *testing.T, sb *types.Sandbox) bool {
	t.Helper()
	conn, err := net.Dial("unix", engine.TunSocketPath(sb.VsockSocket))
	if err != nil {
		return false
	}
	defer func() { _ = conn.Close() }()
	return helloOn(t, conn)
}

// openStream returns a packet stream the host turned on.
func openStream(t *testing.T, sb *types.Sandbox) net.Conn {
	t.Helper()
	conn, err := net.Dial("unix", engine.TunSocketPath(sb.VsockSocket))
	if err != nil {
		t.Fatalf("dial the packet door: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if !helloOn(t, conn) {
		t.Fatal("the host answered off")
	}
	return conn
}

func helloOn(t *testing.T, conn net.Conn) bool {
	t.Helper()
	hello := tunnet.Hello{Version: tunnet.Version}.Encode()
	if _, err := conn.Write(hello[:]); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := tunnet.ReadHello(conn)
	if err != nil {
		t.Fatalf("read host hello: %v", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	return got.Flags&tunnet.FlagOn != 0
}

func doorOpen(sb *types.Sandbox) bool {
	conn, err := net.Dial("unix", engine.TunSocketPath(sb.VsockSocket))
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// udpFrame is a guest UDP datagram to dst with a virtio-net header and no checksum.
func udpFrame(dst netip.Addr) []byte {
	f := make([]byte, tunnet.VnetHdrLen+20+8+16)
	ip := f[tunnet.VnetHdrLen:]
	ip[0], ip[8], ip[9] = 0x45, 64, tunnet.ProtoUDP
	binary.BigEndian.PutUint16(ip[2:], uint16(len(ip)))
	s, d := tunnet.GuestAddr.As4(), dst.As4()
	copy(ip[12:], s[:])
	copy(ip[16:], d[:])
	binary.BigEndian.PutUint16(ip[20:], 40000)
	binary.BigEndian.PutUint16(ip[22:], 443)
	binary.BigEndian.PutUint16(ip[24:], 24)
	return f
}

func tunVerbsOf(eng *fakeEngine) []string {
	eng.mu.Lock()
	defer eng.mu.Unlock()
	return slices.Clone(eng.tunVerbs)
}

type fakeAddrs struct {
	mu       sync.Mutex
	err      error
	next     int
	held     map[string]netip.Addr
	released []string
}

func (f *fakeAddrs) Assign(_ context.Context, id string) (netip.Addr, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return netip.Addr{}, f.err
	}
	if addr, ok := f.held[id]; ok {
		return addr, nil
	}
	if f.held == nil {
		f.held = map[string]netip.Addr{}
	}
	f.next++
	addr := tunnet.HostAt(netip.MustParsePrefix("240.0.0.0/4"), uint64(f.next))
	f.held[id] = addr
	return addr, nil
}

func (f *fakeAddrs) Release(_ context.Context, id string, addr netip.Addr) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.held, id)
	f.released = append(f.released, fmt.Sprintf("%s %s", id, addr))
	return nil
}

func (f *fakeAddrs) releasedList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.released)
}

type fakeUnderlay struct{}

func (fakeUnderlay) Send([]byte, int, netip.AddrPort, uint64) error { return nil }

func (fakeUnderlay) Recv(func([]byte, netip.AddrPort)) error { return net.ErrClosed }
