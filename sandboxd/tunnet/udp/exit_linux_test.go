package udp_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/cocoonstack/sandbox/sandboxd/tunnet"
	"github.com/cocoonstack/sandbox/sandboxd/tunnet/udp"
)

const bulk = 64 << 20

var (
	claimAddr = netip.MustParseAddr("100.80.0.7")
	otherAddr = netip.MustParseAddr("100.80.0.8")
	exitMAC   = [6]byte{0x02, 0xee, 0, 0, 0, 1}
)

func TestTrafficCrossesAKernelExit(t *testing.T) {
	if os.Getenv("SANDBOXD_TUN_EXIT_TEST") == "" {
		t.Skip("needs root, iproute2, nftables and conntrack; set SANDBOXD_TUN_EXIT_TEST=1")
	}
	exitNetns(t)
	under, err := udp.Listen(netip.MustParseAddrPort("10.99.0.1:4789"), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer under.Close()
	r := tunnet.New(under, &tunnet.Overlay{
		Subnet: netip.MustParsePrefix("100.80.0.0/12"), VNI: 7, Port: 4789, MTU: 1450,
		Exits: map[string]tunnet.Exit{"default": {VTEP: netip.MustParseAddr("10.99.0.2"), MAC: exitMAC}},
	})
	go func() { _ = r.Run(t.Context()) }()
	r.Arm(tunnet.Claim{ID: "c1", Addr: claimAddr, Allow: func(netip.Addr, uint16) bool { return true }})
	tun := guestTun(t)
	host, guest := net.Pipe()
	go func() { _ = r.Serve(host, claimAddr) }()
	go pumpGuest(tun, guest)

	inNetns(t, "inet", func() error {
		l, err := net.Listen("tcp", "10.98.0.2:7000")
		if err != nil {
			return err
		}
		c, err := net.ListenPacket("udp", "10.98.0.2:7001")
		if err != nil {
			return err
		}
		go echoTCP(l)
		go echoUDP(c)
		return nil
	})
	waitNeigh(t)

	inNetns(t, "guest", func() error {
		c, err := net.DialTimeout("tcp", "10.98.0.2:7000", 5*time.Second)
		if err != nil {
			diag, _ := runOut("sh", "-c", "ip netns exec guest cat /proc/net/snmp | grep -E '^(Ip|Tcp|Udp):'; ip netns exec exit cat /proc/net/snmp | grep -E '^(Ip|Tcp):'; ip netns exec exit conntrack -L 2>&1 | head; ip -n exit -s link show vxlan0 | head -8")
			t.Log(diag)
			return fmt.Errorf("tcp through the exit: %w", err)
		}
		defer func() { _ = c.Close() }()
		start := time.Now()
		go func() { _, _ = c.Write(bytes.Repeat([]byte{0x5a}, bulk)); _ = c.(*net.TCPConn).CloseWrite() }()
		got, err := io.ReadAll(c)
		if err != nil || len(got) != bulk {
			return fmt.Errorf("tcp echo %d bytes, err %v", len(got), err)
		}
		t.Logf("tcp echo %d MiB both ways in %v", bulk>>20, time.Since(start))
		u, err := net.Dial("udp", "10.98.0.2:7001")
		if err != nil {
			return err
		}
		defer func() { _ = u.Close() }()
		_ = u.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, err = u.Write([]byte("ping")); err != nil {
			return err
		}
		b := make([]byte, 64)
		n, err := u.Read(b)
		if err != nil || !strings.HasPrefix(string(b[:n]), "10.98.0.1") {
			return fmt.Errorf("udp echo %q %v, want the exit's masquerade address", b[:n], err)
		}
		other, _ := net.DialTimeout("tcp", otherAddr.String()+":7000", 500*time.Millisecond)
		if other != nil {
			t.Error("a claim reached another claim's overlay address")
		}
		return nil
	})
	ct := run(t, "ip", "netns", "exec", "exit", "conntrack", "-L", "-s", claimAddr.String())
	if !strings.Contains(ct, "src="+claimAddr.String()) {
		t.Errorf("exit conntrack has no flow from the claim's overlay address:\n%s", ct)
	}
	st := r.Stats()
	t.Logf("stats %+v", st)
	if st.Drops[tunnet.DropPolicy] == 0 {
		t.Error("the packet to another claim was not dropped by policy")
	}
}

func exitNetns(t *testing.T) {
	t.Helper()
	script := `
set -e
for n in guest exit inet; do ip netns del $n 2>/dev/null || true; ip netns add $n; ip -n $n link set lo up; done
ip link del vh 2>/dev/null || true
ip link add vh type veth peer name ve netns exit
ip addr add 10.99.0.1/24 dev vh; ip link set vh up
ip -n exit addr add 10.99.0.2/24 dev ve; ip -n exit link set ve up
ip netns exec exit ethtool -K ve tx off >/dev/null
ip -n exit link add xu type veth peer name iu netns inet
ip -n exit addr add 10.98.0.1/24 dev xu; ip -n exit link set xu up
ip -n inet addr add 10.98.0.2/24 dev iu; ip -n inet link set iu up
ip -n exit link add vxlan0 address 02:ee:00:00:00:01 type vxlan id 7 dstport 4789 local 10.99.0.2 dev ve learning
ip -n exit addr add 192.0.2.1/32 dev vxlan0; ip -n exit link set vxlan0 up
ip -n exit route add 100.80.0.0/12 dev vxlan0
ip netns exec exit sysctl -qw net.ipv4.ip_forward=1 net.ipv4.conf.vxlan0.arp_accept=1 net.ipv4.neigh.vxlan0.base_reachable_time_ms=300000
ip netns exec exit nft -f - <<'NFT'
table ip exit {
  chain overlay_nat { type nat hook postrouting priority srcnat; ip saddr 100.80.0.0/12 oifname != "vxlan0" masquerade; }
  chain overlay_forward { type filter hook forward priority filter; iifname "vxlan0" oifname "vxlan0" drop; }
  chain overlay_input { type filter hook input priority filter; iifname "vxlan0" drop; }
  chain underlay_input { type filter hook input priority filter; udp dport 4789 ip saddr != 10.99.0.1 drop; }
}
NFT
`
	run(t, "sh", "-c", script)
	t.Cleanup(func() {
		_ = exec.Command("sh", "-c", "for n in guest exit inet; do ip netns del $n; done; ip link del vh").Run()
	})
}

func guestTun(t *testing.T) *os.File {
	t.Helper()
	var f *os.File
	inNetns(t, "guest", func() error {
		fd, err := unix.Open("/dev/net/tun", unix.O_RDWR|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		ifr, _ := unix.NewIfreq("tun0")
		ifr.SetUint16(unix.IFF_TUN | unix.IFF_NO_PI | unix.IFF_VNET_HDR)
		if err = unix.IoctlIfreq(fd, unix.TUNSETIFF, ifr); err != nil {
			return err
		}
		if err = unix.IoctlSetInt(fd, unix.TUNSETOFFLOAD, tunnet.OffloadCSUM|tunnet.OffloadTSO4|tunnet.OffloadTSO6|tunnet.OffloadTSOECN); err != nil {
			return err
		}
		f = os.NewFile(uintptr(fd), "tun0")
		return nil
	})
	run(t, "sh", "-c", "ip -n guest addr add 100.64.0.2/30 dev tun0 && ip -n guest link set tun0 mtu 1450 up && ip -n guest route add default via 100.64.0.1")
	return f
}

func pumpGuest(tun *os.File, s net.Conn) {
	h := tunnet.Hello{Version: tunnet.Version, Offloads: tunnet.OffloadCSUM | tunnet.OffloadTSO4 | tunnet.OffloadTSO6 | tunnet.OffloadTSOECN}.Encode()
	if _, err := s.Write(h[:]); err != nil {
		return
	}
	if _, err := tunnet.ReadHello(s); err != nil {
		return
	}
	go func() {
		buf := make([]byte, 4+tunnet.MaxFrame)
		for {
			n, err := tun.Read(buf[4:])
			if err != nil {
				return
			}
			binary.LittleEndian.PutUint32(buf, uint32(n))
			if _, err = s.Write(buf[:4+n]); err != nil {
				return
			}
		}
	}()
	var head [4]byte
	buf := make([]byte, tunnet.MaxFrame)
	for {
		if _, err := io.ReadFull(s, head[:]); err != nil {
			return
		}
		n := int(binary.LittleEndian.Uint32(head[:]))
		if _, err := io.ReadFull(s, buf[:n]); err != nil {
			return
		}
		_, _ = tun.Write(buf[:n])
	}
}

func echoTCP(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
	}
}

func echoUDP(c net.PacketConn) {
	b := make([]byte, 2048)
	for {
		_, from, err := c.ReadFrom(b)
		if err != nil {
			return
		}
		_, _ = c.WriteTo([]byte(from.String()), from)
	}
}

func waitNeigh(t *testing.T) {
	t.Helper()
	for range 50 {
		if strings.Contains(run(t, "ip", "-n", "exit", "neigh", "show", claimAddr.String()), "02:00:64:50:00:07") {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the exit never learned the claim from its gratuitous ARP")
}

// inNetns runs fn on a thread inside netns name; sockets fn opens stay in that netns.
func inNetns(t *testing.T, name string, fn func() error) {
	t.Helper()
	errc := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		f, err := os.Open("/var/run/netns/" + name)
		if err != nil {
			errc <- err
			return
		}
		defer func() { _ = f.Close() }()
		if err = unix.Setns(int(f.Fd()), unix.CLONE_NEWNET); err != nil {
			errc <- err
			return
		}
		errc <- fn()
	}()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func run(t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := runOut(name, args...)
	if err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, out)
	}
	return out
}

func runOut(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return string(out), err
}
