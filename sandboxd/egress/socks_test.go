package egress

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func TestSocksAllowTunnels(t *testing.T) {
	echo := echoServer(t)
	events := make(chan Event, 4)
	_, addr := socksProxy(t, Policy{Allow: []Rule{{Host: "echo.internal"}}}, nil, fixedDial(echo), events)

	conn, reply := socksExchange(t, addr, []byte{socksVersion, 1, socksNoAuth}, socksNameRequest("echo.internal", 443))
	if reply[1] != socksGranted {
		t.Fatalf("reply code = %#x, want granted", reply[1])
	}
	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatalf("write through tunnel: %v", err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(got) != "ping" {
		t.Errorf("tunnel echoed %q, want ping", got)
	}
	if ev := recvEvent(t, events); ev.Method != methodSOCKS || ev.Host != "echo.internal" || ev.Decision != DecisionAllow {
		t.Errorf("audit event = %+v, want SOCKS5 allow echo.internal", ev)
	}
}

func TestSocksDecisionMirrorsConnect(t *testing.T) {
	ca, _ := testCA(t)
	tests := []struct {
		name   string
		policy Policy
		ca     *CA
		want   byte
	}{
		{"bare host allows", Policy{Allow: []Rule{{Host: "echo.internal"}}}, nil, socksGranted},
		{"methods-restricted rule denies", Policy{Allow: []Rule{{Host: "echo.internal", Methods: []string{"GET"}}}}, nil, socksDenied},
		{"explicit CONNECT method allows", Policy{Allow: []Rule{{Host: "echo.internal", Methods: []string{"CONNECT"}}}}, nil, socksGranted},
		{"intercept rule denies", Policy{Allow: []Rule{{Host: "echo.internal", Intercept: true}}}, ca, socksDenied},
		{"secret rule tunnels without injection", Policy{Allow: []Rule{{Host: "echo.internal", Secret: "s"}}}, nil, socksGranted},
		{"unknown host denies", Policy{Allow: []Rule{{Host: "other.internal"}}}, nil, socksDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := make(chan Event, 4)
			_, addr := socksProxy(t, tt.policy, tt.ca, fixedDial(echoServer(t)), events)
			_, reply := socksExchange(t, addr, []byte{socksVersion, 1, socksNoAuth}, socksNameRequest("echo.internal", 443))
			if reply[1] != tt.want {
				t.Errorf("reply code = %#x, want %#x", reply[1], tt.want)
			}
			ev := recvEvent(t, events)
			if allowed := ev.Decision == DecisionAllow; allowed != (tt.want == socksGranted) || ev.Injected != "" {
				t.Errorf("audit event = %+v, want decision matching reply %#x and no injection", ev, tt.want)
			}
		})
	}
}

func TestSocksRefusesWhatItCannotServe(t *testing.T) {
	tests := []struct {
		name     string
		greeting []byte
		request  []byte
		want     []byte
	}{
		{"no-auth not offered", []byte{socksVersion, 1, 0x02}, nil, []byte{socksVersion, socksNoMethod}},
		{"bind command", []byte{socksVersion, 1, socksNoAuth}, []byte{socksVersion, 0x02, 0, socksAtypIPv4, 127, 0, 0, 1, 1, 187}, []byte{socksVersion, socksBadCmd}},
		{"udp associate", []byte{socksVersion, 1, socksNoAuth}, []byte{socksVersion, 0x03, 0, socksAtypIPv4, 127, 0, 0, 1, 1, 187}, []byte{socksVersion, socksBadCmd}},
		{"unknown address type", []byte{socksVersion, 1, socksNoAuth}, []byte{socksVersion, socksConnect, 0, 0x05}, []byte{socksVersion, socksBadAtyp}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := make(chan Event, 4)
			_, addr := socksProxy(t, Policy{Allow: []Rule{{Host: "*"}}}, nil, fixedDial("127.0.0.1:1"), events)
			_, reply := socksExchange(t, addr, tt.greeting, tt.request)
			if string(reply[:2]) != string(tt.want) {
				t.Errorf("reply = %v, want prefix %v", reply, tt.want)
			}
			select {
			case ev := <-events:
				t.Errorf("refused handshake recorded %+v", ev)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}

func TestSocksIPv4TargetMatchesLiteralRule(t *testing.T) {
	events := make(chan Event, 4)
	_, addr := socksProxy(t, Policy{Allow: []Rule{{Host: "127.0.0.1"}}}, nil, fixedDial(echoServer(t)), events)
	request := []byte{socksVersion, socksConnect, 0, socksAtypIPv4, 127, 0, 0, 1, 1, 187}
	_, reply := socksExchange(t, addr, []byte{socksVersion, 1, socksNoAuth}, request)
	if reply[1] != socksGranted {
		t.Fatalf("reply code = %#x, want granted", reply[1])
	}
	if ev := recvEvent(t, events); ev.Host != "127.0.0.1" || ev.Decision != DecisionAllow {
		t.Errorf("audit event = %+v, want allow 127.0.0.1", ev)
	}
}

func TestCloseEndsSocksTunnelAndHandshake(t *testing.T) {
	p, addr := socksProxy(t, Policy{Allow: []Rule{{Host: "echo.internal"}}}, nil, fixedDial(echoServer(t)), nil)
	tunnel, reply := socksExchange(t, addr, []byte{socksVersion, 1, socksNoAuth}, socksNameRequest("echo.internal", 443))
	if reply[1] != socksGranted {
		t.Fatalf("reply code = %#x, want granted", reply[1])
	}
	pending, _ := socksExchange(t, addr, []byte{socksVersion, 1, socksNoAuth}, nil)

	p.Close()

	_, _ = io.WriteString(tunnel, "ping")
	if _, err := io.ReadFull(tunnel, make([]byte, 4)); err == nil {
		t.Error("established SOCKS tunnel survived proxy Close")
	}
	_ = pending.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := pending.Read(make([]byte, 1)); err == nil {
		t.Error("mid-handshake SOCKS connection survived proxy Close")
	}
}

func socksProxy(t *testing.T, policy Policy, ca *CA, dial DialFunc, events chan Event) (*Proxy, string) {
	t.Helper()
	var audit func(Event)
	if events != nil {
		audit = func(ev Event) { events <- ev }
	}
	p := New("sb_1", "acme", policy, nil, ca, dial, audit)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen socks: %v", err)
	}
	t.Cleanup(func() {
		_ = ln.Close()
		p.Close()
	})
	go func() { _ = p.ServeStream(t.Context(), ln) }()
	return p, ln.Addr().String()
}

func socksExchange(t *testing.T, addr string, greeting, request []byte) (net.Conn, []byte) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write(greeting); err != nil {
		t.Fatalf("write greeting: %v", err)
	}
	choice := make([]byte, 2)
	if _, err := io.ReadFull(conn, choice); err != nil {
		t.Fatalf("read method choice: %v", err)
	}
	if choice[1] != socksNoAuth || request == nil {
		return conn, choice
	}
	if _, err := conn.Write(request); err != nil {
		t.Fatalf("write request: %v", err)
	}
	reply := make([]byte, 10)
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	return conn, reply
}

func socksNameRequest(host string, port uint16) []byte {
	req := []byte{socksVersion, socksConnect, 0, socksAtypName, byte(len(host))}
	req = append(req, host...)
	return binary.BigEndian.AppendUint16(req, port)
}
