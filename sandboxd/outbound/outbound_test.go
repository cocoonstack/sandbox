package outbound

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var (
	testKey  = types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall}
	egKey    = types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeSmall}
	egPolicy = &egress.Policy{Allow: []egress.Rule{{Host: "example.com", Secret: "gh"}}}
)

func TestEgressProxyInjectsAndGates(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Seen-Auth", r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(origin.Close)
	host := outboundtest.Hostname(t, origin.URL)

	pol := &egress.Policy{Allow: []egress.Rule{{Host: host, Secret: "gh"}}}
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, pol), func(o *Options) { o.Dial = (&net.Dialer{}).DialContext })

	sb := outboundtest.VsockSandbox(t, "sb_egress", testKey)
	if armErr := h.Arm(t.Context(), sb); armErr != nil {
		t.Fatalf("arm egress: %v", armErr)
	}
	path := engine.EgressSocketPath(sb.VsockSocket)
	client := outboundtest.EgressClient(path)

	resp, err := client.Get(origin.URL + "/")
	if err != nil {
		t.Fatalf("allowed request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "ok" || resp.Header.Get("X-Seen-Auth") != "s3cr3t" {
		t.Errorf("allowed request body=%q injected=%q, want ok/s3cr3t", body, resp.Header.Get("X-Seen-Auth"))
	}

	req, _ := http.NewRequest(http.MethodGet, "http://blocked.example/", nil)
	deny, err := client.Do(req)
	if err != nil {
		t.Fatalf("denied request: %v", err)
	}
	deny.Body.Close()
	if deny.StatusCode != http.StatusForbidden {
		t.Errorf("denied status %d, want 403", deny.StatusCode)
	}

	h.Disarm(sb.ID, true)
	if _, err := net.Dial("unix", path); err == nil {
		t.Error("egress socket still accepts after disarm")
	}
}

func TestArmEgressBindsSocksOnlyWhenOptedIn(t *testing.T) {
	tests := []struct {
		name   string
		policy *egress.Policy
		want   bool
	}{
		{"opted in with a bare host rule", &egress.Policy{Socks5: true, Allow: []egress.Rule{{Host: "example.com"}}}, true},
		{"bare host rule without opting in", &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHost(t, &fakeEngine{}, poolConfig(testKey, tt.policy), nil)
			sb := outboundtest.VsockSandbox(t, "sb_socks", testKey)
			if armErr := h.ArmProxy(t.Context(), sb); armErr != nil {
				t.Fatalf("arm proxy: %v", armErr)
			}
			path := engine.SocksSocketPath(sb.VsockSocket)
			conn, err := net.Dial("unix", path)
			if (err == nil) != tt.want {
				t.Fatalf("socks listener bound = %v, want %v", err == nil, tt.want)
			}
			if conn != nil {
				_ = conn.Close()
			}
			h.Disarm(sb.ID, true)
			if _, err := net.Dial("unix", path); err == nil {
				t.Error("socks socket still accepts after disarm")
			}
		})
	}
}

func TestArmEgressFailsClosedWhenNICUnlockable(t *testing.T) {
	h := newHost(t, &fakeEngine{}, poolConfig(egKey, egPolicy), func(o *Options) { o.LockNIC = true })

	sb := &types.Sandbox{ID: "sb_eg_no_tap", Key: egKey, VMName: "sbx-no-tap-1"}
	if armErr := h.Arm(t.Context(), sb); armErr == nil {
		t.Fatal("Arm must fail closed when the egress-lane NIC cannot be locked")
	}
}

func TestArmEgressFailsClosedWhenTheLockFails(t *testing.T) {
	lockErr := errors.New("nft down")
	h := newHost(t, &fakeEngine{}, poolConfig(egKey, egPolicy), func(o *Options) {
		o.LockNIC = true
		o.Lock = func(string) error { return lockErr }
	})

	sb := &types.Sandbox{ID: "sb_eg_refused", Key: egKey, VMName: "sbx-refused-1", TAP: "tap-x"}
	if armErr := h.Arm(t.Context(), sb); !errors.Is(armErr, lockErr) {
		t.Fatalf("Arm = %v, want the lock failure", armErr)
	}
}

func TestArmEgressLocksEgressLaneWithoutPolicy(t *testing.T) {
	cfg := &config.Config{Pools: []config.PoolSpec{
		{PoolKey: testKey, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "a.test"}}}},
		{PoolKey: egKey},
	}}
	var taps outboundtest.TapLog
	h := newHost(t, &fakeEngine{}, cfg, func(o *Options) {
		o.LockNIC = true
		o.Lock, o.Unlock = taps.Lock, taps.Unlock
	})
	sb := &types.Sandbox{ID: "sb_eg_np", Key: egKey, VMName: "sbx-np-1", TAP: "tap-np"}
	lockErr := h.Arm(t.Context(), sb)
	if got := taps.Locked(); lockErr != nil || !slices.Equal(got, []string{"tap-np"}) {
		t.Fatalf("policyless egress-lane claim locked %v (err %v), want tap-np", got, lockErr)
	}
}

func TestLockFallsBackToALookupForAClaimWithoutARecordedTap(t *testing.T) {
	eng := &fakeEngine{taps: map[string]string{"sbx-old": "tap-fake1"}}
	var taps outboundtest.TapLog
	h := newHost(t, eng, poolConfig(egKey, egPolicy), func(o *Options) {
		o.LockNIC = true
		o.Lock, o.Unlock = taps.Lock, taps.Unlock
	})

	sb := &types.Sandbox{ID: "sb_old", Key: egKey, VMName: "sbx-old"}
	lockErr := h.Arm(t.Context(), sb)
	if calls := eng.lookups(); calls != 1 {
		t.Errorf("fallback looked the VM up %d times, want 1", calls)
	}
	if got := taps.Locked(); lockErr != nil || !slices.Equal(got, []string{"tap-fake1"}) {
		t.Errorf("fallback locked %v (err %v), want tap-fake1", got, lockErr)
	}
}

func TestEgressLaneLocksWithNoPolicyAnywhere(t *testing.T) {
	var taps outboundtest.TapLog
	h := newHost(t, &fakeEngine{}, poolConfig(egKey, nil), func(o *Options) {
		o.LockNIC = true
		o.Lock, o.Unlock = taps.Lock, taps.Unlock
	})
	if h.o.View().guarded {
		t.Fatal("no policy configured; guarded should be false")
	}
	sb := &types.Sandbox{ID: "sb_np", Key: egKey, VMName: "sbx-np", TAP: "tap-nopol"}
	lockErr := h.Arm(t.Context(), sb)
	if got := taps.Locked(); lockErr != nil || !slices.Equal(got, []string{"tap-nopol"}) {
		t.Fatalf("policyless egress lane locked %v (err %v), want tap-nopol", got, lockErr)
	}
}

func TestDisarmKeepsLockWhenRemoveFailed(t *testing.T) {
	var taps outboundtest.TapLog
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, egPolicy), func(o *Options) {
		o.Dial = (&net.Dialer{}).DialContext
		o.Lock, o.Unlock = taps.Lock, taps.Unlock
	})
	sb := outboundtest.VsockSandbox(t, "sb_dz", testKey)
	if armErr := h.ArmProxy(t.Context(), sb); armErr != nil {
		t.Fatalf("arm proxy: %v", armErr)
	}
	h.KeepLock(sb.ID, "tap-dz")
	path := engine.EgressSocketPath(sb.VsockSocket)

	h.Disarm(sb.ID, false)
	if _, err := net.Dial("unix", path); err == nil {
		t.Error("proxy listener still accepts after a failed-remove disarm")
	}
	if got := taps.Unlocked(); len(got) != 0 {
		t.Errorf("failed remove unlocked %v; a still-running VM must stay locked", got)
	}
	h.Disarm(sb.ID, true)
	if got := taps.Unlocked(); !slices.Equal(got, []string{"tap-dz"}) {
		t.Errorf("removal unlocked %v, want the tap the failed remove kept", got)
	}
}

func TestNetRouteFollowsTheLaneAndTheDoors(t *testing.T) {
	h := newHost(t, &fakeEngine{}, &config.Config{}, nil)
	armed := &types.Sandbox{ID: "sb_armed", Key: testKey}
	h.doors[armed.ID] = &doors{}
	for _, tt := range []struct {
		lock bool
		sb   *types.Sandbox
		want types.NetRoute
	}{
		{false, armed, types.NetRouteRelay},
		{false, &types.Sandbox{ID: "sb_closed", Key: testKey}, types.NetRouteNone},
		{false, &types.Sandbox{ID: "sb_nic", Key: egKey}, types.NetRouteDirect},
		{true, &types.Sandbox{ID: "sb_locked", Key: egKey}, types.NetRouteNone},
	} {
		h.o.LockNIC = tt.lock
		if got := h.Route(tt.sb); got != tt.want {
			t.Errorf("%s (lock %v): route %q, want %q", tt.sb.ID, tt.lock, got, tt.want)
		}
	}
}

func TestSweepRunsOnlyOnANodeThatLocksNICs(t *testing.T) {
	for _, tt := range []struct {
		lock bool
		want int
	}{{false, 0}, {true, 1}} {
		var sweeps int
		h := newHost(t, &fakeEngine{}, &config.Config{}, func(o *Options) {
			o.LockNIC = tt.lock
			o.Sweep = func(map[string]bool) error { sweeps++; return nil }
		})
		if err := h.Sweep(nil); err != nil {
			t.Fatalf("sweep (lock %v): %v", tt.lock, err)
		}
		if sweeps != tt.want {
			t.Errorf("sweep calls (lock %v) = %d, want %d", tt.lock, sweeps, tt.want)
		}
	}
}

func TestALiveClaimsEgressFollowsTheView(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(origin.Close)
	host := outboundtest.Hostname(t, origin.URL)
	secrets := testSecrets(t)
	var cur atomic.Pointer[View]
	cur.Store(NewView(poolConfig(testKey, &egress.Policy{Allow: []egress.Rule{{Host: "other.test"}}}), secrets))
	h := newHost(t, &fakeEngine{}, &config.Config{}, func(o *Options) {
		o.View = cur.Load
		o.Dial = (&net.Dialer{}).DialContext
	})
	sb := outboundtest.VsockSandbox(t, "sb_live", testKey)
	if err := h.Arm(t.Context(), sb); err != nil {
		t.Fatalf("arm egress: %v", err)
	}
	client := outboundtest.EgressClient(engine.EgressSocketPath(sb.VsockSocket))
	get := func() int {
		t.Helper()
		resp, err := client.Get(origin.URL + "/")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if code := get(); code != http.StatusForbidden {
		t.Fatalf("before the change: %d, want 403", code)
	}
	cur.Store(NewView(poolConfig(testKey, &egress.Policy{Allow: []egress.Rule{{Host: host}}}), secrets))
	if code := get(); code != http.StatusOK {
		t.Errorf("after the allow-list admits the host: %d, want 200 on the same armed claim", code)
	}
	cur.Store(NewView(poolConfig(testKey, nil), secrets))
	if code := get(); code != http.StatusForbidden {
		t.Errorf("after the pool loses its policy: %d, want 403", code)
	}
}

func TestArmAppliesTheClaimsLimitAndSetEgressLimitMovesIt(t *testing.T) {
	const rate = 512 << 10
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, 2*rate))
	}))
	t.Cleanup(origin.Close)
	pol := &egress.Policy{Allow: []egress.Rule{{Host: outboundtest.Hostname(t, origin.URL)}}}
	var limit atomic.Pointer[types.EgressLimit]
	limit.Store(&types.EgressLimit{RxBytesPerSecond: rate})
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, pol), func(o *Options) {
		o.Dial = (&net.Dialer{}).DialContext
		o.EgressLimit = func(*types.Sandbox) types.EgressLimit { return *limit.Load() }
	})
	sb := outboundtest.VsockSandbox(t, "sb_limit", testKey)
	if err := h.Arm(t.Context(), sb); err != nil {
		t.Fatalf("arm egress: %v", err)
	}
	t.Cleanup(func() { h.Disarm(sb.ID, true) })
	client := outboundtest.EgressClient(engine.EgressSocketPath(sb.VsockSocket))
	fetch := func() time.Duration {
		start := time.Now()
		resp, err := client.Get(origin.URL + "/")
		if err != nil {
			t.Fatalf("proxied GET: %v", err)
		}
		n, _ := io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if n != 2*rate {
			t.Fatalf("read %d bytes, want %d", n, 2*rate)
		}
		return time.Since(start)
	}

	throttled := fetch()
	if throttled < 700*time.Millisecond {
		t.Errorf("download under the armed limit took %v", throttled)
	}
	limit.Store(&types.EgressLimit{})
	h.SetEgressLimit(sb)
	if took := fetch(); took >= throttled/2 {
		t.Errorf("download after the limit was lifted took %v, want under half the throttled %v", took, throttled)
	}
	limit.Store(&types.EgressLimit{RxBytesPerSecond: rate})
	h.SetEgressLimit(sb)
	if took := fetch(); took < 700*time.Millisecond {
		t.Errorf("download after the limit was set again took %v", took)
	}
}

func TestAnArmNeverAppliesAnOlderCapThanASetterRead(t *testing.T) {
	const rate = 512 << 10
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, 2*rate))
	}))
	t.Cleanup(origin.Close)
	pol := &egress.Policy{Allow: []egress.Rule{{Host: outboundtest.Hostname(t, origin.URL)}}}
	var limit atomic.Pointer[types.EgressLimit]
	limit.Store(&types.EgressLimit{})
	reading, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, pol), func(o *Options) {
		o.Dial = (&net.Dialer{}).DialContext
		o.EgressLimit = func(*types.Sandbox) types.EgressLimit {
			l := *limit.Load()
			if first.CompareAndSwap(false, true) {
				close(reading)
				select {
				case <-release:
				case <-time.After(300 * time.Millisecond):
				}
			}
			return l
		}
	})
	sb := outboundtest.VsockSandbox(t, "sb_limit_race", testKey)
	armed := make(chan error, 1)
	go func() { armed <- h.Arm(t.Context(), sb) }()
	<-reading
	limit.Store(&types.EgressLimit{RxBytesPerSecond: rate})
	set := make(chan struct{})
	go func() {
		h.SetEgressLimit(sb)
		close(set)
	}()
	select {
	case <-set:
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-armed; err != nil {
		t.Fatalf("arm egress: %v", err)
	}
	<-set
	t.Cleanup(func() { h.Disarm(sb.ID, true) })

	start := time.Now()
	resp, err := outboundtest.EgressClient(engine.EgressSocketPath(sb.VsockSocket)).Get(origin.URL + "/")
	if err != nil {
		t.Fatalf("proxied GET: %v", err)
	}
	n, _ := io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if took := time.Since(start); n != 2*rate || took < 700*time.Millisecond {
		t.Errorf("read %d bytes in %v: the arm's stale read replaced the cap the setter applied", n, took)
	}
}

func newHost(t *testing.T, eng *fakeEngine, cfg *config.Config, edit func(*Options)) *Host {
	t.Helper()
	v := NewView(cfg, testSecrets(t))
	o := Options{
		Engine:      eng,
		View:        func() *View { return v },
		Record:      func(context.Context, string, string, egress.Event) {},
		RecordDNS:   func(context.Context, string, string, DNSEvent) {},
		Transfer:    func(context.Context, string, string, egress.Event, int64, int64) {},
		EgressLimit: func(*types.Sandbox) types.EgressLimit { return types.EgressLimit{} },
		Lock:        func(string) error { return nil },
		Unlock:      func(string) error { return nil },
	}
	if edit != nil {
		edit(&o)
	}
	return New(o)
}

func poolConfig(key types.PoolKey, pol *egress.Policy) *config.Config {
	return &config.Config{Pools: []config.PoolSpec{{PoolKey: key, Egress: pol}}}
}

func testSecrets(t *testing.T) *egress.SecretStore {
	t.Helper()
	t.Setenv("GH_TOKEN", "s3cr3t")
	s, err := egress.NewSecretStore([]egress.SecretSpec{{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"}})
	if err != nil {
		t.Fatalf("secrets: %v", err)
	}
	return s
}

type fakeEngine struct {
	mu       sync.Mutex
	taps     map[string]string
	inspects int
}

func (f *fakeEngine) Inspect(_ context.Context, name string) (types.VMRecord, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inspects++
	tap, ok := f.taps[name]
	if !ok {
		return types.VMRecord{}, false, nil
	}
	return types.VMRecord{Config: types.VMConfig{Name: name}, NetworkConfigs: []types.VMNetConfig{{TAP: tap}}}, true, nil
}

func (f *fakeEngine) InstallCACert(context.Context, string, []byte) error {
	return nil
}

func (f *fakeEngine) MarkLane(context.Context, string, types.NetRoute) error {
	return nil
}

func (f *fakeEngine) TunUp(context.Context, string) error {
	return nil
}

func (f *fakeEngine) TunDown(context.Context, string) error {
	return nil
}

func (f *fakeEngine) lookups() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inspects
}
