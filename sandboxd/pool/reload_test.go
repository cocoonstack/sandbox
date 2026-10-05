package pool

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var (
	otherKey       = types.PoolKey{Template: "other:1", Net: types.NetNone, Size: types.SizeSmall}
	interceptOnKey = types.PoolKey{Template: "mitm:1", Net: types.NetNone, Size: types.SizeSmall}
)

func TestReloadGivesANewKeyItsSettingsBeforeItsGolden(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	next := *m.cfg
	argv := []string{"node", "-e", "0"}
	next.Pools = []config.PoolSpec{{PoolKey: testKey, Warm: 3, Warmup: argv, Storage: "20G"}}
	res, err := m.ReloadConfig(t.Context(), &next)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	label := "pools[rt:24.04 none small]"
	if want := []string{label + ".warmup", label + ".storage"}; !slices.Equal(res.Changed, want) {
		t.Errorf("changed %v, want %v", res.Changed, want)
	}
	if want := []string{label + " targets (PUT /v1/pools owns them)"}; !slices.Equal(res.Ignored, want) {
		t.Errorf("ignored %v, want %v", res.Ignored, want)
	}
	m.mu.Lock()
	_, created := m.pools[testKey]
	m.mu.Unlock()
	if created {
		t.Fatal("a reload created a pool: targets belong to PUT /v1/pools")
	}
	if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: testKey, Warm: 1}}); err != nil {
		t.Fatalf("SetPools: %v", err)
	}
	waitFor(t, func() bool { return refilledTo(t, m, testKey, 1) })
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if len(eng.coldStorage) == 0 || eng.coldStorage[0] != "20G" || len(eng.warmups) == 0 || !slices.Equal(eng.warmups[0], argv) {
		t.Errorf("golden built with storage %v and warmups %v, want 20G and %v", eng.coldStorage, eng.warmups, argv)
	}
}

func TestReloadRetiresAGoldenWhoseStampChangedAndLeavesClaims(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
	waitFor(t, func() bool { return refilledTo(t, m, testKey, 2) })
	sb := mustClaim(t, m, testKey)
	vm := sb.VMName
	next := *m.cfg
	next.Pools = []config.PoolSpec{{PoolKey: testKey, Warm: 2, Storage: "30G"}}
	if _, err := m.ReloadConfig(t.Context(), &next); err != nil {
		t.Fatalf("reload: %v", err)
	}
	m.mu.Lock()
	golden, warm, live := m.pools[testKey].goldenDir, len(m.pools[testKey].warm), m.claimed[sb.ID]
	m.mu.Unlock()
	if golden != "" || warm != 0 {
		t.Errorf("after a storage change golden %q and %d warm VMs remain, want the golden retired and its warm VMs gone", golden, warm)
	}
	if live == nil || live.VMName != vm {
		t.Errorf("the reload touched a live claim: %+v", live)
	}
	waitFor(t, func() bool { return refilledTo(t, m, testKey, 2) })
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if got := eng.coldStorage[len(eng.coldStorage)-1]; got != "30G" {
		t.Errorf("rebuilt golden storage %q, want 30G", got)
	}
}

func TestReloadRefusesARestartFieldAndKeepsTheView(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	before := m.view.Load()
	next := *m.cfg
	next.Listen = "127.0.0.1:1"
	next.EgressInternalAllow = []string{"10.0.0.0/8"}
	_, err := m.ReloadConfig(t.Context(), &next)
	if !errors.Is(err, ErrReloadRefused) || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("reload with a new listen address: %v, want a refusal naming listen", err)
	}
	if m.view.Load() != before || !reflect.DeepEqual(m.view.Load().out, outbound.NewView(m.cfg, testSecrets(t))) {
		t.Error("a refused reload changed the view")
	}
}

func TestReloadTurnsInterceptOnOnlyForAKeyItHasNotServedWithTheCALoaded(t *testing.T) {
	plain := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1})
	next := *plain.cfg
	next.Pools = []config.PoolSpec{{PoolKey: testKey, Warm: 1}, {PoolKey: otherKey, Egress: interceptPolicy()}}
	if _, err := plain.ReloadConfig(t.Context(), &next); !errors.Is(err, ErrReloadRefused) || !strings.Contains(err.Error(), "egress_ca loaded at boot") {
		t.Errorf("intercept on a node that loaded no CA: %v, want a refusal", err)
	}

	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1}, config.PoolSpec{PoolKey: interceptOnKey, Egress: interceptPolicy()})
	served := *m.cfg
	served.Pools = []config.PoolSpec{{PoolKey: testKey, Warm: 1, Egress: interceptPolicy()}, {PoolKey: interceptOnKey, Egress: interceptPolicy()}}
	if _, err := m.ReloadConfig(t.Context(), &served); !errors.Is(err, ErrReloadRefused) || !strings.Contains(err.Error(), "do not trust the CA") {
		t.Errorf("intercept on a served key: %v, want a refusal", err)
	}
	fresh := *m.cfg
	fresh.Pools = append(slices.Clone(m.cfg.Pools), config.PoolSpec{PoolKey: otherKey, Egress: interceptPolicy()})
	if _, err := m.ReloadConfig(t.Context(), &fresh); err != nil {
		t.Errorf("intercept on a key the node never served: %v", err)
	}
}

func TestAGoldenBuiltAgainstASupersededViewIsDiscarded(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Warmup: []string{"node", "-e", "0"}})
	eng.warmupHook = func() {
		eng.warmupHook = nil
		editView(m, func(v *configView) { v.poolStorage[testKey] = "30G" })
	}
	m.mu.Lock()
	p := m.pools[testKey]
	p.building = true
	m.mu.Unlock()
	m.buildGolden(t.Context(), p)
	m.mu.Lock()
	golden, next := p.goldenDir, p.nextBuild
	m.mu.Unlock()
	if golden != "" || !next.IsZero() {
		t.Errorf("golden %q next build %v: a golden built against the old view must be dropped and rebuilt at once", golden, next)
	}
}

func TestACloneOfARetiredGoldenNeverLands(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1})
	waitFor(t, func() bool { return refilledTo(t, m, testKey, 1) })
	m.mu.Lock()
	p := m.pools[testKey]
	golden, gen := p.goldenDir, p.goldenGen
	p.warm = nil
	p.goldenGen++
	p.refilling++
	m.refillSem <- struct{}{}
	m.mu.Unlock()
	m.refillOne(t.Context(), p, golden, gen)
	if n := warmLen(m, testKey); n != 0 {
		t.Errorf("%d warm VMs landed from a clone of the retired golden", n)
	}
}

func TestAReloadDropsTheLiveProxiesPooledConnections(t *testing.T) {
	var conns, closed atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			close(started)
			<-release
		}
		_, _ = io.WriteString(w, "ok")
	}))
	origin.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		switch s {
		case http.StateNew:
			conns.Add(1)
		case http.StateClosed:
			closed.Add(1)
		}
	}
	origin.Start()
	t.Cleanup(origin.Close)
	host := outboundtest.Hostname(t, origin.URL)
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: &egress.Policy{Allow: []egress.Rule{{Host: host}}}})
	withOutbound(t, m, func(o *outbound.Options) { o.Dial = (&net.Dialer{}).DialContext })
	sb := outboundtest.VsockSandbox(t, "sb_pool", testKey)
	if err := m.out.Arm(t.Context(), sb); err != nil {
		t.Fatalf("arm egress: %v", err)
	}
	client := outboundtest.EgressClient(engine.EgressSocketPath(sb.VsockSocket))
	get := func(path string) error {
		resp, err := client.Get(origin.URL + path)
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.Body.Close()
	}
	inflight := make(chan error, 1)
	go func() { inflight <- get("/slow") }()
	<-started
	next := *m.cfg
	next.EgressInternalAllow = []string{"10.8.0.0/16"}
	if _, err := m.ReloadConfig(t.Context(), &next); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if err := get("/"); err != nil {
		t.Fatalf("get after the reload: %v", err)
	}
	close(release)
	if err := <-inflight; err != nil {
		t.Fatalf("in-flight get: %v", err)
	}
	waitFor(t, func() bool { return closed.Load() == 1 })
	if err := get("/"); err != nil {
		t.Fatalf("get after the in-flight one ended: %v", err)
	}
	if n := conns.Load(); n != 2 {
		t.Errorf("%d origin connections, want the in-flight one closed when it ended and one fresh dial after the reload", n)
	}
}

func TestReloadIsAudited(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(t.Context(), &config.Config{DataDir: dir, AuditLog: true}, newFakeEngine(), testSecrets(t))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	next := *m.cfg
	next.EgressInternalAllow = []string{"10.8.0.0/16"}
	if _, err = m.ReloadConfig(t.Context(), &next); err != nil {
		t.Fatalf("reload: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
	if err != nil || !strings.Contains(string(raw), `"op":"config_reload","changed":["egress_internal_allow"]`) {
		t.Errorf("audit %s, %v; want a config_reload record naming egress_internal_allow", raw, err)
	}
	if !reflect.DeepEqual(m.view.Load().out, outbound.NewView(&next, testSecrets(t))) {
		t.Error("egress_internal_allow did not reach the view")
	}
}

func TestAReloadOfTheEgressLayerChangesTheClusterDigest(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	before := m.ClusterDigest()
	next := *m.cfg
	next.EgressInternalAllow = []string{"10.8.0.0/16"}
	if _, err := m.ReloadConfig(t.Context(), &next); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if m.ClusterDigest() == before {
		t.Error("the digest still covers the boot config after an egress reload")
	}
}

func TestAnInterruptedReloadStillDestroysTheRetiredWarmVMs(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
	waitFor(t, func() bool { return refilledTo(t, m, testKey, 2) })
	m.mu.Lock()
	retired := []string{m.pools[testKey].warm[0].VMName, m.pools[testKey].warm[1].VMName}
	m.mu.Unlock()
	m.refillSem = make(chan struct{}, 1)
	m.refillSem <- struct{}{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	next := *m.cfg
	next.Pools = []config.PoolSpec{{PoolKey: testKey, Warm: 2, Storage: "30G"}}
	done := make(chan error, 1)
	go func() {
		_, err := m.ReloadConfig(ctx, &next)
		done <- err
	}()
	waitFor(t, func() bool { return warmLen(m, testKey) == 0 })
	<-m.refillSem
	if err := <-done; err != nil {
		t.Fatalf("reload: %v", err)
	}
	if removed := eng.removedNames(); !slices.Contains(removed, retired[0]) || !slices.Contains(removed, retired[1]) {
		t.Errorf("removed %v, want both retired warm VMs %v destroyed after the reload's caller went away", removed, retired)
	}
}

func TestAReloadThatUnguardsTheNodeClosesThePreboundDoors(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = outboundtest.SockRoot(t)
	m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}})
	warm := refillWarmVM(t, m)
	pin := pinDoor(t, engine.EgressSocketPath(warm.VsockSocket))
	next := *m.cfg
	next.Pools = []config.PoolSpec{{PoolKey: testKey, Warm: 1}}
	if _, err := m.ReloadConfig(t.Context(), &next); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if conn, err := net.Dial("unix", pin); err == nil {
		_ = conn.Close()
		t.Error("a prebound door is left open on a node with no egress policy")
	}
	if _, err := net.Dial("unix", engine.EgressSocketPath(warm.VsockSocket)); err == nil {
		t.Error("a door refill bound still accepts after the reload removed every policy")
	}
}

func refilledTo(t *testing.T, m *Manager, key types.PoolKey, n int) bool {
	m.refillOnce(t.Context())
	return warmLen(m, key) == n
}

func warmLen(m *Manager, key types.PoolKey) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p := m.pools[key]; p != nil {
		return len(p.warm)
	}
	return 0
}
