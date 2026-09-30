package pool

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var hostOnly = new(false)

func TestClaimEnvFeedsTheLiveProxysSecrets(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Header.Get("Authorization"))
	}))
	t.Cleanup(origin.Close)
	host := mustHostname(t, origin.URL)

	eng := newFakeEngine()
	eng.sockRoot = sockRoot(t)
	pol := &egress.Policy{Allow: []egress.Rule{
		{Host: host, Methods: []string{http.MethodGet}, Secret: "gh"},
		{Host: host, Secret: "gw"},
	}}
	m := envManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: pol})
	m.dial = (&net.Dialer{}).DialContext
	refillWarmVM(t, m)

	sb, err := m.ClaimWarm(t.Context(), testKey, ClaimOptions{Env: types.Env{"GH_TOKEN": {Value: "Bearer claim", Guest: hostOnly}}})
	if err != nil {
		t.Fatalf("ClaimWarm: %v", err)
	}
	client := egressClient(engine.EgressSocketPath(sb.VsockSocket))
	send := func(method string) string {
		t.Helper()
		req, _ := http.NewRequestWithContext(t.Context(), method, origin.URL+"/", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}

	if got := send(http.MethodGet); got != "Bearer claim" {
		t.Errorf("claim env over the node's value: injected %q, want Bearer claim", got)
	}
	if got := send(http.MethodPost); got != "" {
		t.Errorf("claim-only secret with no env: injected %q, want nothing", got)
	}
	if err := m.SetEnv(t.Context(), sb.ID, types.Env{"gw": {Value: "Bearer own", Guest: hostOnly}}, ""); err != nil {
		t.Fatalf("SetEnv: %v", err)
	}
	if got := send(http.MethodGet); got != "node-gh" {
		t.Errorf("after the claim env was dropped: injected %q, want the node's node-gh", got)
	}
	if got := send(http.MethodPost); got != "Bearer own" {
		t.Errorf("after SetEnv: injected %q, want Bearer own on the live proxy", got)
	}
	if err := m.SetEnv(t.Context(), sb.ID, types.Env{"gw": {Value: "Bearer visible"}}, ""); err != nil {
		t.Fatalf("SetEnv guest entry: %v", err)
	}
	if got := send(http.MethodPost); got != "" {
		t.Errorf("a guest entry fed a secret: injected %q, want nothing", got)
	}
}

func TestGuestEnvIsWrittenOnlyWhenAClaimCarriesIt(t *testing.T) {
	eng := newFakeEngine()
	m := envManager(t, eng, t.TempDir())
	bare, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"KEY": {Value: "k", Guest: hostOnly}}})
	if err != nil {
		t.Fatalf("host-only claim: %v", err)
	}
	guest, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{
		"B":      {Value: `a "quoted" $HOME \ path`},
		"A":      {Value: "plain"},
		"SECRET": {Value: "never-in-guest", Guest: hostOnly},
	}})
	if err != nil {
		t.Fatalf("guest claim: %v", err)
	}
	if _, wrote := eng.guestEnvs[bare.VsockSocket]; wrote {
		t.Errorf("a claim with no guest entries wrote %q", eng.guestEnvs[bare.VsockSocket])
	}
	want := "A=\"plain\"\nB=\"a \\\"quoted\\\" \\$HOME \\\\ path\"\n"
	if got := eng.guestEnvs[guest.VsockSocket]; got != want {
		t.Errorf("guest env file %q, want %q", got, want)
	}
}

func TestInheritedGuestsGetTheirEnvFileReplaced(t *testing.T) {
	eng := newFakeEngine()
	m := envManager(t, eng, t.TempDir())
	src, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"SRC": {Value: "s"}, "HOST": {Value: "h", Guest: hostOnly}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	children, err := m.Fork(t.Context(), src.ID, Cred{Token: src.Token}, 1, time.Hour, "", "")
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "", "")
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	bare, err := m.ClaimCheckpoint(t.Context(), ckpt.ID, ClaimOptions{TTL: time.Hour})
	if err != nil {
		t.Fatalf("branch: %v", err)
	}
	own, err := m.ClaimCheckpoint(t.Context(), ckpt.ID, ClaimOptions{TTL: time.Hour, Env: types.Env{"BRANCH": {Value: "b"}}})
	if err != nil {
		t.Fatalf("branch with env: %v", err)
	}
	for name, sb := range map[string]*types.Sandbox{"fork child": children[0], "bare branch": bare} {
		got, wrote := eng.guestEnvs[sb.VsockSocket]
		if sb.Env != nil || !wrote || got != "" {
			t.Errorf("%s: env %v, file written=%t %q, want no env and an emptied file", name, sb.Env, wrote, got)
		}
	}
	if got := eng.guestEnvs[own.VsockSocket]; got != "BRANCH=\"b\"\n" {
		t.Errorf("branch with env wrote %q, want only its own entry", got)
	}
}

func TestSourcesWithoutGuestEnvCostTheirClonesNoWrite(t *testing.T) {
	eng := newFakeEngine()
	m := envManager(t, eng, t.TempDir())
	src, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"HOST": {Value: "h", Guest: hostOnly}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err = m.Fork(t.Context(), src.ID, Cred{Token: src.Token}, 2, time.Hour, "", ""); err != nil {
		t.Fatalf("Fork: %v", err)
	}
	ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "", "")
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if _, err = m.ClaimCheckpoint(t.Context(), ckpt.ID, ClaimOptions{TTL: time.Hour}); err != nil {
		t.Fatalf("branch: %v", err)
	}
	key, _, err := m.Promote(t.Context(), src.ID, Cred{Token: src.Token}, "tpl:env", "")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if _, err = m.ClaimProvision(t.Context(), key, ClaimOptions{}); err != nil {
		t.Fatalf("template claim: %v", err)
	}
	if ckpt.GuestEnv || len(eng.guestEnvs) != 0 {
		t.Errorf("checkpoint guest_env=%t, guest writes %v, want none for a source without guest entries", ckpt.GuestEnv, eng.guestEnvs)
	}
}

func TestATemplateCloneReplacesItsSourcesGuestEnv(t *testing.T) {
	eng := newFakeEngine()
	m := envManager(t, eng, t.TempDir())
	src, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"SRC": {Value: "s"}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	key, _, err := m.Promote(t.Context(), src.ID, Cred{Token: src.Token}, "tpl:env", "")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	clone, err := m.ClaimProvision(t.Context(), key, ClaimOptions{})
	if err != nil {
		t.Fatalf("template claim: %v", err)
	}
	if got, wrote := eng.guestEnvs[clone.VsockSocket]; !wrote || got != "" {
		t.Errorf("template clone file written=%t %q, want an emptied file", wrote, got)
	}
}

func TestAHostOnlyChangeLeavesTheGuestFileAlone(t *testing.T) {
	eng := newFakeEngine()
	m := envManager(t, eng, t.TempDir())
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"G": {Value: "g"}, "H": {Value: "1", Guest: hostOnly}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	delete(eng.guestEnvs, sb.VsockSocket)
	if err = m.SetEnv(t.Context(), sb.ID, types.Env{"G": {Value: "g"}, "H": {Value: "2", Guest: hostOnly}}, ""); err != nil {
		t.Fatalf("host-only rotation: %v", err)
	}
	if got, wrote := eng.guestEnvs[sb.VsockSocket]; wrote {
		t.Errorf("a host-only rotation rewrote the guest file %q", got)
	}
}

func TestAFailedClearIsRepairedByResendingIt(t *testing.T) {
	eng := newFakeEngine()
	m := envManager(t, eng, t.TempDir())
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"G": {Value: "g"}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	eng.guestEnvErr = errors.New("guest write failed")
	if err = m.SetEnv(t.Context(), sb.ID, nil, ""); err == nil {
		t.Fatal("clear reported success with a failed guest write")
	}
	eng.guestEnvErr = nil
	if err = m.SetEnv(t.Context(), sb.ID, nil, ""); err != nil {
		t.Fatalf("resent clear: %v", err)
	}
	if got := eng.guestEnvs[sb.VsockSocket]; got != "" {
		t.Errorf("guest file after the resent clear %q, want it emptied", got)
	}
}

func TestSetEnvChecksOwnershipAndPersistsAcrossARestart(t *testing.T) {
	eng := newFakeEngine()
	dataDir := t.TempDir()
	m := envManager(t, eng, dataDir)
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "acme", Env: types.Env{"gw": {Value: "Bearer a", Guest: hostOnly}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	next := types.Env{"gw": {Value: "Bearer b", Guest: hostOnly}, "SEEN": {Value: "yes"}}
	if err = m.SetEnv(t.Context(), sb.ID, next, "other"); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("another tenant: %v, want ErrUnknownSandbox", err)
	}
	if _, err = m.Env(sb.ID, "other"); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("another tenant's read: %v, want ErrUnknownSandbox", err)
	}
	if err = m.SetEnv(t.Context(), "sb_missing", nil, ""); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("unknown id: %v, want ErrUnknownSandbox", err)
	}
	if err = m.SetEnv(t.Context(), sb.ID, next, "acme"); err != nil {
		t.Fatalf("owning tenant: %v", err)
	}
	read, err := m.Env(sb.ID, "acme")
	if err != nil || read["gw"].Value != "" || read["gw"].InGuest() || read["SEEN"].Value != "yes" {
		t.Errorf("read %+v %v, want the host-only value dropped and the guest one served", read, err)
	}

	m2 := envManager(t, eng, dataDir)
	if err := m2.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	m2.mu.Lock()
	got := m2.claimed[sb.ID].Env
	m2.mu.Unlock()
	if !got.Equal(next) {
		t.Errorf("env after restart %v, want the last set", got)
	}
	if err := m2.SetEnv(t.Context(), sb.ID, types.Env{}, ""); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if claims, _ := newClaimStore(dataDir, false).load(); claims[sb.ID].Env != nil {
		t.Errorf("journal still holds %v after a clear", claims[sb.ID].Env)
	}
	if got := eng.guestEnvs[sb.VsockSocket]; got != "" {
		t.Errorf("clearing left the guest file %q", got)
	}
}

func TestAGuestEnvChangeNeedsARunningGuest(t *testing.T) {
	eng := newFakeEngine()
	m := envManager(t, eng, t.TempDir())
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"G": {Value: "1"}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	if err := m.SetEnv(t.Context(), sb.ID, types.Env{"G": {Value: "2"}}, ""); !errors.Is(err, ErrPaused) {
		t.Errorf("guest change while hibernated: %v, want ErrPaused", err)
	}
	if err := m.SetEnv(t.Context(), sb.ID, types.Env{"G": {Value: "1"}, "H": {Value: "h", Guest: hostOnly}}, ""); err != nil {
		t.Errorf("host-only change while hibernated: %v, want accepted", err)
	}
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if err := m.SetEnv(t.Context(), sb.ID, types.Env{"G": {Value: "2"}}, ""); err != nil {
		t.Fatalf("guest change after wake: %v", err)
	}
	if got := eng.guestEnvs[lockedVsock(m, sb)]; got != "G=\"2\"\n" {
		t.Errorf("guest file %q, want G=2", got)
	}
	delete(eng.guestEnvs, lockedVsock(m, sb))
	if err := m.SetEnv(t.Context(), sb.ID, types.Env{"G": {Value: "2"}}, ""); err != nil {
		t.Fatalf("repeated set: %v", err)
	}
	if got := eng.guestEnvs[lockedVsock(m, sb)]; got != "G=\"2\"\n" {
		t.Errorf("a repeated set left the guest file %q, want it rewritten", got)
	}
}

func TestAFailedEnvDeliveryDestroysTheClaim(t *testing.T) {
	eng := newFakeEngine()
	eng.guestEnvErr = errors.New("guest write failed")
	m := envManager(t, eng, t.TempDir())
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"G": {Value: "1"}}}); err == nil {
		t.Fatal("claim succeeded with an undeliverable env")
	}
	if len(eng.removes) != 1 {
		t.Errorf("removed %v, want the claim's VM", eng.removes)
	}
	m.mu.Lock()
	claimed := len(m.claimed)
	m.mu.Unlock()
	if claimed != 0 {
		t.Errorf("%d claims recorded, want none", claimed)
	}
}

func envManager(t *testing.T, eng *fakeEngine, dataDir string, pools ...config.PoolSpec) *Manager {
	t.Helper()
	t.Setenv("GH_TOKEN", "node-gh")
	secrets := testSecrets(t,
		egress.SecretSpec{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"},
		egress.SecretSpec{Name: "gw", Header: "Authorization"},
	)
	cfg := &config.Config{DataDir: dataDir, Pools: pools, Tenants: []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}}
	m, err := NewManager(t.Context(), cfg, eng, secrets)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	return m
}

func lockedVsock(m *Manager, sb *types.Sandbox) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sb.VsockSocket
}
