package pool

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
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
	for _, tt := range []struct {
		name  string
		patch types.EnvPatch
	}{
		{"put", nil},
		{"remove", types.EnvPatch{"G": nil}},
		{"host-only", types.EnvPatch{"G": {Value: "h", Guest: hostOnly}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := envManager(t, eng, t.TempDir())
			sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"G": {Value: "g"}}})
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			sock := lockedVsock(m, sb)
			write := func() error { return m.SetEnv(t.Context(), sb.ID, nil, "") }
			if tt.patch != nil {
				write = func() error { return m.PatchEnv(t.Context(), sb.ID, tt.patch, "") }
			}
			writeErr := errors.New("guest write failed")
			eng.guestEnvErr = writeErr
			for range 2 {
				if err := write(); !errors.Is(err, writeErr) {
					t.Fatalf("clear with a failed guest write: %v, want %v", err, writeErr)
				}
			}
			eng.guestEnvErr = nil
			seq := m.store.mark().seq
			if err := write(); err != nil {
				t.Fatalf("resent clear: %v", err)
			}
			if got := eng.guestEnvs[sock]; got != "" || m.store.mark().seq != seq {
				t.Errorf("resent clear left guest env %q and journal seq %d, want empty env and seq %d", got, m.store.mark().seq, seq)
			}
		})
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

func TestAGuestEnvChangeOnAFailedClaimIsRefused(t *testing.T) {
	eng := newFakeEngine()
	m := envManager(t, eng, t.TempDir())
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"G": {Value: "1"}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	sb.Transition.Lock()
	m.failLocked(t.Context(), sb, "vmm exited")
	sb.Transition.Unlock()
	if err := m.SetEnv(t.Context(), sb.ID, types.Env{"G": {Value: "2"}}, ""); !errors.Is(err, ErrFailed) {
		t.Errorf("guest change on a failed claim: %v, want ErrFailed", err)
	}
	if got := sb.Env["G"].Value; got != "1" {
		t.Errorf("refused change stored G=%q, want 1", got)
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

func TestPatchEnvMergesAndRepairsTheGuest(t *testing.T) {
	eng := newFakeEngine()
	m := envManager(t, eng, t.TempDir())
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"GW": {Value: "Bearer k", Guest: hostOnly}, "MODE": {Value: "a"}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	sock := lockedVsock(m, sb)
	stored := func() types.Env {
		m.mu.Lock()
		defer m.mu.Unlock()
		return maps.Clone(sb.Env)
	}
	patch := func(p types.EnvPatch) error { return m.PatchEnv(t.Context(), sb.ID, p, "") }

	delete(eng.guestEnvs, sock)
	if err := patch(types.EnvPatch{"GW": {Value: "Bearer k2", Guest: hostOnly}}); err != nil {
		t.Fatalf("host-only patch: %v", err)
	}
	if got, wrote := eng.guestEnvs[sock]; wrote {
		t.Errorf("a host-only patch wrote the guest file %q", got)
	}
	if env := stored(); env["GW"].Value != "Bearer k2" || env["MODE"].Value != "a" {
		t.Errorf("env after the host-only patch = %v", env)
	}
	if err := patch(types.EnvPatch{"META": {Value: "x"}}); err != nil {
		t.Fatalf("guest patch: %v", err)
	}
	if got := eng.guestEnvs[sock]; got != "META=\"x\"\nMODE=\"a\"\n" {
		t.Errorf("guest file after adding META = %q", got)
	}
	if stored()["GW"].Value != "Bearer k2" {
		t.Error("a guest patch dropped the host-only value")
	}

	delete(eng.guestEnvs, sock)
	seq := m.store.mark().seq
	if err := patch(types.EnvPatch{"GW": {Value: "Bearer k2", Guest: hostOnly}, "ABSENT": nil}); err != nil {
		t.Fatalf("no-op patch: %v", err)
	}
	if got := eng.guestEnvs[sock]; got != "META=\"x\"\nMODE=\"a\"\n" || m.store.mark().seq != seq {
		t.Errorf("a no-op patch left guest env %q and journal seq %d, want repaired env and seq %d", got, m.store.mark().seq, seq)
	}
	delete(eng.guestEnvs, sock)
	if err := patch(types.EnvPatch{"META": {Value: "x"}}); err != nil {
		t.Fatalf("resent guest patch: %v", err)
	}
	if got := eng.guestEnvs[sock]; got != "META=\"x\"\nMODE=\"a\"\n" || m.store.mark().seq != seq {
		t.Errorf("a resent guest patch left the guest file %q and moved the journal to seq %d, want it rewritten and the journal untouched", got, m.store.mark().seq)
	}
	if err := patch(types.EnvPatch{"MODE": nil, "META": nil}); err != nil {
		t.Fatalf("removing patch: %v", err)
	}
	if got, wrote := eng.guestEnvs[sock]; !wrote || got != "" {
		t.Errorf("guest file after removing every guest entry = %q (written %t), want emptied", got, wrote)
	}
	if env := stored(); len(env) != 1 || env["GW"].Value != "Bearer k2" {
		t.Errorf("env after removals = %v, want only the host-only entry", env)
	}

	big, drop := types.EnvPatch{}, types.EnvPatch{}
	for i := range 7 {
		big[fmt.Sprintf("BIG_%d", i)] = &types.EnvVar{Value: strings.Repeat("v", 8190), Guest: hostOnly}
		drop[fmt.Sprintf("BIG_%d", i)] = nil
	}
	if err := patch(big); err != nil {
		t.Fatalf("a patch under the total cap: %v", err)
	}
	last := types.EnvPatch{"BIG_7": {Value: strings.Repeat("v", 8190), Guest: hostOnly}}
	if err := last.Validate(); err != nil {
		t.Fatalf("the last entry alone: %v", err)
	}
	if err := patch(last); !errors.Is(err, ErrBadEnv) || strings.Contains(err.Error(), "vvvv") {
		t.Errorf("a patch that takes the merged env past the cap: %v, want ErrBadEnv without the value", err)
	}
	if err := patch(drop); err != nil || len(stored()) != 1 {
		t.Fatalf("dropping the big entries: %v, env %d entries", err, len(stored()))
	}
	if err := m.PatchEnv(t.Context(), sb.ID, types.EnvPatch{"X": nil}, "acme"); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("another tenant's patch: %v, want ErrUnknownSandbox", err)
	}

	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	if err := patch(types.EnvPatch{"MODE": {Value: "b"}}); !errors.Is(err, ErrPaused) {
		t.Errorf("guest patch while hibernated: %v, want ErrPaused", err)
	}
	if err := patch(types.EnvPatch{"GW": {Value: "Bearer k3", Guest: hostOnly}}); err != nil || stored()["GW"].Value != "Bearer k3" {
		t.Errorf("host-only patch while hibernated: %v, env %v", err, stored())
	}
}

func BenchmarkClaimSecretsHeader(b *testing.B) {
	b.Setenv("GH_TOKEN", "node-gh")
	store, err := egress.NewSecretStore([]egress.SecretSpec{{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"}})
	if err != nil {
		b.Fatalf("secrets: %v", err)
	}
	m := &Manager{}
	m.view.Store(&configView{secrets: store})
	b.Run("node-store-only", func(b *testing.B) {
		for b.Loop() {
			_, _, _ = store.Header("gh")
		}
	})
	for _, arm := range []string{"no-claim-env", "claim-env"} {
		sb := &types.Sandbox{}
		if arm == "claim-env" {
			sb.Env = types.Env{"GH_TOKEN": {Value: "Bearer claim", Guest: hostOnly}, "MODE": {Value: "on"}}
		}
		b.Run(arm, func(b *testing.B) {
			for b.Loop() {
				_, _, _ = claimSecrets{m: m, sb: sb}.Header("gh")
			}
		})
	}
}

func envManager(t *testing.T, eng *fakeEngine, dataDir string, pools ...config.PoolSpec) *Manager {
	t.Helper()
	t.Setenv("GH_TOKEN", "node-gh")
	secrets := testSecrets(t,
		egress.SecretSpec{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"},
		egress.SecretSpec{Name: "gw", Header: "Authorization"},
	)
	cfg := &config.Config{DataDir: dataDir, APIToken: "root", Pools: pools}
	m, err := NewManager(t.Context(), cfg, eng, secrets)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	putTenants(t, m, config.TenantSpec{Name: "acme", Token: "acme-tok"})
	return m
}

func lockedVsock(m *Manager, sb *types.Sandbox) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sb.VsockSocket
}
