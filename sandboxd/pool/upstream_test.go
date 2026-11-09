package pool

import (
	"errors"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestUpstreamEnvIsAdmittedHostOnlyAndAllowed(t *testing.T) {
	m := upstreamManager(t)
	for name, tt := range map[string]struct {
		v  types.EnvVar
		ok bool
	}{
		"allowed url": {types.EnvVar{Value: "http://u:p@127.0.0.1:3128", Guest: hostOnly}, true},
		"direct":      {types.EnvVar{Value: "direct", Guest: hostOnly}, true},
		"guest entry": {types.EnvVar{Value: "http://127.0.0.1:3128"}, false},
		"unlisted":    {types.EnvVar{Value: "http://10.0.0.1:3128", Guest: hostOnly}, false},
		"malformed":   {types.EnvVar{Value: "ftp://127.0.0.1:21", Guest: hostOnly}, false},
		"missing":     {types.EnvVar{Guest: hostOnly, Missing: true}, true},
	} {
		err := m.checkEnv(types.Env{"EGRESS_UPSTREAM": tt.v})
		if tt.ok != (err == nil) || (err != nil && !errors.Is(err, ErrBadEnv)) {
			t.Errorf("%s: %v, want ok=%v", name, err, tt.ok)
		}
	}
	editEgress(t, m, func(cfg *config.Config) { cfg.EgressUpstream = nil })
	if err := m.checkEnv(types.Env{"EGRESS_UPSTREAM": {Value: "anything"}}); err != nil {
		t.Errorf("with the feature off the name is an ordinary env entry: %v", err)
	}
}

func TestCheckpointClaimAdmitsTheUpstreamEnvLikeAnyClaim(t *testing.T) {
	m := upstreamManager(t)
	src := mustClaim(t, m, testKey)
	ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "", "")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	for name, v := range map[string]types.EnvVar{
		"guest entry": {Value: "http://u:p@127.0.0.1:3128"},
		"unlisted":    {Value: "http://10.0.0.1:3128", Guest: hostOnly},
	} {
		o := ClaimOptions{TTL: time.Hour, Env: types.Env{"EGRESS_UPSTREAM": v}}
		if _, err := m.ClaimCheckpoint(t.Context(), ckpt.ID, o); !errors.Is(err, ErrBadEnv) {
			t.Errorf("%s: branch claim %v, want ErrBadEnv", name, err)
		}
	}
}

func TestPatchSwitchesTheUpstreamForNewConnections(t *testing.T) {
	m := upstreamManager(t)
	sb := mustClaim(t, m, testKey)
	if err := m.PatchEnv(t.Context(), sb.ID, types.EnvPatch{"EGRESS_UPSTREAM": {Value: "http://127.0.0.1:3001", Guest: hostOnly}}, ""); err != nil {
		t.Fatalf("patch: %v", err)
	}
	upstream := func() string {
		v, _ := sb.HiddenEnv("EGRESS_UPSTREAM")
		return v
	}
	if got := upstream(); got != "http://127.0.0.1:3001" {
		t.Fatalf("upstream %q after the first patch", got)
	}
	if got := sb.Env.Redacted()["EGRESS_UPSTREAM"].Value; got != "" {
		t.Errorf("GET env would show %q", got)
	}
	if err := m.PatchEnv(t.Context(), sb.ID, types.EnvPatch{"EGRESS_UPSTREAM": nil}, ""); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if got := upstream(); got != "" {
		t.Errorf("upstream %q after clearing, want direct", got)
	}
	if err := m.PatchEnv(t.Context(), sb.ID, types.EnvPatch{"EGRESS_UPSTREAM": {Value: "http://10.9.9.9:1", Guest: hostOnly}}, ""); !errors.Is(err, ErrBadEnv) {
		t.Errorf("patch to an unlisted upstream: %v, want ErrBadEnv", err)
	}
}

func upstreamManager(t *testing.T) *Manager {
	t.Helper()
	m := newTestManager(t, newFakeEngine())
	editEgress(t, m, func(cfg *config.Config) {
		cfg.EgressUpstream = &config.EgressUpstreamConfig{ClaimEnv: "EGRESS_UPSTREAM", Allow: []string{"127.0.0.1"}}
	})
	return m
}
