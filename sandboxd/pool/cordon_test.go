package pool

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

func TestCordonRefusesClaimsTrimsWarmAndSurvivesARestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		seedGolden(t, m, testKey, "")
		spec := config.PoolSpec{PoolKey: testKey, Warm: 2}
		if err := m.SetPools(t.Context(), []config.PoolSpec{spec}); err != nil {
			t.Fatalf("SetPools: %v", err)
		}
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return len(infos) == 1 && infos[0].Warm == 2
		})

		if err := m.Cordon(t.Context()); err != nil {
			t.Fatalf("cordon: %v", err)
		}
		assertCordonRefuses(t, m)
		infos, g := m.Info()
		if !g.Cordoned || g.Draining || infos[0].Warm != 0 {
			t.Fatalf("gauges %+v warm=%d, want cordoned, not draining, no warm", g, infos[0].Warm)
		}
		if removed := eng.removedNames(); len(removed) != 2 {
			t.Fatalf("removed=%v, want both warm VMs destroyed", removed)
		}
		if counts := m.WarmCounts(); len(counts) != 0 {
			t.Fatalf("warm counts %v, want none gossiped", counts)
		}

		reborn := newFakeEngine()
		restarted := newTestManagerAt(t, reborn, m.dataDir, spec)
		restarted.refillOnce(t.Context())
		restarted.mu.Lock()
		p := restarted.pools[testKey]
		started := p.building || p.refilling > 0 || len(p.warm) > 0
		restarted.mu.Unlock()
		if _, g = restarted.Info(); !g.Cordoned || started || len(reborn.clones) != 0 {
			t.Fatalf("after restart gauges %+v started %v clones %v, want cordoned with no build or refill", g, started, reborn.clones)
		}
		assertCordonRefuses(t, restarted)

		if err := restarted.Uncordon(t.Context()); err != nil {
			t.Fatalf("uncordon: %v", err)
		}
		waitFor(t, func() bool {
			now, gauges := restarted.Info()
			return !gauges.Cordoned && now[0].Warm == 2
		})
		if _, err := os.Stat(m.cordonPath()); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("cordon file after uncordon: %v, want gone", err)
		}
		if _, err := claimAny(t.Context(), restarted, testKey, ClaimOptions{}); err != nil {
			t.Fatalf("claim after uncordon: %v", err)
		}
	})
}

func TestACloneInFlightAtCordonIsNotKept(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.cloneStall = make(chan struct{})
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
		m.pools[testKey].goldenDir = "/goldens/x"
		m.refillOnce(t.Context())
		synctest.Wait()
		if err := m.Cordon(t.Context()); err != nil {
			t.Fatalf("cordon: %v", err)
		}
		close(eng.cloneStall)
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return infos[0].Refilling == 0
		})
		if infos, _ := m.Info(); infos[0].Warm != 0 || len(eng.removedNames()) != 1 {
			t.Fatalf("warm %d removed %v, want the late clone destroyed", infos[0].Warm, eng.removedNames())
		}
	})
}

func TestACordonedNodeStillDropsAPoolRemovedMidClone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.cloneStall = make(chan struct{})
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
		m.pools[testKey].goldenDir = "/goldens/x"
		m.refillOnce(t.Context())
		synctest.Wait()
		if err := m.Cordon(t.Context()); err != nil {
			t.Fatalf("cordon: %v", err)
		}
		if err := m.SetPools(t.Context(), nil); err != nil {
			t.Fatalf("SetPools: %v", err)
		}
		close(eng.cloneStall)
		waitFor(t, func() bool {
			m.mu.Lock()
			defer m.mu.Unlock()
			p := m.pools[testKey]
			return p == nil || p.settled()
		})
		m.refillOnce(t.Context())
		m.mu.Lock()
		_, kept := m.pools[testKey]
		m.mu.Unlock()
		if kept {
			t.Fatal("the cordoned node kept a removed pool after its clone settled")
		}
	})
}

func TestCordonRefusesForksAndCheckpointClaims(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	src := mustClaim(t, m, testKey)
	ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "step-1", "")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err = m.Cordon(t.Context()); err != nil {
		t.Fatalf("cordon: %v", err)
	}
	if _, err = m.Fork(t.Context(), src.ID, Cred{Token: src.Token}, 1, 0, "", ""); !isCordonErr(err) {
		t.Errorf("fork: %v, want node cordoned", err)
	}
	if _, err = m.ClaimCheckpoint(t.Context(), ckpt.ID, ClaimOptions{}); !isCordonErr(err) {
		t.Errorf("checkpoint claim: %v, want node cordoned", err)
	}
}

func TestACordonedNodeKeepsServingItsOwnClaims(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), archivePool(3600))
	sb := mustClaim(t, m, testKey)
	gone := mustClaim(t, m, testKey)
	cred := Cred{Token: sb.Token}
	if err := m.Cordon(t.Context()); err != nil {
		t.Fatalf("cordon: %v", err)
	}
	if _, err := m.Renew(t.Context(), sb.ID, cred, time.Hour, ""); err != nil {
		t.Fatalf("renew: %v", err)
	}
	hibernateClaim(t, m, sb)
	if _, _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake hibernated: %v", err)
	}
	mustArchive(t, m, sb)
	if _, _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake archived: %v", err)
	}
	if err := m.Release(t.Context(), gone.ID, Cred{Token: gone.Token}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, g := m.Info(); !g.Cordoned || g.Claimed != 1 {
		t.Errorf("gauges %+v, want cordoned with one claim", g)
	}
}

func TestACordonedNodeNeverAdoptsButKeepsItsOwnRowAcrossARestart(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	b := c.node(t, "b", t.TempDir(), keepAnHour)
	lost := mustClaim(t, a, testKey)
	mustArchive(t, a, lost)
	own := mustClaim(t, b, testKey)
	mustArchive(t, b, own)
	mustFlush(t, b)
	if err := b.Cordon(t.Context()); err != nil {
		t.Fatalf("cordon: %v", err)
	}
	c.expire("a")
	mustNotAdopt(t, b, lost, "adoption by a cordoned node")

	restarted := c.restart(t, b)
	restarted.liftFence()
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, g := restarted.Info(); !g.Cordoned {
		t.Fatal("the restart lifted the cordon")
	}
	mustNotAdopt(t, restarted, lost, "adoption by a restarted cordoned node")
	mustAdopt(t, restarted, own, "its own archived claim")
	if _, _, _, err := restarted.WakeAgentSocket(t.Context(), own.ID, own.Token); err != nil {
		t.Fatalf("wake own archived claim: %v", err)
	}

	if err := restarted.Uncordon(t.Context()); err != nil {
		t.Fatalf("uncordon: %v", err)
	}
	mustAdopt(t, restarted, lost, "adoption after uncordon")
}

func TestAMoveLandsOnACordonedNode(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	if err := mc.t.Cordon(t.Context()); err != nil {
		t.Fatalf("cordon: %v", err)
	}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move to a cordoned node: %v", err)
	}
	if _, held := mc.t.byID(sb.ID); !held {
		t.Error("the cordoned node did not take the claim")
	}
	if _, held := mc.s.byID(sb.ID); held {
		t.Error("the source kept the moved claim")
	}
	assertCordonRefuses(t, mc.t)
}

func TestCordonOutlivesAReloadAndAnUndrain(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	if err := m.Cordon(t.Context()); err != nil {
		t.Fatalf("cordon: %v", err)
	}
	next := *m.cfg
	if _, err := m.ReloadConfig(t.Context(), &next); err != nil {
		t.Fatalf("reload: %v", err)
	}
	m.Drain(t.Context())
	m.Undrain(t.Context())
	if _, g := m.Info(); !g.Cordoned || g.Draining {
		t.Fatalf("gauges %+v, want cordoned and not draining", g)
	}
	assertCordonRefuses(t, m)

	m.Drain(t.Context())
	if err := m.Uncordon(t.Context()); err != nil {
		t.Fatalf("uncordon: %v", err)
	}
	if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{}); !errors.Is(err, ErrQuota) || !strings.HasSuffix(err.Error(), "node draining") {
		t.Fatalf("claim after uncordon of a drained node: %v, want node draining", err)
	}
}

func TestCordonIsAudited(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{DataDir: dir, AuditLog: true}
	m, err := NewManager(t.Context(), cfg, newFakeEngine(), testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	if err = m.Cordon(t.Context()); err != nil {
		t.Fatalf("cordon: %v", err)
	}
	if err = m.Uncordon(t.Context()); err != nil {
		t.Fatalf("uncordon: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
	if err != nil || !strings.Contains(string(raw), `"op":"cordon"`) || !strings.Contains(string(raw), `"op":"uncordon"`) {
		t.Errorf("audit %s, %v; want cordon and uncordon records", raw, err)
	}
}

func assertCordonRefuses(t *testing.T, m *Manager) {
	t.Helper()
	if _, err := m.ClaimWarm(t.Context(), testKey, ClaimOptions{}); !isCordonErr(err) {
		t.Fatalf("warm claim: %v, want node cordoned", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); !isCordonErr(err) {
		t.Fatalf("provision claim: %v, want node cordoned", err)
	}
}

func isCordonErr(err error) bool {
	return errors.Is(err, ErrQuota) && strings.HasSuffix(err.Error(), "node cordoned")
}
