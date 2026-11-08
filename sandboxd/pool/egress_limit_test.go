package pool

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var (
	capped   = types.EgressLimit{TxBytesPerSecond: 1 << 20, RxBytesPerSecond: 4 << 20}
	recapped = types.EgressLimit{TxBytesPerSecond: 64 << 10}
)

func TestSetEgressLimitPersistsAndShowsInTheRootRead(t *testing.T) {
	tests := []struct {
		name      string
		hibernate bool
		limits    []types.EgressLimit
	}{
		{"running claim set", false, []types.EgressLimit{capped}},
		{"running claim replaced", false, []types.EgressLimit{capped, recapped}},
		{"running claim cleared", false, []types.EgressLimit{capped, {}}},
		{"hibernated claim set", true, []types.EgressLimit{capped}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(t, newFakeEngine())
			sb := mustClaim(t, m, testKey)
			if tt.hibernate {
				hibernateClaim(t, m, sb)
			}
			for _, l := range tt.limits {
				if err := m.SetEgressLimit(t.Context(), sb.ID, l); err != nil {
					t.Fatalf("set egress limit %+v: %v", l, err)
				}
			}
			want := tt.limits[len(tt.limits)-1]
			if tt.hibernate && hibernated(m) != 1 {
				t.Error("set egress limit woke the hibernated claim")
			}
			if got := journaled(t, m)[sb.ID].EgressLimit; got != want {
				t.Errorf("journaled limit %+v, want %+v", got, want)
			}
			summary, ok := m.Sandbox(sb.ID)
			if !ok || summary.EgressLimit != want {
				t.Errorf("root read %v limit %+v, want %+v", ok, summary.EgressLimit, want)
			}
			if got := m.egressLimit(sb); got != want {
				t.Errorf("outbound accessor %+v, want %+v", got, want)
			}
		})
	}
}

func TestClaimRowOmitsAnUnsetEgressLimit(t *testing.T) {
	for _, tt := range []struct {
		limit types.EgressLimit
		want  bool
	}{{types.EgressLimit{}, false}, {capped, true}, {recapped, true}} {
		raw, err := encodeRow(new(dtoOf(&types.Sandbox{ID: "sb_1", VMName: "vm", EgressLimit: tt.limit})))
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if got := strings.Contains(string(raw), "egress_limit"); got != tt.want {
			t.Errorf("limit %+v: row %s carries egress_limit %v, want %v", tt.limit, raw, got, tt.want)
		}
	}
}

func TestSetEgressLimitRefusesAClaimItDoesNotHold(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	released := mustClaim(t, m, testKey)
	if err := m.Release(t.Context(), released.ID, Cred{Token: released.Token}); err != nil {
		t.Fatalf("release: %v", err)
	}
	for _, id := range []string{"sb_missing", released.ID} {
		if err := m.SetEgressLimit(t.Context(), id, capped); !errors.Is(err, ErrUnknownSandbox) {
			t.Errorf("set egress limit of %s: %v, want ErrUnknownSandbox", id, err)
		}
	}
}

func TestSetEgressLimitRollsBackOnPersistFailure(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	if err := m.SetEgressLimit(t.Context(), sb.ID, capped); err != nil {
		t.Fatalf("set egress limit: %v", err)
	}
	breakStore(t, m)
	if err := m.SetEgressLimit(t.Context(), sb.ID, recapped); err == nil {
		t.Fatal("set egress limit reported success while the claim store was unwritable")
	}
	if got := m.egressLimit(sb); got != capped {
		t.Errorf("limit %+v after a failed persist, want %+v", got, capped)
	}
	healStore(t, m)
	if got := storedLimit(t, m, sb.ID); got != capped {
		t.Errorf("stored limit %+v after the heal, want %+v", got, capped)
	}
	if err := m.SetEgressLimit(t.Context(), sb.ID, recapped); err != nil {
		t.Fatalf("set egress limit after the heal: %v", err)
	}
	if got := storedLimit(t, m, sb.ID); got != recapped {
		t.Errorf("stored limit %+v, want %+v", got, recapped)
	}
}

func TestSetEgressLimitRewritesTheRowThatFollowsTheClaim(t *testing.T) {
	tests := []struct {
		name    string
		cluster func(*testing.T) *failoverCluster
		follow  func(*testing.T, *Manager, *types.Sandbox)
	}{
		{"archive row", newFailoverCluster, mustArchive},
		{"replica row", newReplicaCluster, func(t *testing.T, m *Manager, sb *types.Sandbox) {
			replicateNow(t, m, sb)
			hibernateClaim(t, m, sb)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.cluster(t)
			a := c.node(t, "a", t.TempDir(), keepAnHour)
			sb := mustClaim(t, a, testKey)
			tt.follow(t, a, sb)
			before, _ := a.Sandbox(sb.ID)
			if err := a.SetEgressLimit(t.Context(), sb.ID, capped); err != nil {
				t.Fatalf("set egress limit: %v", err)
			}
			if got := rowClaim(t, c, sb).EgressLimit; got != capped {
				t.Errorf("row limit %+v, want %+v", got, capped)
			}
			if after, _ := a.Sandbox(sb.ID); after.Hibernated != before.Hibernated || after.Archived != before.Archived {
				t.Errorf("hibernated %v archived %v after the set, want %v %v kept", after.Hibernated, after.Archived, before.Hibernated, before.Archived)
			}
		})
	}
}

func TestASetEgressLimitResendRewritesTheArchiveRowAFailedWriteMissed(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	reg := &hookRegistry{Registry: c.reg, failPut: true}
	a.archives = reg
	if err := a.SetEgressLimit(t.Context(), sb.ID, capped); err == nil {
		t.Fatal("set egress limit with the registry down succeeded")
	}
	if got := rowClaim(t, c, sb).EgressLimit; got != (types.EgressLimit{}) {
		t.Fatalf("row limit %+v after a failed write, want none", got)
	}
	reg.failPut = false
	if err := a.SetEgressLimit(t.Context(), sb.ID, capped); err != nil {
		t.Fatalf("resend: %v", err)
	}
	if got := rowClaim(t, c, sb).EgressLimit; got != capped {
		t.Errorf("row limit %+v after the resend, want %+v", got, capped)
	}
}

func TestAForkChildInheritsTheParentsEgressLimit(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	parent := mustClaim(t, m, testKey)
	if err := m.SetEgressLimit(t.Context(), parent.ID, capped); err != nil {
		t.Fatalf("set egress limit: %v", err)
	}
	children, err := m.Fork(t.Context(), parent.ID, Cred{Token: parent.Token}, 2, time.Hour, "", "")
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	rows := journaled(t, m)
	for _, c := range children {
		if got := m.egressLimit(c); got != capped {
			t.Errorf("child %s limit %+v, want the parent's %+v", c.ID, got, capped)
		}
		if got := rows[c.ID].EgressLimit; got != capped {
			t.Errorf("child %s journaled limit %+v, want %+v", c.ID, got, capped)
		}
	}
}

func TestACheckpointBranchKeepsTheLimitCapturedWithIt(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	src := mustClaim(t, m, testKey)
	if err := m.SetEgressLimit(t.Context(), src.ID, capped); err != nil {
		t.Fatalf("set egress limit: %v", err)
	}
	ck, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "", "")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if ck.EgressLimit != capped {
		t.Errorf("checkpoint limit %+v, want %+v", ck.EgressLimit, capped)
	}
	if err = m.SetEgressLimit(t.Context(), src.ID, recapped); err != nil {
		t.Fatalf("set egress limit after the checkpoint: %v", err)
	}
	branch, err := m.ClaimCheckpoint(t.Context(), ck.ID, ClaimOptions{TTL: time.Hour})
	if err != nil {
		t.Fatalf("claim checkpoint: %v", err)
	}
	if got := m.egressLimit(branch); got != capped {
		t.Errorf("branch limit %+v, want the captured %+v", got, capped)
	}
	if got := journaled(t, m)[branch.ID].EgressLimit; got != capped {
		t.Errorf("branch journaled limit %+v, want %+v", got, capped)
	}
}

func TestMoveHoldsOffEgressLimitChangesAndCarriesTheLimit(t *testing.T) {
	t.Run("during the final round", func(t *testing.T) {
		mc := newMoveCluster(t)
		sb := mustClaim(t, mc.s, testKey)
		if err := mc.s.SetEgressLimit(t.Context(), sb.ID, capped); err != nil {
			t.Fatalf("set egress limit: %v", err)
		}
		mc.p.commitHook = func(int) error {
			if err := mc.s.SetEgressLimit(t.Context(), sb.ID, recapped); !errors.Is(err, ErrEvacuating) {
				t.Errorf("set egress limit during the final round: %v, want ErrEvacuating", err)
			}
			return nil
		}
		if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
			t.Fatalf("move: %v", err)
		}
		summary, ok := mc.t.Sandbox(sb.ID)
		if !ok || summary.EgressLimit != capped {
			t.Errorf("target root read %v limit %+v, want %+v", ok, summary.EgressLimit, capped)
		}
		if got := journaled(t, mc.t)[sb.ID].EgressLimit; got != capped {
			t.Errorf("target journaled limit %+v, want %+v", got, capped)
		}
	})
	t.Run("on a received claim", func(t *testing.T) {
		mc := newMoveCluster(t)
		sb := mustClaim(t, mc.s, testKey)
		if err := mc.s.SetEgressLimit(t.Context(), sb.ID, capped); err != nil {
			t.Fatalf("set egress limit: %v", err)
		}
		lossy := &lossyMoves{Registry: mc.moveReg, downAfterCommit: true}
		mc.t.moves = lossy
		if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
			t.Fatalf("move: %v", err)
		}
		if err := mc.t.SetEgressLimit(t.Context(), sb.ID, recapped); !errors.Is(err, ErrEvacuating) {
			t.Errorf("set egress limit on a received claim: %v, want ErrEvacuating", err)
		}
		if summary, ok := mc.t.Sandbox(sb.ID); !ok || summary.EgressLimit != capped {
			t.Errorf("received claim %v limit %+v, want %+v kept", ok, summary.EgressLimit, capped)
		}
		lossy.setDown(false)
		waitFor(t, func() bool { return !mc.t.moveHeld(sb.ID) })
	})
}

func storedLimit(t *testing.T, m *Manager, id string) types.EgressLimit {
	t.Helper()
	claims, err := newClaimStore(filepath.Dir(m.store.path), false).load()
	if err != nil || claims[id] == nil {
		t.Fatalf("load claims: %v %v", claims[id], err)
	}
	return claims[id].EgressLimit
}
