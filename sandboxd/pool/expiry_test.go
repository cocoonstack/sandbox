package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestExpiryArchiveHibernatesAndArchivesARunningClaim(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, types.ExpireArchive, "", "", nil, nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	for round := range 2 {
		expire(m, sb)
		m.reapOnce(t.Context())
		waitFor(t, func() bool { return archivedCount(m) == 1 })
		m.mu.Lock()
		ck, deadline, live, action := sb.ArchiveCk, sb.Deadline, m.claimed[sb.ID] == sb, sb.OnExpire
		m.mu.Unlock()
		if ck == "" || !deadline.IsZero() || !live || action != types.ExpireArchive {
			t.Fatalf("round %d: archive %q deadline %v live %t action %q, want an archive kept forever with the action", round, ck, deadline, live, action)
		}
		if _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
			t.Fatalf("round %d: wake: %v", round, err)
		}
		m.mu.Lock()
		lease := time.Until(sb.Deadline)
		m.mu.Unlock()
		if lease < 59*time.Minute {
			t.Fatalf("round %d: lease after the wake %v, want the claim's hour again", round, lease)
		}
	}
	if m.Counters().Reaps != 0 {
		t.Errorf("reaps = %d, want 0: an archive-on-expiry claim is never destroyed", m.Counters().Reaps)
	}
}

func TestExpiryArchiveArchivesAHibernatedClaimWithoutPoolArchiving(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, types.ExpireArchive, "", "", nil, nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	expire(m, sb)
	m.reapOnce(t.Context())
	waitFor(t, func() bool { return archivedCount(m) == 1 })
}

func TestExpiryDestroyStaysTheDefault(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	for _, action := range []types.ExpireAction{"", types.ExpireDestroy} {
		sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, action, "", "", nil, nil)
		if err != nil {
			t.Fatalf("claim %q: %v", action, err)
		}
		if sb.OnExpire != "" {
			t.Errorf("claim %q recorded %q, want the empty default", action, sb.OnExpire)
		}
		expire(m, sb)
	}
	m.reapOnce(t.Context())
	waitFor(t, func() bool { return m.Counters().Reaps == 2 })
	if archivedCount(m) != 0 {
		t.Error("a default claim was archived")
	}
}

func TestExpiryArchiveRetriesOnlyTheExportAfterItFails(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	failing := &failingPublishStore{Store: m.ckpts}
	failing.fail.Store(true)
	m.ckpts = failing
	sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, types.ExpireArchive, "", "", nil, nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	expire(m, sb)
	m.mu.Lock()
	deadline := sb.Deadline
	m.mu.Unlock()
	m.reapOnce(t.Context())
	waitFor(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		_, busy := m.archiving[sb.ID]
		return !busy && sb.HibernateSnap != ""
	})
	m.mu.Lock()
	hibernated, archived, kept := sb.HibernateSnap != "", sb.ArchiveCk != "", sb.Deadline.Equal(deadline)
	m.mu.Unlock()
	if !hibernated || archived || !kept {
		t.Fatalf("after a failed export: hibernated %t archived %t deadline kept %t, want a hibernated claim with its deadline", hibernated, archived, kept)
	}
	hibernates := failing.publishes.Load()
	failing.fail.Store(false)
	m.reapOnce(t.Context())
	waitFor(t, func() bool { return archivedCount(m) == 1 })
	if m.Counters().Hibernates != 1 || failing.publishes.Load() != hibernates+1 {
		t.Errorf("hibernates %d, exports %d after %d: want one hibernate and one more export", m.Counters().Hibernates, failing.publishes.Load(), hibernates)
	}
}

func TestExpiryArchiveRetriesTheHibernateAfterItFails(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, types.ExpireArchive, "", "", nil, nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	eng.mu.Lock()
	eng.hibernateErr = errors.New("snapshot failed")
	eng.mu.Unlock()
	expire(m, sb)
	m.reapOnce(t.Context())
	waitFor(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		_, busy := m.archiving[sb.ID]
		return !busy
	})
	m.mu.Lock()
	running, live := sb.HibernateSnap == "" && sb.ArchiveCk == "", m.claimed[sb.ID] == sb
	m.mu.Unlock()
	if !running || !live {
		t.Fatalf("after a failed hibernate: running %t live %t, want the running claim kept", running, live)
	}
	eng.mu.Lock()
	eng.hibernateErr = nil
	eng.mu.Unlock()
	m.reapOnce(t.Context())
	waitFor(t, func() bool { return archivedCount(m) == 1 })
}

func TestExpiryArchiveSurvivesAFailedReapPersist(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, types.ExpireArchive, "acme", "", nil, nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	expire(m, sb)
	breakStore(t, m)
	m.reapOnce(t.Context())
	m.mu.Lock()
	_, marked := m.archiving[sb.ID]
	live, tenant := m.claimed[sb.ID] == sb, m.tenantLive["acme"]
	m.mu.Unlock()
	if marked || !live || tenant != 1 {
		t.Fatalf("after a failed reap persist: archiving mark %t live %t tenant count %d, want no mark, live, 1", marked, live, tenant)
	}
	healStore(t, m)
	m.reapOnce(t.Context())
	waitFor(t, func() bool { return archivedCount(m) == 1 })
}

func TestExpiryArchiveWakeBeforeTheExportGrantsAFreshLease(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, types.ExpireArchive, "", "", nil, nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	expire(m, sb)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	if _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake: %v", err)
	}
	m.mu.Lock()
	lease := time.Until(sb.Deadline)
	m.mu.Unlock()
	if lease < 59*time.Minute {
		t.Fatalf("lease after a wake from the expiry hibernate = %v, want the claim's hour again", lease)
	}
	m.reapOnce(t.Context())
	m.mu.Lock()
	_, marked := m.archiving[sb.ID]
	m.mu.Unlock()
	if marked {
		t.Error("the reaper took the woken claim again")
	}
}

func TestRenewSwitchesTheExpireAction(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	cred := Cred{Token: sb.Token}
	for _, tt := range []struct {
		requested, want types.ExpireAction
	}{
		{types.ExpireArchive, types.ExpireArchive},
		{"", types.ExpireArchive},
		{types.ExpireDestroy, ""},
		{"", ""},
	} {
		if _, err := m.Renew(t.Context(), sb.ID, cred, time.Hour, tt.requested); err != nil {
			t.Fatalf("renew %q: %v", tt.requested, err)
		}
		if row, _ := m.Sandbox(sb.ID); row.OnExpire != tt.want {
			t.Errorf("renew %q: summary on_expire %q, want %q", tt.requested, row.OnExpire, tt.want)
		}
	}
}

func TestForkChildrenInheritOrOverrideTheExpireAction(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	parent, err := m.ClaimProvision(t.Context(), testKey, 0, types.ExpireArchive, "", "", nil, nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	for _, tt := range []struct {
		requested, want types.ExpireAction
	}{
		{"", types.ExpireArchive},
		{types.ExpireDestroy, ""},
	} {
		children, err := m.Fork(t.Context(), parent.ID, Cred{Token: parent.Token}, 1, 0, tt.requested, "")
		if err != nil {
			t.Fatalf("fork %q: %v", tt.requested, err)
		}
		if children[0].OnExpire != tt.want {
			t.Errorf("fork %q: child on_expire %q, want %q", tt.requested, children[0].OnExpire, tt.want)
		}
	}
}

func TestCheckpointBranchTakesItsOwnExpireAction(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	src, err := m.ClaimProvision(t.Context(), testKey, 0, types.ExpireArchive, "", "", nil, nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "", "")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	for _, action := range []types.ExpireAction{"", types.ExpireArchive} {
		branch, err := m.ClaimCheckpoint(t.Context(), ckpt.ID, time.Hour, action, "", nil)
		if err != nil {
			t.Fatalf("branch %q: %v", action, err)
		}
		if branch.OnExpire != action {
			t.Errorf("branch %q: on_expire %q, want its own request's", action, branch.OnExpire)
		}
	}
}

func TestArchivableRefusesClaimsThatCannotHibernate(t *testing.T) {
	egress := types.PoolKey{Template: testKey.Template, Net: types.NetEgress, Size: types.SizeSmall}
	for _, tt := range []struct {
		key     types.PoolKey
		volumes bool
		action  types.ExpireAction
		want    error
	}{
		{egress, false, types.ExpireArchive, ErrNoEgressHibernate},
		{testKey, true, types.ExpireArchive, ErrVolumeCapture},
		{egress, true, types.ExpireDestroy, nil},
		{testKey, false, types.ExpireArchive, nil},
	} {
		if err := archivable(tt.key, tt.volumes, tt.action); !errors.Is(err, tt.want) {
			t.Errorf("archivable(%s, volumes=%t, %q) = %v, want %v", tt.key.Net, tt.volumes, tt.action, err, tt.want)
		}
	}
}

func TestArchiveOnExpireRefusesAVolumeClaimOnEveryPath(t *testing.T) {
	path := writeVolumeImage(t, "data.img", "data")
	eng := newFakeEngine()
	m := newVolumePoolManager(t, eng, t.TempDir(), []config.VolumeSpec{{Name: "data", Path: path, Writable: true}})
	m.pools[testKey].warm = append(m.pools[testKey].warm, &types.Sandbox{VMName: "sbx-warm", Key: testKey, VsockSocket: "/vsock/warm"})
	volumes := []types.Volume{{Name: "data"}}
	if _, err := m.ClaimWarm(t.Context(), testKey, 0, types.ExpireArchive, "", "", nil, volumes); !errors.Is(err, ErrVolumeCapture) {
		t.Errorf("warm claim: %v, want ErrVolumeCapture", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, types.ExpireArchive, "", "", nil, volumes); !errors.Is(err, ErrVolumeCapture) {
		t.Errorf("provision claim: %v, want ErrVolumeCapture", err)
	}
	if n := len(eng.clones) + len(eng.colds); n != 0 {
		t.Errorf("refused claims ran %d VM operations, want 0", n)
	}
	sb, err := m.ClaimWarm(t.Context(), testKey, 0, "", "", "", nil, volumes)
	if err != nil {
		t.Fatalf("volume claim: %v", err)
	}
	if _, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, 0, types.ExpireArchive); !errors.Is(err, ErrVolumeCapture) {
		t.Errorf("renew to archive: %v, want ErrVolumeCapture", err)
	}
}

func expire(m *Manager, sb *types.Sandbox) {
	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Second)
	m.mu.Unlock()
}

type failingPublishStore struct {
	store.Store
	fail      atomic.Bool
	publishes atomic.Int32
}

func (s *failingPublishStore) Publish(ctx context.Context, staging, id string) error {
	s.publishes.Add(1)
	if s.fail.Load() {
		return errors.New("store unavailable")
	}
	return s.Store.Publish(ctx, staging, id)
}
