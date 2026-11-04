package pool

import (
	"errors"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestDiskUnderReserveRefusesClaimsAndWakesUntilItRecovers(t *testing.T) {
	m := diskReserveManager(t, "1000P")
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)

	m.checkDisk(t.Context())
	if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{TTL: time.Hour}); !errors.Is(err, ErrQuota) {
		t.Errorf("claim under the reserve: %v, want ErrQuota", err)
	}
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrQuota) {
		t.Errorf("wake under the reserve: %v, want ErrQuota", err)
	}
	if _, g := m.Info(); !g.AtCapacity || g.AtCapacityReason != diskLowReason {
		t.Errorf("gauges %+v, want at capacity for the disk", g)
	}

	next := &config.Config{DataDir: m.dataDir, DiskReserve: &config.DiskReserve{Free: "1K"}, Pools: []config.PoolSpec{{PoolKey: testKey}}}
	m.view.Store(newConfigView(next, testSecrets(t)))
	m.checkDisk(t.Context())
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Errorf("wake above the reserve: %v", err)
	}
	if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{TTL: time.Hour}); err != nil {
		t.Errorf("claim above the reserve: %v", err)
	}
}

func TestDiskUnderReserveAdvertisesNoWarm(t *testing.T) {
	m := diskReserveManager(t, "1000P")
	p := m.pools[testKey]
	p.warm = append(p.warm, &types.Sandbox{VMName: "sbx-warm", Key: testKey, VsockSocket: "/vsock/warm"})
	if n := m.WarmCounts()[p.hash]; n != 1 {
		t.Fatalf("warm count %d above the reserve, want 1", n)
	}
	m.checkDisk(t.Context())
	if n, listed := m.WarmCounts()[p.hash]; listed {
		t.Errorf("warm count %d under the reserve, want the pool unlisted so peers route around the node", n)
	}
}

func TestDiskUnderReserveHoldsHibernatesAndCaptures(t *testing.T) {
	m := diskReserveManager(t, "1000P")
	sb := mustClaim(t, m, testKey)
	cred := Cred{Token: sb.Token}
	m.checkDisk(t.Context())

	if err := m.Hibernate(t.Context(), sb.ID, cred); !errors.Is(err, ErrQuota) || !benignSweepErr(err) {
		t.Errorf("hibernate under the reserve: %v, want ErrQuota, which a sweep does not log as a failure", err)
	}
	if got, _ := m.Sandbox(sb.ID); got.Hibernated {
		t.Errorf("sandbox %s hibernated under the reserve", sb.ID)
	}
	if _, err := m.Checkpoint(t.Context(), sb.ID, cred, "", ""); !errors.Is(err, ErrQuota) {
		t.Errorf("checkpoint under the reserve: %v, want ErrQuota", err)
	}
	if _, _, err := m.Promote(t.Context(), sb.ID, cred, "kept", ""); !errors.Is(err, ErrQuota) {
		t.Errorf("promote under the reserve: %v, want ErrQuota", err)
	}

	m.view.Store(newConfigView(&config.Config{DataDir: m.dataDir, DiskReserve: &config.DiskReserve{Free: "1K"}, Pools: []config.PoolSpec{{PoolKey: testKey}}}, testSecrets(t)))
	m.checkDisk(t.Context())
	if err := m.Hibernate(t.Context(), sb.ID, cred); err != nil {
		t.Errorf("hibernate above the reserve: %v", err)
	}
}

func TestAFailedStatfsKeepsTheDiskLowState(t *testing.T) {
	m := diskReserveManager(t, "1000P")
	m.checkDisk(t.Context())
	unreadable := &config.Config{DataDir: m.dataDir, DiskReserve: &config.DiskReserve{Free: "1K", Dirs: []string{m.dataDir + "/missing"}}, Pools: []config.PoolSpec{{PoolKey: testKey}}}
	m.view.Store(newConfigView(unreadable, testSecrets(t)))
	m.checkDisk(t.Context())
	if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{TTL: time.Hour}); !errors.Is(err, ErrQuota) {
		t.Errorf("claim after a failed statfs: %v, want ErrQuota while the disk was last seen low", err)
	}
}

func diskReserveManager(t *testing.T, free string) *Manager {
	t.Helper()
	cfg := &config.Config{DataDir: t.TempDir(), DiskReserve: &config.DiskReserve{Free: free}, Pools: []config.PoolSpec{{PoolKey: testKey}}}
	m, err := NewManager(t.Context(), cfg, newFakeEngine(), testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	return m
}
