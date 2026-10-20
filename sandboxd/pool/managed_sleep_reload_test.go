package pool

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
)

func TestManagedSleepReloadAndExpiryCompleteDuringWake(t *testing.T) {
	cfg := managedConfig(t)
	cfg.RefillConcurrency = 2
	cfg.Pools[0].IdleHibernateSeconds = 1
	cfg.ManagedVolumeHibernate = &config.ManagedVolumeHibernate{HookArgv: []string{"/hook"}, HookTimeoutSeconds: 1}
	e := &sleepTestEngine{managedTestEngine: newManagedEngine(), generation: 2, reason: "boot", vmID: "immutable-vm", mounted: true}
	m, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	m.managed.process = e.identity
	if err = m.PutTenant(t.Context(), tenants.TenantSpec{Name: "t1", Token: "t1-token"}); err != nil {
		t.Fatal(err)
	}
	createManagedTest(t, m, "data")
	opts := managedOptions("data")
	opts.Volumes[0].AttachOnly = false
	sb, err := m.ClaimProvision(t.Context(), testKey, opts)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return refilledTo(t, m, testKey, 1) })
	waitPoolSettled(t, m, testKey)
	requireSleep(t, m, sb)
	e.hookEntered = make(chan struct{}, 2)
	e.hookContinue = make(chan struct{})
	releaseHook := sync.OnceFunc(func() { close(e.hookContinue) })
	t.Cleanup(releaseHook)
	wake := make(chan error, 1)
	go func() { wake <- m.Wake(context.Background(), sb.ID, Cred{Token: sb.Token}) }()
	select {
	case <-e.hookEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("wake did not enter resume hook")
	}
	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Minute)
	m.mu.Unlock()
	m.reapOnce(t.Context())
	waitFor(t, func() bool { return len(m.maintSem) == 1 })
	if cap(m.maintSem) != 1 {
		t.Fatal("unexpected configured maintenance capacity")
	}
	if _, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Hour, ""); err != nil {
		t.Fatal(err)
	}
	before := m.view.Load()
	next := *cfg
	next.Pools = slices.Clone(cfg.Pools)
	next.Pools[0].Storage = "30G"
	reload := make(chan error, 1)
	go func() { _, err := m.ReloadConfig(context.Background(), &next); reload <- err }()
	waitFor(t, func() bool { return m.view.Load() != before })
	releaseHook()
	completed := false
	select {
	case err := <-reload:
		if err != nil {
			t.Fatal(err)
		}
		completed = true
	case <-time.After(time.Second):
	}
	if !completed {
		<-m.maintSem
		err := <-reload
		m.maintSem <- struct{}{}
		if err != nil {
			t.Errorf("reload rescue failed: %v", err)
		}
	}
	if err := <-wake; err != nil {
		t.Errorf("wake after cleanup: %v", err)
	}
	waitFor(t, func() bool { return len(m.maintSem) == 0 })
	if !completed {
		t.Fatal("reload and managed wake did not complete until the blocked expiry worker's maintenance permit was borrowed")
	}
}
