package pool

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

func TestConcurrentRenewsPreserveLeaseOrderInUsage(t *testing.T) {
	for _, tt := range []struct {
		name   string
		leases [2]time.Duration
	}{
		{"extend", [2]time.Duration{time.Minute, time.Hour}},
		{"shorten", [2]time.Duration{time.Hour, time.Minute}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(t, newFakeEngine())
			sb := mustClaim(t, m, testKey)
			var wg sync.WaitGroup
			defer wg.Wait()
			m.store.writeMu.Lock()
			unlock := sync.OnceFunc(m.store.writeMu.Unlock)
			defer unlock()
			var deadlines, observed [2]time.Time
			for i, ttl := range tt.leases {
				previous := m.deadlineOf(sb)
				wg.Go(func() {
					if _, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, ttl, ""); err != nil {
						t.Errorf("renew: %v", err)
					}
				})
				waitFor(t, func() bool { return !m.deadlineOf(sb).Equal(previous) })
				deadlines[i], observed[i] = m.deadlineOf(sb), time.Now()
			}
			unlock()
			wg.Wait()
			events := slices.DeleteFunc(usageJournal(t, m), func(ev usage.Event) bool { return ev.Event != "renew" })
			if len(events) != len(tt.leases) {
				t.Fatalf("renew events = %d, want %d", len(events), len(tt.leases))
			}
			for i, deadline := range deadlines {
				at := slices.IndexFunc(events, func(ev usage.Event) bool { return ev.Deadline.Equal(deadline) })
				if at < 0 {
					t.Fatalf("no renew event for deadline %v", deadline)
				}
				if ev := events[at]; ev.Time.After(observed[i]) {
					t.Errorf("renew %d event time %v follows observed lease mutation %v", i, ev.Time, observed[i])
				}
			}
			slices.SortFunc(events, func(a, b usage.Event) int { return a.Time.Compare(b.Time) })
			if !events[0].Time.Before(events[1].Time) || !events[1].Deadline.Equal(deadlines[1]) {
				t.Errorf("usage order %+v does not end with the latest lease %v", events, deadlines[1])
			}
			loaded, err := newClaimStore(m.dataDir, false).load()
			if err != nil {
				t.Fatalf("read claims: %v", err)
			}
			if loaded[sb.ID] == nil || !loaded[sb.ID].Deadline.Equal(events[1].Deadline) {
				t.Errorf("persisted claim %+v disagrees with the last usage deadline %v", loaded[sb.ID], events[1].Deadline)
			}
		})
	}
}
