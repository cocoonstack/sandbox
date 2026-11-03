package pool

import (
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

func TestRenewUsagePreservesOrderWhenPublishedBackwards(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newTestManager(t, newFakeEngine())
		sb := mustClaim(t, m, testKey)
		first := usage.Event{Time: time.Now(), Event: "renew", ID: sb.ID, Deadline: time.Now().Add(time.Minute)}
		time.Sleep(time.Second)
		second := usage.Event{Time: time.Now(), Event: "renew", ID: sb.ID, Deadline: time.Now().Add(time.Hour)}
		publishFirst, done := make(chan struct{}), make(chan struct{})
		go func() {
			<-publishFirst
			m.recordUsage(t.Context(), first)
			close(done)
		}()
		m.recordUsage(t.Context(), second)
		time.Sleep(time.Second)
		close(publishFirst)
		<-done
		events := usageJournal(t, m)
		if len(events) != 3 || !events[1].Deadline.Equal(second.Deadline) || !events[2].Deadline.Equal(first.Deadline) {
			t.Fatalf("journal %+v, want the newer renew before the older one", events)
		}
		slices.SortFunc(events, func(a, b usage.Event) int { return a.Time.Compare(b.Time) })
		if last := events[len(events)-1]; !last.Deadline.Equal(second.Deadline) {
			t.Errorf("folded deadline %v, want the newer grant %v", last.Deadline, second.Deadline)
		}
	})
}

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
			m.store.writeMu.Lock()
			unlock := sync.OnceFunc(m.store.writeMu.Unlock)
			defer unlock()
			done := make(chan error, len(tt.leases))
			var deadlines [2]time.Time
			var observed [2]time.Time
			for i, ttl := range tt.leases {
				previous := m.deadlineOf(sb)
				go func() {
					_, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, ttl, "")
					done <- err
				}()
				waitFor(t, func() bool { return !m.deadlineOf(sb).Equal(previous) })
				deadlines[i], observed[i] = m.deadlineOf(sb), time.Now()
			}
			unlock()
			for range tt.leases {
				if err := <-done; err != nil {
					t.Fatalf("renew: %v", err)
				}
			}
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
