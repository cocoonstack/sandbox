package pool

import "testing"

func TestKickRefillCoalescesAndNeverBlocks(t *testing.T) {
	m := &Manager{refillKick: make(chan struct{}, 1)}
	for range 3 {
		m.kickRefill()
	}
	if len(m.refillKick) != 1 {
		t.Fatalf("pending kicks = %d, want 1", len(m.refillKick))
	}
	<-m.refillKick
	m.kickRefill()
	if len(m.refillKick) != 1 {
		t.Fatalf("kick after drain not delivered")
	}
}
