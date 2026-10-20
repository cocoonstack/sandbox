package pool

import (
	"context"
	"sync"
	"testing"
	"time"
)

type metadataWriteGate struct {
	*fakeEngine
	entered chan struct{}
	resume  chan struct{}
}

func (e *metadataWriteGate) WriteInstanceMetadata(ctx context.Context, socket string, doc []byte) error {
	close(e.entered)
	<-e.resume
	return e.fakeEngine.WriteInstanceMetadata(ctx, socket, doc)
}

func TestAgentSocketAvailableDuringMetadataUpdate(t *testing.T) {
	e := newFakeEngine()
	m := newTestManager(t, e)
	t.Cleanup(func() { _ = m.Close() })
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gate := &metadataWriteGate{fakeEngine: e, entered: make(chan struct{}), resume: make(chan struct{})}
	m.eng = gate
	done := make(chan error, 1)
	go func() { done <- m.SetInstanceMetadata(t.Context(), sb.ID, []byte(`{"role":"test"}`)) }()
	finish := sync.OnceFunc(func() {
		close(gate.resume)
		if updateErr := <-done; updateErr != nil {
			t.Errorf("metadata update: %v", updateErr)
		}
	})
	t.Cleanup(finish)
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("metadata update did not enter engine")
	}
	socket, err := m.AgentSocket(sb.ID, sb.Token)
	finish()
	if err != nil || socket != sb.VsockSocket {
		t.Fatalf("running claim unavailable during metadata update: socket=%q err=%v", socket, err)
	}
}
