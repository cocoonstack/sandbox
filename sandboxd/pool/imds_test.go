package pool

import (
	"errors"
	"testing"
)

func TestSetInstanceMetadataWritesARunningSandboxsDocument(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	doc := []byte(`{"region":"local","tags":["a"]}`)
	for range 2 {
		if err := m.SetInstanceMetadata(t.Context(), sb.ID, doc); err != nil {
			t.Fatalf("SetInstanceMetadata: %v", err)
		}
	}
	if got := eng.metadataDocs[sb.VsockSocket]; got != string(doc) {
		t.Errorf("guest document = %s, want %s", got, doc)
	}
	if err := m.SetInstanceMetadata(t.Context(), "sb_nope", doc); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("unknown id: %v, want ErrUnknownSandbox", err)
	}
	eng.metadataErr = ErrNoInstanceMetadata
	if err := m.SetInstanceMetadata(t.Context(), sb.ID, doc); !errors.Is(err, ErrNoInstanceMetadata) {
		t.Errorf("image without a responder: %v, want ErrNoInstanceMetadata", err)
	}
}

func TestSetInstanceMetadataRefusesAPausedSandboxWithoutWaking(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	doc := []byte(`{}`)

	paused := mustClaim(t, m, testKey)
	hibernateClaim(t, m, paused)
	restores := len(eng.restores)
	if err := m.SetInstanceMetadata(t.Context(), paused.ID, doc); !errors.Is(err, ErrPaused) {
		t.Errorf("hibernated: %v, want ErrPaused", err)
	}
	if len(eng.restores) != restores || paused.HibernateSnap == "" {
		t.Error("SetInstanceMetadata restored a hibernated sandbox")
	}

	running := mustClaim(t, m, testKey)
	running.Transition.Lock()
	if err := m.SetInstanceMetadata(t.Context(), running.ID, doc); !errors.Is(err, ErrPaused) {
		t.Errorf("mid-transition: %v, want ErrPaused", err)
	}
	running.Transition.Unlock()
	running.PendingSnap = "snap"
	if err := m.SetInstanceMetadata(t.Context(), running.ID, doc); !errors.Is(err, ErrPaused) {
		t.Errorf("pending hibernate: %v, want ErrPaused", err)
	}
	m.mu.Lock()
	running.PendingSnap, running.ArchiveCk = "", "ck"
	m.store.set(running)
	m.mu.Unlock()
	if err := m.SetInstanceMetadata(t.Context(), running.ID, doc); !errors.Is(err, ErrArchived) {
		t.Errorf("archived: %v, want ErrArchived", err)
	}
	if len(eng.metadataDocs) != 0 {
		t.Errorf("guest documents written = %v, want none", eng.metadataDocs)
	}
}
