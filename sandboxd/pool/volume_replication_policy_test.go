package pool

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestLiveWriterDoesNotWaitForAnotherClaimsRenewal(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	other := mustClaim(t, m, testKey)
	attached := make(chan struct{}, 1)
	eng.liveVolumeHook = func(op string) error {
		if op == "attach" {
			attached <- struct{}{}
		}
		return nil
	}
	resume := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	defer release()
	renewed := make(chan error, 1)
	var writes atomic.Int32
	m.store.replaceFile = func(raw []byte) error {
		switch writes.Add(1) {
		case 1:
			if err := m.store.replace(raw); err != nil {
				return err
			}
			before := m.store.mark().seq
			go func() {
				_, err := m.Renew(t.Context(), other.ID, Cred{Token: other.Token}, time.Hour, "")
				renewed <- err
			}()
			deadline := time.Now().Add(3 * time.Second)
			for m.store.mark().seq == before {
				if time.Now().After(deadline) {
					return errors.New("renew did not stage its unrelated claim")
				}
				time.Sleep(time.Millisecond)
			}
			return nil
		case 2:
			<-resume
		}
		return m.store.replace(raw)
	}
	changed := make(chan error, 1)
	go func() {
		_, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}})
		changed <- err
	}()
	finished := false
	select {
	case <-attached:
	case err := <-changed:
		finished = true
		t.Errorf("unrelated renewal rejected writable attach: %v", err)
	case <-time.After(3 * time.Second):
		t.Error("unrelated renewal prevented writable device application")
	}
	release()
	if !finished {
		if err := <-changed; err != nil {
			t.Errorf("writable attach after renewal: %v", err)
		}
	}
	if err := <-renewed; err != nil {
		t.Fatal(err)
	}
	view, err := m.GetVolumes(sb.ID, "")
	if err != nil || len(view.Volumes) != 1 {
		t.Fatalf("writer did not attach: %+v, %v", view, err)
	}
}

func TestArchiveCheckpointCleanupIgnoresUnrelatedClaimWrites(t *testing.T) {
	m, _ := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	checkpoint, _, err := m.publishCheckpoint(t.Context(), sb, store.CheckpointID(randHex(8)), "orphan", "", true)
	if err != nil {
		t.Fatal(err)
	}
	ck := checkpoint.ID
	other := mustClaim(t, m, testKey)
	m.mu.Lock()
	other.LeaseSeconds++
	m.store.set(other)
	m.mu.Unlock()
	if m.store.synced() {
		t.Fatal("expected an unrelated unacknowledged claim write")
	}
	if err := m.deleteArchiveCk(t.Context(), ck); err != nil {
		t.Fatalf("unrelated claim delayed checkpoint cleanup: %v", err)
	}
	if ckExists(t, m, ck) || m.deleteMarked(ck) {
		t.Fatal("unreferenced checkpoint or delete marker remained")
	}
}
