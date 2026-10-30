package usage

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var errDown = errors.New("store down")

func TestShipLandsEachLineOnceAcrossRestarts(t *testing.T) {
	path, store := journalAt(t), &memStore{}
	appendEvents(t, path, "a", "b", "c")
	newShipper(path, store).drain(t.Context())
	assertLanded(t, store, "a,b,c")
	newShipper(path, store).drain(t.Context())
	assertLanded(t, store, "a,b,c")
	appendEvents(t, path, "d", "e")
	newShipper(path, store).drain(t.Context())
	assertLanded(t, store, "a,b,c,d,e")
}

func TestShipFollowsARotation(t *testing.T) {
	path, store := journalAt(t), &memStore{}
	s := newShipper(path, store)
	appendEvents(t, path, "a", "b")
	s.drain(t.Context())
	appendEvents(t, path, "c")
	rotate(t, path)
	appendEvents(t, path, "d")
	s.drain(t.Context())
	assertLanded(t, store, "a,b,c,d")
	appendEvents(t, path, "e")
	newShipper(path, store).drain(t.Context())
	assertLanded(t, store, "a,b,c,d,e")
}

func TestShipBackfillsOnceTheStoreReturns(t *testing.T) {
	path, store := journalAt(t), &memStore{}
	s := newShipper(path, store)
	appendEvents(t, path, "a")
	s.drain(t.Context())
	store.setDown(true)
	appendEvents(t, path, "b")
	s.drain(t.Context())
	rotate(t, path)
	appendEvents(t, path, "c")
	s.drain(t.Context())
	if !s.failing {
		t.Error("shipper with the store down is not failing")
	}
	assertLanded(t, store, "a")
	store.setDown(false)
	s.drain(t.Context())
	assertLanded(t, store, "a,b,c")
	if s.failing {
		t.Error("shipper still failing after the store returned")
	}
}

func TestShipConvergesAfterALostReply(t *testing.T) {
	path, store := journalAt(t), &memStore{}
	s := newShipper(path, store)
	appendEvents(t, path, "a", "b")
	store.loseReplies(1)
	s.drain(t.Context())
	assertLanded(t, store, "a,b")
	appendEvents(t, path, "c")
	s.drain(t.Context())
	assertLanded(t, store, "a,b,c")
	if s.failing {
		t.Error("shipper failing after the resync")
	}
}

func TestShipLandsWhatTheJournalKeepsAfterTwoRotations(t *testing.T) {
	path, store := journalAt(t), &memStore{}
	s := newShipper(path, store)
	appendEvents(t, path, "a")
	s.drain(t.Context())
	store.setDown(true)
	appendEvents(t, path, "b")
	rotate(t, path)
	appendEvents(t, path, "c")
	rotate(t, path)
	appendEvents(t, path, "d")
	s.drain(t.Context())
	store.setDown(false)
	s.drain(t.Context())
	assertLanded(t, store, "a,c,d")
}

func TestShipStartsOverOnANewJournal(t *testing.T) {
	path, store := journalAt(t), &memStore{}
	appendEvents(t, path, "a", "b")
	newShipper(path, store).drain(t.Context())
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove journal: %v", err)
	}
	appendEvents(t, path, "c")
	newShipper(path, store).drain(t.Context())
	assertLanded(t, store, "a,b,c")
}

func TestShipLandsNoLineTwiceAfterTheJournalLosesItsLandedTail(t *testing.T) {
	path, store := journalAt(t), &memStore{}
	appendEvents(t, path, "a", "b", "c")
	newShipper(path, store).drain(t.Context())
	keepLines(t, path, 2)
	appendEvents(t, path, "d")
	newShipper(path, store).drain(t.Context())
	assertLanded(t, store, "a,b,c,d")
}

func TestShipSkipsAnUndecodableLineAndWaitsForATornOne(t *testing.T) {
	path, store := journalAt(t), &memStore{}
	s := newShipper(path, store)
	appendEvents(t, path, "a")
	appendRaw(t, path, "{\"t\":\"x\"\n")
	appendEvents(t, path, "b")
	appendRaw(t, path, `{"t":"2026-10-30T00:00:00Z","ev":"claim",`)
	s.drain(t.Context())
	assertLanded(t, store, "a,b")
	appendRaw(t, path, `"id":"c"}`+"\n")
	s.drain(t.Context())
	assertLanded(t, store, "a,b,c")
}

func TestShipDrainsABacklogPastOneBatch(t *testing.T) {
	path, store := journalAt(t), &memStore{}
	ids := make([]string, shipBatch+5)
	for i := range ids {
		ids[i] = fmt.Sprintf("sb_%d", i)
	}
	appendEvents(t, path, ids...)
	newShipper(path, store).drain(t.Context())
	assertLanded(t, store, strings.Join(ids, ","))
	if store.lands != 2 {
		t.Errorf("%d lands for %d lines, want 2", store.lands, len(ids))
	}
}

func TestShipLandsEveryTickUntilItsContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path, store := journalAt(t), &memStore{}
		appendEvents(t, path, "a")
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			Ship(ctx, path, "n1", store)
		}()
		synctest.Wait()
		assertLanded(t, store, "a")
		appendEvents(t, path, "b")
		time.Sleep(shipEvery)
		synctest.Wait()
		assertLanded(t, store, "a,b")
		cancel()
		<-done
	})
}

func journalAt(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), JournalFile)
	appendRaw(t, path, "")
	return path
}

func newShipper(path string, store Store) *shipper {
	return &shipper{path: path, node: "n1", store: store}
}

func appendEvents(t *testing.T, path string, ids ...string) {
	t.Helper()
	var b strings.Builder
	for _, id := range ids {
		line, err := json.Marshal(Event{Time: time.Now(), Event: "claim", ID: id})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	appendRaw(t, path, b.String())
}

func appendRaw(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer func() { _ = f.Close() }()
	if _, err = f.WriteString(s); err != nil {
		t.Fatalf("append: %v", err)
	}
}

func keepLines(t *testing.T, path string, n int) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	lines := strings.SplitAfter(string(raw), "\n")
	if err = os.WriteFile(path, []byte(strings.Join(lines[:n], "")), 0o600); err != nil {
		t.Fatalf("cut journal: %v", err)
	}
}

func rotate(t *testing.T, path string) {
	t.Helper()
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	appendRaw(t, path, "")
}

func assertLanded(t *testing.T, store *memStore, want string) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	ids := make([]string, len(store.recs))
	for i, r := range store.recs {
		ids[i] = r.ID
		if r.Cursor != int64(i)+1 || r.Node != "n1" {
			t.Errorf("record %d: cursor %d node %s, want cursor %d and node n1", i, r.Cursor, r.Node, i+1)
		}
	}
	if got := strings.Join(ids, ","); got != want {
		t.Errorf("landed %s, want %s", got, want)
	}
}

// memStore lands like the PostgreSQL store: whole batches that skip a landed digest.
type memStore struct {
	mu    sync.Mutex
	recs  []Record
	down  bool
	lose  int
	lands int
}

func (m *memStore) Last(_ context.Context, node string) ([32]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var mark [32]byte
	if m.down {
		return mark, errDown
	}
	for _, r := range m.recs {
		if r.Node == node {
			mark = r.Digest
		}
	}
	return mark, nil
}

func (m *memStore) Land(_ context.Context, recs []Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return errDown
	}
	for _, r := range recs {
		if !slices.ContainsFunc(m.recs, func(o Record) bool { return o.Node == r.Node && o.Digest == r.Digest }) {
			r.Cursor = int64(len(m.recs)) + 1
			m.recs = append(m.recs, r)
		}
	}
	m.lands++
	if m.lose > 0 {
		m.lose--
		return errDown
	}
	return nil
}

func (m *memStore) List(context.Context, Query) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.recs), nil
}

func (m *memStore) setDown(down bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.down = down
}

func (m *memStore) loseReplies(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lose = n
}
