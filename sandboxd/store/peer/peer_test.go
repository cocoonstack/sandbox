package peer

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/dir"
	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
)

const testID = "ck_00000000000000aa"

func TestPeerBackendContract(t *testing.T) {
	storetest.RunContract(t, New(localStore(t), nil, nil))
}

// TestInertWithoutOwnersOrPuller: a node with no mesh must degrade to its local
// backend, not fail. Healing is an addition, never a dependency.
func TestInertWithoutOwnersOrPuller(t *testing.T) {
	s := New(localStore(t), nil, nil)
	if _, _, _, err := s.Fetch(t.Context(), testID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Fetch error = %v, want store.ErrNotFound (a miss stays a miss)", err)
	}
}

// TestHealPublishesLocally is L3's whole point: after healing once, the record
// is local, so the next branch is served at L1 speed and this node becomes an
// owner that can serve its peers. The transfer must be paid once, not per use.
func TestHealPublishesLocally(t *testing.T) {
	local := localStore(t)
	puller := &fakePuller{records: map[string]map[string]string{"peer-a:7777": record()}}
	s := New(local, func(string) []string { return []string{"peer-a:7777"} }, puller)

	dir1, _, release, err := s.Fetch(t.Context(), testID)
	if err != nil {
		t.Fatalf("Fetch after heal: %v", err)
	}
	release()
	if got, err := os.ReadFile(filepath.Join(dir1, "mem")); err != nil || string(got) != "guest-pages" {
		t.Fatalf("healed export = %q, %v; want the peer's bytes", got, err)
	}

	// The record is now in the LOCAL backend: reading it directly, with no
	// wrapper and no puller, must succeed.
	if _, err := local.ReadMeta(t.Context(), testID); err != nil {
		t.Fatalf("record not published locally after heal: %v", err)
	}
	// And a second Fetch must not touch the network again.
	if _, _, release2, err := s.Fetch(t.Context(), testID); err != nil {
		t.Fatalf("second Fetch: %v", err)
	} else {
		release2()
	}
	if len(puller.asked) != 1 {
		t.Errorf("puller called %d times (%v); the transfer must be paid once", len(puller.asked), puller.asked)
	}
}

// TestHealTriesNextOwner: a gossiped view can be stale or a peer can be broken.
// One bad owner must not fail the heal while another can still serve it.
func TestHealTriesNextOwner(t *testing.T) {
	puller := &fakePuller{
		records:  map[string]map[string]string{"peer-b:7777": record()},
		failAddr: "peer-a:7777",
	}
	s := New(localStore(t), func(string) []string { return []string{"peer-a:7777", "peer-b:7777"} }, puller)

	_, _, release, err := s.Fetch(t.Context(), testID)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	release()
	if len(puller.asked) != 2 {
		t.Errorf("asked = %v, want both owners tried in order", puller.asked)
	}
}

// TestHealAllOwnersMissStaysNotFound: when every owner answers "not found" the
// gossiped view was simply stale. That must surface as store.ErrNotFound so the
// caller's existing not-found handling (a 404, not a 500) is unchanged.
func TestHealAllOwnersMissStaysNotFound(t *testing.T) {
	puller := &fakePuller{records: map[string]map[string]string{}}
	s := New(localStore(t), func(string) []string { return []string{"peer-a:7777"} }, puller)

	err := fetchErr(t, s)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Fetch error = %v, want store.ErrNotFound", err)
	}
}

// TestHealNoOwners: nothing gossiped the record, so there is nobody to ask.
func TestHealNoOwners(t *testing.T) {
	puller := &fakePuller{}
	s := New(localStore(t), func(string) []string { return nil }, puller)

	if _, err := s.ReadMeta(t.Context(), testID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ReadMeta error = %v, want store.ErrNotFound", err)
	}
	if len(puller.asked) != 0 {
		t.Errorf("puller was called with no owners: %v", puller.asked)
	}
}

// TestHealErrorIsReported: a peer that fails for a real reason (not a miss)
// must not be flattened into "not found" — that would hide a broken transfer
// behind a 404 and send the operator looking for a deleted checkpoint.
func TestHealErrorIsReported(t *testing.T) {
	puller := &fakePuller{failAddr: "peer-a:7777"}
	s := New(localStore(t), func(string) []string { return []string{"peer-a:7777"} }, puller)

	err := fetchErr(t, s)
	if err == nil || errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Fetch error = %v, want the peer failure surfaced", err)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("peer exploded")) {
		t.Errorf("error = %v, want the underlying peer failure named", err)
	}
}

// TestMetasStaysLocal: a cluster-wide listing is the control plane's job (it
// already scatter-gathers every node). If each node answered for its peers the
// same record would come back N times.
func TestMetasStaysLocal(t *testing.T) {
	puller := &fakePuller{records: map[string]map[string]string{"peer-a:7777": record()}}
	s := New(localStore(t), func(string) []string { return []string{"peer-a:7777"} }, puller)

	metas, err := s.Metas(t.Context())
	if err != nil {
		t.Fatalf("Metas: %v", err)
	}
	if len(metas) != 0 {
		t.Errorf("Metas returned %d record(s); a peer's records must not appear in a local listing", len(metas))
	}
}

func localStore(t *testing.T) *dir.Store {
	t.Helper()
	st, err := dir.New(t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("dir.New: %v", err)
	}
	return st
}

// fakePuller serves records from an in-memory table, recording who was asked.
type fakePuller struct {
	records  map[string]map[string]string // addr → file → content
	asked    []string
	failAddr string
}

func (p *fakePuller) Pull(_ context.Context, addr, _, dst string) error {
	p.asked = append(p.asked, addr)
	if addr == p.failAddr {
		return errors.New("peer exploded")
	}
	files, ok := p.records[addr]
	if !ok {
		return ErrNotFound
	}
	for name, content := range files {
		full := filepath.Join(dst, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
			return err
		}
	}
	return nil
}

func record() map[string]string {
	return map[string]string{
		store.MetaFile:                        `{"id":"` + testID + `"}`,
		filepath.Join(store.ExportDir, "mem"): "guest-pages",
	}
}

// TestPeerBackendContract: wrapping must not change local behavior. Every
// operation but a Fetch/ReadMeta miss is the local backend's, so the wrapper
// has to satisfy the same store contract the dir backend does.

// fetchErr keeps the three unused Fetch results out of the assertions.
func fetchErr(t *testing.T, s *Store) error {
	t.Helper()
	_, _, release, err := s.Fetch(t.Context(), testID) //nolint:dogsled // only err is asserted
	if release != nil {
		release()
	}
	return err
}
