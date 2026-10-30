package pg

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/metastore/metastoretest"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

func TestLandListsInCursorOrderAndResumesFromTheMark(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1")
	ctx := t.Context()
	if mark, err := s.Last(ctx, "n1"); err != nil || mark != ([32]byte{}) {
		t.Fatalf("mark of a node with nothing landed: %+v %v", mark, err)
	}
	t0 := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	full := usage.Event{
		Time: t0.Add(time.Microsecond), Event: "claim", ID: "sb_1", VMName: "sbx-1", KeyHash: "k", Tenant: "acme",
		Volumes: []string{"data", "ro"}, VolumesRW: []string{"data"}, Reference: "ref", Upstream: "up:1", Secret: "gh", Sent: 7, Received: 9,
	}
	land(t, s, record("n1", 1, full), record("n2", 1, usage.Event{Time: t0, Event: "claim", ID: "sb_2"}))
	land(t, s, record("n1", 2, usage.Event{Time: t0.Add(time.Hour), Event: "fork", ID: "sb_1", Children: []string{"sb_3"}}))

	recs, err := s.List(ctx, usage.Query{Limit: 10})
	if err != nil || len(recs) != 3 {
		t.Fatalf("list: %d %v", len(recs), err)
	}
	if got := recs[0]; got.Node != "n1" || !equalEvents(got.Event, full) {
		t.Errorf("first record %+v, want n1 %+v", got, full)
	}
	if !slices.IsSortedFunc(recs, func(a, b usage.Record) int { return cmp.Compare(a.Cursor, b.Cursor) }) || recs[2].Children[0] != "sb_3" {
		t.Errorf("records out of cursor order or lost children: %+v", recs)
	}
	if mark, _ := s.Last(ctx, "n1"); mark != digest("n1", 2) {
		t.Errorf("mark of n1 %x, want the digest of its second line", mark)
	}
}

func TestListFiltersAndPages(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1")
	ctx := t.Context()
	t0 := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	for i, tenant := range []string{"acme", "beta", "acme", "acme", "beta"} {
		land(t, s, record("n1", int64(i)+1, usage.Event{Time: t0.Add(time.Duration(i) * time.Minute), Event: "claim", ID: fmt.Sprintf("%s%d", tenant, i+1), Tenant: tenant}))
	}
	all, err := s.List(ctx, usage.Query{Limit: 10})
	if err != nil || len(all) != 5 {
		t.Fatalf("list: %d %v", len(all), err)
	}
	ids := func(q usage.Query) string {
		t.Helper()
		q.Limit = max(q.Limit, 10)
		recs, listErr := s.List(ctx, q)
		if listErr != nil {
			t.Fatalf("list %+v: %v", q, listErr)
		}
		var out []string
		for _, r := range recs {
			out = append(out, r.ID)
		}
		return strings.Join(out, ",")
	}
	for _, tt := range []struct {
		name string
		q    usage.Query
		want string
	}{
		{"tenant", usage.Query{Tenant: "acme"}, "acme1,acme3,acme4"},
		{"since", usage.Query{Since: t0.Add(2 * time.Minute)}, "acme3,acme4,beta5"},
		{"until", usage.Query{Until: t0.Add(2 * time.Minute)}, "acme1,beta2"},
		{"window and tenant", usage.Query{Tenant: "beta", Since: t0.Add(time.Minute), Until: t0.Add(4 * time.Minute)}, "beta2"},
		{"after", usage.Query{After: all[2].Cursor}, "acme4,beta5"},
		{"after and tenant", usage.Query{Tenant: "acme", After: all[0].Cursor}, "acme3,acme4"},
	} {
		if got := ids(tt.q); got != tt.want {
			t.Errorf("%s: %s, want %s", tt.name, got, tt.want)
		}
	}
	page, err := s.List(ctx, usage.Query{Limit: 2})
	if err != nil || len(page) != 2 || page[1].Cursor != all[1].Cursor {
		t.Errorf("first page of 2: %+v %v", page, err)
	}
	if empty, err := s.List(ctx, usage.Query{After: all[4].Cursor, Limit: 10}); err != nil || len(empty) != 0 {
		t.Errorf("page past the last cursor: %+v %v", empty, err)
	}
}

func TestCellsKeepTheirOwnRows(t *testing.T) {
	dsn := metastoretest.PGSchema(t)
	c1, c2 := open(t, dsn, "c1"), open(t, dsn, "c2")
	ev := usage.Event{Time: time.Now(), Event: "claim", ID: "sb_1"}
	land(t, c1, record("n1", 1, ev))
	land(t, c2, record("n1", 1, ev))
	if recs, err := c2.List(t.Context(), usage.Query{Limit: 10}); err != nil || len(recs) != 1 {
		t.Errorf("c2 lists %d rows %v, want its own 1", len(recs), err)
	}
	if mark, _ := c2.Last(t.Context(), "n1"); mark != digest("n1", 1) {
		t.Errorf("c2's mark of n1 %x, want its one line", mark)
	}
}

func TestANulInAReferenceLands(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1")
	land(t, s, record("n1", 1, usage.Event{Time: time.Now(), Event: "egress", ID: "sb_1", Reference: "a\x00b"}))
	if recs, err := s.List(t.Context(), usage.Query{Limit: 1}); err != nil || len(recs) != 1 || recs[0].Reference != "a\uFFFDb" {
		t.Errorf("list: %+v %v", recs, err)
	}
}

func TestLandWaitsForTheCellLock(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1")
	ctx := t.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err = tx.Exec(ctx, cellLock, "c1"); err != nil {
		t.Fatalf("take the cell lock: %v", err)
	}
	landed := make(chan error, 1)
	go func() {
		landed <- s.Land(ctx, []usage.Record{record("n1", 1, usage.Event{Time: time.Now(), Event: "claim", ID: "sb_1"})})
	}()
	select {
	case err = <-landed:
		t.Fatalf("land under a held cell lock returned %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	_ = tx.Rollback(ctx)
	if err = <-landed; err != nil {
		t.Errorf("land after the lock: %v", err)
	}
}

func TestSchemaIndexesTheQueries(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1")
	var n int
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_indexes
		WHERE schemaname = current_schema() AND tablename = 'sandboxd_usage'`).Scan(&n); err != nil || n != 5 {
		t.Errorf("usage index count %d %v, want 5 (primary key, node digest, node cursor, tenant, time)", n, err)
	}
}

func TestShipLandsAJournalOnceAcrossRestarts(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1")
	path := filepath.Join(t.TempDir(), usage.JournalFile)
	writeJournal(t, path, "sb_1", "sb_2")
	ship(t, s, path, 2)
	writeJournal(t, path, "sb_3")
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	writeJournal(t, path, "sb_4")
	ship(t, s, path, 4)
	ship(t, s, path, 4)
	recs, err := s.List(t.Context(), usage.Query{Limit: 10})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for i, r := range recs {
		if want := fmt.Sprintf("sb_%d", i+1); r.ID != want || r.Node != "n1" {
			t.Errorf("record %d: %s node %s, want %s n1", i, r.ID, r.Node, want)
		}
	}
}

func TestShipLandsNoLineTwiceAfterTheJournalLosesItsLandedTail(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1")
	path := filepath.Join(t.TempDir(), usage.JournalFile)
	writeJournal(t, path, "sb_1", "sb_2", "sb_3")
	ship(t, s, path, 3)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	if err = os.WriteFile(path, []byte(strings.Join(strings.SplitAfter(string(raw), "\n")[:2], "")), 0o600); err != nil {
		t.Fatalf("cut journal: %v", err)
	}
	writeJournal(t, path, "sb_4")
	ship(t, s, path, 4)
	recs, err := s.List(t.Context(), usage.Query{Limit: 10})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for i, r := range recs {
		if want := fmt.Sprintf("sb_%d", i+1); r.ID != want {
			t.Errorf("record %d: %s, want %s", i, r.ID, want)
		}
	}
}

func TestLandSkipsALandedLine(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1")
	first, second := usage.Event{Time: time.Now(), Event: "claim", ID: "sb_1"}, usage.Event{Time: time.Now(), Event: "release", ID: "sb_1"}
	land(t, s, record("n1", 1, first))
	land(t, s, record("n1", 1, first), record("n1", 2, second))
	recs, err := s.List(t.Context(), usage.Query{Limit: 10})
	if err != nil || len(recs) != 2 || recs[0].Event.Event != "claim" || recs[1].Event.Event != "release" {
		t.Errorf("rows after relanding a digest: %+v %v, want the claim and the release once each", recs, err)
	}
	if mark, _ := s.Last(t.Context(), "n1"); mark != digest("n1", 2) {
		t.Errorf("mark %x, want line 2", mark)
	}
}

func open(t *testing.T, dsn, cell string) *Store {
	t.Helper()
	s, err := Open(t.Context(), dsn, cell)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func land(t *testing.T, s *Store, recs ...usage.Record) {
	t.Helper()
	if err := s.Land(t.Context(), recs); err != nil {
		t.Fatalf("land: %v", err)
	}
}

func record(node string, line int64, ev usage.Event) usage.Record {
	return usage.Record{Node: node, Digest: digest(node, line), Event: ev}
}

func digest(node string, line int64) [32]byte {
	return sha256.Sum256(fmt.Appendf(nil, "%s/%d", node, line))
}

func equalEvents(a, b usage.Event) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func writeJournal(t *testing.T, path string, ids ...string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer func() { _ = f.Close() }()
	for _, id := range ids {
		line, _ := json.Marshal(usage.Event{Time: time.Now(), Event: "claim", ID: id})
		if _, err = f.Write(append(line, '\n')); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
}

func ship(t *testing.T, s *Store, path string, want int) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		usage.Ship(ctx, path, "n1", s)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for count(t, s) < want && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done
	if n := count(t, s); n != want {
		t.Fatalf("%d rows landed, want %d", n, want)
	}
}

func count(t *testing.T, s *Store) int {
	t.Helper()
	recs, err := s.List(t.Context(), usage.Query{Limit: 100})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	return len(recs)
}
