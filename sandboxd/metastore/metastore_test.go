package metastore

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/cocoonstack/sandbox/sandboxd/metastore/metastoretest"
)

func TestListenFiresSyncedAgainAfterAReconnect(t *testing.T) {
	dsn := metastoretest.PGSchema(t)
	_, schema, _ := strings.CutLast(dsn, "=")
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cfg.RuntimeParams["application_name"] = schema
	var synced atomic.Int32
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Listen(ctx, cfg, "t_listen", func() { synced.Add(1) }, func(string) {})
	}()
	t.Cleanup(func() { cancel(); <-done })
	if !eventually(func() bool { return synced.Load() == 1 }) {
		t.Fatalf("synced fired %d times on the first connect, want 1", synced.Load())
	}
	admin, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = admin.Close(t.Context()) }()
	if _, err = admin.Exec(t.Context(), "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1", schema); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	if !eventually(func() bool { return synced.Load() == 2 }) {
		t.Errorf("synced fired %d times after the listener reconnected, want 2", synced.Load())
	}
}

func eventually(cond func() bool) bool {
	for range 200 {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
