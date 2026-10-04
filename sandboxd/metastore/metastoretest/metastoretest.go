// Package metastoretest gives a test its own schema on the PostgreSQL meta store.
package metastoretest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// PGSchema returns a DSN on SANDBOXD_TEST_PG_URL whose search_path is a new schema dropped after the test, or skips without the env.
func PGSchema(t *testing.T) string {
	t.Helper()
	url := os.Getenv("SANDBOXD_TEST_PG_URL")
	if url == "" {
		t.Skip("SANDBOXD_TEST_PG_URL unset")
	}
	raw := make([]byte, 6)
	_, _ = rand.Read(raw)
	name := "t_" + hex.EncodeToString(raw)
	exec := func(ctx context.Context, sql string) error {
		conn, err := pgx.Connect(ctx, url)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
		_, err = conn.Exec(ctx, sql)
		return err
	}
	if err := exec(t.Context(), "CREATE SCHEMA "+name); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() { _ = exec(context.Background(), "DROP SCHEMA "+name+" CASCADE") })
	sep := "?"
	if strings.Contains(url, "?") {
		sep = "&"
	}
	return url + sep + "search_path=" + name
}
