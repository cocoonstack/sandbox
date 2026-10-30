// Package pg keeps the usage events of every node of a cell in one shared PostgreSQL table.
package pg

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cocoonstack/sandbox/sandboxd/metastore"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

const (
	table = "sandboxd_usage"

	// cellLock serializes a cell's lands, so cursors commit in order and a reader paging by cursor never skips a row.
	cellLock = `SELECT pg_advisory_xact_lock(hashtextextended('sandboxd_usage/' || $1, 0))`

	insertRow = `INSERT INTO sandboxd_usage (cell, node, line_sha256, t, ev, id, vm, key, tenant, volumes, volumes_rw, children, ref, upstream, secret, tx, rx)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
		ON CONFLICT (cell, node, line_sha256) DO NOTHING`

	listColumns = `cursor, node, t, ev, id, vm, key, tenant, volumes, volumes_rw, children, ref, upstream, secret, tx, rx`
)

//go:embed schema.sql
var schema string

var _ usage.Store = (*Store)(nil)

// Store is the shared usage table.
type Store struct {
	pool *pgxpool.Pool
	cell string
}

// Open connects to cell's rows and creates the table when it is missing.
func Open(ctx context.Context, dsn, cell string) (*Store, error) {
	pool, _, err := metastore.Open(ctx, dsn, table, schema)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool, cell: cell}, nil
}

func (s *Store) Last(ctx context.Context, node string) ([32]byte, error) {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	var mark [32]byte
	var digest []byte
	err := s.pool.QueryRow(qctx, `SELECT line_sha256 FROM sandboxd_usage WHERE cell = $1 AND node = $2 ORDER BY cursor DESC LIMIT 1`, s.cell, node).Scan(&digest)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return mark, err
	}
	copy(mark[:], digest)
	return mark, nil
}

func (s *Store) Land(ctx context.Context, recs []usage.Record) error {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	return pgx.BeginFunc(qctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(qctx, cellLock, s.cell); err != nil {
			return err
		}
		var batch pgx.Batch
		for _, r := range recs {
			batch.Queue(insertRow, s.cell, r.Node, r.Digest[:], r.Time, r.Event.Event, r.ID, r.VMName, r.KeyHash, r.Tenant, r.Volumes, r.VolumesRW, r.Children,
				strings.ReplaceAll(r.Reference, "\x00", "\uFFFD"), r.Upstream, r.Secret, r.Sent, r.Received)
		}
		return tx.SendBatch(qctx, &batch).Close()
	})
}

func (s *Store) List(ctx context.Context, q usage.Query) ([]usage.Record, error) {
	sql := `SELECT ` + listColumns + ` FROM sandboxd_usage WHERE cell = $1 AND cursor > $2`
	args := []any{s.cell, q.After}
	where := func(cond string, arg any) {
		args = append(args, arg)
		sql += " AND " + cond + " $" + strconv.Itoa(len(args))
	}
	if q.Tenant != "" {
		where("tenant =", q.Tenant)
	}
	if !q.Since.IsZero() {
		where("t >=", q.Since)
	}
	if !q.Until.IsZero() {
		where("t <", q.Until)
	}
	args = append(args, q.Limit)
	sql += " ORDER BY cursor LIMIT $" + strconv.Itoa(len(args))
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	rows, err := s.pool.Query(qctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scan)
}

// Close releases the connection pool.
func (s *Store) Close() {
	s.pool.Close()
}

func scan(row pgx.CollectableRow) (usage.Record, error) {
	var rec usage.Record
	err := row.Scan(&rec.Cursor, &rec.Node, &rec.Time, &rec.Event.Event, &rec.ID, &rec.VMName, &rec.KeyHash, &rec.Tenant,
		&rec.Volumes, &rec.VolumesRW, &rec.Children, &rec.Reference, &rec.Upstream, &rec.Secret, &rec.Sent, &rec.Received)
	if err != nil {
		return usage.Record{}, fmt.Errorf("scan usage row: %w", err)
	}
	rec.Time = rec.Time.UTC()
	return rec, nil
}
