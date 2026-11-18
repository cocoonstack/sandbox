// Package pg keeps the usage events of every node of a cell in one shared PostgreSQL table.
package pg

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cocoonstack/sandbox/sandboxd/metastore"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

const (
	table = "sandboxd_usage"

	// cellLock serializes a cell's lands, so cursors commit in order and a reader paging by cursor never skips a row.
	cellLock = `SELECT pg_advisory_xact_lock(hashtextextended('sandboxd_usage/' || $1, 0))`

	insertRow = `INSERT INTO sandboxd_usage (cell, node, line_sha256, t, ev, id, vm, key, template, net, size, tenant, deadline, volumes, volumes_rw, children, ref, upstream, secret, tx, rx)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
		ON CONFLICT (cell, node, line_sha256) DO NOTHING`

	listColumns = `cursor, node, t, ev, id, vm, key, template, net, size, tenant, deadline, volumes, volumes_rw, children, ref, upstream, secret, tx, rx`

	lastLine  = `SELECT line_sha256 FROM sandboxd_usage WHERE cell = $1 AND node = $2 ORDER BY cursor DESC LIMIT 1`
	firstLine = `SELECT line_sha256 FROM sandboxd_usage WHERE cell = $1 AND node = $2 ORDER BY cursor LIMIT 1`

	pruneBatch = 1000

	pruneRows = `WITH win AS (
			SELECT cursor, t FROM sandboxd_usage WHERE cell = $1 AND node = $2 AND cursor > $3 ORDER BY cursor LIMIT $4 + 1),
		floor AS (
			SELECT max(cursor) AS cursor FROM win WHERE cursor < coalesce((SELECT min(cursor) FROM win WHERE t >= $5), 9223372036854775807)),
		gone AS (
			DELETE FROM sandboxd_usage WHERE cell = $1 AND cursor IN (SELECT cursor FROM win WHERE cursor < (SELECT cursor FROM floor))
			RETURNING cursor)
		SELECT count(*), coalesce(max(cursor), 0) FROM gone`
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
	return s.line(ctx, lastLine, node)
}

func (s *Store) First(ctx context.Context, node string) ([32]byte, error) {
	return s.line(ctx, firstLine, node)
}

func (s *Store) Prune(ctx context.Context, node string, before time.Time) (int64, error) {
	var total, after int64
	for {
		var n int64
		if err := s.scanRow(ctx, pruneRows, []any{s.cell, node, after, pruneBatch, before}, &n, &after); err != nil {
			return total, err
		}
		total += n
		if n < pruneBatch {
			return total, nil
		}
	}
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
			batch.Queue(insertRow, s.cell, r.Node, r.Digest[:], r.Time, r.Event.Event, r.ID, r.VMName, r.KeyHash, r.Template, r.Net, r.Size, r.Tenant,
				pgtype.Timestamptz{Time: r.Deadline, Valid: !r.Deadline.IsZero()}, r.Volumes, r.VolumesRW, r.Children,
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

func (s *Store) line(ctx context.Context, query, node string) ([32]byte, error) {
	var mark [32]byte
	var digest []byte
	if err := s.scanRow(ctx, query, []any{s.cell, node}, &digest); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return mark, err
	}
	copy(mark[:], digest)
	return mark, nil
}

func (s *Store) scanRow(ctx context.Context, query string, args []any, dest ...any) error {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	return s.pool.QueryRow(qctx, query, args...).Scan(dest...)
}

func scan(row pgx.CollectableRow) (usage.Record, error) {
	var rec usage.Record
	var deadline pgtype.Timestamptz
	err := row.Scan(&rec.Cursor, &rec.Node, &rec.Time, &rec.Event.Event, &rec.ID, &rec.VMName, &rec.KeyHash, &rec.Template, &rec.Net, &rec.Size, &rec.Tenant, &deadline,
		&rec.Volumes, &rec.VolumesRW, &rec.Children, &rec.Reference, &rec.Upstream, &rec.Secret, &rec.Sent, &rec.Received)
	if err != nil {
		return usage.Record{}, fmt.Errorf("scan usage row: %w", err)
	}
	rec.Time = rec.Time.UTC()
	rec.Deadline = deadline.Time.UTC()
	return rec, nil
}
