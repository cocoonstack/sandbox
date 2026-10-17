// Package pg keeps the archive registry in one shared PostgreSQL table.
package pg

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/metastore"
)

const table = "sandboxd_archives"

//go:embed schema.sql
var schema string

var _ archives.Registry = (*Registry)(nil)

// Registry is the shared archive table.
type Registry struct {
	pool *pgxpool.Pool
	cell string
}

// Open connects to cell's rows and creates the table when it is missing.
func Open(ctx context.Context, dsn, cell string) (*Registry, error) {
	pool, _, err := metastore.Open(ctx, dsn, table, schema)
	if err != nil {
		return nil, err
	}
	return &Registry{pool: pool, cell: cell}, nil
}

func (r *Registry) Put(ctx context.Context, rec archives.Record) (int64, error) {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	var epoch int64
	err := r.pool.QueryRow(qctx, `INSERT INTO sandboxd_archives (id, tenant, token_sha256, owner, epoch, checkpoint, deadline, claim, cell)
		VALUES ($1, $2, $3, $4, nextval('sandboxd_archives_epoch'), $5, $6, $7, $8)
		ON CONFLICT (cell, id) DO UPDATE SET tenant = EXCLUDED.tenant, token_sha256 = EXCLUDED.token_sha256,
			epoch = EXCLUDED.epoch, consumed = false, checkpoint = EXCLUDED.checkpoint,
			deadline = EXCLUDED.deadline, claim = EXCLUDED.claim, updated_at = now()
		WHERE sandboxd_archives.owner = EXCLUDED.owner
		RETURNING epoch`,
		rec.ID, rec.Tenant, rec.TokenSHA256, rec.Owner, rec.Checkpoint, nullTime(rec.Deadline), []byte(rec.Claim), r.cell).Scan(&epoch)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, archives.ErrLost
	case err != nil:
		return 0, unavailable(err)
	}
	return epoch, nil
}

func (r *Registry) Take(ctx context.Context, id string, expect int64, owner string) (int64, error) {
	return r.advance(ctx, `UPDATE sandboxd_archives SET owner = $4, epoch = nextval('sandboxd_archives_epoch'), updated_at = now()
		WHERE cell = $1 AND id = $2 AND epoch = $3 AND NOT consumed
		RETURNING epoch`, r.cell, id, expect, owner)
}

func (r *Registry) Consume(ctx context.Context, id string, expect int64) (int64, error) {
	return r.advance(ctx, `UPDATE sandboxd_archives SET consumed = true, epoch = nextval('sandboxd_archives_epoch'), updated_at = now()
		WHERE cell = $1 AND id = $2 AND epoch = $3 AND NOT consumed
		RETURNING epoch`, r.cell, id, expect)
}

func (r *Registry) Delete(ctx context.Context, id string, epoch int64) error {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	tag, err := r.pool.Exec(qctx, `DELETE FROM sandboxd_archives WHERE cell = $3 AND id = $1 AND epoch = $2`, id, epoch, r.cell)
	switch {
	case err != nil:
		return unavailable(err)
	case tag.RowsAffected() == 0:
		return archives.ErrLost
	}
	return nil
}

func (r *Registry) Get(ctx context.Context, id string) (archives.Record, bool, error) {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	rec, err := scan(r.pool.QueryRow(qctx, `SELECT id, tenant, token_sha256, owner, epoch, consumed, checkpoint, deadline, updated_at, claim
		FROM sandboxd_archives WHERE cell = $2 AND id = $1`, id, r.cell))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return archives.Record{}, false, nil
	case err != nil:
		return archives.Record{}, false, err
	}
	return rec, true, nil
}

func (r *Registry) Sweepable(ctx context.Context, now time.Time, owner string) ([]archives.Record, error) {
	return r.collect(ctx, `SELECT id, tenant, token_sha256, owner, epoch, consumed, checkpoint, deadline, updated_at, NULL::jsonb
		FROM sandboxd_archives WHERE cell = $2 AND (consumed OR deadline < $1 OR owner = $3)`, now, r.cell, owner)
}

func (r *Registry) Rows(ctx context.Context, ids []string) (map[string]archives.Record, error) {
	recs, err := r.collect(ctx, `SELECT id, tenant, token_sha256, owner, epoch, consumed, checkpoint, deadline, updated_at, NULL::jsonb
		FROM sandboxd_archives WHERE cell = $1 AND id = ANY($2)`, r.cell, ids)
	if err != nil {
		return nil, err
	}
	rows := make(map[string]archives.Record, len(recs))
	for _, rec := range recs {
		rows[rec.ID] = rec
	}
	return rows, nil
}

// Close releases the connection pool.
func (r *Registry) Close() {
	r.pool.Close()
}

func (r *Registry) advance(ctx context.Context, sql string, args ...any) (int64, error) {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	var epoch int64
	err := r.pool.QueryRow(qctx, sql, args...).Scan(&epoch)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, archives.ErrLost
	case err != nil:
		return 0, unavailable(err)
	}
	return epoch, nil
}

func (r *Registry) collect(ctx context.Context, sql string, args ...any) ([]archives.Record, error) {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	rows, err := r.pool.Query(qctx, sql, args...)
	if err != nil {
		return nil, unavailable(err)
	}
	recs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (archives.Record, error) { return scan(row) })
	if err != nil {
		return nil, unavailable(err)
	}
	return recs, nil
}

func scan(row pgx.Row) (archives.Record, error) {
	var (
		rec      archives.Record
		deadline *time.Time
		claim    []byte
	)
	err := row.Scan(&rec.ID, &rec.Tenant, &rec.TokenSHA256, &rec.Owner, &rec.Epoch, &rec.Consumed, &rec.Checkpoint, &deadline, &rec.UpdatedAt, &claim)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return archives.Record{}, err
	case err != nil:
		return archives.Record{}, unavailable(err)
	}
	if deadline != nil {
		rec.Deadline = *deadline
	}
	rec.Claim = claim
	return rec, nil
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func unavailable(err error) error {
	return fmt.Errorf("%w: %w", archives.ErrUnavailable, err)
}
