// Package pg keeps the move registry in one shared PostgreSQL table.
package pg

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cocoonstack/sandbox/sandboxd/metastore"
	"github.com/cocoonstack/sandbox/sandboxd/moves"
)

const table = "sandboxd_moves"

var (
	//go:embed schema.sql
	schema string

	cas = metastore.CAS{Lost: moves.ErrLost, Unavailable: moves.ErrUnavailable}
)

var _ moves.Registry = (*Registry)(nil)

// Registry is the shared move table.
type Registry struct {
	pool *pgxpool.Pool
	cell string
}

// Open connects to cell's records and creates the table when it is missing.
func Open(ctx context.Context, dsn, cell string) (*Registry, error) {
	pool, _, err := metastore.Open(ctx, dsn, table, schema)
	if err != nil {
		return nil, err
	}
	return &Registry{pool: pool, cell: cell}, nil
}

func (r *Registry) Begin(ctx context.Context, id, from, to string) (int64, error) {
	return cas.Advance(ctx, r.pool, `INSERT INTO sandboxd_moves (cell, id, from_node, to_node, state, epoch)
		VALUES ($1, $2, $3, $4, 'pending', nextval('sandboxd_moves_epoch'))
		ON CONFLICT (cell, id) DO UPDATE SET from_node = EXCLUDED.from_node, to_node = EXCLUDED.to_node,
			state = EXCLUDED.state, epoch = EXCLUDED.epoch, updated_at = now()
		WHERE sandboxd_moves.state IN ('committed', 'aborted')
		RETURNING epoch`, r.cell, id, from, to)
}

func (r *Registry) Advance(ctx context.Context, id string, expect int64, from, to moves.State) (int64, error) {
	if err := moves.VerifyAdvance(from, to); err != nil {
		return 0, err
	}
	return cas.Advance(ctx, r.pool, `UPDATE sandboxd_moves SET state = $5, epoch = nextval('sandboxd_moves_epoch'), updated_at = now()
		WHERE cell = $1 AND id = $2 AND epoch = $3 AND state = $4
		RETURNING epoch`, r.cell, id, expect, from, to)
}

func (r *Registry) Get(ctx context.Context, id string) (moves.Record, bool, error) {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	rec, err := scan(r.pool.QueryRow(qctx, `SELECT id, from_node, to_node, state, epoch, updated_at
		FROM sandboxd_moves WHERE cell = $1 AND id = $2`, r.cell, id))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return moves.Record{}, false, nil
	case err != nil:
		return moves.Record{}, false, cas.Down(err)
	}
	return rec, true, nil
}

func (r *Registry) Delete(ctx context.Context, id string, epoch int64) error {
	return cas.Delete(ctx, r.pool, `DELETE FROM sandboxd_moves WHERE cell = $1 AND id = $2 AND epoch = $3`, r.cell, id, epoch)
}

func (r *Registry) Stale(ctx context.Context, before time.Time) ([]moves.Record, error) {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	rows, err := r.pool.Query(qctx, `SELECT id, from_node, to_node, state, epoch, updated_at
		FROM sandboxd_moves WHERE cell = $1 AND updated_at < $2`, r.cell, before)
	if err != nil {
		return nil, cas.Down(err)
	}
	recs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (moves.Record, error) { return scan(row) })
	if err != nil {
		return nil, cas.Down(err)
	}
	return recs, nil
}

// Close releases the connection pool.
func (r *Registry) Close() {
	r.pool.Close()
}

func scan(row pgx.Row) (moves.Record, error) {
	var rec moves.Record
	err := row.Scan(&rec.ID, &rec.From, &rec.To, &rec.State, &rec.Epoch, &rec.UpdatedAt)
	return rec, err
}
