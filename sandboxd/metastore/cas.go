package metastore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ScanFunc[T any] func(pgx.Row) (T, error)

// CAS runs a registry's compare-and-set queries and answers with its sentinels.
type CAS struct {
	Lost        error
	Unavailable error
}

// Advance runs sql, which returns the new epoch of the one row it moves; Lost when it moves none.
func (c CAS) Advance(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (int64, error) {
	qctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	var epoch int64
	err := pool.QueryRow(qctx, sql, args...).Scan(&epoch)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, c.Lost
	case err != nil:
		return 0, c.Down(err)
	}
	return epoch, nil
}

// Delete runs sql, which removes the one row it matches; Lost when it removes none.
func (c CAS) Delete(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) error {
	qctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	tag, err := pool.Exec(qctx, sql, args...)
	switch {
	case err != nil:
		return c.Down(err)
	case tag.RowsAffected() == 0:
		return c.Lost
	}
	return nil
}

func (c CAS) One[T any](ctx context.Context, pool *pgxpool.Pool, scan ScanFunc[T], sql string, args ...any) (T, bool, error) {
	qctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	v, err := scan(pool.QueryRow(qctx, sql, args...))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return *new(T), false, nil
	case err != nil:
		return *new(T), false, c.Down(err)
	}
	return v, true, nil
}

func (c CAS) All[T any](ctx context.Context, pool *pgxpool.Pool, scan ScanFunc[T], sql string, args ...any) ([]T, error) {
	qctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	rows, err := pool.Query(qctx, sql, args...)
	if err != nil {
		return nil, c.Down(err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (T, error) { return scan(row) })
	if err != nil {
		return nil, c.Down(err)
	}
	return out, nil
}

// Down wraps err in Unavailable.
func (c CAS) Down(err error) error {
	return fmt.Errorf("%w: %w", c.Unavailable, err)
}
