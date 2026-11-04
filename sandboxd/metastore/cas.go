package metastore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

// Down wraps err in Unavailable.
func (c CAS) Down(err error) error {
	return fmt.Errorf("%w: %w", c.Unavailable, err)
}
