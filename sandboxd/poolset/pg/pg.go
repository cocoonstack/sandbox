// Package pg keeps each cell's pool set in one shared PostgreSQL row whose writes NOTIFY every node.
package pg

import (
	"context"
	_ "embed"
	"encoding/json/v2"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cocoonstack/sandbox/sandboxd/metastore"
	"github.com/cocoonstack/sandbox/sandboxd/poolset"
)

const (
	table   = "sandboxd_pool_sets"
	channel = "sandboxd_pool_sets"
)

//go:embed schema.sql
var schema string

var _ poolset.Shared = (*Source)(nil)

// Source is one cell's row in the shared pool-set table.
type Source struct {
	pool      *pgxpool.Pool
	listenCfg *pgx.ConnConfig
	cell      string
}

// Open connects and creates the table when it is missing.
func Open(ctx context.Context, dsn, cell string) (*Source, error) {
	pool, listenCfg, err := metastore.Open(ctx, dsn, table, schema)
	if err != nil {
		return nil, err
	}
	return &Source{pool: pool, listenCfg: listenCfg, cell: cell}, nil
}

func (s *Source) Load(ctx context.Context) (*poolset.Set, int64, error) {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	var (
		set     poolset.Set
		raw     []byte
		version int64
	)
	err := s.pool.QueryRow(qctx, `SELECT config_seed, pools, version FROM sandboxd_pool_sets WHERE cell = $1`, s.cell).Scan(&set.ConfigSeed, &raw, &version)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, 0, nil
	case err != nil:
		return nil, 0, unavailable(err)
	}
	if err = json.Unmarshal(raw, &set.Pools); err != nil {
		return nil, 0, fmt.Errorf("decode cell %q pool set: %w", s.cell, err)
	}
	return &set, version, nil
}

func (s *Source) Store(ctx context.Context, set poolset.Set) (int64, error) {
	raw, err := json.Marshal(set.Pools)
	if err != nil {
		return 0, fmt.Errorf("encode pool set: %w", err)
	}
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	var version int64
	if err = s.pool.QueryRow(qctx, `WITH up AS (
			INSERT INTO sandboxd_pool_sets (cell, config_seed, pools, version) VALUES ($1, $2, $3, 1)
			ON CONFLICT (cell) DO UPDATE SET config_seed = EXCLUDED.config_seed, pools = EXCLUDED.pools,
				version = sandboxd_pool_sets.version + 1, updated_at = now()
			RETURNING version)
		SELECT version, pg_notify($4, $1) FROM up`, s.cell, set.ConfigSeed, raw, channel).Scan(&version, nil); err != nil {
		return 0, unavailable(err)
	}
	return version, nil
}

func (s *Source) Watch(ctx context.Context, changed func()) {
	metastore.Listen(ctx, s.listenCfg, channel, changed, func(cell string) {
		if cell == s.cell {
			changed()
		}
	})
}

func (s *Source) Close() error {
	s.pool.Close()
	return nil
}

func unavailable(err error) error {
	return fmt.Errorf("%w: %w", poolset.ErrUnavailable, err)
}
